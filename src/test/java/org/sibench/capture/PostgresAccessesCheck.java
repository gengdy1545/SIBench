package org.sibench.capture;

import static org.junit.Assert.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.config.Engine;
import org.sibench.generation.NativeHistory;
import org.sibench.generation.PostgresDatabase;
import org.sibench.generation.PostgresTestSettings;
import org.sibench.schema.SourceSchema;
import org.sibench.trace.IndexKey;

/**
 * Native accesses are compared with hand-checkable rows, not SQL result cardinalities. Every case
 * runs through the live socket, the receiver and the production projection, so executor origins,
 * completed executions, command IDs and rollback all apply.
 */
public class PostgresAccessesCheck {
    @Rule public TemporaryFolder temporary=new TemporaryFolder();
    @FunctionalInterface private interface Step {void run(Connection database)throws Exception;}
    @FunctionalInterface private interface Workload {void run(PostgresDatabase cluster)throws Exception;}
    private static void sql(Connection db,String text)throws Exception{try(var q=db.createStatement()){q.execute(text);}}
    private static List<Long> values(Connection db,String text)throws Exception {
        var values=new ArrayList<Long>();try(var q=db.createStatement();var rows=q.executeQuery(text)){while(rows.next())values.add(rows.getLong(1));}return values;
    }
    private static String plan(Connection db,String query)throws Exception {
        var plan=new StringBuilder();
        try(var q=db.createStatement();var rows=q.executeQuery("EXPLAIN (COSTS OFF) "+query)){while(rows.next())plan.append(rows.getString(1)).append('\n');}
        return plan.toString();
    }
    private static IndexCatalog sysbench()throws Exception {
        return IndexCatalog.fromSchema(SourceSchema.of("sysbench",Path.of("benchmark/sysbench"),Map.of("tables",1L)));
    }
    private static IndexCatalog hybench()throws Exception {
        return IndexCatalog.fromSchema(SourceSchema.hybench(Path.of("benchmark/HyBench-2024")));
    }
    private static List<IndexOperationTrace.Operation> searches(List<IndexOperationTrace.Operation> operations) {
        return operations.stream().filter(o->o.kind()==IndexOperationTrace.SEARCH).toList();
    }

    /** Loads {@code schema} uncaptured, installs RowID identities for {@code tables}, then projects the workload's capture. */
    private List<IndexOperationTrace.Operation> project(IndexCatalog catalog,List<String> tables,Step schema,Workload workload)throws Exception {
        String runtime=System.getProperty("sibench.postgres.runtime");assertNotNull("Set sibench.postgres.runtime",runtime);
        Path root=temporary.newFolder().toPath(),sockets=Files.createTempDirectory("pg-accesses-"),endpoint=sockets.resolve("capture.sock");
        var executor=Executors.newSingleThreadExecutor();
        try(var capture=new PostgresCapture(endpoint,root.resolve("pending"),16L<<20,1024);
                var cluster=PostgresDatabase.create(Path.of(runtime),root.resolve("source"),endpoint,PostgresTestSettings.SETTINGS)) {
            try(var setup=cluster.connect("setup")) {
                schema.run(setup);
                setup.setAutoCommit(false);NativeHistory.initialize(setup,Engine.POSTGRES);
                for(String table:tables)
                    NativeHistory.install(setup,Engine.POSTGRES,catalog.tableId(table),"public",table,catalog.primary(catalog.tableId(table)).column());
                PostgresCapture.install(setup);setup.commit();
            }
            var future=executor.submit(()->{
                var operations=new ArrayList<IndexOperationTrace.Operation>();
                try(var control=cluster.connect("projection")) {
                    control.setAutoCommit(false);var units=WorkUnits.create(root);
                    try(var lines=units.lines();var versions=new PostgresVersions(control);
                            var projection=new PostgresProjection(control,catalog,root,versions,units,true,operations::add,null)) {
                        for(PostgresTransactions.Chunk next;(next=capture.transactions.next(()->capture.watermark(control)))!=null;)
                            try(var chunk=next){projection.accept(chunk);}
                    }
                    control.commit();
                }catch(Throwable failure){capture.fail(failure);throw failure;}
                return operations;
            });
            workload.run(cluster);
            try(var control=cluster.connect("close")){capture.finishSources(control);}
            return future.get(30,TimeUnit.SECONDS);
        }finally{executor.shutdownNow();Files.deleteIfExists(endpoint);Files.delete(sockets);}
    }

    @Test public void projectsIndexVisibleRowsBeforeFilterAggregationAndAbort()throws Exception {
        var catalog=sysbench();
        var operations=project(catalog,List.of("sbtest1"),setup->{
            sql(setup,"CREATE TABLE sbtest1(id INTEGER PRIMARY KEY,k INTEGER,c CHAR(120),pad CHAR(60))");
            sql(setup,"INSERT INTO sbtest1 SELECT i,i/4,'value','pad' FROM generate_series(1,20) i");
            sql(setup,"CREATE INDEX k_1 ON sbtest1(k)");sql(setup,"VACUUM ANALYZE sbtest1");
        },cluster->{
            try(var client=cluster.connect("sibench:accesses")) {
                sql(client,"SET enable_seqscan=off");sql(client,"SET enable_bitmapscan=off");
                assertEquals(List.of(4L),values(client,"SELECT count(*) FROM sbtest1 WHERE id BETWEEN 5 AND 8"));
                assertEquals(List.of(6L,8L,10L),values(client,"SELECT id FROM sbtest1 WHERE id>=5 AND id%2=0 ORDER BY id LIMIT 3"));
                // The index found row 5 before the statement failed: an incomplete execution exports no search.
                assertThrows(java.sql.SQLException.class,()->values(client,"SELECT 1/(id-id) FROM sbtest1 WHERE id=5"));
                client.setAutoCommit(false);sql(client,"UPDATE sbtest1 SET k=9 WHERE id=5");
                assertEquals(List.of(5L),values(client,"SELECT id FROM sbtest1 WHERE k=9"));client.rollback();client.setAutoCommit(true);
                assertEquals(List.of(),values(client,"SELECT id FROM sbtest1 WHERE k=9"));
            }
        });
        var searches=searches(operations);
        assertEquals(5,searches.size());
        var aggregate=searches.get(0);assertEquals(0,aggregate.index());assertEquals(4,aggregate.expectedCount());
        assertEquals(IndexKey.ofInt(5),aggregate.range().lower());assertEquals(IndexKey.ofInt(8),aggregate.range().upper());
        var early=searches.get(1);assertEquals(6,early.expectedCount());assertEquals(6,early.range().limit());
        var update=searches.get(2);assertEquals(0,update.index());assertEquals(1,update.expectedCount());
        assertEquals(IndexKey.ofInt(5),update.lower());
        // The aborted transaction's later read sees its own earlier command's write.
        var own=searches.get(3);assertEquals(1,own.index());assertEquals(1,own.expectedCount());
        assertEquals(IndexKey.ofInt(9),own.lower());assertFalse(own.localWrites().isEmpty());
        var after=searches.get(4);assertEquals(1,after.index());assertEquals(0,after.expectedCount());
        assertTrue(after.localWrites().isEmpty());
        assertEquals(2,operations.stream().filter(o->o.kind()==IndexOperationTrace.TRANSACTION_ABORT).count());
    }

    @Test public void statementSnapshotsAdvanceInReadCommittedAndRemainFixedInRepeatableRead()throws Exception {
        var catalog=sysbench();
        var operations=project(catalog,List.of("sbtest1"),setup->{
            sql(setup,"CREATE TABLE sbtest1(id INTEGER PRIMARY KEY,k INTEGER,c CHAR(120),pad CHAR(60))");
            sql(setup,"CREATE INDEX k_1 ON sbtest1(k)");sql(setup,"INSERT INTO sbtest1 VALUES(1,10,'a','a'),(2,10,'b','b')");
        },cluster->{
            try(var writer=cluster.connect("sibench:writer");var rc=cluster.connect("sibench:rc");var rr=cluster.connect("sibench:rr")) {
                sql(rc,"SET enable_seqscan=off");sql(rr,"SET enable_seqscan=off");sql(writer,"SET enable_seqscan=off");
                rc.setAutoCommit(false);rr.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);rr.setAutoCommit(false);
                writer.setAutoCommit(false);
                assertEquals(List.of(10L),values(rc,"SELECT k FROM sbtest1 WHERE id=1"));
                assertEquals(List.of(10L),values(rr,"SELECT k FROM sbtest1 WHERE id=2"));
                sql(writer,"UPDATE sbtest1 SET k=20 WHERE id=1");sql(writer,"UPDATE sbtest1 SET k=20 WHERE id=2");writer.commit();
                assertEquals(List.of(20L),values(rc,"SELECT k FROM sbtest1 WHERE id=1"));
                assertEquals(List.of(10L),values(rr,"SELECT k FROM sbtest1 WHERE id=2"));rc.commit();rr.commit();
            }
        });
        long written=operations.stream().filter(IndexOperationTrace::isMutation).mapToLong(IndexOperationTrace.Operation::transactionId)
                .distinct().reduce((a,b)->{throw new AssertionError("One writer transaction");}).orElseThrow();
        var reads=searches(operations).stream().filter(o->o.transactionId()!=written).toList();
        // Read committed takes a new snapshot per statement; repeatable read keeps its first one.
        assertEquals(List.of(0L,written),reads.stream().filter(o->o.lower().equals(IndexKey.ofInt(1)))
                .map(IndexOperationTrace.Operation::readTimestamp).toList());
        assertEquals(List.of(0L,0L),reads.stream().filter(o->o.lower().equals(IndexKey.ofInt(2)))
                .map(IndexOperationTrace.Operation::readTimestamp).toList());
    }

    /**
     * HyBench AP-5 under a generic plan: a nested loop drives a BitmapAnd whose constant-key input
     * was bound at plan start. Rescanning that never-fetched input must not end the cycle or seal
     * the other input, so the heap verdict belongs to the intersection of both indexes.
     */
    @Test public void parameterizedBitmapAndKeepsAConstantInputBoundAtPlanStart()throws Exception {
        var catalog=hybench();
        String query="SELECT cu.custid,sum(tr.amount) FROM company co,transfer tr,customer cu WHERE cu.custid=tr.targetid"
                +" AND cu.companyid=co.companyid AND tr.sourceid=co.companyid AND co.companyid=$1 AND tr.type='salary'"
                +" GROUP BY cu.custid";
        // The dev SF1 run chose this plan on its own; tiny tables need the joins pinned to reach it.
        var settings=List.of("plan_cache_mode=force_generic_plan","enable_hashjoin=off","enable_mergejoin=off");
        var operations=project(catalog,List.of("company","customer","transfer"),setup->{
            sql(setup,"CREATE TABLE company(companyid INT PRIMARY KEY)");sql(setup,"INSERT INTO company VALUES(301),(302)");
            sql(setup,"CREATE TABLE customer(custid INT PRIMARY KEY,companyid INT)");
            sql(setup,"INSERT INTO customer SELECT i,301 FROM generate_series(1,300) i");
            sql(setup,"CREATE INDEX idx_customer_1 ON customer(companyid)");
            sql(setup,"CREATE TABLE transfer(id INT PRIMARY KEY,sourceid INT,targetid INT,amount REAL,type CHAR(10))");
            // 1200 transfers from company 301, 297 of them salaries; 4800 between customers.
            sql(setup,"INSERT INTO transfer SELECT i,CASE WHEN i<=1200 THEN 301 ELSE 1+i%300 END,1+(i*7)%300,i,"
                    +"CASE WHEN i<=297 THEN 'salary' ELSE (ARRAY['invest','donate','transfer','business'])[1+i%4] END"
                    +" FROM generate_series(1,6000) i");
            for(String index:List.of("idx_transfer_1 ON transfer(sourceid)","idx_transfer_2 ON transfer(targetid)",
                    "idx_transfer_3 ON transfer(type)"))sql(setup,"CREATE INDEX "+index);
            sql(setup,"VACUUM ANALYZE");
            for(String setting:settings)sql(setup,"SET "+setting);
            sql(setup,"PREPARE q(int) AS "+query);
            // An outer nested loop rescans the bitmap heap scan and so both BitmapAnd inputs.
            String plan=plan(setup,"EXECUTE q(301)");
            assertTrue(plan,plan.matches("(?s).*Nested Loop.*Seq Scan on company.*Bitmap Heap Scan on transfer.*BitmapAnd.*idx_transfer_1.*idx_transfer_3.*"));
        },cluster->{
            try(var client=cluster.connect("sibench:bitmap-and")) {
                for(String setting:settings)sql(client,"SET "+setting);
                sql(client,"PREPARE q(int) AS "+query);
                assertEquals(297,values(client,"EXECUTE q(301)").size());
            }
        });
        var group=searches(operations).stream().filter(o->(o.flags()&IndexOperationTrace.INTERSECT_GROUP)!=0).toList();
        assertEquals(2,group.size());
        for(var member:group){assertEquals(297,member.expectedCount());assertTrue(member.range().point());}
        assertEquals(List.of(catalog.named("idx_transfer_1").indexId(),catalog.named("idx_transfer_3").indexId()),
                group.stream().map(IndexOperationTrace.Operation::index).sorted().toList());
    }

    @Test public void bitmapAndCyclesWithAnEmptyParameterizedInputKeepTheirOwnCandidates()throws Exception {
        var catalog=hybench();
        // HyBench IQ-4's first branch: the parameterized input runs first and is empty for company 302.
        String query="SELECT co.name,t.amount FROM company co,transfer t WHERE t.sourceid=$1 AND t.type='invest'"
                +" AND t.targetid=co.companyid";
        var settings=List.of("plan_cache_mode=force_generic_plan","enable_hashjoin=off","enable_mergejoin=off");
        var operations=project(catalog,List.of("company","transfer"),setup->{
            sql(setup,"CREATE TABLE company(companyid INT PRIMARY KEY,name VARCHAR(20))");
            sql(setup,"INSERT INTO company VALUES(301,'a'),(302,'b')");
            sql(setup,"CREATE TABLE transfer(id INT PRIMARY KEY,sourceid INT,targetid INT,amount REAL,type CHAR(10))");
            // 1200 transfers from customer 1, of which the first 40 go to company 301 and are no investments.
            sql(setup,"INSERT INTO transfer SELECT i,CASE WHEN i<=1200 THEN 1 ELSE 2+i%300 END,"
                    +"CASE WHEN i<=40 THEN 301 ELSE 1+(i*7)%300 END,i,CASE WHEN i<=40 THEN 'salary'"
                    +" ELSE (ARRAY['invest','donate','transfer','business'])[1+i%4] END FROM generate_series(1,6000) i");
            for(String index:List.of("idx_transfer_1 ON transfer(sourceid)","idx_transfer_2 ON transfer(targetid)",
                    "idx_transfer_3 ON transfer(type)"))sql(setup,"CREATE INDEX "+index);
            sql(setup,"VACUUM ANALYZE");
            for(String setting:settings)sql(setup,"SET "+setting);
            sql(setup,"PREPARE q(int) AS "+query);
            String plan=plan(setup,"EXECUTE q(1)");
            assertTrue(plan,plan.matches(
                    "(?s).*Nested Loop.*Seq Scan on company.*Bitmap Heap Scan on transfer.*Filter.*BitmapAnd.*idx_transfer_2.*idx_transfer_1.*"));
        },cluster->{
            try(var client=cluster.connect("sibench:bitmap-and-empty")) {
                for(String setting:settings)sql(client,"SET "+setting);
                sql(client,"PREPARE q(int) AS "+query);
                try(var q=client.createStatement();var rows=q.executeQuery("EXECUTE q(1)")){assertFalse(rows.next());}
            }
        });
        var searches=searches(operations);
        int sources=catalog.named("idx_transfer_1").indexId(),targets=catalog.named("idx_transfer_2").indexId();
        // Company 301: all 40 candidates of the intersection are visible; the type filter runs after the index.
        var group=searches.stream().filter(o->(o.flags()&IndexOperationTrace.INTERSECT_GROUP)!=0).toList();
        assertEquals(List.of(sources,targets),group.stream().map(IndexOperationTrace.Operation::index).sorted().toList());
        for(var member:group)assertEquals(40,member.expectedCount());
        // Company 302: the empty targetid input ends the BitmapAnd before the sourceid input runs.
        assertEquals(1,searches.stream().filter(o->o.index()==targets&&o.expectedCount()==0&&!o.grouped()).count());
        assertEquals(3,searches.size());
    }

    @Test public void bitmapCandidatesOnBlocksAddedAfterTheScanStartedAreInvisible()throws Exception {
        var catalog=hybench();
        // The one-time filter runs the insert after the bitmap heap scan opened and before its index
        // scan: the new rows' index entries point past the heap's size at scan start.
        String query="WITH ins AS (INSERT INTO transfer SELECT i,1,5,i,'invest' FROM generate_series(100001,104000) i RETURNING 1)"
                +" SELECT count(*) FROM transfer WHERE sourceid=1 AND (SELECT count(*) FROM ins)>0";
        var operations=project(catalog,List.of("transfer"),setup->{
            sql(setup,"CREATE TABLE transfer(id INT PRIMARY KEY,sourceid INT,targetid INT,amount REAL,type CHAR(10))");
            sql(setup,"INSERT INTO transfer SELECT i,CASE WHEN i<=1200 THEN 1 ELSE 2+i%300 END,1+(i*7)%300,i,'invest'"
                    +" FROM generate_series(1,6000) i");
            // Captured row changes carry every column the Trace indexes.
            for(String index:List.of("idx_transfer_1 ON transfer(sourceid)","idx_transfer_2 ON transfer(targetid)",
                    "idx_transfer_3 ON transfer(type)"))sql(setup,"CREATE INDEX "+index);
            sql(setup,"VACUUM ANALYZE");
            // The all-visible heap would otherwise favor an index-only scan over the bitmap under test.
            sql(setup,"SET enable_indexonlyscan=off");
            String plan=plan(setup,query);
            assertTrue(plan,plan.matches("(?s).*One-Time Filter.*Bitmap Heap Scan on transfer.*idx_transfer_1.*"));
        },cluster->{
            try(var client=cluster.connect("sibench:bitmap-extended")) {
                sql(client,"SET enable_indexonlyscan=off");client.setAutoCommit(false);
                assertEquals(List.of(1200L),values(client,query));
                client.rollback();
            }
        });
        int sources=catalog.named("idx_transfer_1").indexId();
        var searches=searches(operations).stream().filter(o->o.index()==sources).toList();
        assertEquals(1,searches.size());
        assertEquals(1200,searches.get(0).expectedCount());
        // The statement's own insertions follow the scan's start, so its search carries none of them.
        assertTrue(searches.get(0).localWrites().isEmpty());
    }
}
