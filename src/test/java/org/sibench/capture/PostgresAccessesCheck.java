package org.sibench.capture;

import static org.junit.Assert.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.generation.PostgresDatabase;
import org.sibench.schema.SourceSchema;
import org.sibench.trace.IndexKey;

/** Native accesses are compared with hand-checkable rows, not SQL result cardinalities. */
public class PostgresAccessesCheck {
    @Rule public TemporaryFolder temporary=new TemporaryFolder();
    private static void sql(Connection db,String text)throws Exception{try(var q=db.createStatement()){q.execute(text);}}
    private static List<Long> values(Connection db,String text)throws Exception {
        var values=new ArrayList<Long>();try(var q=db.createStatement();var rows=q.executeQuery(text)){while(rows.next())values.add(rows.getLong(1));}return values;
    }
    @Test public void projectsIndexVisibleRowsBeforeFilterAggregationAndAbort()throws Exception {
        String runtime=System.getProperty("sibench.postgres.runtime");assertNotNull("Set sibench.postgres.runtime",runtime);
        Path root=temporary.getRoot().toPath().resolve("source");
        try(var cluster=PostgresDatabase.create(Path.of(runtime),root)) {
            try(var setup=cluster.connect("setup")) {
                sql(setup,"CREATE TABLE sbtest1(id INTEGER PRIMARY KEY,k INTEGER,c CHAR(120),pad CHAR(60))");
                sql(setup,"INSERT INTO sbtest1 SELECT i,i/4,'value','pad' FROM generate_series(1,20) i");
                sql(setup,"CREATE INDEX k_1 ON sbtest1(k)");sql(setup,"VACUUM ANALYZE sbtest1");
            }
            try(var client=cluster.connect("sibench:accesses")) {
                sql(client,"SET enable_seqscan=off");sql(client,"SET enable_bitmapscan=off");
                assertEquals(List.of(4L),values(client,"SELECT count(*) FROM sbtest1 WHERE id BETWEEN 5 AND 8"));
                assertEquals(List.of(6L,8L,10L),values(client,"SELECT id FROM sbtest1 WHERE id>=5 AND id%2=0 ORDER BY id LIMIT 3"));
                client.setAutoCommit(false);sql(client,"UPDATE sbtest1 SET k=9 WHERE id=5");
                assertEquals(List.of(5L),values(client,"SELECT id FROM sbtest1 WHERE k=9"));client.rollback();client.setAutoCommit(true);
                assertEquals(List.of(),values(client,"SELECT id FROM sbtest1 WHERE k=9"));
            }
        }
        var catalog=IndexCatalog.fromSchema(SourceSchema.of("sysbench",Path.of("benchmark/sysbench"),Map.of("tables",1L)));
        var accesses=new ArrayList<PostgresAccesses.Access>();
        try(var files=Files.list(root.resolve("capture"))) {
            for(Path file:files.toList())PostgresAccesses.read(file,catalog,true,accesses::add);
        }
        assertEquals(5,accesses.size());
        var aggregate=accesses.get(0).projection();assertEquals(4,aggregate.count());
        assertEquals(IndexKey.ofInt(5),aggregate.parts().get(0).range().lower());
        assertEquals(IndexKey.ofInt(8),aggregate.parts().get(0).range().upper());
        var early=accesses.get(1).projection();assertEquals(6,early.count());
        assertEquals(6,early.parts().get(0).range().limit());
        assertEquals(1,accesses.get(2).projection().count());
        assertEquals(1,accesses.get(3).projection().count());
        assertEquals(0,accesses.get(4).projection().count());
        assertEquals(1,accesses.get(3).projection().parts().get(0).index());
    }

    @Test public void statementSnapshotsAdvanceInReadCommittedAndRemainFixedInRepeatableRead()throws Exception {
        String runtime=System.getProperty("sibench.postgres.runtime");assertNotNull("Set sibench.postgres.runtime",runtime);
        Path root=temporary.getRoot().toPath().resolve("snapshots");
        try(var cluster=PostgresDatabase.create(Path.of(runtime),root);
                var writer=cluster.connect("setup");var rc=cluster.connect("sibench:rc");var rr=cluster.connect("sibench:rr")) {
            sql(writer,"CREATE TABLE sbtest1(id INTEGER PRIMARY KEY,k INTEGER)");sql(writer,"INSERT INTO sbtest1 VALUES(1,10)");
            sql(rc,"SET enable_seqscan=off");sql(rr,"SET enable_seqscan=off");
            rc.setAutoCommit(false);rr.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);rr.setAutoCommit(false);
            assertEquals(List.of(10L),values(rc,"SELECT k FROM sbtest1 WHERE id=1"));
            assertEquals(List.of(10L),values(rr,"SELECT k FROM sbtest1 WHERE id=1"));
            sql(writer,"UPDATE sbtest1 SET k=20 WHERE id=1");
            assertEquals(List.of(20L),values(rc,"SELECT k FROM sbtest1 WHERE id=1"));
            assertEquals(List.of(10L),values(rr,"SELECT k FROM sbtest1 WHERE id=1"));rc.commit();rr.commit();
        }
        var catalog=IndexCatalog.fromSchema(SourceSchema.of("sysbench",Path.of("benchmark/sysbench"),Map.of("tables",1L)));
        try(var files=Files.list(root.resolve("capture"))) {
            var paths=files.toList();assertEquals(2,paths.size());
            for(Path file:paths) {
                var accesses=new ArrayList<PostgresAccesses.Access>();PostgresAccesses.read(file,catalog,true,accesses::add);
                assertEquals(2,accesses.size());
                var statements=new java.util.HashMap<Long,Long>();String[] application={null};
                PostgresFacts.read(file,e->{
                    application[0]=e.get("application").getAsString();
                    if(e.get("event").getAsString().equals("executor_start")&&e.get("depth").getAsInt()==1)
                        statements.put(e.get("req").getAsLong(),e.get("snapshot_csn").getAsLong());
                });
                for(var access:accesses)assertEquals(statements.get(access.query()).longValue(),access.snapshotCsn());
                if(application[0].equals("sibench:rc"))assertTrue(accesses.get(0).snapshotCsn()<accesses.get(1).snapshotCsn());
                else {assertEquals("sibench:rr",application[0]);assertEquals(accesses.get(0).snapshotCsn(),accesses.get(1).snapshotCsn());}
            }
        }
    }

    /**
     * HyBench AP-5 under a generic plan: a nested loop drives a BitmapAnd whose constant-key input
     * was bound at plan start. Rescanning that never-fetched input must not end the cycle or seal
     * the other input, so the heap verdict belongs to the intersection of both indexes.
     */
    @Test public void parameterizedBitmapAndKeepsAConstantInputBoundAtPlanStart()throws Exception {
        String runtime=System.getProperty("sibench.postgres.runtime");assertNotNull("Set sibench.postgres.runtime",runtime);
        Path root=temporary.getRoot().toPath().resolve("bitmap-and");
        String query="SELECT cu.custid,sum(tr.amount) FROM company co,transfer tr,customer cu WHERE cu.custid=tr.targetid"
                +" AND cu.companyid=co.companyid AND tr.sourceid=co.companyid AND co.companyid=$1 AND tr.type='salary'"
                +" GROUP BY cu.custid";
        try(var cluster=PostgresDatabase.create(Path.of(runtime),root)) {
            try(var setup=cluster.connect("setup")) {
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
                // The dev SF1 run chose this plan on its own; tiny tables need the joins pinned to reach it.
                for(String setting:List.of("plan_cache_mode=force_generic_plan","enable_hashjoin=off","enable_mergejoin=off"))sql(setup,"SET "+setting);
                sql(setup,"PREPARE q(int) AS "+query);
                var plan=new StringBuilder();
                try(var q=setup.createStatement();var rows=q.executeQuery("EXPLAIN (COSTS OFF) EXECUTE q(301)")){while(rows.next())plan.append(rows.getString(1)).append('\n');}
                // An outer nested loop rescans the bitmap heap scan and so both BitmapAnd inputs.
                assertTrue(plan.toString(),plan.toString().matches("(?s).*Nested Loop.*Seq Scan on company.*Bitmap Heap Scan on transfer.*BitmapAnd.*idx_transfer_1.*idx_transfer_3.*"));
            }
            try(var client=cluster.connect("sibench:bitmap-and")) {
                for(String setting:List.of("plan_cache_mode=force_generic_plan","enable_hashjoin=off","enable_mergejoin=off"))sql(client,"SET "+setting);
                sql(client,"PREPARE q(int) AS "+query);
                assertEquals(297,values(client,"EXECUTE q(301)").size());
            }
        }
        var catalog=IndexCatalog.fromSchema(SourceSchema.hybench(Path.of("benchmark/HyBench-2024")));
        var accesses=new ArrayList<PostgresAccesses.Access>();
        try(var files=Files.list(root.resolve("capture"))) {
            for(Path file:files.toList())PostgresAccesses.read(file,catalog,true,accesses::add);
        }
        var intersections=accesses.stream().filter(a->a.projection().mode()==PostgresAccessProjection.Mode.INTERSECTION).toList();
        assertEquals(1,intersections.size());
        var group=intersections.get(0).projection();assertEquals(297,group.count());
        assertEquals(List.of(catalog.named("idx_transfer_1").indexId(),catalog.named("idx_transfer_3").indexId()),
                group.parts().stream().map(PostgresAccessProjection.Part::index).sorted().toList());
        for(var part:group.parts())assertTrue(part.range().point());
    }

    @Test public void bitmapAndCyclesWithAnEmptyParameterizedInputKeepTheirOwnCandidates()throws Exception {
        String runtime=System.getProperty("sibench.postgres.runtime");assertNotNull("Set sibench.postgres.runtime",runtime);
        Path root=temporary.getRoot().toPath().resolve("bitmap-and-empty");
        // HyBench IQ-4's first branch: the parameterized input runs first and is empty for company 302.
        String query="SELECT co.name,t.amount FROM company co,transfer t WHERE t.sourceid=$1 AND t.type='invest'"
                +" AND t.targetid=co.companyid";
        var settings=List.of("plan_cache_mode=force_generic_plan","enable_hashjoin=off","enable_mergejoin=off");
        try(var cluster=PostgresDatabase.create(Path.of(runtime),root)) {
            try(var setup=cluster.connect("setup")) {
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
                var plan=new StringBuilder();
                try(var q=setup.createStatement();var rows=q.executeQuery("EXPLAIN (COSTS OFF) EXECUTE q(1)")){while(rows.next())plan.append(rows.getString(1)).append('\n');}
                assertTrue(plan.toString(),plan.toString().matches(
                        "(?s).*Nested Loop.*Seq Scan on company.*Bitmap Heap Scan on transfer.*Filter.*BitmapAnd.*idx_transfer_2.*idx_transfer_1.*"));
            }
            try(var client=cluster.connect("sibench:bitmap-and-empty")) {
                for(String setting:settings)sql(client,"SET "+setting);
                sql(client,"PREPARE q(int) AS "+query);
                try(var q=client.createStatement();var rows=q.executeQuery("EXECUTE q(1)")){assertFalse(rows.next());}
            }
        }
        var catalog=IndexCatalog.fromSchema(SourceSchema.hybench(Path.of("benchmark/HyBench-2024")));
        var accesses=new ArrayList<PostgresAccesses.Access>();
        try(var files=Files.list(root.resolve("capture"))) {
            for(Path file:files.toList())PostgresAccesses.read(file,catalog,true,accesses::add);
        }
        // Company 301: all 40 candidates of the intersection are visible; the type filter runs after the index.
        var intersections=accesses.stream().filter(a->a.projection().mode()==PostgresAccessProjection.Mode.INTERSECTION).toList();
        assertEquals(1,intersections.size());
        assertEquals(40,intersections.get(0).projection().count());
        // Company 302: the empty targetid input ends the BitmapAnd before the sourceid input runs.
        int targets=catalog.named("idx_transfer_2").indexId();
        var alone=accesses.stream().filter(a->a.projection().mode()==PostgresAccessProjection.Mode.INDEPENDENT
                &&a.projection().parts().get(0).index()==targets).toList();
        assertEquals(1,alone.size());
        assertEquals(0,alone.get(0).projection().count());
    }

    @Test public void bitmapCandidatesOnBlocksAddedAfterTheScanStartedAreInvisible()throws Exception {
        String runtime=System.getProperty("sibench.postgres.runtime");assertNotNull("Set sibench.postgres.runtime",runtime);
        Path root=temporary.getRoot().toPath().resolve("bitmap-extended");
        // The one-time filter runs the insert after the bitmap heap scan opened and before its index
        // scan: the new rows' index entries point past the heap's size at scan start.
        String query="WITH ins AS (INSERT INTO transfer SELECT i,1,5,i,'invest' FROM generate_series(100001,104000) i RETURNING 1)"
                +" SELECT count(*) FROM transfer WHERE sourceid=1 AND (SELECT count(*) FROM ins)>0";
        try(var cluster=PostgresDatabase.create(Path.of(runtime),root)) {
            try(var setup=cluster.connect("setup")) {
                sql(setup,"CREATE TABLE transfer(id INT PRIMARY KEY,sourceid INT,targetid INT,amount REAL,type CHAR(10))");
                sql(setup,"INSERT INTO transfer SELECT i,CASE WHEN i<=1200 THEN 1 ELSE 2+i%300 END,1+(i*7)%300,i,'invest'"
                        +" FROM generate_series(1,6000) i");
                sql(setup,"CREATE INDEX idx_transfer_1 ON transfer(sourceid)");
                sql(setup,"VACUUM ANALYZE");
                var plan=new StringBuilder();
                try(var q=setup.createStatement();var rows=q.executeQuery("EXPLAIN (COSTS OFF) "+query)){while(rows.next())plan.append(rows.getString(1)).append('\n');}
                assertTrue(plan.toString(),plan.toString().matches("(?s).*One-Time Filter.*Bitmap Heap Scan on transfer.*idx_transfer_1.*"));
            }
            try(var client=cluster.connect("sibench:bitmap-extended")) {
                client.setAutoCommit(false);
                assertEquals(List.of(1200L),values(client,query));
                client.rollback();
            }
        }
        var catalog=IndexCatalog.fromSchema(SourceSchema.hybench(Path.of("benchmark/HyBench-2024")));
        var accesses=new ArrayList<PostgresAccesses.Access>();
        try(var files=Files.list(root.resolve("capture"))) {
            for(Path file:files.toList())PostgresAccesses.read(file,catalog,true,accesses::add);
        }
        int sources=catalog.named("idx_transfer_1").indexId();
        var searches=accesses.stream().filter(a->a.projection().parts().get(0).index()==sources).toList();
        assertEquals(1,searches.size());
        assertEquals(1200,searches.get(0).projection().count());
    }
}
