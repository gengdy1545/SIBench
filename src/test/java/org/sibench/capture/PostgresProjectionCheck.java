package org.sibench.capture;

import static org.junit.Assert.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.config.Engine;
import org.sibench.generation.NativeHistory;
import org.sibench.generation.PostgresDatabase;
import org.sibench.replay.ReplayConfig;
import org.sibench.replay.ReplayProjection;
import org.sibench.schema.SourceSchema;

public class PostgresProjectionCheck {
    @Rule public TemporaryFolder temporary=new TemporaryFolder();
    private static void sql(Connection db,String text)throws Exception {try(var q=db.createStatement()){q.execute(text);}}
    private static long number(Connection db,String text)throws Exception {
        try(var q=db.createStatement();var r=q.executeQuery(text)){assertTrue(r.next());return r.getLong(1);}
    }
    @Test public void directTracePreservesLocalAbortFailedStatementAndReinsertions()throws Exception {
        String runtime=System.getProperty("sibench.postgres.runtime");assertNotNull(runtime);
        Path root=temporary.getRoot().toPath(),socketRoot=Files.createTempDirectory("pg-project-");
        var executor=Executors.newSingleThreadExecutor();
        var catalog=IndexCatalog.fromSchema(SourceSchema.of("sysbench",Path.of("benchmark/sysbench"),Map.of("tables",1L)));
        var baseline=new ArrayList<IndexOperationTrace.Operation>();
        var failedPhysicalWrite=new java.util.concurrent.atomic.AtomicBoolean();
        var sourceClosed=new java.util.concurrent.CountDownLatch(1);
        try(var capture=new PostgresCapture(socketRoot.resolve("capture.sock"),root.resolve("pending"),16L<<20,1024);
                var cluster=PostgresDatabase.create(Path.of(runtime),root.resolve("source"),socketRoot.resolve("capture.sock"))) {
            try(var setup=cluster.connect("setup")) {
                sql(setup,"CREATE TABLE sbtest1(id INTEGER PRIMARY KEY,k INTEGER,c CHAR(120),pad CHAR(60))");
                sql(setup,"CREATE INDEX k_1 ON sbtest1(k)");sql(setup,"INSERT INTO sbtest1 VALUES(1,10,'a','a'),(2,20,'b','b')");
                setup.setAutoCommit(false);NativeHistory.initialize(setup,Engine.POSTGRES);
                NativeHistory.install(setup,Engine.POSTGRES,1,"public","sbtest1","id");PostgresCapture.install(setup);setup.commit();
                assertEquals(Map.of("sbtest1",2L),NativeBaseline.write(setup,org.sibench.config.Engine.POSTGRES,catalog.definitions(),baseline::add));setup.commit();
                setup.setAutoCommit(true);sql(setup,"VACUUM ANALYZE");
                sql(setup,"CREATE FUNCTION reject_four() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                        +"IF EXISTS(SELECT 1 FROM inserted WHERE id=4) THEN RAISE EXCEPTION 'late statement failure'; END IF; RETURN NULL; END $$");
                sql(setup,"CREATE TRIGGER z_reject_four AFTER INSERT ON sbtest1 REFERENCING NEW TABLE AS inserted "
                        +"FOR EACH STATEMENT EXECUTE FUNCTION reject_four()");
            }
            var future=executor.submit(()->{
                // Future native RowIDs must already exist when earlier outcomes are projected.
                sourceClosed.await();
                var operations=new ArrayList<IndexOperationTrace.Operation>();
                try(var control=cluster.connect("projection")) {
                    control.setAutoCommit(false);
                    var units=WorkUnits.create(root);
                    try(var lines=units.lines();var versions=new PostgresVersions(control);
                            var projection=new PostgresProjection(control,catalog,root,versions,units,operations::add)) {
                        for(PostgresTransactions.Chunk next;(next=capture.transactions.next(()->capture.watermark(control)))!=null;)
                            try(var chunk=next){
                                if(!chunk.committed)chunk.read(e->{
                                    if(e.get("event").getAsString().equals("row_change")&&e.get("operation").getAsString().equals("insert")
                                            &&e.getAsJsonObject("new").get("id").getAsInt()==4)failedPhysicalWrite.set(true);
                                });
                                projection.accept(chunk);
                            }
                    }
                    control.commit();
                }catch(Throwable failure){capture.fail(failure);throw failure;}
                return operations;
            });
            try(var worker=cluster.connect("sibench:projection")) {
                sql(worker,"SET enable_seqscan=off");sql(worker,"SET enable_bitmapscan=off");worker.setAutoCommit(false);
                sql(worker,"UPDATE sbtest1 SET k=11 WHERE id=1");sql(worker,"INSERT INTO sbtest1 VALUES(3,30,'c','c')");
                assertEquals(1,number(worker,"SELECT count(*) FROM sbtest1 WHERE k=11"));
                assertEquals(30,number(worker,"SELECT k FROM sbtest1 WHERE id=3"));
                assertThrows(java.sql.SQLException.class,()->sql(worker,"INSERT INTO sbtest1 VALUES(4,40,'d','d')"));
                worker.rollback();
                assertEquals(0,number(worker,"SELECT count(*) FROM sbtest1 WHERE id=3"));worker.commit();
                sql(worker,"DELETE FROM sbtest1 WHERE id=1");sql(worker,"INSERT INTO sbtest1 VALUES(1,50,'e','e')");
                sql(worker,"UPDATE sbtest1 SET k=51 WHERE id=1");
                assertEquals(51,number(worker,"SELECT k FROM sbtest1 WHERE id=1"));worker.commit();
                assertEquals(1,number(worker,"SELECT count(*) FROM sbtest1 WHERE k=51"));worker.commit();
                try(var writer=cluster.connect("sibench:writer")) {
                    worker.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
                    assertEquals(1,number(worker,"SELECT count(*) FROM sbtest1 WHERE k=51"));
                    sql(writer,"UPDATE sbtest1 SET k=60 WHERE id=1");
                    assertEquals(1,number(worker,"SELECT count(*) FROM sbtest1 WHERE k=51"));worker.commit();
                    worker.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                    assertEquals(1,number(worker,"SELECT count(*) FROM sbtest1 WHERE k=60"));
                    sql(writer,"UPDATE sbtest1 SET k=61 WHERE id=1");
                    assertEquals(0,number(worker,"SELECT count(*) FROM sbtest1 WHERE k=60"));worker.commit();
                }
            }
            try(var control=cluster.connect("close")){capture.finishSources(control);}
            sourceClosed.countDown();
            var operations=future.get(10,TimeUnit.SECONDS);
            assertTrue("The failed statement really emitted a row change before its error",failedPhysicalWrite.get());
            assertEquals(1,operations.stream().filter(o->o.kind()==IndexOperationTrace.TRANSACTION_ABORT).count());
            assertEquals(7,operations.stream().filter(o->IndexOperationTrace.isMutation(o)&&o.index()==0).count());
            assertTrue(operations.stream().anyMatch(o->o.kind()==IndexOperationTrace.SEARCH&&o.mutationOrdinal()==2&&o.expectedCount()==1));
            assertFalse(operations.stream().anyMatch(o->IndexOperationTrace.isMutation(o)&&org.sibench.trace.IndexKey.ofInt(4).equals(o.key())));
            long original=baseline.stream().filter(o->o.index()==0&&o.key().equals(org.sibench.trace.IndexKey.ofInt(1))).findFirst().orElseThrow().rowId();
            long replacement=operations.stream().filter(o->IndexOperationTrace.isMutation(o)&&o.index()==0&&(o.flags()&IndexOperationTrace.BUSINESS_INSERT)!=0
                    &&o.key().equals(org.sibench.trace.IndexKey.ofInt(1))).findFirst().orElseThrow().rowId();
            assertNotEquals(original,replacement);
            var changes=operations.stream().filter(o->IndexOperationTrace.isMutation(o)&&o.index()==0
                    &&o.key().equals(org.sibench.trace.IndexKey.ofInt(1))).toList();
            assertEquals(java.util.List.of(original,original,replacement,replacement,replacement,replacement),
                    changes.stream().map(IndexOperationTrace.Operation::rowId).toList());
            try(var reference=new IndexReferenceReplay(true,true)) {
                for(var operation:baseline)reference.accept(operation);
                for(var operation:operations)
                    if(!IndexOperationTrace.isControl(operation)||operation.kind()==IndexOperationTrace.TRANSACTION_ABORT)
                        reference.accept(operation);
                assertTrue(reference.queries()>=7);
            }
            Path exported=root.resolve("committed.pb.zst");
            var writer=new ProtoTrace.Writer(exported);
            try(writer){for(var operation:operations)writer.append(operation);}
            var committed=new ArrayList<IndexOperationTrace.Operation>();
            ProtoTrace.read(exported,writer.sha256(),writer.logicalSha256(),committed::add);
            // The aborted UPDATE/INSERT disappear; DELETE/INSERT/UPDATE of id=1 collapse to one.
            assertEquals(3,committed.stream().filter(o->IndexOperationTrace.isMutation(o)&&o.index()==0).count());
            assertTrue(committed.stream().noneMatch(o->(o.flags()&IndexOperationTrace.TRANSACTION_LOCAL_SEARCH)!=0));
            var config=new ReplayConfig(true,0,0,0,0,"operation");
            try(var reference=new IndexReferenceReplay(true,true);
                    var replay=new ReplayProjection(config,true,reference::acceptCommitted)) {
                for(var operation:baseline)reference.acceptCommitted(operation);
                for(var operation:committed)replay.accept(operation);
                replay.finish();
            }
            assertEquals(2,capture.reports().size());
        }finally{executor.shutdownNow();Files.deleteIfExists(socketRoot.resolve("capture.sock"));Files.delete(socketRoot);}
    }

    @Test public void bitmapAndCyclesKeepOneSearchWhenTheirFirstInputIsReplaced()throws Exception {
        String runtime=System.getProperty("sibench.postgres.runtime");assertNotNull(runtime);
        Path root=temporary.getRoot().toPath(),socketRoot=Files.createTempDirectory("pg-project-");
        var executor=Executors.newSingleThreadExecutor();
        var catalog=IndexCatalog.fromSchema(SourceSchema.hybench(Path.of("benchmark/HyBench-2024")));
        var sourceClosed=new java.util.concurrent.CountDownLatch(1);
        try(var capture=new PostgresCapture(socketRoot.resolve("capture.sock"),root.resolve("pending"),16L<<20,1024);
                var cluster=PostgresDatabase.create(Path.of(runtime),root.resolve("source"),socketRoot.resolve("capture.sock"))) {
            try(var setup=cluster.connect("setup")) {
                sql(setup,"CREATE TABLE company(companyid INT PRIMARY KEY,name VARCHAR(20))");
                sql(setup,"INSERT INTO company VALUES(301,'a'),(302,'b')");
                sql(setup,"CREATE TABLE transfer(id INT PRIMARY KEY,sourceid INT,targetid INT,amount REAL,type CHAR(10))");
                sql(setup,"INSERT INTO transfer SELECT i,CASE WHEN i<=1200 THEN 1 ELSE 2+i%300 END,"
                        +"CASE WHEN i<=40 THEN 301 ELSE 1+(i*7)%300 END,i,CASE WHEN i<=40 THEN 'salary'"
                        +" ELSE (ARRAY['invest','donate','transfer','business'])[1+i%4] END FROM generate_series(1,6000) i");
                for(String index:java.util.List.of("idx_transfer_1 ON transfer(sourceid)","idx_transfer_2 ON transfer(targetid)"))
                    sql(setup,"CREATE INDEX "+index);
                setup.setAutoCommit(false);NativeHistory.initialize(setup,Engine.POSTGRES);
                for(String table:java.util.List.of("company","transfer"))
                    NativeHistory.install(setup,Engine.POSTGRES,catalog.tableId(table),"public",table,
                            table.equals("company")?"companyid":"id");
                PostgresCapture.install(setup);setup.commit();setup.setAutoCommit(true);sql(setup,"VACUUM ANALYZE");
            }
            var future=executor.submit(()->{
                sourceClosed.await();
                var operations=new ArrayList<IndexOperationTrace.Operation>();
                try(var control=cluster.connect("projection")) {
                    control.setAutoCommit(false);
                    var units=WorkUnits.create(root);
                    try(var lines=units.lines();var versions=new PostgresVersions(control);
                            var projection=new PostgresProjection(control,catalog,root,versions,units,operations::add)) {
                        for(PostgresTransactions.Chunk next;(next=capture.transactions.next(()->capture.watermark(control)))!=null;)
                            try(var chunk=next){projection.accept(chunk);}
                    }
                    control.commit();
                }catch(Throwable failure){capture.fail(failure);throw failure;}
                return operations;
            });
            // A constant key binds its input at plan start; the outer row's rescan replaces that unit first.
            String query="SELECT co.name,t.amount FROM company co,transfer t WHERE t.sourceid=1 AND t.type='invest'"
                    +" AND t.targetid=co.companyid";
            for(String name:java.util.List.of("plan","sibench:bitmap-and"))
                try(var session=cluster.connect(name)) {
                    for(String setting:java.util.List.of("enable_hashjoin=off","enable_mergejoin=off"))sql(session,"SET "+setting);
                    if(name.equals("plan")) {
                        var plan=new StringBuilder();
                        try(var q=session.createStatement();var rows=q.executeQuery("EXPLAIN (COSTS OFF) "+query)){while(rows.next())plan.append(rows.getString(1)).append('\n');}
                        assertTrue(plan.toString(),plan.toString().matches("(?s).*Nested Loop.*Seq Scan on company.*BitmapAnd.*idx_transfer_2.*idx_transfer_1.*"));
                    } else try(var q=session.createStatement();var rows=q.executeQuery(query)){assertFalse(rows.next());}
                }
            try(var control=cluster.connect("close")){capture.finishSources(control);}
            sourceClosed.countDown();
            var searches=future.get(10,TimeUnit.SECONDS).stream().filter(o->o.kind()==IndexOperationTrace.SEARCH).toList();
            int sources=catalog.named("idx_transfer_1").indexId(),targets=catalog.named("idx_transfer_2").indexId();
            // Company 301 intersects 40 visible candidates; 302 has no target rows, so its sourceid input never runs.
            assertEquals(1,searches.stream().filter(o->o.index()==sources&&o.expectedCount()==40).count());
            assertEquals(1,searches.stream().filter(o->o.index()==targets&&o.expectedCount()==40).count());
            assertEquals(1,searches.stream().filter(o->o.index()==targets&&o.expectedCount()==0).count());
            assertEquals(3,searches.size());
        }finally{executor.shutdownNow();Files.deleteIfExists(socketRoot.resolve("capture.sock"));Files.delete(socketRoot);}
    }

    @Test public void longReadOnlyTransactionsLeaveAsPiecesAtTheirSnapshots()throws Exception {
        String runtime=System.getProperty("sibench.postgres.runtime");assertNotNull(runtime);
        Path root=temporary.getRoot().toPath(),socketRoot=Files.createTempDirectory("pg-project-");
        var executor=Executors.newSingleThreadExecutor();
        var catalog=IndexCatalog.fromSchema(SourceSchema.of("sysbench",Path.of("benchmark/sysbench"),Map.of("tables",1L)));
        var baseline=new ArrayList<IndexOperationTrace.Operation>();
        // The reader's events exceed this 1 MiB pending budget; prefixes beyond 2 KiB leave as pieces.
        try(var capture=new PostgresCapture(socketRoot.resolve("capture.sock"),root.resolve("pending"),1L<<20,1024);
                var cluster=PostgresDatabase.create(Path.of(runtime),root.resolve("source"),socketRoot.resolve("capture.sock"))) {
            try(var setup=cluster.connect("setup")) {
                sql(setup,"CREATE TABLE sbtest1(id INTEGER PRIMARY KEY,k INTEGER,c CHAR(120),pad CHAR(60))");
                sql(setup,"CREATE INDEX k_1 ON sbtest1(k)");sql(setup,"INSERT INTO sbtest1 VALUES(1,10,'a','a'),(2,20,'b','b')");
                setup.setAutoCommit(false);NativeHistory.initialize(setup,Engine.POSTGRES);
                NativeHistory.install(setup,Engine.POSTGRES,1,"public","sbtest1","id");PostgresCapture.install(setup);setup.commit();
                NativeBaseline.write(setup,Engine.POSTGRES,catalog.definitions(),baseline::add);setup.commit();
                setup.setAutoCommit(true);sql(setup,"VACUUM ANALYZE");
            }
            // Projection runs concurrently with the sources and releases pieces once the watermark passes them.
            var future=executor.submit(()->{
                var operations=new ArrayList<IndexOperationTrace.Operation>();
                try(var control=cluster.connect("projection")) {
                    control.setAutoCommit(false);
                    var units=WorkUnits.create(root);
                    try(var lines=units.lines();var versions=new PostgresVersions(control);
                            var projection=new PostgresProjection(control,catalog,root,versions,units,operations::add)) {
                        for(PostgresTransactions.Chunk next;(next=capture.transactions.next(()->capture.watermark(control)))!=null;)
                            try(var chunk=next){projection.accept(chunk);}
                    }
                    control.commit();
                }catch(Throwable failure){capture.fail(failure);throw failure;}
                return operations;
            });
            var seen=new ArrayList<Long>();
            try(var reader=cluster.connect("sibench:reader");var writer=cluster.connect("sibench:writer")) {
                sql(reader,"SET enable_seqscan=off");sql(reader,"SET enable_bitmapscan=off");reader.setAutoCommit(false);
                // Like a HyBench query client: one READ COMMITTED transaction that never writes or commits.
                for(int i=0;i<1000;i++) {
                    if(i%100==99)sql(writer,"INSERT INTO sbtest1 VALUES("+(100+i)+","+(15+i%3)+",'w','w')");
                    seen.add(number(reader,"SELECT count(*) FROM sbtest1 WHERE k BETWEEN 10 AND 20"));
                }
                reader.rollback();
            }
            try(var control=cluster.connect("close")){capture.finishSources(control);}
            var operations=future.get(30,TimeUnit.SECONDS);
            assertTrue(((Number)capture.transactions.metrics().get("read_only_pieces")).longValue()>1);
            assertEquals(12L,(long)seen.get(seen.size()-1));
            var counts=operations.stream().filter(o->o.kind()==IndexOperationTrace.SEARCH&&o.index()==1)
                    .map(IndexOperationTrace.Operation::expectedCount).toList();
            assertEquals(seen,counts);
            // Every snapshot count matches an independent index at its read point; the reader resolves once, as an abort.
            assertEquals(1,operations.stream().filter(o->o.kind()==IndexOperationTrace.TRANSACTION_ABORT).count());
            try(var reference=new IndexReferenceReplay(true,true)) {
                for(var operation:baseline)reference.accept(operation);
                for(var operation:operations)
                    if(!IndexOperationTrace.isControl(operation)||operation.kind()==IndexOperationTrace.TRANSACTION_ABORT)
                        reference.accept(operation);
                assertEquals(1000,reference.queries());
            }
        }finally{executor.shutdownNow();Files.deleteIfExists(socketRoot.resolve("capture.sock"));Files.delete(socketRoot);}
    }

    @Test public void fullQueueHoldsReadOnlySourcesWithoutBlockingTheirCompletions()throws Exception {
        String runtime=System.getProperty("sibench.postgres.runtime");assertNotNull(runtime);
        Path root=temporary.getRoot().toPath(),socketRoot=Files.createTempDirectory("pg-project-");
        var executor=Executors.newSingleThreadExecutor();
        var catalog=IndexCatalog.fromSchema(SourceSchema.of("sysbench",Path.of("benchmark/sysbench"),Map.of("tables",1L)));
        var baseline=new ArrayList<IndexOperationTrace.Operation>();
        var sourceClosed=new java.util.concurrent.CountDownLatch(1);
        long budget=Long.getLong("sibench.test.queue_bytes",4L<<20);
        try(var capture=new PostgresCapture(socketRoot.resolve("capture.sock"),root.resolve("pending"),budget,4096);
                var cluster=PostgresDatabase.create(Path.of(runtime),root.resolve("source"),socketRoot.resolve("capture.sock"))) {
            try(var setup=cluster.connect("setup")) {
                sql(setup,"CREATE TABLE sbtest1(id INTEGER PRIMARY KEY,k INTEGER,c CHAR(120),pad CHAR(60))");
                sql(setup,"CREATE INDEX k_1 ON sbtest1(k)");
                sql(setup,"INSERT INTO sbtest1 SELECT i,i,'c','p' FROM generate_series(1,100) i");
                setup.setAutoCommit(false);NativeHistory.initialize(setup,Engine.POSTGRES);
                NativeHistory.install(setup,Engine.POSTGRES,1,"public","sbtest1","id");PostgresCapture.install(setup);setup.commit();
                NativeBaseline.write(setup,Engine.POSTGRES,catalog.definitions(),baseline::add);setup.commit();
                setup.setAutoCommit(true);sql(setup,"VACUUM ANALYZE");
            }
            // The consumer starts only after the sources close, so completed transactions fill the queue first.
            var future=executor.submit(()->{
                sourceClosed.await();
                var operations=new ArrayList<IndexOperationTrace.Operation>();
                try(var control=cluster.connect("projection")) {
                    control.setAutoCommit(false);
                    var units=WorkUnits.create(root);
                    try(var lines=units.lines();var versions=new PostgresVersions(control);
                            var projection=new PostgresProjection(control,catalog,root,versions,units,operations::add)) {
                        for(PostgresTransactions.Chunk next;(next=capture.transactions.next(()->capture.watermark(control)))!=null;)
                            try(var chunk=next){projection.accept(chunk);capture.updateHold(control);}
                    }
                    control.commit();
                }catch(Throwable failure){capture.fail(failure);throw failure;}
                return operations;
            });
            try(var reader=cluster.connect("sibench:reader");var writer=cluster.connect("sibench:writer")) {
                sql(reader,"SET enable_seqscan=off");sql(reader,"SET enable_bitmapscan=off");
                reader.setAutoCommit(false);writer.setAutoCommit(false);
                assertEquals(100,number(reader,"SELECT count(*) FROM sbtest1 WHERE k BETWEEN 1 AND 100"));
                for(int i=0;i<100;i++){sql(writer,"UPDATE sbtest1 SET k=k+1000 WHERE id<=30");writer.commit();}
                // Starting a statement while the queue is full must not stop this source's own completion.
                assertEquals(70,number(reader,"SELECT count(*) FROM sbtest1 WHERE k BETWEEN 1 AND 100"));
                reader.rollback();
            }
            try(var control=cluster.connect("close")){capture.finishSources(control);}
            sourceClosed.countDown();
            var operations=future.get(60,TimeUnit.SECONDS);
            assertTrue(((Number)capture.transactions.metrics().get("peak_encoded_bytes")).longValue()>=budget/2);
            assertTrue(((Number)capture.transactions.metrics().get("full_queue_checks")).longValue()>0);
            assertEquals(java.util.List.of(100L,70L),operations.stream().filter(o->o.kind()==IndexOperationTrace.SEARCH&&o.index()==1)
                    .map(IndexOperationTrace.Operation::expectedCount).toList());
            try(var reference=new IndexReferenceReplay(true,true)) {
                for(var operation:baseline)reference.accept(operation);
                for(var operation:operations)
                    if(!IndexOperationTrace.isControl(operation)||operation.kind()==IndexOperationTrace.TRANSACTION_ABORT)
                        reference.accept(operation);
            }
        }finally{executor.shutdownNow();Files.deleteIfExists(socketRoot.resolve("capture.sock"));Files.delete(socketRoot);}
    }
}
