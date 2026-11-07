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
        long workerPid;
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
                workerPid=worker.unwrap(org.postgresql.PGConnection.class).getBackendPID();
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
            var config=new ReplayConfig(true,0,0,0,0,"operation");
            try(var reference=new IndexReferenceReplay(true,true);var load=new ReplayProjection(config,false,reference::accept);
                    var replay=new ReplayProjection(config,true,reference::accept)) {
                for(var operation:baseline)load.accept(operation);load.finish();
                for(var operation:operations)replay.accept(operation);replay.finish();assertTrue(reference.queries()>=7);
            }
            assertEquals(2,capture.reports().size());
            long last;
            try(var control=cluster.connect("independent-acceptance")) {
                NativeHistory.verifyCommits(control);control.setAutoCommit(false);NativeHistory.seal(control);control.commit();
                last=number(control,"SELECT max(version) FROM sibench_history.transactions");
            }
            cluster.close();Path moved=root.resolve("moved-data");Files.move(cluster.data(),moved);
            var inventory=org.sibench.generation.PostgresData.inventory(moved,Path.of(runtime),last);
            Path reader=root.resolve("reader");
            try(var rows=org.sibench.generation.PostgresData.open(moved,inventory,Path.of(runtime),reader)) {
                assertEquals(10,((Number)rows.snapshotRow(1,original,0).orElseThrow().get("k")).intValue());
                assertTrue(rows.snapshotRow(1,original,last).isEmpty());
                assertEquals(61,((Number)rows.snapshotRow(1,replacement,last).orElseThrow().get("k")).intValue());
                for(long request:new long[]{10,12,13})assertEquals(51,((Number)rows.rowBeforeStatement(1,replacement,workerPid,request).orElseThrow().get("k")).intValue());
                assertEquals(60,((Number)rows.rowBeforeStatement(1,replacement,workerPid,14).orElseThrow().get("k")).intValue());
                assertEquals(61,((Number)rows.rowBeforeStatement(1,replacement,workerPid,15).orElseThrow().get("k")).intValue());
                assertThrows(java.sql.SQLException.class,()->rows.rowBeforeStatement(1,original,workerPid,4));
                assertThrows(IllegalArgumentException.class,()->rows.snapshotRow(1,replacement,last+1));
            }
            assertFalse(Files.exists(reader));
            assertEquals(inventory,org.sibench.generation.PostgresData.inventory(moved,Path.of(runtime),last));
        }finally{executor.shutdownNow();Files.deleteIfExists(socketRoot.resolve("capture.sock"));Files.delete(socketRoot);}
    }
}
