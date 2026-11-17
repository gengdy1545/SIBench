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
import org.sibench.schema.SourceSchema;

public class PostgresProjectionCheck {
    @Rule public TemporaryFolder temporary=new TemporaryFolder();
    private static void sql(Connection db,String text)throws Exception {try(var q=db.createStatement()){q.execute(text);}}
    private static long number(Connection db,String text)throws Exception {
        try(var q=db.createStatement();var r=q.executeQuery(text)){assertTrue(r.next());return r.getLong(1);}
    }
    @Test public void internalRollbackRestoresWritesAndIdentitiesButKeepsCompletedReads()throws Exception {
        String runtime=System.getProperty("sibench.postgres.runtime");assertNotNull(runtime);
        Path root=temporary.getRoot().toPath(),sockets=Files.createTempDirectory("pg-subtransaction-");
        var executor=Executors.newSingleThreadExecutor();
        var source=SourceSchema.of("sysbench",Path.of("benchmark/sysbench"),Map.of("tables",1L));
        try(var capture=new PostgresCapture(sockets.resolve("capture.sock"),root.resolve("pending"),16L<<20,1024);
                var cluster=PostgresDatabase.create(Path.of(runtime),root.resolve("source"),sockets.resolve("capture.sock"),org.sibench.generation.PostgresTestSettings.SETTINGS)) {
            var trace=new PostgresTrace(root.resolve("trace"),source,true,null);
            try(var setup=cluster.connect("setup")) {
                sql(setup,"CREATE TABLE sbtest1(id INTEGER PRIMARY KEY,k INTEGER,c CHAR(120),pad CHAR(60))");
                sql(setup,"CREATE INDEX k_1 ON sbtest1(k)");
                sql(setup,"INSERT INTO sbtest1 VALUES(1,10,'a','a'),(2,10,'b','b')");
                setup.setAutoCommit(false);NativeHistory.initialize(setup,Engine.POSTGRES);
                NativeHistory.install(setup,Engine.POSTGRES,1,"public","sbtest1","id");PostgresCapture.install(setup);setup.commit();
                trace.baseline(setup,Map.of("sbtest1",2L));setup.commit();setup.setAutoCommit(true);
                sql(setup,"CREATE FUNCTION caught_changes() RETURNS bigint LANGUAGE plpgsql AS $$ BEGIN "
                        +"UPDATE sbtest1 SET k=15 WHERE id=1; "
                        +"BEGIN UPDATE sbtest1 SET k=20 WHERE id=1; "
                        +"IF (SELECT count(*) FROM sbtest1 WHERE k=20)<>1 THEN RAISE EXCEPTION 'bad private read'; END IF; "
                        +"BEGIN UPDATE sbtest1 SET k=30 WHERE id=1; "
                        +"IF (SELECT count(*) FROM sbtest1 WHERE k=30)<>1 THEN RAISE EXCEPTION 'bad child read'; END IF; "
                        +"EXCEPTION WHEN division_by_zero THEN NULL; END; "
                        +"RAISE EXCEPTION 'rollback parent'; EXCEPTION WHEN raise_exception THEN NULL; END; "
                        +"IF (SELECT count(*) FROM sbtest1 WHERE k=15)<>1 THEN RAISE EXCEPTION 'lost parent write'; END IF; "
                        +"BEGIN DELETE FROM sbtest1 WHERE id=2; INSERT INTO sbtest1 VALUES(2,40,'b','b'); "
                        +"IF (SELECT count(*) FROM sbtest1 WHERE k=40)<>1 THEN RAISE EXCEPTION 'bad reinsert'; END IF; "
                        +"RAISE EXCEPTION 'rollback identity'; EXCEPTION WHEN raise_exception THEN NULL; END; "
                        +"UPDATE sbtest1 SET k=11 WHERE id=2; RETURN (SELECT count(*) FROM sbtest1 WHERE k=20); END $$");
                sql(setup,"CREATE FUNCTION read_then_fail() RETURNS bigint LANGUAGE plpgsql AS $$ BEGIN "
                        +"UPDATE sbtest1 SET k=25 WHERE id=1; "
                        +"IF (SELECT count(*) FROM sbtest1 WHERE k=25)<>1 THEN RAISE EXCEPTION 'bad read'; END IF; "
                        +"RAISE EXCEPTION 'failed outer statement'; END $$");
                sql(setup,"CREATE FUNCTION reinsert_twice() RETURNS bigint LANGUAGE plpgsql AS $$ BEGIN "
                        +"INSERT INTO sbtest1 VALUES(3,60,'c','c'); DELETE FROM sbtest1 WHERE id=3; "
                        +"INSERT INTO sbtest1 VALUES(3,61,'c','c'); RETURN (SELECT count(*) FROM sbtest1 WHERE k=61); END $$");
            }
            var future=executor.submit(()->{
                try(var control=cluster.connect("projection")){control.setAutoCommit(false);trace.workload(control,capture);}
                catch(Exception failure){capture.fail(failure);failure.printStackTrace();throw failure;}return null;
            });
            try(var worker=cluster.connect("sibench:subtransactions")) {
                sql(worker,"SET enable_seqscan=off");sql(worker,"SET enable_bitmapscan=off");worker.setAutoCommit(false);
                for(boolean commit:new boolean[]{false,true}) {
                    assertEquals(0,number(worker,"SELECT caught_changes()"));
                    assertEquals(1,number(worker,"SELECT count(*) FROM sbtest1 WHERE k=15"));
                    if(commit)worker.commit();else worker.rollback();
                }
                sql(worker,"SAVEPOINT restore_row");sql(worker,"DELETE FROM sbtest1 WHERE id=2");
                sql(worker,"INSERT INTO sbtest1 VALUES(2,50,'b','b')");
                assertEquals(1,number(worker,"SELECT count(*) FROM sbtest1 WHERE k=50"));
                sql(worker,"ROLLBACK TO SAVEPOINT restore_row");
                assertEquals(1,number(worker,"SELECT count(*) FROM sbtest1 WHERE k=11"));
                sql(worker,"RELEASE SAVEPOINT restore_row");worker.commit();
                assertThrows(java.sql.SQLException.class,()->number(worker,"SELECT read_then_fail()"));worker.rollback();
                assertEquals(0,number(worker,"SELECT count(*) FROM sbtest1 WHERE k=25"));worker.commit();
                assertEquals(1,number(worker,"SELECT reinsert_twice()"));worker.rollback();
            }
            try(var control=cluster.connect("finish")){capture.finishSources(control);}
            future.get(15,TimeUnit.SECONDS);trace.publish(Map.of());
            assertEquals("passed",org.sibench.replay.ReferenceReplay.run(root.resolve("trace"),
                    new ReplayConfig(true,0,0,0,0,"operation", 1, 4096, 60, 1),root.resolve("reference.json")).get("status"));
            var operations=new ArrayList<IndexOperationTrace.Operation>();
            var file=org.sibench.trace.TraceManifest.read(root.resolve("trace")).getAsJsonObject("proto_files")
                    .getAsJsonObject("workload");
            ProtoTrace.read(root.resolve("trace").resolve("workload.pb.zst"),file.get("sha256").getAsString(),
                    file.get("logical_sha256").getAsString(),operations::add);
            assertEquals(2,operations.stream().filter(o->IndexOperationTrace.isMutation(o)&&o.index()==0).count());
            for(long key:new long[]{20,25,30,40,50}) {
                var indexKey=org.sibench.trace.IndexKey.ofInt(key);
                assertTrue("Completed private read at "+key,operations.stream().anyMatch(o->o.kind()==IndexOperationTrace.SEARCH
                        &&o.index()==1&&indexKey.equals(o.lower())&&o.expectedCount()==1&&!o.localWrites().isEmpty()));
                assertFalse("Rolled-back secondary write at "+key,operations.stream().anyMatch(o->IndexOperationTrace.isMutation(o)
                        &&o.index()==1&&indexKey.equals(o.indexedValue())));
            }
        }finally{executor.shutdownNow();Files.deleteIfExists(sockets.resolve("capture.sock"));Files.deleteIfExists(sockets);}
    }
    @Test public void transactionIdentitySpillsAreReclaimedBeforeTheNextTransaction()throws Exception {
        String runtime=System.getProperty("sibench.postgres.runtime");assertNotNull(runtime);
        Path root=temporary.getRoot().toPath(),sockets=Files.createTempDirectory("pg-identity-spill-");
        var executor=Executors.newSingleThreadExecutor();
        var source=SourceSchema.of("sysbench",Path.of("benchmark/sysbench"),Map.of("tables",1L));
        try(var capture=new PostgresCapture(sockets.resolve("capture.sock"),root.resolve("pending"),16L<<20,1024);
                var cluster=PostgresDatabase.create(Path.of(runtime),root.resolve("source"),sockets.resolve("capture.sock"),org.sibench.generation.PostgresTestSettings.SETTINGS)) {
            var trace=new PostgresTrace(root.resolve("trace"),source,true,null);
            try(var setup=cluster.connect("setup")) {
                sql(setup,"CREATE TABLE sbtest1(id INTEGER PRIMARY KEY,k INTEGER,c CHAR(120),pad CHAR(60))");
                sql(setup,"CREATE INDEX k_1 ON sbtest1(k)");
                setup.setAutoCommit(false);NativeHistory.initialize(setup,Engine.POSTGRES);
                NativeHistory.install(setup,Engine.POSTGRES,1,"public","sbtest1","id");PostgresCapture.install(setup);setup.commit();
                trace.baseline(setup,Map.of("sbtest1",0L));setup.commit();
            }
            var future=executor.submit(()->{
                try(var control=cluster.connect("projection")){control.setAutoCommit(false);trace.workload(control,capture);}
                catch(Exception failure){capture.fail(failure);throw failure;}return null;
            });
            try(var worker=cluster.connect("sibench:spills")) {
                sql(worker,"INSERT INTO sbtest1 SELECT i,i%10,'c','p' FROM generate_series(1,66000) i");
                sql(worker,"INSERT INTO sbtest1 VALUES(66001,1,'c','p')");
            }
            try(var control=cluster.connect("finish")){capture.finishSources(control);}
            future.get(20,TimeUnit.SECONDS);trace.publish(Map.of());
            assertFalse(Files.exists(root.resolve("trace/local-identities")));
            assertFalse(Files.exists(root.resolve("trace/aborted-identities")));
            assertEquals("passed",org.sibench.replay.ReferenceReplay.run(root.resolve("trace"),
                    new ReplayConfig(true,0,0,0,0,"operation", 1, 4096, 60, 1),root.resolve("reference.json")).get("status"));
        }finally{executor.shutdownNow();Files.deleteIfExists(sockets.resolve("capture.sock"));Files.deleteIfExists(sockets);}
    }
    @Test public void sameStatementReadsKeepTheirOwnCommandVisibilityThroughCommitAndRollback() throws Exception {
        String runtime=System.getProperty("sibench.postgres.runtime");assertNotNull(runtime);
        Path root=temporary.getRoot().toPath(),sockets=Files.createTempDirectory("pg-command-capture-");
        var executor=Executors.newFixedThreadPool(2);
        var source=SourceSchema.of("sysbench",Path.of("benchmark/sysbench"),Map.of("tables",1L));
        try(var capture=new PostgresCapture(sockets.resolve("capture.sock"),root.resolve("pending"),16L<<20,1024);
                var cluster=PostgresDatabase.create(Path.of(runtime),root.resolve("source"),sockets.resolve("capture.sock"),org.sibench.generation.PostgresTestSettings.SETTINGS)) {
            var trace=new PostgresTrace(root.resolve("trace"),source,true,null);
            try(var setup=cluster.connect("setup")) {
                sql(setup,"CREATE TABLE sbtest1(id INTEGER PRIMARY KEY,k INTEGER,c CHAR(120),pad CHAR(60))");
                sql(setup,"CREATE INDEX k_1 ON sbtest1(k)");
                sql(setup,"INSERT INTO sbtest1 VALUES(1,10,'a','a'),(2,10,'b','b')");
                setup.setAutoCommit(false);NativeHistory.initialize(setup,Engine.POSTGRES);
                NativeHistory.install(setup,Engine.POSTGRES,1,"public","sbtest1","id");PostgresCapture.install(setup);setup.commit();
                trace.baseline(setup,Map.of("sbtest1",2L));setup.commit();setup.setAutoCommit(true);
                sql(setup,"CREATE FUNCTION before_change() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                        +"IF NEW.id=1 AND NEW.k=20 THEN UPDATE sbtest1 SET k=21 WHERE id=2; "
                        +"IF (SELECT count(*) FROM sbtest1 WHERE k=21)<>1 THEN RAISE EXCEPTION 'nested write invisible'; END IF; "
                        +"END IF; RETURN NEW; END $$");
                sql(setup,"CREATE TRIGGER a_before BEFORE UPDATE ON sbtest1 FOR EACH ROW EXECUTE FUNCTION before_change()");
                sql(setup,"CREATE FUNCTION after_change() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                        +"IF TG_OP='DELETE' THEN IF (SELECT count(*) FROM sbtest1 WHERE id=OLD.id)<>0 "
                        +"THEN RAISE EXCEPTION 'deleted row visible'; END IF; RETURN OLD; "
                        +"ELSE IF (SELECT count(*) FROM sbtest1 WHERE k=NEW.k)<>1 "
                        +"THEN RAISE EXCEPTION 'new row invisible'; END IF; RETURN NEW; END IF; END $$");
                // This trigger sorts before the old capture trigger; its reads must still see the write.
                sql(setup,"CREATE TRIGGER a_after AFTER INSERT OR UPDATE OR DELETE ON sbtest1 FOR EACH ROW EXECUTE FUNCTION after_change()");
                sql(setup,"CREATE FUNCTION stable_count(integer) RETURNS bigint LANGUAGE sql STABLE AS "
                        +"'SELECT count(*) FROM sbtest1 WHERE k=$1'");
                sql(setup,"CREATE FUNCTION change_and_count() RETURNS bigint LANGUAGE plpgsql VOLATILE AS $$ BEGIN "
                        +"UPDATE sbtest1 SET k=30 WHERE id=1; RETURN (SELECT count(*) FROM sbtest1 WHERE k=30); END $$");
                sql(setup,"CREATE FUNCTION wait_for_writer() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                        +"IF NEW.k=50 THEN PERFORM pg_advisory_lock(713113); "
                        +"IF (SELECT count(*) FROM sbtest1 WHERE k=60)<>1 THEN RAISE EXCEPTION 'fresh snapshot missed commit'; END IF; "
                        +"PERFORM pg_advisory_unlock(713113); END IF; RETURN NEW; END $$");
                sql(setup,"CREATE TRIGGER b_wait AFTER UPDATE ON sbtest1 FOR EACH ROW EXECUTE FUNCTION wait_for_writer()");
            }
            var future=executor.submit(()->{
                try(var control=cluster.connect("projection")){control.setAutoCommit(false);trace.workload(control,capture);}
                catch(Exception failure){capture.fail(failure);throw failure;}
                return null;
            });
            try(var worker=cluster.connect("sibench:commands")) {
                sql(worker,"SET enable_seqscan=off");sql(worker,"SET enable_bitmapscan=off");worker.setAutoCommit(false);
                for(boolean commit:new boolean[]{false,true}) {
                    sql(worker,"UPDATE sbtest1 SET k=20 WHERE id=1");
                    try(var q=worker.createStatement();var rows=q.executeQuery("SELECT change_and_count(),stable_count(20),stable_count(30)")) {
                        assertTrue(rows.next());assertEquals(1,rows.getLong(1));
                        // STABLE reads retain the outer command snapshot after the VOLATILE write.
                        assertEquals(1,rows.getLong(2));assertEquals(0,rows.getLong(3));
                    }
                    sql(worker,"INSERT INTO sbtest1 VALUES(3,40,'c','c')");sql(worker,"DELETE FROM sbtest1 WHERE id=3");
                    assertEquals(0,number(worker,"WITH inserted AS (INSERT INTO sbtest1 VALUES(4,70,'d','d') RETURNING id) "
                            +"SELECT count(*) FROM sbtest1 WHERE k=70 AND (SELECT count(*) FROM inserted)>0"));
                    sql(worker,"DELETE FROM sbtest1 WHERE id=4");
                    if(commit)worker.commit();else worker.rollback();
                }
                assertEquals(1,number(worker,"SELECT count(*) FROM sbtest1 WHERE k=30"));worker.commit();
                try(var gate=cluster.connect("gate");var writer=cluster.connect("sibench:writer")) {
                    sql(gate,"SELECT pg_advisory_lock(713113)");
                    var update=executor.submit(()->{sql(worker,"UPDATE sbtest1 SET k=50 WHERE id=1");worker.commit();return null;});
                    long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);
                    while(number(gate,"SELECT count(*) FROM pg_stat_activity WHERE application_name='sibench:commands' AND wait_event='advisory'")==0) {
                        if(System.nanoTime()>deadline)fail("Reader did not reach the controlled trigger wait");
                        Thread.sleep(10);
                    }
                    sql(writer,"UPDATE sbtest1 SET k=60 WHERE id=2");sql(gate,"SELECT pg_advisory_unlock(713113)");
                    update.get(5,TimeUnit.SECONDS);
                }
            }
            try(var control=cluster.connect("finish")){capture.finishSources(control);}
            future.get(15,TimeUnit.SECONDS);trace.publish(Map.of());
            assertEquals("passed",org.sibench.replay.ReferenceReplay.run(root.resolve("trace"),
                    new ReplayConfig(true,0,0,0,0,"operation", 1, 4096, 60, 1),root.resolve("reference.json")).get("status"));
            var operations=new ArrayList<IndexOperationTrace.Operation>();
            var manifest=org.sibench.trace.TraceManifest.read(root.resolve("trace"));
            var file=manifest.getAsJsonObject("proto_files").getAsJsonObject("workload");
            ProtoTrace.read(root.resolve("trace").resolve("workload.pb.zst"),file.get("sha256").getAsString(),file.get("logical_sha256").getAsString(),operations::add);
            var reads=operations.stream().filter(o->o.kind()==IndexOperationTrace.SEARCH).toList();
            assertTrue(reads.size()>=25);assertTrue(reads.stream().anyMatch(o->!o.localWrites().isEmpty()));
            assertTrue(reads.stream().anyMatch(o->o.transactionId()==0&&!o.localWrites().isEmpty()));
            assertTrue(reads.stream().collect(java.util.stream.Collectors.groupingBy(IndexOperationTrace.Operation::workUnitId))
                    .values().stream().anyMatch(group->group.stream().map(IndexOperationTrace.Operation::readTimestamp).distinct().count()>1));
            assertEquals(6,operations.stream().filter(o->IndexOperationTrace.isMutation(o)&&o.index()==0).count());
        }finally{executor.shutdownNow();Files.deleteIfExists(sockets.resolve("capture.sock"));Files.deleteIfExists(sockets);}
    }
    @Test public void successfulRollbackReadsKeepStatementSnapshotsWithoutFailedOrSequentialReads() throws Exception {
        String runtime=System.getProperty("sibench.postgres.runtime");assertNotNull(runtime);
        Path root=temporary.getRoot().toPath(),sockets=Files.createTempDirectory("pg-read-capture-");
        var executor=Executors.newSingleThreadExecutor();
        var source=SourceSchema.of("sysbench",Path.of("benchmark/sysbench"),Map.of("tables",1L));
        try(var capture=new PostgresCapture(sockets.resolve("capture.sock"),root.resolve("pending"),16L<<20,1024);
                var cluster=PostgresDatabase.create(Path.of(runtime),root.resolve("source"),sockets.resolve("capture.sock"),org.sibench.generation.PostgresTestSettings.SETTINGS)) {
            var trace=new PostgresTrace(root.resolve("trace"),source,true,null);
            try(var setup=cluster.connect("setup")) {
                sql(setup,"CREATE TABLE sbtest1(id INTEGER PRIMARY KEY,k INTEGER,c CHAR(120),pad CHAR(60))");
                sql(setup,"CREATE INDEX k_1 ON sbtest1(k)");
                sql(setup,"INSERT INTO sbtest1 VALUES(1,10,'a','a'),(2,10,'b','b')");
                setup.setAutoCommit(false);NativeHistory.initialize(setup,Engine.POSTGRES);
                NativeHistory.install(setup,Engine.POSTGRES,1,"public","sbtest1","id");PostgresCapture.install(setup);setup.commit();
                trace.baseline(setup,Map.of("sbtest1",2L));setup.commit();
            }
            var future=executor.submit(()->{
                try(var control=cluster.connect("projection")){control.setAutoCommit(false);trace.workload(control,capture);}
                catch(Exception failure){capture.fail(failure);throw failure;}
                return null;
            });
            try(var reader=cluster.connect("sibench:reader");var writer=cluster.connect("sibench:writer")) {
                sql(reader,"SET enable_seqscan=off");sql(reader,"SET enable_bitmapscan=off");
                reader.setAutoCommit(false);reader.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                assertEquals(2,number(reader,"SELECT count(*) FROM sbtest1 WHERE k=10"));
                sql(writer,"INSERT INTO sbtest1 VALUES(3,10,'c','c')");
                assertEquals(3,number(reader,"SELECT count(*) FROM sbtest1 WHERE k=10"));reader.rollback();
                assertThrows(java.sql.SQLException.class,()->number(reader,"SELECT sum(1/(id-id)) FROM sbtest1 WHERE id=1"));reader.rollback();
                sql(reader,"SET enable_seqscan=on");sql(reader,"SET enable_indexscan=off");sql(reader,"SET enable_indexonlyscan=off");
                assertEquals(3,number(reader,"SELECT count(*) FROM sbtest1 WHERE k=10"));reader.rollback();
            }
            try(var reader=cluster.connect("sibench:portal")) {
                sql(reader,"SET enable_seqscan=off");sql(reader,"SET enable_bitmapscan=off");reader.setAutoCommit(false);
                try(var query=reader.prepareStatement("SELECT 1/(id-3) FROM sbtest1 WHERE id>=1")) {
                    query.setFetchSize(2);var rows=query.executeQuery();assertTrue(rows.next());assertTrue(rows.next());
                    assertThrows(java.sql.SQLException.class,rows::next);
                }
                reader.rollback();
                // Leave the fully fetched portal for connection-close rollback instead of ExecutorEnd cleanup.
                var query=reader.prepareStatement("SELECT id FROM sbtest1 WHERE k=10");query.setFetchSize(2);
                var rows=query.executeQuery();int count=0;while(rows.next())count++;assertEquals(3,count);
            }
            try(var control=cluster.connect("finish")){capture.finishSources(control);}
            future.get(15,TimeUnit.SECONDS);trace.publish(Map.of());
            var operations=new ArrayList<IndexOperationTrace.Operation>();
            var manifest=org.sibench.trace.TraceManifest.read(root.resolve("trace"));
            var file=manifest.getAsJsonObject("proto_files").getAsJsonObject("workload");
            ProtoTrace.read(root.resolve("trace").resolve("workload.pb.zst"),file.get("sha256").getAsString(),file.get("logical_sha256").getAsString(),operations::add);
            var secondary=operations.stream().filter(o->o.kind()==IndexOperationTrace.SEARCH&&o.index()>0).toList();
            assertEquals(java.util.List.of(2L,3L,3L),secondary.stream().map(IndexOperationTrace.Operation::expectedCount).toList());
            assertTrue(secondary.get(0).readTimestamp()<secondary.get(1).readTimestamp());
            assertEquals(0,operations.stream().filter(o->o.kind()==IndexOperationTrace.SEARCH&&o.index()==0).count());
            assertEquals(2,operations.stream().filter(IndexOperationTrace::isMutation).count());
            assertEquals("passed",org.sibench.replay.ReferenceReplay.run(root.resolve("trace"),
                    new ReplayConfig(true,0,0,0,0,"operation", 1, 4096, 60, 1),root.resolve("reference.json")).get("status"));
        }finally{executor.shutdownNow();Files.deleteIfExists(sockets.resolve("capture.sock"));Files.deleteIfExists(sockets);}
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
                var cluster=PostgresDatabase.create(Path.of(runtime),root.resolve("source"),socketRoot.resolve("capture.sock"),org.sibench.generation.PostgresTestSettings.SETTINGS)) {
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
                            var projection=new PostgresProjection(control,catalog,root,versions,units,true,operations::add,null)) {
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
                // Both row changes precede the statement-level identity facts of this insert.
                sql(worker,"INSERT INTO sbtest1 VALUES(5,70,'f','f'),(6,80,'g','g')");
                assertEquals(70,number(worker,"SELECT k FROM sbtest1 WHERE id=5"));worker.commit();
            }
            var nativeIds=new java.util.HashMap<org.sibench.trace.IndexKey,Long>();
            try(var control=cluster.connect("close")) {
                capture.finishSources(control);
                try(var q=control.createStatement();var rows=q.executeQuery("SELECT id,_sib_row_id FROM sibench_history.identities_1 WHERE _sib_tx<>0")) {
                    while(rows.next())nativeIds.put(org.sibench.trace.IndexKey.ofInt(rows.getInt(1)),rows.getLong(2));
                }
            }
            sourceClosed.countDown();
            var operations=future.get(10,TimeUnit.SECONDS);
            var inserted=new java.util.HashMap<org.sibench.trace.IndexKey,Long>();
            for(var o:operations)if(IndexOperationTrace.isMutation(o)&&o.index()==0&&(o.flags()&IndexOperationTrace.BUSINESS_INSERT)!=0
                    &&o.kind()!=IndexOperationTrace.LOGICAL_DELETE&&!o.key().equals(org.sibench.trace.IndexKey.ofInt(3)))inserted.put(o.key(),o.rowId());
            assertEquals("Committed insertions use PG's own RowID allocations",nativeIds,inserted);
            assertTrue("The failed statement really emitted a row change before its error",failedPhysicalWrite.get());
            assertEquals(1,operations.stream().filter(o->o.kind()==IndexOperationTrace.TRANSACTION_ABORT).count());
            assertEquals(9,operations.stream().filter(o->IndexOperationTrace.isMutation(o)&&o.index()==0).count());
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
            var writer=new ProtoTrace.Writer(exported, true);
            try(writer){for(var operation:operations)writer.append(operation);}
            var committed=new ArrayList<IndexOperationTrace.Operation>();
            ProtoTrace.read(exported,writer.sha256(),writer.logicalSha256(),committed::add);
            assertEquals(5,committed.stream().filter(o->IndexOperationTrace.isMutation(o)&&o.index()==0).count());
            assertTrue(committed.stream().anyMatch(o->!o.localWrites().isEmpty()));
            try(var reference=new IndexReferenceReplay(true,true)) {
                for(var operation:baseline)reference.acceptCommitted(operation);
                for(var operation:committed)if(!IndexOperationTrace.isControl(operation))reference.acceptCommitted(operation);
                assertTrue(reference.queries()>=7);
            }
            assertEquals(2,capture.reports().size());
        }finally{executor.shutdownNow();Files.deleteIfExists(socketRoot.resolve("capture.sock"));Files.delete(socketRoot);}
    }

    @Test public void insertionWithoutItsNativeIdentityFactIsRejected()throws Exception {
        String runtime=System.getProperty("sibench.postgres.runtime");assertNotNull(runtime);
        Path root=temporary.getRoot().toPath(),socketRoot=Files.createTempDirectory("pg-project-");
        var executor=Executors.newSingleThreadExecutor();
        var catalog=IndexCatalog.fromSchema(SourceSchema.of("sysbench",Path.of("benchmark/sysbench"),Map.of("tables",1L)));
        try(var capture=new PostgresCapture(socketRoot.resolve("capture.sock"),root.resolve("pending"),16L<<20,1024);
                var cluster=PostgresDatabase.create(Path.of(runtime),root.resolve("source"),socketRoot.resolve("capture.sock"),org.sibench.generation.PostgresTestSettings.SETTINGS)) {
            try(var setup=cluster.connect("setup")) {
                sql(setup,"CREATE TABLE sbtest1(id INTEGER PRIMARY KEY,k INTEGER,c CHAR(120),pad CHAR(60))");
                sql(setup,"CREATE INDEX k_1 ON sbtest1(k)");sql(setup,"INSERT INTO sbtest1 VALUES(1,10,'a','a')");
                setup.setAutoCommit(false);NativeHistory.initialize(setup,Engine.POSTGRES);
                NativeHistory.install(setup,Engine.POSTGRES,1,"public","sbtest1","id");PostgresCapture.install(setup);setup.commit();
                setup.setAutoCommit(true);
                // The identity row is still written, so only the native identity fact is missing.
                sql(setup,"ALTER TABLE sibench_history.identities_1 DISABLE TRIGGER sibench_row_identity");
            }
            var future=executor.submit(()->{
                try(var control=cluster.connect("projection")) {
                    control.setAutoCommit(false);var units=WorkUnits.create(root);
                    try(var lines=units.lines();var versions=new PostgresVersions(control);
                            var projection=new PostgresProjection(control,catalog,root,versions,units,true,ignored->{},null)) {
                        for(PostgresTransactions.Chunk next;(next=capture.transactions.next(()->capture.watermark(control)))!=null;)
                            try(var chunk=next){projection.accept(chunk);}
                    }
                }catch(Throwable failure){capture.fail(failure);throw failure;}
                return null;
            });
            try(var worker=cluster.connect("sibench:projection")) {
                sql(worker,"INSERT INTO sbtest1 VALUES(2,20,'b','b')");
                assertEquals(1,number(worker,"SELECT count(*) FROM sibench_history.identities_1 WHERE id=2"));
            }
            try(var control=cluster.connect("close")){capture.finishSources(control);}catch(Exception ignored){}
            var failure=assertThrows(java.util.concurrent.ExecutionException.class,()->future.get(10,TimeUnit.SECONDS));
            assertTrue(String.valueOf(failure.getCause()),failure.getCause().getMessage().contains("lacks its native RowID"));
        }finally{executor.shutdownNow();Files.deleteIfExists(socketRoot.resolve("capture.sock"));Files.delete(socketRoot);}
    }

    @Test public void bitmapAndCyclesKeepOneSearchWhenTheirFirstInputIsReplaced()throws Exception {
        String runtime=System.getProperty("sibench.postgres.runtime");assertNotNull(runtime);
        Path root=temporary.getRoot().toPath(),socketRoot=Files.createTempDirectory("pg-project-");
        var executor=Executors.newSingleThreadExecutor();
        var catalog=IndexCatalog.fromSchema(SourceSchema.hybench(Path.of("benchmark/HyBench-2024")));
        var sourceClosed=new java.util.concurrent.CountDownLatch(1);
        try(var capture=new PostgresCapture(socketRoot.resolve("capture.sock"),root.resolve("pending"),16L<<20,1024);
                var cluster=PostgresDatabase.create(Path.of(runtime),root.resolve("source"),socketRoot.resolve("capture.sock"),org.sibench.generation.PostgresTestSettings.SETTINGS)) {
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
                            var projection=new PostgresProjection(control,catalog,root,versions,units,true,operations::add,null)) {
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

    @Test public void readOnlyTransactionsWaitForTheirOutcomeBeforeExport()throws Exception {
        String runtime=System.getProperty("sibench.postgres.runtime");assertNotNull(runtime);
        Path root=temporary.getRoot().toPath(),socketRoot=Files.createTempDirectory("pg-project-");
        var executor=Executors.newSingleThreadExecutor();
        var catalog=IndexCatalog.fromSchema(SourceSchema.of("sysbench",Path.of("benchmark/sysbench"),Map.of("tables",1L)));
        var baseline=new ArrayList<IndexOperationTrace.Operation>();
        // The reader crosses the former prefix-release threshold but stays within the transaction budget.
        try(var capture=new PostgresCapture(socketRoot.resolve("capture.sock"),root.resolve("pending"),16L<<20,1024);
                var cluster=PostgresDatabase.create(Path.of(runtime),root.resolve("source"),socketRoot.resolve("capture.sock"),org.sibench.generation.PostgresTestSettings.SETTINGS)) {
            try(var setup=cluster.connect("setup")) {
                sql(setup,"CREATE TABLE sbtest1(id INTEGER PRIMARY KEY,k INTEGER,c CHAR(120),pad CHAR(60))");
                sql(setup,"CREATE INDEX k_1 ON sbtest1(k)");sql(setup,"INSERT INTO sbtest1 VALUES(1,10,'a','a'),(2,20,'b','b')");
                setup.setAutoCommit(false);NativeHistory.initialize(setup,Engine.POSTGRES);
                NativeHistory.install(setup,Engine.POSTGRES,1,"public","sbtest1","id");PostgresCapture.install(setup);setup.commit();
                NativeBaseline.write(setup,Engine.POSTGRES,catalog.definitions(),baseline::add);setup.commit();
                setup.setAutoCommit(true);sql(setup,"VACUUM ANALYZE");
            }
            // Other committed transactions can drain while the reader remains unresolved.
            var future=executor.submit(()->{
                var operations=new ArrayList<IndexOperationTrace.Operation>();
                try(var control=cluster.connect("projection")) {
                    control.setAutoCommit(false);
                    var units=WorkUnits.create(root);
                    try(var lines=units.lines();var versions=new PostgresVersions(control);
                            var projection=new PostgresProjection(control,catalog,root,versions,units,true,operations::add,null)) {
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
                assertEquals(12,number(reader,"SELECT count(*) FROM sbtest1 WHERE k BETWEEN 10 AND 20"));
                reader.commit();
            }
            try(var control=cluster.connect("close")){capture.finishSources(control);}
            var operations=future.get(30,TimeUnit.SECONDS);
            assertEquals(12L,(long)seen.get(seen.size()-1));
            var counts=operations.stream().filter(o->o.kind()==IndexOperationTrace.SEARCH&&o.index()==1)
                    .map(IndexOperationTrace.Operation::expectedCount).toList();
            var allSeen=new ArrayList<>(seen);allSeen.add(12L);
            assertEquals(allSeen,counts);
            assertTrue(operations.stream().filter(o->o.kind()==IndexOperationTrace.SEARCH).allMatch(o->o.transactionId()>0));
            // Raw source validation still sees the abort and all of its reads.
            assertEquals(1,operations.stream().filter(o->o.kind()==IndexOperationTrace.TRANSACTION_ABORT).count());
            try(var reference=new IndexReferenceReplay(true,true)) {
                for(var operation:baseline)reference.accept(operation);
                for(var operation:operations)
                    if(!IndexOperationTrace.isControl(operation)||operation.kind()==IndexOperationTrace.TRANSACTION_ABORT)
                        reference.accept(operation);
                assertEquals(1001,reference.queries());
            }
            var file=root.resolve("committed.pb.zst");var trace=new ProtoTrace.Writer(file, true);
            try(trace){for(var operation:operations)trace.accept(operation);}
            var exported=new ArrayList<IndexOperationTrace.Operation>();
            ProtoTrace.read(file,trace.sha256(),trace.logicalSha256(),exported::add);
            assertFalse(exported.stream().anyMatch(o->o.kind()==IndexOperationTrace.TRANSACTION_ABORT));
            var queries=exported.stream().filter(o->o.kind()==IndexOperationTrace.SEARCH).toList();
            assertEquals(1001,queries.size());assertEquals(12,queries.get(queries.size()-1).expectedCount());
            try(var reference=new IndexReferenceReplay(true,true)) {
                for(var operation:baseline)reference.accept(operation);
                for(var operation:exported)if(!IndexOperationTrace.isControl(operation))reference.acceptCommitted(operation);
                assertEquals(1001,reference.queries());
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
        // Sized to the 100 writer transactions below: their sealed facts must reach half the budget.
        long budget=Long.getLong("sibench.test.queue_bytes",1L<<20);
        try(var capture=new PostgresCapture(socketRoot.resolve("capture.sock"),root.resolve("pending"),budget,4096);
                var cluster=PostgresDatabase.create(Path.of(runtime),root.resolve("source"),socketRoot.resolve("capture.sock"),org.sibench.generation.PostgresTestSettings.SETTINGS)) {
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
                            var projection=new PostgresProjection(control,catalog,root,versions,units,true,operations::add,null)) {
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
