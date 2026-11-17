package org.sibench.capture;

import static org.junit.Assert.*;
import java.nio.file.Files;
import java.nio.file.Path;
import com.google.gson.JsonObject;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.config.Engine;
import org.sibench.generation.NativeHistory;
import org.sibench.generation.PostgresDatabase;

/** Explicit native transport check; no full raw artifact or final data audit is attached to capture. */
public class PostgresCaptureCheck {
    @Rule public TemporaryFolder temporary=new TemporaryFolder();
    private static void sql(Connection db,String text)throws Exception {try(var q=db.createStatement()){q.execute(text);}}
    private static long number(Connection db,String text)throws Exception {
        try(var q=db.createStatement();var r=q.executeQuery(text)){assertTrue(r.next());return r.getLong(1);}
    }
    @Test public void sourceWaitsForConsumptionInsteadOfGrowingThePendingQueue()throws Exception {
        String runtime=System.getProperty("sibench.postgres.runtime");assertNotNull(runtime);
        Path sockets=Files.createTempDirectory("pg-credit-"),endpoint=sockets.resolve("capture.sock");
        var executor=Executors.newSingleThreadExecutor();
        try(var capture=new PostgresCapture(endpoint,temporary.getRoot().toPath().resolve("pending"),16L<<20,256);
                var cluster=PostgresDatabase.create(Path.of(runtime),temporary.getRoot().toPath().resolve("source"),endpoint,org.sibench.generation.PostgresTestSettings.SETTINGS)) {
            try(var setup=cluster.connect("setup")) {
                setup.setAutoCommit(false);NativeHistory.initialize(setup,Engine.POSTGRES);PostgresCapture.install(setup);setup.commit();
            }
            var producer=executor.submit(()->{
                try(var source=cluster.connect("sibench:credit")) {
                    for(int i=0;i<512;i++)assertEquals(1,number(source,"SELECT 1"));
                }
                try(var control=cluster.connect("finish")){capture.finishSources(control);}return null;
            });
            long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(10);
            while(((Number)capture.transactions.metrics().get("pending_transactions")).intValue()<128) {
                capture.check();if(System.nanoTime()>deadline)fail("Source did not reach its consumption window");Thread.sleep(5);
            }
            assertThrows(java.util.concurrent.TimeoutException.class,()->producer.get(100,TimeUnit.MILLISECONDS));
            long requests=0;
            try(var control=cluster.connect("consumer")) {
                for(PostgresTransactions.Chunk next;(next=capture.transactions.next(()->capture.watermark(control)))!=null;)
                    try(var chunk=next){if(chunk.hasRequests)requests++;}
            }
            producer.get(10,TimeUnit.SECONDS);assertEquals(512,requests);
            assertTrue(((Number)capture.transactions.metrics().get("peak_transactions")).intValue()<=128);
            assertEquals(0,((Number)capture.transactions.metrics().get("pending_transactions")).intValue());
        }finally{executor.shutdownNow();Files.deleteIfExists(endpoint);Files.delete(sockets);}
    }
    @Test public void deliversWhileSourcesRunAndRetainsAbortedActionsWithoutRawFiles()throws Exception {
        String runtime=System.getProperty("sibench.postgres.runtime");assertNotNull(runtime);
        Path socketRoot=Files.createTempDirectory("pg-stream-"),endpoint=socketRoot.resolve("capture.sock");
        var executor=Executors.newSingleThreadExecutor();
        Path source=temporary.getRoot().toPath().resolve("source");
        try(var capture=new PostgresCapture(endpoint,temporary.getRoot().toPath().resolve("pending"),16L<<20,1024);
                var cluster=PostgresDatabase.create(Path.of(runtime),source,endpoint,org.sibench.generation.PostgresTestSettings.SETTINGS)) {
            try(var setup=cluster.connect("setup")) {
                sql(setup,"CREATE TABLE items(id BIGINT PRIMARY KEY,value BIGINT)");sql(setup,"INSERT INTO items VALUES(1,10),(2,20)");
                setup.setAutoCommit(false);NativeHistory.initialize(setup,Engine.POSTGRES);
                NativeHistory.install(setup,Engine.POSTGRES,1,"public","items","id");PostgresCapture.install(setup);setup.commit();
            }
            var progressed=new java.util.concurrent.CountDownLatch(1);
            var future=executor.submit(()->{
                var events=new ArrayList<com.google.gson.JsonObject>();long previous=0;
                try(var control=cluster.connect("capture-consumer")) {
                    for(PostgresTransactions.Chunk next;(next=capture.transactions.next(()->capture.watermark(control)))!=null;) {
                        try(var chunk=next) {
                            assertTrue("Completion order",chunk.csn>=previous);previous=chunk.csn;
                            chunk.read(event->{events.add(event);if(event.get("event").getAsString().equals("row_change"))progressed.countDown();});
                        }
                    }
                }catch(Throwable error){capture.fail(error);throw error;}
                return events;
            });
            long aborted,committed;
            try(var first=cluster.connect("sibench:first");var second=cluster.connect("sibench:second");var observer=cluster.connect("observer")) {
                first.setAutoCommit(false);sql(first,"UPDATE items SET value=11 WHERE id=1");
                aborted=number(first,"SELECT txid_current()");assertEquals(11,number(first,"SELECT value FROM items WHERE id=1"));
                second.setAutoCommit(false);sql(second,"UPDATE items SET value=22 WHERE id=2");
                committed=number(second,"SELECT txid_current()");second.commit();
                assertTrue("Trace facts must arrive before sources finish",progressed.await(5,TimeUnit.SECONDS));
                assertEquals(10,number(observer,"SELECT value FROM items WHERE id=1"));
                first.rollback();assertEquals(22,number(observer,"SELECT value FROM items WHERE id=2"));
            }
            try(var control=cluster.connect("capture-close")){capture.finishSources(control);}
            var events=future.get(10,TimeUnit.SECONDS);
            assertTrue(events.stream().anyMatch(e->e.get("event").getAsString().equals("row_change")&&e.get("xid").getAsLong()==aborted));
            var outcomes=events.stream().filter(e->List.of("transaction_commit","transaction_abort").contains(e.get("event").getAsString())
                    &&List.of(committed,aborted).contains(e.get("xid").getAsLong())).toList();
            assertEquals(2,outcomes.size());assertEquals(committed,outcomes.get(0).get("xid").getAsLong());
            assertEquals(aborted,outcomes.get(1).get("xid").getAsLong());
            assertEquals("transaction_abort",outcomes.get(1).get("event").getAsString());
            assertEquals(2,capture.reports().size());
            assertEquals(0L,capture.transactions.metrics().get("pending_encoded_bytes"));
            try(var files=Files.walk(temporary.getRoot().toPath())) {
                assertFalse("Facts are never written as raw stream files",files.anyMatch(f->f.getFileName().toString().matches("pg-[0-9]+\\.jsonl.*")));
            }
        }finally{executor.shutdownNow();Files.deleteIfExists(endpoint);Files.delete(socketRoot);}
        assertFalse(Files.exists(source.resolve("data/postmaster.pid")));
    }
    @Test public void identitiesAndCapturePreserveAbortsAndReinsertions()throws Exception {
        String runtime=System.getProperty("sibench.postgres.runtime");
        assertNotNull("Set sibench.postgres.runtime to a freshly built instrumented runtime",runtime);
        Path root=temporary.getRoot().toPath().resolve("source"),socketRoot=Files.createTempDirectory("pg-history-"),endpoint=socketRoot.resolve("capture.sock");
        var executor=Executors.newSingleThreadExecutor();long aborted,original,replacement;List<JsonObject> events;
        try(var capture=new PostgresCapture(endpoint,temporary.getRoot().toPath().resolve("pending"),16L<<20,1024);
                var cluster=PostgresDatabase.create(Path.of(runtime),root,endpoint,org.sibench.generation.PostgresTestSettings.SETTINGS)) {
            try(var setup=cluster.connect("setup")) {
                assertEquals(1,number(setup,"SELECT (current_setting('wal_level')='minimal' "
                        +"AND current_setting('max_wal_senders')='0' AND current_setting('max_parallel_maintenance_workers')='0' "
                        +"AND current_setting('fsync')='off' "
                        +"AND current_setting('synchronous_commit')='off' AND current_setting('full_page_writes')='off')::int"));
                PostgresDatabase.unloggedTables(setup);
                sql(setup,"CREATE TABLE items(id BIGINT PRIMARY KEY,value BIGINT)");
                assertEquals(1,number(setup,"SELECT (relpersistence='u')::int FROM pg_class WHERE oid='items'::regclass"));
                sql(setup,"INSERT INTO items VALUES(1,10),(2,20)");
                PostgresDatabase.finishLoad(setup);
                assertEquals(0,number(setup,"SELECT count(*) FROM pg_event_trigger WHERE evtname='sibench_unlogged_tables'"));
                setup.setAutoCommit(false);NativeHistory.initialize(setup,Engine.POSTGRES);
                NativeHistory.install(setup,Engine.POSTGRES,1,"public","items","id");PostgresCapture.install(setup);setup.commit();
                assertEquals(0,number(setup,"SELECT count(*) FROM pg_class c JOIN pg_namespace n ON c.relnamespace=n.oid WHERE n.nspname='sibench_history' AND c.relkind='r' AND c.relpersistence<>'u'"));
                original=number(setup,"SELECT _sib_row_id FROM sibench_history.identities_1 WHERE id=1");
                // An uncaptured insertion would be missing from the Trace.
                setup.setAutoCommit(true);
                assertTrue(assertThrows(SQLException.class,()->sql(setup,"INSERT INTO items VALUES(9,90)"))
                        .getMessage().contains("captured source session"));
            }
            var future=executor.submit(()->{
                var received=new ArrayList<JsonObject>();
                try(var control=cluster.connect("capture-consumer")) {
                    for(PostgresTransactions.Chunk next;(next=capture.transactions.next(()->capture.watermark(control)))!=null;)
                        try(var chunk=next){chunk.read(received::add);}
                }catch(Throwable error){capture.fail(error);throw error;}
                return received;
            });
            try(var writer=cluster.connect("sibench:writer");var observer=cluster.connect("observer")) {
                sql(writer,"SET enable_seqscan=off");writer.setAutoCommit(false);
                sql(writer,"UPDATE items SET value=11 WHERE id=1");
                aborted=number(writer,"SELECT txid_current()");
                assertEquals(11,number(writer,"SELECT value FROM items WHERE id=1"));
                assertEquals(10,number(observer,"SELECT value FROM items WHERE id=1"));
                sql(writer,"INSERT INTO items VALUES(3,30)");
                assertEquals(30,number(writer,"SELECT value FROM items WHERE id=3"));
                assertEquals(0,number(observer,"SELECT count(*) FROM items WHERE id=3"));
                assertThrows(SQLException.class,()->sql(writer,"INSERT INTO items VALUES(4,40),(1,99)"));
                writer.rollback();
                assertEquals(0,number(observer,"SELECT count(*) FROM items WHERE id=3"));
                sql(writer,"UPDATE items SET value=12 WHERE id=1");writer.commit();
                sql(writer,"DELETE FROM items WHERE id=1");writer.commit();
                sql(writer,"INSERT INTO items VALUES(1,14)");writer.commit();
                // Updates keep the RowID; a deleted and reinserted key gets a new one.
                assertEquals(original,number(observer,"SELECT _sib_row_id FROM sibench_history.identities_1 WHERE id=1 AND _sib_tx=0"));
                replacement=number(observer,"SELECT _sib_row_id FROM sibench_history.identities_1 WHERE id=1 AND _sib_tx<>0");
                assertNotEquals(original,replacement);
                assertThrows(SQLException.class,()->sql(writer,"UPDATE items SET id=3-id"));writer.rollback();
                // Only RowID identities are kept; no before-image history table exists.
                assertEquals(0,number(observer,"SELECT count(*) FROM pg_class c JOIN pg_namespace n ON c.relnamespace=n.oid "
                        +"WHERE n.nspname='sibench_history' AND c.relname LIKE 'rows%'"));
            }
            try(var control=cluster.connect("capture-close")){capture.finishSources(control);}
            events=future.get(10,TimeUnit.SECONDS);
        }finally{executor.shutdownNow();Files.deleteIfExists(endpoint);Files.delete(socketRoot);}
        assertTrue(events.stream().anyMatch(e->"row_change".equals(e.get("event").getAsString())&&e.get("xid").getAsLong()==aborted));
        for(var event:events)if(event.get("event").getAsString().equals("row_change"))
            for(String image:List.of("old","new"))if(!event.get(image).isJsonNull())
                assertEquals(java.util.Set.of("id"),event.getAsJsonObject(image).keySet());
        assertTrue(events.stream().anyMatch(e->"transaction_abort".equals(e.get("event").getAsString())&&e.get("xid").getAsLong()==aborted));
        assertTrue(events.stream().anyMatch(e->"row_identity".equals(e.get("event").getAsString())&&e.get("xid").getAsLong()==aborted
                &&e.get("primary_key").getAsString().equals("3")));
        assertFalse(events.stream().anyMatch(e->"row_identity".equals(e.get("event").getAsString())&&e.get("xid").getAsLong()==aborted
                &&e.get("primary_key").getAsString().equals("4")));
        assertTrue(events.stream().anyMatch(e->"scan_summary".equals(e.get("event").getAsString())&&e.get("xid").getAsLong()==aborted));
        long failedRequest=events.stream().filter(e->"executor_start".equals(e.get("event").getAsString())
                &&e.get("depth").getAsInt()==1&&e.get("sql").getAsString().contains("VALUES(4,40),(1,99)"))
                .findFirst().orElseThrow().get("req").getAsLong();
        // The failed statement wrote row 4 before its duplicate key; that write has no RowID allocation.
        assertTrue(events.stream().anyMatch(e->"row_change".equals(e.get("event").getAsString())&&e.get("req").getAsLong()==failedRequest
                &&!e.get("new").isJsonNull()&&e.getAsJsonObject("new").get("id").getAsString().equals("4")));
        assertFalse(events.stream().anyMatch(e->"executor_end".equals(e.get("event").getAsString())
                &&e.get("depth").getAsInt()==1&&e.get("req").getAsLong()==failedRequest));
        assertFalse(events.stream().anyMatch(e->"scan_summary".equals(e.get("event").getAsString())&&e.get("relation").getAsString().startsWith("identity_")));
        assertFalse(Files.exists(root.resolve("data/postmaster.pid")));
        assertThrows(java.io.IOException.class,()->PostgresDatabase.create(Path.of(runtime),root,org.sibench.generation.PostgresTestSettings.SETTINGS));
    }
}
