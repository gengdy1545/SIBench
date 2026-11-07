package org.sibench.capture;

import static org.junit.Assert.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
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
                var cluster=PostgresDatabase.create(Path.of(runtime),temporary.getRoot().toPath().resolve("source"),endpoint)) {
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
                var cluster=PostgresDatabase.create(Path.of(runtime),source,endpoint)) {
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
            try(var files=Files.list(cluster.capture())){assertEquals(0,files.count());}
        }finally{executor.shutdownNow();Files.deleteIfExists(endpoint);Files.delete(socketRoot);}
        assertFalse(Files.exists(source.resolve("data/postmaster.pid")));
    }
}
