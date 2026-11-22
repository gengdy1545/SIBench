package org.sibench.adapters;

import static org.junit.Assert.*;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.adapters.proto.*;
import org.sibench.capture.IndexOperationTrace;
import org.sibench.trace.IndexKey;
import org.sibench.trace.IndexEntry;

/** Failure and shutdown boundaries remain required without transaction publication. */
public class IndexExecutorTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test(timeout = 10000)
    public void readersHonorTheConfiguredExecutionBudget() throws Exception {
        var admitted = new java.util.concurrent.CountDownLatch(4);
        var fifth = new java.util.concurrent.CountDownLatch(1);
        var release = new java.util.concurrent.CountDownLatch(1);
        var entered = new java.util.concurrent.atomic.AtomicInteger();
        var backend = (IndexBackend) java.lang.reflect.Proxy.newProxyInstance(IndexBackend.class.getClassLoader(),
                new Class<?>[]{IndexBackend.class}, (proxy, method, args) -> {
                    if (method.getName().equals("statistics")) return java.util.Map.of();
                    if (method.getName().equals("describe")) return description();
                    if (method.getName().equals("get")) {
                        if (entered.incrementAndGet() == 5) fifth.countDown();
                        admitted.countDown();
                        if (!release.await(3, TimeUnit.SECONDS)) throw new IOException("Read gate timed out");
                        return null;
                    }
                    throw new AssertionError("Unexpected backend call: " + method.getName());
                });
        try (var executor = IndexTestSupport.executor(backend)) {
            var calls = new java.util.ArrayList<java.util.concurrent.CompletableFuture<IndexResponse>>();
            try {
                for (int i = 0; i < 5; i++) calls.add(executor.submit(GetRequest.getDefaultInstance()));
                assertTrue(admitted.await(2, TimeUnit.SECONDS));
                assertFalse(fifth.await(100, TimeUnit.MILLISECONDS));
            } finally { release.countDown(); }
            for (var call : calls) assertEquals(IndexStatus.OK, call.get(2, TimeUnit.SECONDS).status());
        }
    }

    @Test(timeout = 15000)
    public void asynchronousWritesDoNotOccupyWorkersAndStillWaitForAck() throws Exception {
        int count = 2 * Runtime.getRuntime().availableProcessors() + 1;
        var admitted = new java.util.concurrent.CountDownLatch(count);
        var ack = new java.util.concurrent.CompletableFuture<Void>();
        var flushes = new java.util.concurrent.atomic.AtomicInteger();
        var backend = asynchronousBackend(ack, admitted, flushes);
        try (var executor = IndexTestSupport.executor(backend)) {
            var calls = new java.util.ArrayList<java.util.concurrent.CompletableFuture<IndexResponse>>();
            try {
                for (int i = 0; i < count; i++) calls.add(executor.submit(IndexServerTest.put(i, 1, i)));
                assertTrue("queued writes must reach storage before earlier ACKs", admitted.await(3, TimeUnit.SECONDS));
                assertTrue(calls.stream().noneMatch(java.util.concurrent.CompletableFuture::isDone));
                assertEquals(IndexStatus.INVALID, executor.submit(WriteBatchRequest.getDefaultInstance()).get(2, TimeUnit.SECONDS).status());
            } finally { ack.complete(null); }
            for (var call : calls) assertEquals(IndexStatus.OK, call.get(2, TimeUnit.SECONDS).status());
        }
        assertEquals(1, flushes.get());
    }

    @Test(timeout = 15000)
    public void shutdownWaitsForAsyncStorageAfterTheClientCancels() throws Exception {
        var admitted = new java.util.concurrent.CountDownLatch(1);
        var ack = new java.util.concurrent.CompletableFuture<Void>();
        var flushes = new java.util.concurrent.atomic.AtomicInteger();
        var closer = java.util.concurrent.Executors.newSingleThreadExecutor();
        try (var executor = IndexTestSupport.executor(asynchronousBackend(ack, admitted, flushes))) {
            try {
                var call = executor.submit(IndexServerTest.put(1, 1, 1));
                assertTrue(admitted.await(2, TimeUnit.SECONDS));
                call.cancel(false);
                var closed = closer.submit(() -> { executor.close(); return null; });
                assertThrows(java.util.concurrent.TimeoutException.class, () -> closed.get(100, TimeUnit.MILLISECONDS));
                assertEquals(0, flushes.get());
                ack.completeExceptionally(new IOException("may have applied before failure"));
                closed.get(2, TimeUnit.SECONDS);
                assertEquals(1, flushes.get());
            } finally { ack.complete(null); }
        } finally { closer.shutdownNow(); }
    }

    @Test(timeout = 10000)
    public void persistenceSamplesSeparateQueueAndExecutionTimes() throws Exception {
        var execution = IndexTestSupport.execution(1, 1);
        var backend = (IndexBackend) java.lang.reflect.Proxy.newProxyInstance(IndexBackend.class.getClassLoader(),
                new Class<?>[]{IndexBackend.class}, (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "get": Thread.sleep(200); return null;
                        case "writeAsync": return java.util.concurrent.CompletableFuture.completedFuture(null);
                        case "flush": return null;
                        case "statistics": return java.util.Map.of("counter", 7);
                        case "describe": return description();
                        default: throw new AssertionError("Unexpected backend call: " + method.getName());
                    }
                });
        try (var executor = new IndexExecutor(backend, execution)) {
            var first = executor.submit(GetRequest.getDefaultInstance());
            var second = executor.submit(GetRequest.getDefaultInstance());
            var put = executor.submit(PutRequest.newBuilder().setKey(com.google.protobuf.ByteString.copyFromUtf8("k")).setTs(1).build());
            for (var call : java.util.List.of(first, second, put)) assertEquals(IndexStatus.OK, call.get(2, TimeUnit.SECONDS).status());
        }
        // Shutdown persists once and samples; one reader means the second Get waited for the first.
        var lines = java.nio.file.Files.readAllLines(execution.statistics());
        assertEquals(1, lines.size());
        var line = com.google.gson.JsonParser.parseString(lines.get(0)).getAsJsonObject();
        var service = line.getAsJsonObject("service");
        java.util.function.Function<String, com.google.gson.JsonObject> stat = service::getAsJsonObject;
        assertEquals(2L, stat.apply("read_execute").get("requests").getAsLong());
        assertTrue(stat.apply("read_execute").get("micros").getAsLong() >= 400_000);
        assertTrue(stat.apply("read_queue").get("micros").getAsLong() >= 190_000);
        assertEquals(1L, stat.apply("write_execute").get("requests").getAsLong());
        assertEquals(1L, stat.apply("flush_call").get("requests").getAsLong());
        assertEquals(7, line.getAsJsonObject("engine").get("counter").getAsInt());
        assertEquals(1, line.getAsJsonObject("execution").get("read_threads").getAsInt());
        assertEquals("fixture", line.getAsJsonObject("backend").get("name").getAsString());
    }

    private static IndexDescription description() {
        return new IndexDescription("fixture", ValueKind.ROW_ID, Acknowledgement.ACK_MEMORY, false, "test", "fixture");
    }

    @Test public void statisticsRequireFreshOutputAndPreserveExistingEvidence() throws Exception {
        var execution = IndexTestSupport.execution();
        java.nio.file.Files.writeString(execution.statistics(), "retained evidence");
        var backend = asynchronousBackend(new java.util.concurrent.CompletableFuture<>(),
                new java.util.concurrent.CountDownLatch(1), new java.util.concurrent.atomic.AtomicInteger());
        assertThrows(java.nio.file.FileAlreadyExistsException.class, () -> new IndexExecutor(backend, execution));
        assertEquals("retained evidence", java.nio.file.Files.readString(execution.statistics()));
    }

    @Test(timeout = 5000) public void failedFlushIsRecordedWithoutReplacingThePersistenceError() throws Exception {
        var execution = IndexTestSupport.execution();
        var backend = (IndexBackend) java.lang.reflect.Proxy.newProxyInstance(IndexBackend.class.getClassLoader(),
                new Class<?>[]{IndexBackend.class}, (proxy, method, args) -> {
                    return switch (method.getName()) {
                        case "describe" -> description();
                        case "statistics" -> java.util.Map.of();
                        case "writeAsync" -> java.util.concurrent.CompletableFuture.completedFuture(null);
                        case "flush" -> throw new IOException("original persistence error");
                        default -> throw new AssertionError(method.getName());
                    };
                });
        var executor = new IndexExecutor(backend, execution);
        assertEquals(IndexStatus.OK, executor.submit(IndexServerTest.put(1, 1, 1)).get(2, TimeUnit.SECONDS).status());
        var error = assertThrows(IOException.class, executor::close);
        assertEquals("original persistence error", error.getCause().getMessage());
        var sample = com.google.gson.JsonParser.parseString(java.nio.file.Files.readString(execution.statistics())).getAsJsonObject();
        assertEquals(1L, sample.getAsJsonObject("service").getAsJsonObject("flush_call").get("failed").getAsLong());
        assertTrue(sample.get("error").getAsString().contains("Background persistence failed"));
    }

    private static IndexBackend asynchronousBackend(java.util.concurrent.CompletableFuture<Void> ack,
            java.util.concurrent.CountDownLatch admitted, java.util.concurrent.atomic.AtomicInteger flushes) {
        return (IndexBackend) java.lang.reflect.Proxy.newProxyInstance(IndexBackend.class.getClassLoader(),
                new Class<?>[]{IndexBackend.class}, (proxy, method, args) -> {
                    if (method.getName().equals("statistics")) return java.util.Map.of();
                    if (method.getName().equals("describe")) return description();
                    if (method.getName().equals("writeAsync")) { admitted.countDown(); return ack; }
                    if (method.getName().equals("flush")) { flushes.incrementAndGet(); return null; }
                    throw new AssertionError("Unexpected backend call: " + method.getName());
                });
    }

    @Test(timeout=15000)
    public void persistenceRunsWithoutAClientFlushAndItsFailureRejectsFurtherCalls() throws Exception {
        var flushed=new java.util.concurrent.CountDownLatch(1);
        try(var backend=VersionedIndexBackend.create(temporary.getRoot().toPath().resolve("maintenance"),ValueKind.ROW_ID)) {
            var failing=(IndexBackend)java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{IndexBackend.class},(proxy,method,args)->{
                if(method.getName().equals("flush")) { flushed.countDown(); throw new IOException("storage unavailable"); }
                try { return method.invoke(backend,args); } catch(java.lang.reflect.InvocationTargetException error) { throw error.getCause(); }
            });
            var executor=IndexTestSupport.executor(failing);
            var write=IndexServerTest.put(7,1,70);
            assertEquals(IndexStatus.OK,executor.submit(write).get(2,TimeUnit.SECONDS).status());
            assertTrue(flushed.await(8,TimeUnit.SECONDS));
            var error=assertThrows(IOException.class,executor::close);
            assertTrue(error.getMessage().contains("Background persistence"));
            assertEquals("storage unavailable",error.getCause().getMessage());
            assertEquals(IndexStatus.UNAVAILABLE,executor.submit(write).get(2,TimeUnit.SECONDS).status());
        }
    }

    @Test(timeout = 10000)
    public void failedWritesThatMayHaveAppliedStillReachPersistence() throws Exception {
        try (var backend = VersionedIndexBackend.create(temporary.getRoot().toPath().resolve("partial"), ValueKind.ROW_ID)) {
            var flushes = new java.util.concurrent.atomic.AtomicInteger();
            var partial = (IndexBackend) java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[] {IndexBackend.class}, (proxy, method, args) -> {
                        if (method.getName().equals("flush")) flushes.incrementAndGet();
                        Object result;
                        try { result = method.invoke(backend, args); }
                        catch (java.lang.reflect.InvocationTargetException error) { throw error.getCause(); }
                        if (method.getName().equals("writeAsync")) throw new IOException("applied, then the call failed");
                        return result;
                    });
            var rejected = IndexTestSupport.executor(partial);
            assertEquals(IndexStatus.INVALID, rejected.submit(WriteBatchRequest.getDefaultInstance()).get(2, TimeUnit.SECONDS).status());
            rejected.close();
            assertEquals("A validation rejection changes no state", 0, flushes.get());
            var failed = IndexTestSupport.executor(partial);
            assertEquals(IndexStatus.UNAVAILABLE, failed.submit(IndexServerTest.put(7, 1, 70)).get(2, TimeUnit.SECONDS).status());
            failed.close();
            assertEquals(1, flushes.get());
        }
    }

    @Test(timeout = 10000)
    public void closeWaitsForRunningBackendEvenWhenItsResponseIsCancelled() throws Exception {
        var entered = new java.util.concurrent.CountDownLatch(1);
        var release = new java.util.concurrent.CountDownLatch(1);
        var pool = java.util.concurrent.Executors.newSingleThreadExecutor();
        try (var backend = VersionedIndexBackend.create(temporary.getRoot().toPath().resolve("close"), ValueKind.ROW_ID)) {
            var blocked = (IndexBackend) java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[] {IndexBackend.class}, (proxy, method, args) -> {
                        if (method.getName().equals("writeAsync")) {
                            entered.countDown();
                            // Native work may not stop when its Java worker is interrupted.
                            boolean interrupted = false;
                            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                            while (release.getCount() != 0) {
                                try {
                                    if (!release.await(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS))
                                        throw new IOException("release timeout");
                                } catch (InterruptedException ignored) { interrupted = true; }
                            }
                            if (interrupted) Thread.currentThread().interrupt();
                        }
                        try { return method.invoke(backend, args); }
                        catch (java.lang.reflect.InvocationTargetException error) { throw error.getCause(); }
                    });
            try (var executor = IndexTestSupport.executor(blocked)) {
                var written = executor.submit(IndexServerTest.put(7,0,70));
                assertTrue(entered.await(2, TimeUnit.SECONDS));
                var closed = pool.submit(() -> { executor.close(); return null; });
                assertThrows(java.util.concurrent.TimeoutException.class, () -> closed.get(100, TimeUnit.MILLISECONDS));
                release.countDown();
                assertEquals(IndexStatus.UNAVAILABLE, written.get(3, TimeUnit.SECONDS).status());
                closed.get(3, TimeUnit.SECONDS);
                assertEquals(70, IndexTestSupport.scan(backend, ScanRequest.getDefaultInstance()).get(0).asReadOnlyByteBuffer().getLong());
            }
        } finally { release.countDown(); pool.shutdownNow(); }
    }

    @Test(timeout = 10000)
    public void aFailureAfterAResultChunkStillFailsTheQuery() throws Exception {
        int rows = IndexExecution.CHUNK_RESULTS + 5;
        var fail = new java.util.concurrent.atomic.AtomicBoolean();
        var backend = new IndexTestSupport.BlockingBackend() {
            public IndexDescription describe() { return new IndexDescription("fixture",ValueKind.ROW_ID,Acknowledgement.ACK_MEMORY,false,"test",location()); }
            public void write(Mutation write) { throw new UnsupportedOperationException(); }
            public ByteString get(GetRequest get) { throw new UnsupportedOperationException(); }
            public void scan(ScanRequest scan, ValueSink sink) throws IOException {
                for (int key = 1; key <= rows; key++) {
                    if (fail.get() && key == IndexExecution.CHUNK_RESULTS + 2) throw new IOException("failed after chunk");
                    sink.accept(IndexEntry.rowId(key));
                }
            }
            public void flush() {}
            public void bind(String binding) {}
            public String location() { return "literal-result-fixture"; }
            public void close() {}
        };
        try (var executor = IndexTestSupport.executor(backend)) {
            var request = ScanRequest.newBuilder().build();
            var operation = IndexOperationTrace.search(1, 1, 0,
                    new IndexOperationTrace.Range(null, false, null, false, false, 0), 0, rows, 0);
            var chunks = new java.util.concurrent.CopyOnWriteArrayList<IndexResponse>();
            var last = executor.submit(request, chunks::add).get(3, TimeUnit.SECONDS);
            assertEquals(1, chunks.size());
            assertEquals(IndexExecution.CHUNK_RESULTS, chunks.get(0).values().size());
            assertEquals(5, last.values().size());
            var received = new TraceRequests(ValueKind.ROW_ID).receive(operation, 1);
            received.add(chunks.get(0));
            received.finish(last);
            fail.set(true);
            chunks.clear();
            var failed = executor.submit(request.toBuilder().build(), chunks::add).get(3, TimeUnit.SECONDS);
            assertEquals(1, chunks.size());
            assertEquals(IndexStatus.UNAVAILABLE, failed.status());
            var rejected = new TraceRequests(ValueKind.ROW_ID).receive(operation, 2);
            rejected.add(chunks.get(0));
            assertThrows(IOException.class, () -> rejected.finish(failed));
        }
    }
}
