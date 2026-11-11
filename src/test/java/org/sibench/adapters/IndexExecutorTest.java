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

    @Test(timeout=10000)
    public void persistenceRunsWithoutAClientFlushAndItsFailureRejectsFurtherCalls() throws Exception {
        var flushed=new java.util.concurrent.CountDownLatch(1);
        try(var backend=VersionedIndexBackend.create(temporary.getRoot().toPath().resolve("maintenance"),ValueKind.ROW_ID)) {
            var failing=(IndexBackend)java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{IndexBackend.class},(proxy,method,args)->{
                if(method.getName().equals("flush")) { flushed.countDown(); throw new IOException("storage unavailable"); }
                try { return method.invoke(backend,args); } catch(java.lang.reflect.InvocationTargetException error) { throw error.getCause(); }
            });
            var executor=new IndexExecutor(failing,20);
            var write=IndexServerTest.put(7,1,70);
            assertEquals(IndexStatus.OK,executor.submit(write).get(2,TimeUnit.SECONDS).status());
            assertTrue(flushed.await(2,TimeUnit.SECONDS));
            var error=assertThrows(IOException.class,executor::close);
            assertTrue(error.getMessage().contains("Background persistence"));
            assertEquals("storage unavailable",error.getCause().getMessage());
            assertEquals(IndexStatus.UNAVAILABLE,executor.submit(write).get(2,TimeUnit.SECONDS).status());
        }
    }

    @Test(timeout = 10000)
    public void flushCannotCertifyAnAcceptedFailedRequest() throws Exception {
        try (var backend = VersionedIndexBackend.create(temporary.getRoot().toPath().resolve("failed"), ValueKind.ROW_ID);
                var executor = new IndexExecutor(backend)) {
            var invalid = PutRequest.newBuilder().setTs(-1).build();
            assertEquals(IndexStatus.INVALID, executor.submit(invalid).get(3, TimeUnit.SECONDS).status());
            assertThrows(IOException.class, executor::flush);
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
                        if (method.getName().equals("write")) {
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
            try (var executor = new IndexExecutor(blocked)) {
                var written = executor.submit(IndexServerTest.put(7,0,70));
                assertTrue(entered.await(2, TimeUnit.SECONDS));
                var closed = pool.submit(() -> { executor.close(); return null; });
                assertThrows(java.util.concurrent.TimeoutException.class, () -> closed.get(100, TimeUnit.MILLISECONDS));
                release.countDown();
                assertEquals(IndexStatus.UNAVAILABLE, written.get(3, TimeUnit.SECONDS).status());
                closed.get(3, TimeUnit.SECONDS);
                assertEquals(70, backend.scan(ScanRequest.getDefaultInstance()).get(0).asReadOnlyByteBuffer().getLong());
            }
        } finally { release.countDown(); pool.shutdownNow(); }
    }

    @Test(timeout = 10000)
    public void aFailureAfterAResultChunkStillFailsTheQueryAndFlush() throws Exception {
        int rows = IndexExecution.CHUNK_RESULTS + 5;
        var fail = new java.util.concurrent.atomic.AtomicBoolean();
        var backend = new IndexBackend() {
            public IndexDescription describe() { return new IndexDescription("fixture",ValueKind.ROW_ID,Acknowledgement.ACK_MEMORY,"test",location()); }
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
        try (var executor = new IndexExecutor(backend)) {
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
            assertThrows(IOException.class, executor::flush);
        }
    }
}
