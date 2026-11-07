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

/** Failure and shutdown boundaries remain required without transaction publication. */
public class IndexExecutorTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test(timeout = 10000)
    public void flushCannotCertifyAnAcceptedFailedRequest() throws Exception {
        try (var backend = VersionedIndexBackend.create(temporary.getRoot().toPath().resolve("failed"), ValueKind.ROW_ID);
                var executor = new IndexExecutor(backend)) {
            var invalid = Request.newBuilder().setId(1).setWrite(Write.newBuilder()
                    .setKind(Write.Kind.INSERT).setKey(EntryKey.getDefaultInstance())).build();
            assertEquals(Status.INVALID, executor.submit(invalid).get(3, TimeUnit.SECONDS).getStatus());
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
                var written = executor.submit(Request.newBuilder().setId(1).setWrite(Write.newBuilder()
                        .setKind(Write.Kind.INSERT).setKey(EntryKey.newBuilder().setTable(1)
                                .setPrimaryKey(ByteString.copyFrom(IndexKey.ofInt(7).bytes())))
                        .setValue(Value.newBuilder().setRowId(70))).build());
                assertTrue(entered.await(2, TimeUnit.SECONDS));
                var closed = pool.submit(() -> { executor.close(); return null; });
                assertThrows(java.util.concurrent.TimeoutException.class, () -> closed.get(100, TimeUnit.MILLISECONDS));
                release.countDown();
                assertEquals(Status.UNAVAILABLE, written.get(3, TimeUnit.SECONDS).getStatus());
                closed.get(3, TimeUnit.SECONDS);
                assertEquals(70, backend.search(Search.newBuilder().setTable(1).build()).get(0).getValue().getRowId());
            }
        } finally { release.countDown(); pool.shutdownNow(); }
    }

    @Test(timeout = 10000)
    public void aFailureAfterAResultChunkStillFailsTheQueryAndFlush() throws Exception {
        int rows = IndexExecution.CHUNK_RESULTS + 5;
        var fail = new java.util.concurrent.atomic.AtomicBoolean();
        var backend = new IndexBackend() {
            public DescribeResponse describe() { return DescribeResponse.getDefaultInstance(); }
            public void write(Write write) { throw new UnsupportedOperationException(); }
            public void search(Search search, ResultSink sink) throws IOException {
                for (int key = 1; key <= rows; key++) {
                    if (fail.get() && key == IndexExecution.CHUNK_RESULTS + 2) throw new IOException("failed after chunk");
                    sink.accept(Result.newBuilder().setPrimaryKey(ByteString.copyFrom(IndexKey.ofInt(key).bytes()))
                            .setValue(Value.newBuilder().setRowId(key)).build());
                }
            }
            public void flush() {}
            public void bind(String binding) {}
            public String location() { return "literal-result-fixture"; }
            public void close() {}
        };
        try (var executor = new IndexExecutor(backend)) {
            var request = Request.newBuilder().setId(1).setSearch(Search.newBuilder().setTable(1)).build();
            var operation = IndexOperationTrace.search(1, 1, 0,
                    new IndexOperationTrace.Range(null, false, null, false, false, 0), 0, rows, 0);
            var chunks = new java.util.concurrent.CopyOnWriteArrayList<Response>();
            var last = executor.submit(request, chunks::add).get(3, TimeUnit.SECONDS);
            assertEquals(1, chunks.size());
            assertEquals(IndexExecution.CHUNK_RESULTS, chunks.get(0).getResultsCount());
            assertEquals(5, last.getResultsCount());
            var received = new TraceRequests(ValueKind.ROW_ID).receive(operation, 1);
            received.add(chunks.get(0));
            received.finish(last);
            fail.set(true);
            chunks.clear();
            var failed = executor.submit(request.toBuilder().setId(2).build(), chunks::add).get(3, TimeUnit.SECONDS);
            assertEquals(1, chunks.size());
            assertEquals(Status.UNAVAILABLE, failed.getStatus());
            var rejected = new TraceRequests(ValueKind.ROW_ID).receive(operation, 2);
            rejected.add(chunks.get(0));
            assertThrows(IOException.class, () -> rejected.finish(failed));
            assertThrows(IOException.class, executor::flush);
        }
    }
}
