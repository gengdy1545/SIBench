package org.sibench.adapters;

import static org.junit.Assert.*;

import com.google.protobuf.ByteString;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.adapters.proto.Commit;
import org.sibench.adapters.proto.EntryKey;
import org.sibench.adapters.proto.Request;
import org.sibench.adapters.proto.Response;
import org.sibench.adapters.proto.Search;
import org.sibench.adapters.proto.Status;
import org.sibench.adapters.proto.Value;
import org.sibench.adapters.proto.ValueKind;
import org.sibench.adapters.proto.Write;
import org.sibench.trace.IndexKey;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class PublishedIndexTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    private static final class Pool implements AutoCloseable {
        final java.util.concurrent.ExecutorService executor = Executors.newFixedThreadPool(2);

        <T> java.util.concurrent.Future<T> submit(java.util.concurrent.Callable<T> task) {
            return executor.submit(task);
        }

        <T> java.util.concurrent.Future<T> submitWaiting(java.util.concurrent.Callable<T> task)
                throws Exception {
            var future = submitObserved(task);
            assertFalse("request completed instead of waiting", future.isDone());
            return future;
        }

        <T> java.util.concurrent.Future<T> submitObserved(java.util.concurrent.Callable<T> task)
                throws Exception {
            var thread = new java.util.concurrent.atomic.AtomicReference<Thread>();
            var future = submit(() -> {
                thread.set(Thread.currentThread());
                return task.call();
            });
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            while (System.nanoTime() < deadline) {
                Thread worker = thread.get();
                if (worker != null && worker.getState() == Thread.State.WAITING) {
                    return future;
                }
                if (future.isDone()) return future;
                Thread.sleep(1);
            }
            throw new AssertionError("request did not reach its wait within 3 seconds");
        }

        @Override
        public void close() throws InterruptedException {
            executor.shutdownNow();
            assertTrue("request threads leaked", executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    private static EntryKey key(long id) {
        return EntryKey.newBuilder()
                .setTable(1)
                .setIndex(0)
                .setPrimaryKey(ByteString.copyFrom(IndexKey.ofInt(id).bytes()))
                .build();
    }

    private static Request insert(long sequence, long id, long rowId) {
        return Request.newBuilder()
                .setId(sequence)
                .setWrite(
                        Write.newBuilder()
                                .setKind(Write.Kind.INSERT)
                                .setKey(key(id))
                                .setVersion(0)
                                .setValue(Value.newBuilder().setRowId(rowId)))
                .build();
    }

    private static Request write(
            long sequence, long transaction, long operation, long id, long version, long rowId) {
        return Request.newBuilder()
                .setId(sequence)
                .setTransactionId(transaction)
                .setTransactionOperation(operation)
                .setWrite(
                        Write.newBuilder()
                                .setKind(Write.Kind.UPSERT)
                                .setKey(key(id))
                                .setVersion(version)
                                .setValue(Value.newBuilder().setRowId(rowId)))
                .build();
    }

    private static Request search(long sequence, long readVersion) {
        return Request.newBuilder()
                .setId(sequence)
                .setSearch(Search.newBuilder().setTable(1).setIndex(0).setReadVersion(readVersion))
                .build();
    }

    private static Request localSearch(
            long sequence,
            long transaction,
            long operation,
            long readVersion,
            long localVersion) {
        return Request.newBuilder()
                .setId(sequence)
                .setTransactionId(transaction)
                .setTransactionOperation(operation)
                .setSearch(
                        Search.newBuilder()
                                .setTable(1)
                                .setIndex(0)
                                .setReadVersion(readVersion)
                                .setLocalVersion(localVersion))
                .build();
    }

    private static Request commit(
            long sequence, long transaction, long version, long writes, long operations) {
        return Request.newBuilder()
                .setId(sequence)
                .setTransactionId(transaction)
                .setTransactionOperation(0)
                .setCommit(
                        Commit.newBuilder()
                                .setVersion(version)
                                .setWrites(writes)
                                .setOperations(operations))
                .build();
    }

    private static Response execute(PublishedIndex published, Request request) {
        return published.submit(request).join();
    }

    private RocksDbBackend backend() throws IOException {
        return RocksDbBackend.create(
                temporary.getRoot().toPath().resolve("db-" + System.nanoTime()), ValueKind.ROW_ID);
    }

    @Test
    public void readWaitsForTheCompleteCommitMarkerNotForTheHighestSeenVersion() throws Exception {
        try (var backend = backend();
                var published = new PublishedIndex(backend);
                var pool = new Pool()) {
            assertEquals(Status.OK, execute(published, insert(1, 1, 1)).getStatus());
            assertEquals(Status.OK, execute(published, write(2, 100, 1, 1, 1, 2)).getStatus());
            var read = pool.submitWaiting(() -> execute(published, search(3, 1)));
            assertFalse("search must wait for version 1's commit", read.isDone());

            assertEquals(Status.OK, execute(published, commit(4, 100, 1, 1, 1)).getStatus());
            var response = read.get(5, TimeUnit.SECONDS);
            assertEquals(Status.OK, response.getStatus());
            assertEquals(1, response.getResultsCount());
            assertEquals(2, response.getResults(0).getValue().getRowId());
            assertEquals(1, published.publishedVersion());
        }
    }

    @Test
    public void transactionOperationsExecuteInPositionOrderWhenTheyArriveOutOfOrder()
            throws Exception {
        try (var backend = backend();
                var published = new PublishedIndex(backend);
                var pool = new Pool()) {
            assertEquals(Status.OK, execute(published, insert(1, 1, 1)).getStatus());
            var second = pool.submitWaiting(() -> execute(published, write(2, 200, 2, 2, 1, 22)));
            assertFalse("operation 2 must wait for operation 1", second.isDone());
            assertEquals(Status.OK, execute(published, write(3, 200, 1, 1, 1, 11)).getStatus());
            assertEquals(Status.OK, second.get(5, TimeUnit.SECONDS).getStatus());
            assertEquals(Status.OK, execute(published, commit(4, 200, 1, 2, 2)).getStatus());

            var response = execute(published, search(5, 1));
            assertEquals(2, response.getResultsCount());
        }
    }

    @Test
    public void localReadSeesThePrefixBeforeItAndNotLaterWrites() throws Exception {
        try (var backend = backend();
                var published = new PublishedIndex(backend);
                var pool = new Pool()) {
            assertEquals(Status.OK, execute(published, insert(1, 1, 1)).getStatus());
            assertEquals(Status.OK, execute(published, write(2, 300, 1, 1, 1, 2)).getStatus());
            // Later write arrives first; lane order must let the intervening read see value 2.
            var later = pool.submitWaiting(() -> execute(published, write(4, 300, 3, 1, 1, 3)));
            var local = pool.submit(() -> execute(published, localSearch(3, 300, 2, 0, 1)));
            var response = local.get(5, TimeUnit.SECONDS);
            assertEquals(Status.OK, response.getStatus());
            assertEquals(1, response.getResultsCount());
            assertEquals(2, response.getResults(0).getValue().getRowId());
            assertEquals(Status.OK, later.get(5, TimeUnit.SECONDS).getStatus());
            assertEquals(Status.OK, execute(published, commit(5, 300, 1, 2, 3)).getStatus());
        }
    }

    @Test
    public void independentTransactionsCanRunWhileAnotherTransactionBlocks() throws Exception {
        try (var delegate = backend();
                var pool = new Pool()) {
            var entered = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            var blocked = new java.util.concurrent.atomic.AtomicBoolean();
            var backend =
                    new IndexBackend() {
                        @Override
                        public org.sibench.adapters.proto.DescribeResponse describe() {
                            return delegate.describe();
                        }

                        @Override
                        public void write(Write write) throws IOException {
                            if (write.getVersion() == 1 && blocked.compareAndSet(false, true)) {
                                entered.countDown();
                                try {
                                    if (!release.await(5, TimeUnit.SECONDS))
                                        throw new IOException("release timeout");
                                } catch (InterruptedException interrupted) {
                                    Thread.currentThread().interrupt();
                                    throw new IOException(interrupted);
                                }
                            }
                            delegate.write(write);
                        }

                        @Override
                        public void search(Search search, ResultSink sink) throws IOException {
                            delegate.search(search, sink);
                        }

                        @Override
                        public void flush() throws IOException {
                            delegate.flush();
                        }

                        @Override
                        public long publishedVersion() throws IOException {
                            return delegate.publishedVersion();
                        }


                        @Override
                        public void bind(String binding) throws IOException {
                            delegate.bind(binding);
                        }

                        @Override
                        public String location() {
                            return delegate.location();
                        }

                        @Override
                        public void recordPublishedVersion(long version) throws IOException {
                            delegate.recordPublishedVersion(version);
                        }

                        @Override
                        public void close() {}
                    };
            try (var published = new PublishedIndex(backend)) {
                assertEquals(Status.OK, execute(published, insert(1, 1, 1)).getStatus());
                var first = pool.submit(() -> execute(published, write(2, 400, 1, 1, 1, 11)));
                assertTrue(entered.await(2, TimeUnit.SECONDS));
                var second = pool.submit(() -> execute(published, write(3, 401, 1, 2, 2, 22)));
                // Transaction 401 is not blocked by 400's backend stall.
                assertEquals(Status.OK, second.get(2, TimeUnit.SECONDS).getStatus());
                release.countDown();
                assertEquals(Status.OK, first.get(5, TimeUnit.SECONDS).getStatus());
                assertEquals(Status.OK, execute(published, commit(4, 400, 1, 1, 1)).getStatus());
                assertEquals(Status.OK, execute(published, commit(5, 401, 2, 1, 1)).getStatus());
            }
        }
    }

    @Test
    public void closingWhileAReadWaitsFailsTheWaitAndDoesNotPublish() throws Exception {
        try (var backend = backend();
                var published = new PublishedIndex(backend);
                var pool = new Pool()) {
            assertEquals(Status.OK, execute(published, insert(1, 1, 1)).getStatus());
            assertEquals(Status.OK, execute(published, write(2, 500, 1, 1, 1, 2)).getStatus());
            var read = pool.submitWaiting(() -> execute(published, search(3, 1)));
            assertFalse(read.isDone());
            published.close();
            var response = read.get(5, TimeUnit.SECONDS);
            assertEquals(Status.UNAVAILABLE, response.getStatus());
            assertEquals(0, published.publishedVersion());
        }
    }

    @Test(timeout = 10000)
    public void parkedOperationRunsWithoutItsSubmitterAndPublishes() throws Exception {
        try (var delegate = backend()) {
            var entered = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            var blocked = new java.util.concurrent.atomic.AtomicBoolean();
            var backend = new IndexBackend() {
                @Override
                public org.sibench.adapters.proto.DescribeResponse describe() {
                    return delegate.describe();
                }

                @Override
                public void write(Write write) throws IOException {
                    if (write.getVersion() == 1 && blocked.compareAndSet(false, true)) {
                        entered.countDown();
                        try {
                            if (!release.await(5, TimeUnit.SECONDS))
                                throw new IOException("release timeout");
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            throw new IOException(interrupted);
                        }
                    }
                    delegate.write(write);
                }

                @Override
                public void search(Search search, ResultSink sink) throws IOException {
                    delegate.search(search, sink);
                }

                @Override
                public void flush() throws IOException {
                    delegate.flush();
                }

                @Override
                public long publishedVersion() throws IOException {
                    return delegate.publishedVersion();
                }


                @Override
                public void bind(String binding) throws IOException {
                    delegate.bind(binding);
                }

                @Override
                public String location() {
                    return delegate.location();
                }

                @Override
                public void recordPublishedVersion(long version) throws IOException {
                    delegate.recordPublishedVersion(version);
                }

                @Override
                public void close() {}
            };
            try (var published = new PublishedIndex(backend)) {
                assertEquals(Status.OK, execute(published, insert(1, 1, 1)).getStatus());
                var first = published.submit(write(2, 1000, 1, 1, 1, 11));
                assertTrue(entered.await(2, TimeUnit.SECONDS));
                // Operation 2 and the marker arrive while operation 1 is in the backend; nobody
                // waits on them, the lane must still run operation 2 and publish the version.
                var second = published.submit(write(3, 1000, 2, 2, 1, 22));
                var marker = published.submit(commit(4, 1000, 1, 2, 2));
                assertFalse(second.isDone());
                assertFalse(marker.isDone());
                release.countDown();
                assertEquals(Status.OK, marker.get(5, TimeUnit.SECONDS).getStatus());
                assertEquals(Status.OK, second.get(1, TimeUnit.SECONDS).getStatus());
                assertEquals(Status.OK, first.get(1, TimeUnit.SECONDS).getStatus());
                assertEquals(1, published.publishedVersion());
                assertEquals(2, execute(published, search(5, 1)).getResultsCount());
            }
        }
    }

    @Test(timeout = 10000)
    public void commitArrivingBeforeOperationsWaitsForEveryDeclaredWrite() throws Exception {
        try (var backend = backend(); var published = new PublishedIndex(backend); var pool = new Pool()) {
            var marker = pool.submitWaiting(() -> execute(published, commit(1, 600, 1, 2, 2)));
            assertEquals(Status.OK, execute(published, write(2, 600, 1, 1, 1, 11)).getStatus());
            assertEquals(0, published.publishedVersion());
            assertFalse(marker.isDone());
            assertEquals(Status.OK, execute(published, write(3, 600, 2, 2, 1, 22)).getStatus());
            assertEquals(Status.OK, marker.get(3, TimeUnit.SECONDS).getStatus());
            var read = execute(published, search(4, 1));
            assertEquals(Status.OK, read.getStatus());
            assertEquals(List.of(11L, 22L), read.getResultsList().stream()
                    .map(r -> r.getValue().getRowId()).toList());
        }
    }

    @Test(timeout = 10000)
    public void laterCompleteVersionCannotPublishAcrossAMissingCommit() throws Exception {
        try (var backend = backend(); var published = new PublishedIndex(backend); var pool = new Pool()) {
            assertEquals(Status.OK, execute(published, write(1, 702, 1, 2, 2, 22)).getStatus());
            var marker = pool.submitWaiting(() -> execute(published, commit(2, 702, 2, 1, 1)));
            assertEquals(0, published.publishedVersion());
            var read = pool.submitWaiting(() -> execute(published, search(3, 2)));
            assertEquals(Status.OK, execute(published, write(4, 701, 1, 1, 1, 11)).getStatus());
            assertEquals(Status.OK, execute(published, commit(5, 701, 1, 1, 1)).getStatus());
            assertEquals(Status.OK, marker.get(3, TimeUnit.SECONDS).getStatus());
            assertEquals(2, published.publishedVersion());
            assertEquals(List.of(11L, 22L), read.get(3, TimeUnit.SECONDS).getResultsList().stream()
                    .map(r -> r.getValue().getRowId()).toList());
        }
    }

    @Test(timeout = 10000)
    public void rejectedLateWriteCannotChangeAnAlreadyPublishedSnapshot() throws Exception {
        try (var backend = backend(); var published = new PublishedIndex(backend)) {
            assertEquals(Status.OK, execute(published, write(1, 800, 1, 1, 1, 11)).getStatus());
            assertEquals(Status.OK, execute(published, commit(2, 800, 1, 1, 1)).getStatus());
            var before = execute(published, search(3, 1));
            assertEquals(11, before.getResults(0).getValue().getRowId());
            // The lane has been retired. A stale operation must fail before touching RocksDB.
            var rejected = execute(published, write(4, 800, 1, 1, 1, 99));
            assertNotEquals(Status.OK, rejected.getStatus());
            var after = execute(published, search(5, 1));
            assertEquals(Status.OK, after.getStatus());
            assertEquals("rejected write mutated the published version", before.getResultsList(), after.getResultsList());
        }
    }

    @Test(timeout = 10000)
    public void lateWriteUnderAnotherTransactionCannotReuseAPublishedVersion() throws Exception {
        try (var backend = backend(); var published = new PublishedIndex(backend)) {
            assertEquals(Status.OK, execute(published, write(1, 810, 1, 1, 1, 11)).getStatus());
            assertEquals(Status.OK, execute(published, commit(2, 810, 1, 1, 1)).getStatus());
            var rejected = execute(published, write(3, 811, 1, 1, 1, 99));
            assertNotEquals(Status.OK, rejected.getStatus());
            assertEquals(11, execute(published, search(4, 1)).getResults(0).getValue().getRowId());
        }
    }

    @Test(timeout = 10000)
    public void duplicatePendingPositionCannotReplaceTheOriginalWrite() throws Exception {
        try (var backend = backend(); var published = new PublishedIndex(backend); var pool = new Pool()) {
            var original = pool.submitWaiting(() -> execute(published, write(1, 900, 2, 2, 1, 22)));
            var duplicate = pool.submitObserved(() -> execute(published, write(2, 900, 2, 2, 1, 99)));
            assertEquals(Status.OK, execute(published, write(3, 900, 1, 1, 1, 11)).getStatus());
            assertEquals(Status.OK, original.get(3, TimeUnit.SECONDS).getStatus());
            assertNotEquals("duplicate position accepted", Status.OK, duplicate.get(3, TimeUnit.SECONDS).getStatus());
            assertEquals(Status.OK, execute(published, commit(4, 900, 1, 2, 2)).getStatus());
            assertEquals(List.of(11L, 22L), execute(published, search(5, 1)).getResultsList().stream()
                    .map(r -> r.getValue().getRowId()).toList());
        }
    }

    @Test(timeout = 30000)
    public void completedTransactionLanesAreReleasedButLateOperationsStayRefused() throws Exception {
        int transactions = 5_000;
        try (var backend = backend(); var published = new PublishedIndex(backend)) {
            assertEquals(Status.OK, execute(published, insert(1, 1, 1)).getStatus());
            long sequence = 1;
            for (int transaction = 1; transaction <= transactions; transaction++) {
                assertEquals(
                        Status.OK,
                        published.submit(
                                        write(
                                                ++sequence,
                                                transaction,
                                                1,
                                                1,
                                                transaction,
                                                transaction + 1))
                                .join()
                                .getStatus());
                assertEquals(
                        Status.OK,
                        published.submit(commit(++sequence, transaction, transaction, 1, 1))
                                .join()
                                .getStatus());
            }
            assertEquals("completed lanes were retained", 0, published.laneCount());
            assertEquals(transactions, published.publishedVersion());
            assertNotEquals(
                    Status.OK,
                    published.submit(
                                    write(
                                            ++sequence,
                                            transactions,
                                            1,
                                            1,
                                            transactions,
                                            transactions + 2))
                            .join()
                            .getStatus());
            assertEquals(
                    transactions + 1,
                    execute(published, search(++sequence, transactions)).getResults(0).getValue().getRowId());
        }
    }

    @Test(timeout = 10000)
    public void reopenedIndexResumesItsRecordedWatermark() throws Exception {
        java.nio.file.Path path = temporary.getRoot().toPath().resolve("resumed");
        try (var backend = RocksDbBackend.create(path, ValueKind.ROW_ID);
                var published = new PublishedIndex(backend)) {
            assertEquals(Status.OK, execute(published, insert(1, 1, 1)).getStatus());
            assertEquals(Status.OK, execute(published, write(2, 1100, 1, 1, 1, 11)).getStatus());
            assertEquals(Status.OK, execute(published, commit(3, 1100, 1, 1, 1)).getStatus());
            assertEquals(1, published.flush());
        }
        try (var backend = RocksDbBackend.open(path, ValueKind.ROW_ID);
                var published = new PublishedIndex(backend)) {
            assertEquals(1, published.publishedVersion());
            // A read of the recorded version runs at once; a late write to it is still refused.
            assertEquals(11, execute(published, search(4, 1)).getResults(0).getValue().getRowId());
            assertNotEquals(Status.OK, execute(published, write(5, 1101, 1, 1, 1, 99)).getStatus());
            assertEquals(11, execute(published, search(6, 1)).getResults(0).getValue().getRowId());
        }
    }

    @Test(timeout = 60000)
    public void aLargeSearchIsAnsweredInChunksAndAFailureAfterAChunkFailsTheQuery() throws Exception {
        int rows = IndexExecution.CHUNK_RESULTS + 5;
        var operation = org.sibench.capture.IndexOperationTrace.search(
                3, 1, 0, new org.sibench.capture.IndexOperationTrace.Range(null, false, null, false, false, 0), 1, rows, 0);
        try (var delegate = backend()) {
            var failAfter = new java.util.concurrent.atomic.AtomicInteger(-1);
            var backend = new IndexBackend() {
                @Override
                public org.sibench.adapters.proto.DescribeResponse describe() {
                    return delegate.describe();
                }

                @Override
                public void write(Write write) throws IOException {
                    delegate.write(write);
                }

                @Override
                public void search(Search search, ResultSink sink) throws IOException {
                    int[] seen = {0};
                    delegate.search(search, result -> {
                        if (seen[0]++ == failAfter.get()) throw new IOException("backend failed mid-search");
                        sink.accept(result);
                    });
                }

                @Override
                public void flush() throws IOException {
                    delegate.flush();
                }

                @Override
                public long publishedVersion() throws IOException {
                    return delegate.publishedVersion();
                }


                @Override
                public void bind(String binding) throws IOException {
                    delegate.bind(binding);
                }

                @Override
                public String location() {
                    return delegate.location();
                }

                @Override
                public void recordPublishedVersion(long version) throws IOException {
                    delegate.recordPublishedVersion(version);
                }

                @Override
                public void close() {}
            };
            try (var published = new PublishedIndex(backend)) {
                for (int id = 1; id <= rows; id++)
                    assertEquals(Status.OK, execute(published, insert(id, id, id)).getStatus());
                assertEquals(Status.OK, execute(published, write(rows + 1, 1200, 1, 1, 1, 1)).getStatus());
                assertEquals(Status.OK, execute(published, commit(rows + 2, 1200, 1, 1, 1)).getStatus());

                var chunks = new java.util.concurrent.CopyOnWriteArrayList<Response>();
                Response last = published.submit(search(rows + 3, 1), chunks::add).get(30, TimeUnit.SECONDS);
                assertEquals(1, chunks.size());
                assertTrue(chunks.get(0).getMore());
                assertEquals(IndexExecution.CHUNK_RESULTS, chunks.get(0).getResultsCount());
                assertEquals(5, last.getResultsCount());
                var received = new TraceRequests(ValueKind.ROW_ID).receive(operation, rows + 3);
                for (Response chunk : chunks) received.add(chunk);
                received.finish(last);

                failAfter.set(IndexExecution.CHUNK_RESULTS + 1);
                var failedChunks = new java.util.concurrent.CopyOnWriteArrayList<Response>();
                Response failed = published.submit(search(rows + 4, 1), failedChunks::add).get(30, TimeUnit.SECONDS);
                assertEquals(1, failedChunks.size());
                assertEquals(Status.UNAVAILABLE, failed.getStatus());
                var refused = new TraceRequests(ValueKind.ROW_ID).receive(operation, rows + 4);
                for (Response chunk : failedChunks) refused.add(chunk);
                assertThrows(IOException.class, () -> refused.finish(failed));
            }
        }
    }
}
