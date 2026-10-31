package org.sibench.adapters;

import static org.junit.Assert.*;

import com.google.protobuf.ByteString;

import io.grpc.ManagedChannelBuilder;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.adapters.proto.DescribeRequest;
import org.sibench.adapters.proto.EntryKey;
import org.sibench.adapters.proto.IndexServiceGrpc;
import org.sibench.adapters.proto.Request;
import org.sibench.adapters.proto.Response;
import org.sibench.adapters.proto.Search;
import org.sibench.adapters.proto.Status;
import org.sibench.adapters.proto.Value;
import org.sibench.adapters.proto.ValueKind;
import org.sibench.adapters.proto.Write;
import org.sibench.capture.IndexOperationTrace;
import org.sibench.trace.IndexKey;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/** The service publishes complete commits and streams full query results. */
public class IndexServerTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test(timeout = 10000)
    public void foreignStreamCannotChangeAnAlreadyBoundIndex() throws Exception {
        try (var backend = RocksDbBackend.create(temporary.getRoot().toPath().resolve("binding"), ValueKind.ROW_ID);
                var server = IndexServer.start(0, backend)) {
            backend.bind("original");
            var row = IndexOperationTrace.write(1, IndexOperationTrace.INSERT, 1, 0,
                    IndexKey.ofInt(7), 0, 1, 0, 70);
            try (var client = new GrpcService("127.0.0.1:" + server.port(),
                    new TraceRequests(ValueKind.ROW_ID), "foreign", 2)) {
                var response = client.factory().open(0).send(1, row).response().toCompletableFuture();
                assertThrows(java.util.concurrent.ExecutionException.class, () -> response.get(3, TimeUnit.SECONDS));
            }
            assertTrue(backend.search(Search.newBuilder().setTable(1).build()).isEmpty());
        }
    }

    @Test(timeout = 10000)
    public void flushWaitsForAnAcceptedBaselineWrite() throws Exception {
        var entered = new java.util.concurrent.CountDownLatch(1);
        var release = new java.util.concurrent.CountDownLatch(1);
        var pool = java.util.concurrent.Executors.newSingleThreadExecutor();
        try (var backend = RocksDbBackend.create(temporary.getRoot().toPath().resolve("flush-boundary"), ValueKind.ROW_ID)) {
            var blocked = (IndexBackend) java.lang.reflect.Proxy.newProxyInstance(
                    getClass().getClassLoader(), new Class<?>[] {IndexBackend.class}, (proxy, method, args) -> {
                        if (method.getName().equals("write")) {
                            entered.countDown();
                            if (!release.await(5, TimeUnit.SECONDS)) throw new java.io.IOException("release timeout");
                        }
                        try { return method.invoke(backend, args); }
                        catch (java.lang.reflect.InvocationTargetException error) { throw error.getCause(); }
                    });
            try (var server = IndexServer.start(0, blocked);
                    var client = new GrpcService("127.0.0.1:" + server.port(),
                            new TraceRequests(ValueKind.ROW_ID), "flush-test", 2)) {
                var row = IndexOperationTrace.write(1, IndexOperationTrace.INSERT, 1, 0,
                        IndexKey.ofInt(7), 0, 1, 0, 70);
                var written = client.factory().open(0).send(1, row).response().toCompletableFuture();
                assertTrue(entered.await(2, TimeUnit.SECONDS));
                var flushed = pool.submit(() -> client.flush("flush-test"));
                assertThrows(java.util.concurrent.TimeoutException.class,
                        () -> flushed.get(100, TimeUnit.MILLISECONDS));
                release.countDown();
                written.get(2, TimeUnit.SECONDS);
                assertEquals(0, flushed.get(2, TimeUnit.SECONDS).getPublishedVersion());
                assertEquals(1, backend.search(Search.newBuilder().setTable(1).build()).size());
            }
        } finally {
            release.countDown();
            pool.shutdownNow();
        }
    }

    @Test(timeout = 10000)
    public void controlCallsHaveDeadlinesAndUnresponsiveDescribeTimesOut() throws Exception {
        var server = ServerBuilder.forPort(0).addService(new IndexServiceGrpc.IndexServiceImplBase() {
            @Override public void describe(org.sibench.adapters.proto.DescribeRequest request,
                    StreamObserver<org.sibench.adapters.proto.DescribeResponse> response) {}
            @Override public void flush(org.sibench.adapters.proto.FlushRequest request,
                    StreamObserver<org.sibench.adapters.proto.FlushResponse> response) {
                var deadline = io.grpc.Context.current().getDeadline();
                response.onNext(org.sibench.adapters.proto.FlushResponse.newBuilder().setIndexBinding(
                        deadline != null && deadline.timeRemaining(TimeUnit.SECONDS)
                                <= PublishedIndex.FLUSH_TIMEOUT_SECONDS ? "bounded" : "unbounded").build());
                response.onCompleted();
            }
        }).build().start();
        try (var client = new GrpcService("127.0.0.1:" + server.getPort(),
                new TraceRequests(ValueKind.ROW_ID), "deadlines", 1)) {
            var error = assertThrows(io.grpc.StatusRuntimeException.class, client::describe);
            assertEquals(io.grpc.Status.Code.DEADLINE_EXCEEDED, error.getStatus().getCode());
            assertEquals("bounded", client.flush("deadlines").getIndexBinding());
        } finally { server.shutdownNow(); }
    }

    @Test
    public void flushEvidenceMustMatchTheExpectedBindingVersionAndLocation() throws Exception {
        var correct = org.sibench.adapters.proto.FlushResponse.newBuilder()
                .setIndexBinding("trace-a").setPublishedVersion(4).setIndexLocation("index").build();
        ServiceReplay.requireFlush(correct, "trace-a", 4);
        for (var wrong : java.util.List.of(correct.toBuilder().setPublishedVersion(0).build(),
                correct.toBuilder().setIndexBinding("trace-b").build(),
                correct.toBuilder().clearIndexLocation().build()))
            assertThrows(java.io.IOException.class, () -> ServiceReplay.requireFlush(wrong, "trace-a", 4));
    }

    @Test(timeout = 10000)
    public void grpcSerializationCompletesLocalSendWithoutWaitingForAResponse() throws Exception {
        var server = ServerBuilder.forPort(0).addService(new IndexServiceGrpc.IndexServiceImplBase() {
            @Override public StreamObserver<Request> execute(StreamObserver<Response> responses) {
                return new StreamObserver<>() {
                    public void onNext(Request request) {}
                    public void onError(Throwable error) {}
                    public void onCompleted() {}
                };
            }
        }).build().start();
        try (var grpc = new GrpcService("127.0.0.1:" + server.getPort(), new TraceRequests(ValueKind.ROW_ID), "trace-a mode=rowid", 5)) {
            var target = grpc.factory().open(0);
            var operation = IndexOperationTrace.write(1, IndexOperationTrace.INSERT, 1, 0,
                    IndexKey.ofInt(7), 0, 1, 0, 70);
            var handle = target.send(1, operation);
            handle.sent().toCompletableFuture().get(5, TimeUnit.SECONDS);
            assertFalse(handle.response().toCompletableFuture().isDone());
        } finally {
            server.shutdownNow();
        }
    }

    @Test
    public void replayRequiresCompleteCommitPublication() throws Exception {
        try (var backend = RocksDbBackend.create(temporary.getRoot().toPath().resolve("capabilities"), ValueKind.ROW_ID)) {
            var description = backend.describe().toBuilder().setCompleteCommitPublication(true).build();
            var incomplete = assertThrows(java.io.IOException.class, () -> ServiceReplay.require(
                    description.toBuilder().setCompleteCommitPublication(false).build(), ValueKind.ROW_ID, temporary.getRoot().toPath()));
            assertTrue(incomplete.getMessage().contains("complete commits"));
        }
    }

    @Test(timeout = 5000)
    public void closeDoesNotWaitForAServiceThatNeverCompletesItsStream() throws Exception {
        var server = ServerBuilder.forPort(0)
                .addService(new IndexServiceGrpc.IndexServiceImplBase() {
                    @Override
                    public StreamObserver<Request> execute(StreamObserver<Response> responses) {
                        return new StreamObserver<>() {
                            public void onNext(Request request) {
                                responses.onNext(Response.newBuilder()
                                        .setId(request.getId())
                                        .setStatus(Status.OK)
                                        .build());
                            }
                            public void onError(Throwable error) {}
                            public void onCompleted() {}
                        };
                    }
                })
                .build()
                .start();
        try (var grpc = new GrpcService(
                "127.0.0.1:" + server.getPort(), new TraceRequests(ValueKind.ROW_ID), "trace-a mode=rowid", 5)) {
            var target = grpc.factory().open(0);
            var insert = IndexOperationTrace.write(
                    1, IndexOperationTrace.INSERT, 1, 0, IndexKey.ofInt(7), 0, 0, 0, 70);
            target.send(1, insert).response().toCompletableFuture().get(5, TimeUnit.SECONDS);
            long start = System.nanoTime();
            grpc.close();
            assertTrue(
                    "close waited for the service's stream completion",
                    System.nanoTime() - start < TimeUnit.SECONDS.toNanos(5));
        } finally {
            server.shutdownNow();
        }
    }

    @Test
    public void completePublicationWaitsForTheDriverCommitMarker() throws Exception {
        try (var backend = RocksDbBackend.create(temporary.getRoot().toPath().resolve("published"), ValueKind.ROW_ID);
                var server = IndexServer.start(0, backend);
                var grpc = new GrpcService("127.0.0.1:" + server.port(), new TraceRequests(ValueKind.ROW_ID), "trace-a mode=rowid", 5)) {
            assertTrue(grpc.describe().getCompleteCommitPublication());
            assertEquals("sibench.index.IndexService", IndexServiceGrpc.getServiceDescriptor().getName());
            var target = grpc.factory().open(0);
            // The target's requests must use the same request builder state as its commit marker.
            var insert =
                    IndexOperationTrace.write(
                            1, IndexOperationTrace.INSERT, 1, 0, IndexKey.ofInt(7), 0, 0, 0, 70);
            target.send(1, insert).response().toCompletableFuture().get(10, TimeUnit.SECONDS);
            var update =
                    IndexOperationTrace.write(
                                    2,
                                    IndexOperationTrace.UPSERT,
                                    1,
                                    0,
                                    IndexKey.ofInt(7),
                                    1,
                                    0,
                                    0,
                                    71)
                            .withTransaction(100, 1);
            target.send(2, update).response().toCompletableFuture().get(10, TimeUnit.SECONDS);
            var search =
                    IndexOperationTrace.search(
                            3,
                            1,
                            0,
                            new org.sibench.capture.IndexOperationTrace.Range(
                                    null, false, null, false, false, 0),
                            1,
                            1,
                            0);
            var reader = grpc.factory().open(1);
            var waited = reader.send(3, search).response().toCompletableFuture();
            Thread.sleep(150);
            assertFalse("search waits for the commit marker", waited.isDone());
            target.sendCommit(4, 100, 1).response().toCompletableFuture().get(10, TimeUnit.SECONDS);
            waited.get(10, TimeUnit.SECONDS);
            var flushed = grpc.flush("trace-a mode=rowid");
            assertEquals(1, flushed.getPublishedVersion());
            assertEquals("trace-a mode=rowid", flushed.getIndexBinding());
            assertTrue(flushed.getIndexLocation().endsWith("published"));
            assertEquals(1, grpc.flush("trace-a mode=rowid").getPublishedVersion());
            // The index holds trace-a: a Flush for another trace, or naming none, is refused.
            var other = assertThrows(io.grpc.StatusRuntimeException.class, () -> grpc.flush("trace-b mode=rowid"));
            assertTrue(other.getMessage(), other.getMessage().contains("Index holds trace-a"));
            var none = assertThrows(io.grpc.StatusRuntimeException.class, () -> grpc.flush(""));
            assertEquals(io.grpc.Status.Code.INVALID_ARGUMENT, none.getStatus().getCode());
        }
    }

    @Test(timeout = 60000)
    public void aLargeResultArrivesInChunksWhoseDigestMatchesTheReference() throws Exception {
        int rows = IndexExecution.CHUNK_RESULTS + 4_464;
        try (var backend = RocksDbBackend.create(temporary.getRoot().toPath().resolve("chunks"), ValueKind.ROW_ID);
                var server = IndexServer.start(0, backend)) {
            var channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.port()).usePlaintext().build();
            var chunks = new java.util.concurrent.ConcurrentHashMap<Long, java.util.List<Response>>();
            var done = new java.util.concurrent.ConcurrentHashMap<Long, CompletableFuture<Response>>();
            var headers = new io.grpc.Metadata();
            headers.put(GrpcService.INDEX_BINDING, "trace-a mode=rowid");
            var stream = IndexServiceGrpc.newStub(channel).withInterceptors(
                    io.grpc.stub.MetadataUtils.newAttachHeadersInterceptor(headers)).execute(new StreamObserver<Response>() {
                public void onNext(Response response) {
                    if (response.getMore())
                        chunks.computeIfAbsent(response.getId(), k -> new java.util.concurrent.CopyOnWriteArrayList<>()).add(response);
                    else done.computeIfAbsent(response.getId(), k -> new CompletableFuture<>()).complete(response);
                }

                public void onError(Throwable error) {}

                public void onCompleted() {}
            });
            for (int id = 1; id <= rows; id++) {
                done.computeIfAbsent((long) id, k -> new CompletableFuture<>());
                stream.onNext(Request.newBuilder().setId(id).setWrite(Write.newBuilder()
                        .setKind(Write.Kind.INSERT)
                        .setKey(EntryKey.newBuilder().setTable(1).setIndex(0).setPrimaryKey(ByteString.copyFrom(IndexKey.ofInt(id).bytes())))
                        .setVersion(0)
                        .setValue(Value.newBuilder().setRowId(id))).build());
            }
            for (int id = 1; id <= rows; id++)
                assertEquals(Status.OK, done.get((long) id).get(30, TimeUnit.SECONDS).getStatus());
            long searchId = rows + 1L;
            var last = done.computeIfAbsent(searchId, k -> new CompletableFuture<>());
            stream.onNext(Request.newBuilder().setId(searchId)
                    .setSearch(Search.newBuilder().setTable(1).setIndex(0).setReadVersion(0)).build());
            Response response = last.get(30, TimeUnit.SECONDS);
            var partial = chunks.getOrDefault(searchId, java.util.List.of());
            assertEquals(1, partial.size());
            assertEquals(IndexExecution.CHUNK_RESULTS, partial.get(0).getResultsCount());
            assertEquals(4_464, response.getResultsCount());

            var operation = IndexOperationTrace.search(
                    9, 1, 0, new IndexOperationTrace.Range(null, false, null, false, false, 0), 0, rows, 0);
            var received = new TraceRequests(ValueKind.ROW_ID).receive(operation, searchId);
            for (Response chunk : partial) received.add(chunk);
            received.finish(response);
            // The incremental digest of the chunks equals the reference replay's digest of the rows.
            var lookups = new java.util.ArrayList<org.sibench.capture.IndexReferenceReplay.LookupResult>();
            for (int id = 1; id <= rows; id++)
                lookups.add(new org.sibench.capture.IndexReferenceReplay.LookupResult(IndexKey.ofInt(id), 0, id));
            var digest = new org.sibench.replay.QueryDigest.Incremental(java.util.List.of(operation), rows, true);
            for (Response chunk : partial) chunk.getResultsList().forEach(digest::add);
            response.getResultsList().forEach(digest::add);
            assertEquals(org.sibench.replay.QueryDigest.of(java.util.List.of(operation), lookups, true), digest.hex());

            // A changed result, a lost chunk and a chunk of another request are all refused.
            var tampered = partial.get(0).toBuilder();
            tampered.setResults(7, tampered.getResults(7).toBuilder().setValue(Value.newBuilder().setRowId(-1)));
            var changed = new org.sibench.replay.QueryDigest.Incremental(java.util.List.of(operation), rows, true);
            tampered.build().getResultsList().forEach(changed::add);
            response.getResultsList().forEach(changed::add);
            assertNotEquals(digest.hex(), changed.hex());
            var lost = new TraceRequests(ValueKind.ROW_ID).receive(operation, searchId);
            assertThrows(java.io.IOException.class, () -> lost.finish(response));
            var foreign = new TraceRequests(ValueKind.ROW_ID).receive(operation, searchId + 1);
            assertThrows(java.io.IOException.class, () -> foreign.add(partial.get(0)));
            stream.onCompleted();
            channel.shutdownNow();
        }
    }

    @Test
    public void writeBatchesLoadBaselineInsertsAndRefuseAnyOtherWriteWhole() throws Exception {
        try (var backend = RocksDbBackend.create(temporary.getRoot().toPath().resolve("batch"), ValueKind.ROW_ID)) {
            assertTrue(backend.describe().getBaselineWriteBatches());
            var insert = org.sibench.adapters.proto.Write.newBuilder()
                    .setKind(org.sibench.adapters.proto.Write.Kind.INSERT)
                    .setVersion(0)
                    .setValue(org.sibench.adapters.proto.Value.newBuilder().setRowId(5));
            var batch = org.sibench.adapters.proto.WriteBatch.newBuilder();
            for (long key = 1; key <= 3; key++)
                batch.addWrites(insert.clone().setKey(org.sibench.adapters.proto.EntryKey.newBuilder()
                        .setTable(1).setIndex(0).setPrimaryKey(com.google.protobuf.ByteString.copyFrom(IndexKey.ofInt(key).bytes()))));
            var ok = IndexExecution.execute(backend, Request.newBuilder().setId(1).setBatch(batch).build());
            assertEquals(Status.OK, ok.getStatus());
            var all = Search.newBuilder().setTable(1).setIndex(0).build();
            assertEquals(3, backend.search(all).size());
            for (var badKey : java.util.List.of(EntryKey.getDefaultInstance(),
                    EntryKey.newBuilder().setTable(1).build(),
                    EntryKey.newBuilder().setTable(1).setPrimaryKey(ByteString.copyFromUtf8("key"))
                            .setIndexedValue(ByteString.copyFromUtf8("unexpected")).build())) {
                var malformed = batch.clone().clearWrites().addWrites(batch.getWrites(0).toBuilder().setKey(
                        EntryKey.newBuilder().setTable(1).setPrimaryKey(ByteString.copyFrom(IndexKey.ofInt(9).bytes()))))
                        .addWrites(insert.clone().setKey(badKey));
                var rejected = IndexExecution.execute(backend, Request.newBuilder().setBatch(malformed).build());
                assertEquals(Status.INVALID, rejected.getStatus());
                assertEquals(3, backend.search(all).size());
            }
            // A batch with a workload write is refused before any of it is applied.
            var mixed = batch.clone().clearWrites()
                    .addWrites(insert.clone().setKey(org.sibench.adapters.proto.EntryKey.newBuilder()
                            .setTable(1).setIndex(0).setPrimaryKey(com.google.protobuf.ByteString.copyFrom(IndexKey.ofInt(9).bytes()))))
                    .addWrites(insert.clone().setKind(org.sibench.adapters.proto.Write.Kind.UPSERT).setVersion(1)
                            .setKey(org.sibench.adapters.proto.EntryKey.newBuilder()
                                    .setTable(1).setIndex(0).setPrimaryKey(com.google.protobuf.ByteString.copyFrom(IndexKey.ofInt(10).bytes()))));
            var refused = IndexExecution.execute(backend, Request.newBuilder().setId(2).setBatch(mixed).build());
            assertEquals(Status.INVALID, refused.getStatus());
            assertEquals(3, backend.search(all).size());
        }
    }
}
