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

    @Test
    public void replayRejectsServicesWithoutTheCurrentPublicationProtocol() throws Exception {
        try (var backend = RocksDbBackend.create(temporary.getRoot().toPath().resolve("capabilities"), ValueKind.ROW_ID)) {
            var description = backend.describe().toBuilder().setCompleteCommitPublication(true).build();
            var old = assertThrows(java.io.IOException.class, () -> ServiceReplay.require(
                    description.toBuilder().setProtocolVersion(1).build(), ValueKind.ROW_ID, temporary.getRoot().toPath()));
            assertTrue(old.getMessage().contains("Unsupported protocol version"));
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
                "127.0.0.1:" + server.getPort(), new TraceRequests(ValueKind.ROW_ID))) {
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
                var grpc = new GrpcService("127.0.0.1:" + server.port(), new TraceRequests(ValueKind.ROW_ID))) {
            assertTrue(grpc.describe().getCompleteCommitPublication());
            assertEquals(2, grpc.describe().getProtocolVersion());
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
            var stream = IndexServiceGrpc.newStub(channel).execute(new StreamObserver<Response>() {
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
