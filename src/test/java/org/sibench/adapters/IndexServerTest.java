package org.sibench.adapters;

import static org.junit.Assert.*;

import com.google.protobuf.ByteString;

import io.grpc.ManagedChannelBuilder;
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

/** An ordered server executes by send position, not by the stream a request arrives on. */
public class IndexServerTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    private static final class Stream {
        final io.grpc.ManagedChannel channel;
        final StreamObserver<Request> requests;
        final java.util.Map<Long, CompletableFuture<Response>> responses = new java.util.concurrent.ConcurrentHashMap<>();

        Stream(int port) {
            channel = ManagedChannelBuilder.forAddress("127.0.0.1", port).usePlaintext().build();
            requests = IndexServiceGrpc.newStub(channel).execute(new StreamObserver<>() {
                public void onNext(Response response) {
                    responses.computeIfAbsent(response.getId(), k -> new CompletableFuture<>()).complete(response);
                }

                public void onError(Throwable error) {}

                public void onCompleted() {}
            });
        }

        CompletableFuture<Response> send(Request request) {
            var response = responses.computeIfAbsent(request.getId(), k -> new CompletableFuture<>());
            requests.onNext(request);
            return response;
        }

        void close() {
            requests.onCompleted();
            channel.shutdownNow();
        }
    }

    private static Request write(long sequence) throws Exception {
        return Request.newBuilder().setId(sequence).setSequence(sequence).setWrite(Write.newBuilder()
                .setKind(Write.Kind.UPSERT)
                .setKey(EntryKey.newBuilder().setTable(1).setIndex(0).setPrimaryKey(ByteString.copyFrom(IndexKey.ofInt(7).bytes())))
                .setVersion(1)
                .setValue(Value.newBuilder().setRowId(70))).build();
    }

    private static Request search(long sequence) {
        return Request.newBuilder().setId(sequence).setSequence(sequence).setSearch(Search.newBuilder()
                .setTable(1).setIndex(0).setReadVersion(1)).build();
    }

    @Test
    public void laterPositionsWaitForEarlierOnesFromOtherStreams() throws Exception {
        try (var backend = RocksDbBackend.create(temporary.getRoot().toPath().resolve("ordered"), ValueKind.ROW_ID);
                var server = IndexServer.start(0, backend, true)) {
            var channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.port()).usePlaintext().build();
            var description = IndexServiceGrpc.newBlockingStub(channel).describe(DescribeRequest.getDefaultInstance());
            assertTrue(description.getCrossClientOrdering());
            assertTrue(description.getTransactionLocalReads());
            assertTrue(description.getFlushMakesDurable());
            assertEquals(org.sibench.adapters.proto.Acknowledgement.ACK_WAL, description.getWriteAck());
            channel.shutdownNow();
            var reader = new Stream(server.port());
            var writer = new Stream(server.port());
            // The search (position 2) arrives first, on its own stream; it must not run yet.
            var found = reader.send(search(2));
            Thread.sleep(200);
            assertFalse(found.isDone());
            assertEquals(Status.OK, writer.send(write(1)).get(10, TimeUnit.SECONDS).getStatus());
            Response response = found.get(10, TimeUnit.SECONDS);
            assertEquals(Status.OK, response.getStatus());
            assertEquals(1, response.getResultsCount());
            assertEquals(70, response.getResults(0).getValue().getRowId());
            // A position that already ran is refused rather than executed twice.
            assertEquals(Status.INVALID, writer.send(write(1).toBuilder().setId(9).build()).get(10, TimeUnit.SECONDS).getStatus());
            reader.close();
            writer.close();
        }
    }

    @Test
    public void completePublicationWaitsForTheDriverCommitMarker() throws Exception {
        try (var backend = RocksDbBackend.create(temporary.getRoot().toPath().resolve("published"), ValueKind.ROW_ID);
                var server = IndexServer.start(0, backend, false, true);
                var grpc = new GrpcService("127.0.0.1:" + server.port(), new TraceRequests(ValueKind.ROW_ID))) {
            assertTrue(grpc.describe().getCompleteCommitPublication());
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
            var waited = target.send(3, search).response().toCompletableFuture();
            Thread.sleep(150);
            assertFalse("search waits for the commit marker", waited.isDone());
            target.sendCommit(4, 100, 1).response().toCompletableFuture().get(10, TimeUnit.SECONDS);
            waited.get(10, TimeUnit.SECONDS);
            assertEquals(1, grpc.flush());
        }
    }

    @Test
    public void unorderedServersRunEachStreamAsItArrives() throws Exception {
        try (var backend = RocksDbBackend.create(temporary.getRoot().toPath().resolve("unordered"), ValueKind.ROW_ID);
                var server = IndexServer.start(0, backend, false)) {
            var stream = new Stream(server.port());
            // Positions are not awaited: the search runs before the write and finds nothing.
            assertEquals(0, stream.send(search(2)).get(10, TimeUnit.SECONDS).getResultsCount());
            assertEquals(Status.OK, stream.send(write(1)).get(10, TimeUnit.SECONDS).getStatus());
            stream.close();
        }
    }
}
