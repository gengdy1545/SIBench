package org.sibench.adapters;

import static org.junit.Assert.*;

import org.junit.Test;
import org.sibench.adapters.proto.Request;
import org.sibench.adapters.proto.SearchGroup;
import org.sibench.adapters.proto.ValueKind;
import org.sibench.capture.IndexOperationTrace;
import org.sibench.capture.IndexOperationTrace.Operation;
import org.sibench.capture.IndexOperationTrace.Range;
import org.sibench.trace.IndexKey;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** The members of one search group travel as one request, and its count is checked once. */
public class ServiceClientTest {
    @Test
    public void sendWaitsForTheTransportSignalEvenWhenTheResponseIsImmediate() throws Exception {
        var sent = new java.util.concurrent.CompletableFuture<Void>();
        var client = new ServiceClient(new TraceRequests(org.sibench.adapters.proto.ValueKind.ROW_ID)) {
            @Override java.util.concurrent.CompletableFuture<org.sibench.adapters.proto.Response> transmit(
                    Request request, java.util.function.Consumer<org.sibench.adapters.proto.Response> partial) {
                return java.util.concurrent.CompletableFuture.completedFuture(
                        org.sibench.adapters.proto.Response.newBuilder().setId(request.getId())
                                .setStatus(org.sibench.adapters.proto.Status.OK).build());
            }
            @Override java.util.concurrent.CompletableFuture<Void> localSend() { return sent; }
        };
        var operation = IndexOperationTrace.write(1, IndexOperationTrace.INSERT, 1, 0,
                IndexKey.ofInt(7), 0, 1, 0, 70);
        var handle = client.send(1, operation);
        assertTrue(handle.response().toCompletableFuture().isDone());
        assertFalse(handle.sent().toCompletableFuture().isDone());
        sent.complete(null);
        handle.sent().toCompletableFuture().get();
    }

    /** Records what reaches the transport; the backend answers with fixed counts. */
    private static final class Recorder extends ServiceClient {
        final List<Request> sent = new ArrayList<>();
        long results;

        Recorder(TraceRequests requests) {
            super(requests);
        }

        @Override
        java.util.concurrent.CompletableFuture<org.sibench.adapters.proto.Response> transmit(
                Request request, java.util.function.Consumer<org.sibench.adapters.proto.Response> partial) {
            sent.add(request);
            var response = org.sibench.adapters.proto.Response.newBuilder()
                    .setId(request.getId())
                    .setStatus(org.sibench.adapters.proto.Status.OK);
            for (int i = 0; i < results; i++)
                response.addResults(org.sibench.adapters.proto.Result.newBuilder().setVersion(1));
            return java.util.concurrent.CompletableFuture.completedFuture(response.build());
        }
    }

    private static Operation member(int position, int size, long count, int flags) {
        return IndexOperationTrace.search(
                10, 3, 1, Range.equal(IndexKey.ofInt(7)), null, 5, count, flags, position, size);
    }

    @Test
    public void groupMembersTravelAsOneRequest() throws IOException {
        var requests = new TraceRequests(ValueKind.ROW_ID);
        var client = new Recorder(requests);
        client.results = 2;
        var first = client.send(1, member(0, 3, 2, IndexOperationTrace.INTERSECT_GROUP));
        var middle = client.send(2, member(1, 3, 2, IndexOperationTrace.INTERSECT_GROUP));
        // Held members are not sent on their own.
        assertEquals(List.of(), client.sent);
        var last = client.send(3, member(2, 3, 2, IndexOperationTrace.INTERSECT_GROUP));
        assertEquals(1, client.sent.size());
        assertEquals(3, client.sent.get(0).getGroup().getMembersCount());
        assertEquals(SearchGroup.Mode.INTERSECTION, client.sent.get(0).getGroup().getMode());
        // Every member completes with the group's response; a group is one native query.
        for (var handle : List.of(first, middle, last)) {
            handle.response().toCompletableFuture().join();
            handle.sent().toCompletableFuture().join();
        }
        assertEquals(1, requests.queries());
        assertEquals(2, requests.checkedResults());
    }

    @Test
    public void aGroupThatReturnsAnotherCountFailsEveryMember() throws IOException {
        var requests = new TraceRequests(ValueKind.ROW_ID);
        var client = new Recorder(requests);
        client.results = 1;
        client.send(1, member(0, 2, 4, IndexOperationTrace.UNION_GROUP));
        var last = client.send(2, member(1, 2, 4, IndexOperationTrace.UNION_GROUP));
        var failure = assertThrows(
                java.util.concurrent.CompletionException.class,
                () -> last.response().toCompletableFuture().join());
        assertTrue(failure.getCause().getMessage().contains("Search count mismatch"));
    }

    @Test
    public void anInterruptedGroupIsRefused() throws IOException {
        var requests = new TraceRequests(ValueKind.ROW_ID);
        var client = new Recorder(requests);
        client.send(1, member(0, 2, 1, IndexOperationTrace.UNION_GROUP));
        var alone = IndexOperationTrace.search(11, 3, 1, Range.equal(IndexKey.ofInt(7)), 5, 1, 0);
        assertTrue(assertThrows(IOException.class, () -> client.send(2, alone))
                .getMessage().contains("Search group interrupted"));
    }

    private static Operation insert(long event, long key, long unit) {
        return IndexOperationTrace.write(event, IndexOperationTrace.INSERT, 3, 0, IndexKey.ofInt(key), 0, 1, key, key)
                .withWorkUnitId(unit);
    }

    @Test
    public void baselineInsertsTravelInBatchesFlushedByUnitOtherRequestsAndFlush() throws IOException {
        var requests = new TraceRequests(ValueKind.ROW_ID);
        requests.baselineBatch(3);
        var client = new Recorder(requests);
        var responses = new ArrayList<java.util.concurrent.CompletableFuture<Void>>();
        for (long key = 1; key <= 3; key++)
            responses.add(client.send(key, insert(key, key, 1)).response().toCompletableFuture());
        assertEquals(1, client.sent.size());
        assertEquals(3, client.sent.get(0).getBatch().getWritesCount());
        assertTrue(responses.stream().allMatch(java.util.concurrent.CompletableFuture::isDone));
        // A new work unit sends the held part of the previous one first.
        client.send(4, insert(4, 4, 1));
        client.send(5, insert(5, 5, 2));
        assertEquals(2, client.sent.size());
        assertEquals(1, client.sent.get(1).getBatch().getWritesCount());
        // Any other request sends the held inserts before it.
        client.send(6, IndexOperationTrace.write(6, IndexOperationTrace.UPSERT, 3, 0, IndexKey.ofInt(6), 1, 1, 6, 6));
        assertEquals(4, client.sent.size());
        assertEquals(1, client.sent.get(2).getBatch().getWritesCount());
        assertTrue(client.sent.get(3).hasWrite());
        client.send(7, insert(7, 7, 3));
        assertEquals(4, client.sent.size());
        client.flush();
        assertEquals(5, client.sent.size());
        assertEquals(1, client.sent.get(4).getBatch().getWritesCount());
    }

    @Test
    public void storedPayloadsReachTheTransportWithoutReconstruction() throws Exception {
        var encoded = new TraceRequests(ValueKind.ROW_ID);
        var requests = new TraceRequests(ValueKind.ROW_ID);
        requests.baselineBatch(2);
        var client = new Recorder(requests);
        var first = insert(1, 1, 1);
        var second = insert(2, 2, 1);
        var a = encoded.request(0, first);
        var b = encoded.request(0, second);
        client.send(1, first, a);
        client.send(2, second, b).response().toCompletableFuture().join();
        assertSame(a.getWrite(), client.sent.get(0).getBatch().getWrites(0));
        assertSame(b.getWrite(), client.sent.get(0).getBatch().getWrites(1));
        var update = IndexOperationTrace.write(3, IndexOperationTrace.UPSERT, 3, 0, IndexKey.ofInt(1),
                1, 0, 0, 3).withTransaction(9, 1).withWorkUnitId(2);
        var storedWrite = encoded.request(0, update);
        client.send(3, update, storedWrite).response().toCompletableFuture().join();
        assertSame(storedWrite.getWrite(), client.sent.get(1).getWrite());
        var members = List.of(member(0, 2, 0, IndexOperationTrace.UNION_GROUP).withTransaction(9, 1),
                member(1, 2, 0, IndexOperationTrace.UNION_GROUP).withTransaction(9, 1));
        var storedGroup = encoded.group(0, members);
        // Source operation positions may include filtered requests; actual send counts differ.
        storedGroup = storedGroup.toBuilder().setTransactionOperation(7).build();
        client.send(4, members.get(0), storedGroup);
        client.send(5, members.get(1), storedGroup).response().toCompletableFuture().join();
        assertSame(storedGroup.getGroup(), client.sent.get(2).getGroup());
        assertEquals(5, client.sent.get(2).getId());
        assertEquals(2, client.sent.get(2).getTransactionOperation());
        client.sendCommit(6, 9, 1).response().toCompletableFuture().join();
        assertEquals(2, client.sent.get(3).getCommit().getOperations());
        assertEquals(1, client.sent.get(3).getCommit().getWrites());

        var snapshots = new TraceRequests(ValueKind.ROW_ID);
        snapshots.snapshotReads();
        var search = IndexOperationTrace.search(12, 3, 0, Range.equal(IndexKey.ofInt(1)), 1, 0, 0)
                .withTransaction(9, 0);
        var storedSearch = encoded.request(0, search);
        var snapshotClient = new Recorder(snapshots);
        snapshotClient.send(1, search, storedSearch).response().toCompletableFuture().join();
        assertSame(storedSearch.getSearch(), snapshotClient.sent.get(0).getSearch());
        assertEquals(0, snapshotClient.sent.get(0).getTransactionId());
        assertEquals(0, snapshotClient.sent.get(0).getTransactionOperation());
        snapshotClient.results = 1;
        var wrong = snapshotClient.send(2, search, storedSearch).response().toCompletableFuture();
        assertTrue(assertThrows(java.util.concurrent.CompletionException.class, wrong::join)
                .getCause().getMessage().contains("Search count mismatch"));
    }

    @Test
    public void aFailedBatchFailsEveryInsertInIt() throws IOException {
        var requests = new TraceRequests(ValueKind.ROW_ID);
        requests.baselineBatch(2);
        var client = new ServiceClient(requests) {
            @Override
            java.util.concurrent.CompletableFuture<org.sibench.adapters.proto.Response> transmit(
                    Request request, java.util.function.Consumer<org.sibench.adapters.proto.Response> partial) {
                return java.util.concurrent.CompletableFuture.completedFuture(
                        org.sibench.adapters.proto.Response.newBuilder()
                                .setId(request.getId())
                                .setStatus(org.sibench.adapters.proto.Status.UNAVAILABLE)
                                .build());
            }
        };
        var first = client.send(1, insert(1, 1, 1)).response().toCompletableFuture();
        var second = client.send(2, insert(2, 2, 1)).response().toCompletableFuture();
        assertTrue(first.isCompletedExceptionally());
        assertTrue(second.isCompletedExceptionally());
    }
}
