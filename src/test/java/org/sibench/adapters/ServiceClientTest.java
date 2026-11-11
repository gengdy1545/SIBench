package org.sibench.adapters;

import static org.junit.Assert.*;
import static org.sibench.adapters.TraceRequestsTest.send;

import org.junit.Test;
import com.google.protobuf.Message;
import org.sibench.adapters.proto.*;
import org.sibench.capture.IndexOperationTrace;
import org.sibench.capture.IndexOperationTrace.Operation;
import org.sibench.capture.IndexOperationTrace.Range;
import org.sibench.trace.IndexKey;
import org.sibench.trace.IndexEntry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Each group member travels independently, and the combined native count is checked once. */
public class ServiceClientTest {
    @Test
    public void sendWaitsForTheTransportSignalEvenWhenTheResponseIsImmediate() throws Exception {
        var sent = new java.util.concurrent.CompletableFuture<Void>();
        var client = new ServiceClient(new TraceRequests(ValueKind.ROW_ID)) {
            @Override java.util.concurrent.CompletableFuture<IndexResponse> transmit(
                    Message request, java.util.function.Consumer<IndexResponse> partial) {
                return java.util.concurrent.CompletableFuture.completedFuture(
                        IndexResponse.ok(java.util.List.of()));
            }
            @Override java.util.concurrent.CompletableFuture<Void> localSend() { return sent; }
        };
        var operation = IndexOperationTrace.write(1, IndexOperationTrace.INSERT, 1, 0,
                IndexKey.ofInt(7), 0, 1, 0, 70);
        var handle = send(client, 1, operation);
        assertTrue(handle.response().toCompletableFuture().isDone());
        assertFalse(handle.sent().toCompletableFuture().isDone());
        sent.complete(null);
        handle.sent().toCompletableFuture().get();
    }

    @Test
    public void missingStoredPayloadIsRejectedBeforeTransmission() throws IOException {
        var client = new Recorder(new TraceRequests(ValueKind.ROW_ID));
        assertThrows(IOException.class, () -> client.send(1, insert(1, 1, 1), null));
        assertTrue(client.sent.isEmpty());
    }

    /** Records what reaches the transport; the backend answers with fixed counts. */
    private static final class Recorder extends ServiceClient {
        final List<Message> sent = new ArrayList<>();
        long results;

        Recorder(TraceRequests requests) {
            super(requests);
        }

        @Override
        java.util.concurrent.CompletableFuture<IndexResponse> transmit(
                Message request, java.util.function.Consumer<IndexResponse> partial) {
            sent.add(request);
            var values = new ArrayList<com.google.protobuf.ByteString>();
            for (int i = 0; i < results; i++)
                values.add(IndexEntry.rowId(i));
            return java.util.concurrent.CompletableFuture.completedFuture(IndexResponse.ok(values));
        }
    }

    private static Operation member(int position, int size, long count, int flags) {
        return IndexOperationTrace.search(
                10, 3, 1, Range.equal(IndexKey.ofInt(7)), null, 5, count, flags, position, size);
    }

    @Test
    public void groupMembersTravelAsIndividualRequests() throws IOException {
        var requests = new TraceRequests(ValueKind.ROW_ID);
        var client = new Recorder(requests);
        client.results = 2;
        var members = List.of(member(0, 3, 2, IndexOperationTrace.INTERSECT_GROUP),
                member(1, 3, 2, IndexOperationTrace.INTERSECT_GROUP),
                member(2, 3, 2, IndexOperationTrace.INTERSECT_GROUP));
        var stored = TraceOperation.newBuilder().setReadGroup(requests.group(members)).build();
        var first = client.send(1, members.get(0), stored);
        var middle = client.send(2, members.get(1), stored);
        assertEquals(2, client.sent.size());
        assertFalse(first.response().toCompletableFuture().isDone());
        assertTrue(first.businessResponse().toCompletableFuture().isDone());
        var last = client.send(3, members.get(2), stored);
        assertEquals(3, client.sent.size());
        assertTrue(client.sent.stream().allMatch(ScanRequest.class::isInstance));
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
        var members = List.of(member(0, 2, 4, IndexOperationTrace.UNION_GROUP),
                member(1, 2, 4, IndexOperationTrace.UNION_GROUP));
        var stored = TraceOperation.newBuilder().setReadGroup(requests.group(members)).build();
        client.send(1, members.get(0), stored);
        var last = client.send(2, members.get(1), stored);
        var failure = assertThrows(
                java.util.concurrent.CompletionException.class,
                () -> last.response().toCompletableFuture().join());
        assertTrue(failure.getCause().getMessage().contains("Search count mismatch"));
    }

    @Test
    public void anInterruptedGroupIsRefused() throws IOException {
        var requests = new TraceRequests(ValueKind.ROW_ID);
        requests.baselineBatch(2);
        var client = new Recorder(requests);
        var members = List.of(member(0, 2, 1, IndexOperationTrace.UNION_GROUP),
                member(1, 2, 1, IndexOperationTrace.UNION_GROUP));
        var stored = TraceOperation.newBuilder().setReadGroup(requests.group(members)).build();
        client.send(1, members.get(0), stored);
        var alone = IndexOperationTrace.search(11, 3, 1, Range.equal(IndexKey.ofInt(7)), 5, 1, 0);
        assertTrue(assertThrows(IOException.class, () -> send(client, 2, alone))
                .getMessage().contains("Read group interrupted"));
        assertThrows(IOException.class, () -> send(client, 3, insert(12, 1, 1)));
        assertEquals(1, client.sent.size());
    }

    @Test
    public void malformedStoredReadGroupsFailBeforeTransmission() throws IOException {
        var requests = new TraceRequests(ValueKind.ROW_ID);
        var client = new Recorder(requests);
        var members = List.of(member(0, 2, 0, IndexOperationTrace.UNION_GROUP),
                member(1, 2, 0, IndexOperationTrace.UNION_GROUP));
        var stored = TraceOperation.newBuilder().setReadGroup(requests.group(members)).build();
        for (var invalid : List.of(TraceOperation.getDefaultInstance(), stored.toBuilder()
                .setReadGroup(stored.getReadGroup().toBuilder().removeRequests(1)).build())) {
            assertThrows(IOException.class, () -> client.send(1, members.get(0), invalid));
        }
        assertThrows(IOException.class, () -> client.send(1, members.get(1), stored));
        assertTrue(client.sent.isEmpty());
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
            responses.add(send(client, key, insert(key, key, 1)).response().toCompletableFuture());
        assertEquals(1, client.sent.size());
        assertEquals(3, ((WriteBatchRequest) client.sent.get(0)).getMutationsCount());
        assertTrue(responses.stream().allMatch(java.util.concurrent.CompletableFuture::isDone));
        // A new work unit sends the held part of the previous one first.
        send(client, 4, insert(4, 4, 1));
        send(client, 5, insert(5, 5, 2));
        assertEquals(2, client.sent.size());
        assertEquals(1, ((WriteBatchRequest) client.sent.get(1)).getMutationsCount());
        // Any other request sends the held inserts before it.
        send(client, 6, IndexOperationTrace.write(6, IndexOperationTrace.UPSERT, 3, 0, IndexKey.ofInt(6), 1, 1, 6, 6));
        assertEquals(4, client.sent.size());
        assertEquals(1, ((WriteBatchRequest) client.sent.get(2)).getMutationsCount());
        assertTrue(client.sent.get(3) instanceof PutRequest);
        send(client, 7, insert(7, 7, 3));
        assertEquals(4, client.sent.size());
        client.flush();
        assertEquals(5, client.sent.size());
        assertEquals(1, ((WriteBatchRequest) client.sent.get(4)).getMutationsCount());
    }

    @Test
    public void storedPayloadsReachTheTransportWithoutReconstruction() throws Exception {
        var encoded = new TraceRequests(ValueKind.ROW_ID);
        var requests = new TraceRequests(ValueKind.ROW_ID);
        requests.baselineBatch(2);
        var client = new Recorder(requests);
        var first = insert(1, 1, 1);
        var second = insert(2, 2, 1);
        var a = TraceOperation.newBuilder().setPut((PutRequest) encoded.request(first)).build();
        var b = TraceOperation.newBuilder().setPut((PutRequest) encoded.request(second)).build();
        client.send(1, first, a);
        client.send(2, second, b).response().toCompletableFuture().join();
        assertSame(a.getPut(), ((WriteBatchRequest) client.sent.get(0)).getMutations(0).getPut());
        assertSame(b.getPut(), ((WriteBatchRequest) client.sent.get(0)).getMutations(1).getPut());
        var update = IndexOperationTrace.write(3, IndexOperationTrace.UPSERT, 3, 0, IndexKey.ofInt(1),
                1, 0, 0, 3).withWorkUnitId(2);
        var storedWrite = TraceOperation.newBuilder().setPut((PutRequest) encoded.request(update)).build();
        client.send(3, update, storedWrite).response().toCompletableFuture().join();
        assertSame(storedWrite.getPut(), client.sent.get(1));
        var members = List.of(member(0, 2, 0, IndexOperationTrace.UNION_GROUP),
                member(1, 2, 0, IndexOperationTrace.UNION_GROUP));
        var storedGroup = TraceOperation.newBuilder().setReadGroup(encoded.group(members)).build();
        client.send(4, members.get(0), storedGroup);
        client.send(5, members.get(1), storedGroup).response().toCompletableFuture().join();
        assertEquals(2, requests.maxGroupMembers());
        assertSame(storedGroup.getReadGroup().getRequests(0).getScan(), client.sent.get(2));
        assertSame(storedGroup.getReadGroup().getRequests(1).getScan(), client.sent.get(3));
        var snapshots = new TraceRequests(ValueKind.ROW_ID);
        var search = IndexOperationTrace.search(12, 3, 0, Range.equal(IndexKey.ofInt(1)), 1, 0, 0);
        var storedSearch = TraceOperation.newBuilder().setGet((GetRequest) encoded.request(search)).build();
        var snapshotClient = new Recorder(snapshots);
        snapshotClient.send(1, search, storedSearch).response().toCompletableFuture().join();
        assertSame(storedSearch.getGet(), snapshotClient.sent.get(0));
        snapshotClient.results = 1;
        var wrong = snapshotClient.send(2, search, storedSearch).response().toCompletableFuture();
        assertTrue(assertThrows(java.util.concurrent.CompletionException.class, wrong::join)
                .getCause().getMessage().contains("Search count mismatch"));
    }

    @Test
    public void baselineBatchesPreserveKeysVersionsAndSelectedValues() throws IOException {
        for (var kind : List.of(ValueKind.ROW_ID, ValueKind.LOCATOR)) {
            var requests = new TraceRequests(kind);
            requests.baselineBatch(2);
            var client = new Recorder(requests);
            send(client, 1, insert(1, 7, 1));
            send(client, 2, insert(2, 8, 1)).response().toCompletableFuture().join();
            var batch = (WriteBatchRequest) client.sent.get(0);
            assertEquals(2, batch.getMutationsCount());
            for (int i = 0; i < 2; i++) {
                var put = batch.getMutations(i).getPut();
                assertEquals(new IndexEntry(3, 0, null, IndexKey.ofInt(7 + i)).bytes(), put.getKey());
                assertEquals(0, put.getTs());
                assertEquals(kind == ValueKind.ROW_ID ? IndexEntry.rowId(7 + i)
                        : IndexEntry.locator(1, 7 + i), put.getValue());
            }
        }
    }

    @Test
    public void aFailedBatchFailsEveryInsertInIt() throws IOException {
        var requests = new TraceRequests(ValueKind.ROW_ID);
        requests.baselineBatch(2);
        var client = new ServiceClient(requests) {
            @Override
            java.util.concurrent.CompletableFuture<IndexResponse> transmit(
                    Message request, java.util.function.Consumer<IndexResponse> partial) {
                return java.util.concurrent.CompletableFuture.completedFuture(
                        IndexResponse.error(IndexStatus.UNAVAILABLE, ""));
            }
        };
        var first = send(client, 1, insert(1, 1, 1)).response().toCompletableFuture();
        var second = send(client, 2, insert(2, 2, 1)).response().toCompletableFuture();
        assertTrue(first.isCompletedExceptionally());
        assertTrue(second.isCompletedExceptionally());
    }
}
