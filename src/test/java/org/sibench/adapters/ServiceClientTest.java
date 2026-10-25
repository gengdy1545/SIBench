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
    /** Records what reaches the transport; the backend answers with fixed counts. */
    private static final class Recorder extends ServiceClient {
        final List<Request> sent = new ArrayList<>();
        long results;

        Recorder(TraceRequests requests) {
            super(requests);
        }

        @Override
        java.util.concurrent.CompletableFuture<org.sibench.adapters.proto.Response> transmit(Request request) {
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
}
