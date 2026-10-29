package org.sibench.adapters;

import org.sibench.adapters.proto.Request;
import org.sibench.adapters.proto.Response;
import org.sibench.capture.IndexOperationTrace.Operation;
import org.sibench.replay.ReplayTarget;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * One benchmark client of an index service. Operations become protocol requests in send order;
 * the members of a search group (consecutive in the trace, one source access, always on one
 * client) are held until the last one arrives and travel as one group request, whose response
 * completes every member. Baseline INSERTs may likewise travel as write batches: a batch is sent
 * when full, when the source work unit changes, before any other request and on {@link #flush()}.
 */
abstract class ServiceClient implements ReplayTarget {
    private final TraceRequests requests;
    private final List<Operation> group = new ArrayList<>();
    private final List<CompletableFuture<Void>> groupResponses = new ArrayList<>();
    private final List<Operation> batch = new ArrayList<>();
    private final List<CompletableFuture<Void>> batchResponses = new ArrayList<>();
    private long batchId;

    ServiceClient(TraceRequests requests) {
        this.requests = requests;
    }

    /**
     * Hands the request to the transport; the future completes with its final response, after
     * every earlier chunk of a large result went to {@code partial}.
     */
    abstract CompletableFuture<Response> transmit(
            Request request, java.util.function.Consumer<Response> partial) throws IOException;

    private static final java.util.function.Consumer<Response> NO_CHUNKS =
            chunk -> {
                throw new IllegalStateException("Chunked response to a request without results");
            };

    @Override
    public final void flush() throws IOException {
        sendBatch();
    }

    /** Sends the held Baseline INSERTs as one request whose response completes each of them. */
    private void sendBatch() throws IOException {
        if (batch.isEmpty()) return;
        var inserts = List.copyOf(batch);
        var responses = List.copyOf(batchResponses);
        batch.clear();
        batchResponses.clear();
        transmit(requests.batch(batchId, inserts), NO_CHUNKS)
                .whenComplete(
                        (result, error) -> {
                            Throwable failure = error;
                            if (failure == null) {
                                try {
                                    requests.checkBatch(result);
                                } catch (IOException mismatch) {
                                    failure = mismatch;
                                }
                            }
                            for (var member : responses)
                                if (failure == null) member.complete(null);
                                else member.completeExceptionally(failure);
                        });
    }

    @Override
    public final SendHandle sendCommit(long dispatchSequence, long transactionId, long version)
            throws IOException {
        sendBatch();
        if (!group.isEmpty()) throw new IOException("Commit marker inside a search group");
        var answer = transmit(requests.commit(dispatchSequence, transactionId, version), NO_CHUNKS);
        CompletableFuture<Void> response =
                answer.thenApply(
                        result -> {
                            try {
                                requests.checkCommit(result);
                                return null;
                            } catch (IOException mismatch) {
                                throw new java.util.concurrent.CompletionException(mismatch);
                            }
                        });
        return new SendHandle(CompletableFuture.completedFuture(null), response);
    }

    @Override
    public final SendHandle send(long dispatchSequence, Operation operation) throws IOException {
        int limit = requests.baselineBatch();
        if (limit > 1 && TraceRequests.baselineInsert(operation)) {
            if (!batch.isEmpty() && batch.get(0).workUnitId() != operation.workUnitId()) sendBatch();
            var response = new CompletableFuture<Void>();
            batch.add(operation);
            batchResponses.add(response);
            // The request takes the dispatch id of its last insert, unique on this stream.
            batchId = dispatchSequence;
            if (batch.size() == limit) sendBatch();
            return new SendHandle(CompletableFuture.completedFuture(null), response);
        }
        sendBatch();
        if (operation.grouped()) {
            group.add(operation);
            var response = new CompletableFuture<Void>();
            groupResponses.add(response);
            // Held members are sent with the last one; nothing else can come between them.
            if (group.size() < operation.expansionSize())
                return new SendHandle(CompletableFuture.completedFuture(null), response);
            var members = List.copyOf(group);
            var responses = List.copyOf(groupResponses);
            group.clear();
            groupResponses.clear();
            var request = requests.group(dispatchSequence, members);
            var received = requests.receive(operation, request.getId());
            var answer = transmit(request, received.partial());
            answer.whenComplete((result, error) -> {
                Throwable failure = error;
                if (failure == null) {
                    try {
                        received.finish(result);
                    } catch (IOException mismatch) {
                        failure = mismatch;
                    }
                }
                for (var member : responses)
                    if (failure == null) member.complete(null);
                    else member.completeExceptionally(failure);
            });
            return new SendHandle(CompletableFuture.completedFuture(null), response);
        }
        if (!group.isEmpty()) throw new IOException("Search group interrupted on its client");
        var request = requests.request(dispatchSequence, operation);
        var received = requests.receive(operation, request.getId());
        var answer = transmit(request, received.partial());
        CompletableFuture<Void> response = answer.thenApply(result -> {
            try {
                received.finish(result);
                return null;
            } catch (IOException mismatch) {
                throw new java.util.concurrent.CompletionException(mismatch);
            }
        });
        return new SendHandle(CompletableFuture.completedFuture(null), response);
    }
}
