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
 * completes every member.
 */
abstract class ServiceClient implements ReplayTarget {
    private final TraceRequests requests;
    private final List<Operation> group = new ArrayList<>();
    private final List<CompletableFuture<Void>> groupResponses = new ArrayList<>();

    ServiceClient(TraceRequests requests) {
        this.requests = requests;
    }

    /** Hands the request to the transport; the future completes with its response. */
    abstract CompletableFuture<Response> transmit(Request request) throws IOException;

    @Override
    public final SendHandle sendCommit(long dispatchSequence, long transactionId, long version)
            throws IOException {
        if (!group.isEmpty()) throw new IOException("Commit marker inside a search group");
        var answer = transmit(requests.commit(dispatchSequence, transactionId, version));
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
            var answer = transmit(requests.group(dispatchSequence, members));
            answer.whenComplete((result, error) -> {
                Throwable failure = error;
                if (failure == null) {
                    try {
                        requests.check(operation, result);
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
        var answer = transmit(requests.request(dispatchSequence, operation));
        CompletableFuture<Void> response = answer.thenApply(result -> {
            try {
                requests.check(operation, result);
                return null;
            } catch (IOException mismatch) {
                throw new java.util.concurrent.CompletionException(mismatch);
            }
        });
        return new SendHandle(CompletableFuture.completedFuture(null), response);
    }
}
