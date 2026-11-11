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
 * One replay client. Group members use individual calls and retain their business completion
 * times; client-side union/intersection gates their validation. Only baseline writes are buffered
 * into batches, sent when full, at a work-unit change, before other calls and on flush().
 */
abstract class ServiceClient implements ReplayTarget {
    private final TraceRequests requests;
    private final List<Operation> group = new ArrayList<>();
    private final List<CompletableFuture<java.util.List<org.sibench.adapters.proto.Result>>> groupResults = new ArrayList<>();
    private CompletableFuture<Void> groupResponse;
    private final List<Operation> batch = new ArrayList<>();
    private final List<org.sibench.adapters.proto.PutRequest> batchWrites = new ArrayList<>();
    private Request groupRequest;
    private CompletableFuture<Void> batchResponse;

    ServiceClient(TraceRequests requests) {
        this.requests = requests;
    }

    /**
     * Hands the request to the transport; the future completes with its final response, after
     * every earlier chunk of a large result went to {@code partial}.
     */
    abstract CompletableFuture<Response> transmit(
            Request request, java.util.function.Consumer<Response> partial) throws IOException;

    /** The last actual request reached its local transport; in-process targets execute immediately. */
    CompletableFuture<Void> localSend() {
        return CompletableFuture.completedFuture(null);
    }

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
        var response = batchResponse;
        var request = requests.batch(inserts, batchWrites);
        batch.clear();
        batchWrites.clear();
        batchResponse = null;
        transmit(request, NO_CHUNKS)
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
                            if (failure == null) response.complete(null);
                            else response.completeExceptionally(failure);
                        });
    }

    @Override
    public final SendHandle send(long dispatchSequence, Operation operation) throws IOException {
        return send(dispatchSequence, operation, null);
    }

    @Override
    public final SendHandle send(long dispatchSequence, Operation operation, Request stored) throws IOException {
        int limit = requests.baselineBatch();
        if (limit > 1 && TraceRequests.baselineInsert(operation)) {
            if (!batch.isEmpty() && batch.get(0).workUnitId() != operation.workUnitId()) sendBatch();
            if (batch.isEmpty()) batchResponse = new CompletableFuture<>();
            var response = batchResponse;
            batch.add(operation);
            if (stored != null) {
                if (!stored.hasPut()) throw new IOException("Missing stored Baseline write");
                batchWrites.add(stored.getPut());
            }
            if (batch.size() == limit) sendBatch();
            return new SendHandle(localSend(), response);
        }
        sendBatch();
        if (operation.grouped()) {
            if (group.isEmpty()) { groupRequest = stored; groupResponse = new CompletableFuture<>(); }
            else if (groupRequest != stored) throw new IOException("Stored search group interrupted");
            if (operation.expansionPosition() != group.size()) throw new IOException("Search group member out of order");
            group.add(operation);
            var checked = groupResponse;
            var request = stored == null ? requests.query(operation)
                    : TraceRequests.read(stored.getGroup().getMembers(operation.expansionPosition()));
            var items = new ArrayList<org.sibench.adapters.proto.Result>();
            var answer = transmit(request, chunk -> {
                if (!chunk.getMore()
                        || chunk.getStatus() != org.sibench.adapters.proto.Status.OK)
                    throw new IllegalArgumentException("Invalid group member result chunk");
                items.addAll(chunk.getResultsList());
            }).thenApply(result -> {
                if (result.getMore()
                        || result.getStatus() != org.sibench.adapters.proto.Status.OK)
                    throw new java.util.concurrent.CompletionException(new IOException("Group member request failed"));
                items.addAll(result.getResultsList());
                return List.copyOf(items);
            });
            answer.whenComplete((result,error) -> { if (error != null) checked.completeExceptionally(error); });
            groupResults.add(answer);
            var sent = localSend();
            if (group.size() == operation.expansionSize()) {
                var members = List.copyOf(group);
                var answers = List.copyOf(groupResults);
                var source = groupRequest == null ? requests.group(members)
                        : requests.forward(groupRequest, members);
                group.clear(); groupResults.clear(); groupRequest = null;
                CompletableFuture.allOf(answers.toArray(CompletableFuture[]::new)).whenComplete((unused,error) -> {
                    try {
                        if (error != null) { checked.completeExceptionally(error); return; }
                        var results = TraceRequests.combine(source.getGroup(), answers.stream().map(CompletableFuture::join).toList());
                        requests.check(operation, org.sibench.adapters.proto.Response.newBuilder()
                                .setStatus(org.sibench.adapters.proto.Status.OK).addAllResults(results).build());
                        checked.complete(null);
                    } catch (IOException | RuntimeException failure) { checked.completeExceptionally(failure); }
                });
            }
            return new SendHandle(sent, checked, answer.thenApply(result -> null));
        }
        if (!group.isEmpty()) throw new IOException("Search group interrupted on its client");
        var request = stored == null ? requests.request(operation)
                : requests.forward(stored, List.of(operation));
        var received = requests.receive(operation, dispatchSequence);
        var answer = transmit(request, received.partial());
        CompletableFuture<Void> response = answer.thenApply(result -> {
            try {
                received.finish(result);
                return null;
            } catch (IOException mismatch) {
                throw new java.util.concurrent.CompletionException(mismatch);
            }
        });
        return new SendHandle(localSend(), response);
    }
}
