package org.sibench.adapters;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import org.sibench.adapters.proto.Mutation;
import org.sibench.adapters.proto.PutRequest;
import org.sibench.adapters.proto.TraceOperation;
import org.sibench.adapters.proto.WriteBatchRequest;
import org.sibench.capture.IndexOperationTrace.Operation;
import org.sibench.replay.ReplayTarget;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;

/**
 * Forwards stored Trace requests. Read-group calls retain individual business completion times;
 * their combined validation gates completion. Only baseline writes are batched.
 */
abstract class ServiceClient implements ReplayTarget {
    private final TraceRequests requests;
    private final List<CompletableFuture<List<ByteString>>> groupResults = new ArrayList<>();
    private TraceOperation groupRequest;
    private CompletableFuture<Void> groupResponse;
    private final List<PutRequest> batch = new ArrayList<>();
    private long batchWorkUnit;
    private CompletableFuture<Void> batchResponse;

    private static final Consumer<IndexResponse> NO_CHUNKS = chunk -> {
        throw new IllegalStateException("Chunked response to a request without results");
    };

    ServiceClient(TraceRequests requests) {
        this.requests = requests;
    }

    /** Delivers every partial chunk before completing with the final response. */
    abstract CompletableFuture<IndexResponse> transmit(
            Message request, Consumer<IndexResponse> partial) throws IOException;

    /** The last request reached its local transport; in-process targets execute immediately. */
    CompletableFuture<Void> localSend() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public final void flush() throws IOException {
        sendBatch();
    }

    private void sendBatch() throws IOException {
        if (batch.isEmpty()) return;
        var request = WriteBatchRequest.newBuilder();
        for (PutRequest put : batch) request.addMutations(Mutation.newBuilder().setPut(put));
        var response = batchResponse;
        batch.clear();
        batchResponse = null;
        transmit(request.build(), NO_CHUNKS).whenComplete((result, error) -> {
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
    public final SendHandle send(long dispatchSequence, Operation operation, TraceOperation stored) throws IOException {
        if (stored == null) throw new IOException("Service replay requires a stored Trace operation");
        if (operation.grouped()) {
            sendBatch();
            return sendGroup(operation, stored);
        }
        if (groupRequest != null) throw new IOException("Read group interrupted on its client");
        var request = requests.forward(stored, operation);
        int limit = requests.baselineBatch();
        if (limit > 1 && TraceRequests.baselineInsert(operation)) {
            if (!batch.isEmpty() && batchWorkUnit != operation.workUnitId()) sendBatch();
            if (batch.isEmpty()) {
                batchWorkUnit = operation.workUnitId();
                batchResponse = new CompletableFuture<>();
            }
            var response = batchResponse;
            batch.add((PutRequest) request);
            if (batch.size() == limit) sendBatch();
            return new SendHandle(localSend(), response);
        }
        sendBatch();
        var received = requests.receive(operation, dispatchSequence);
        var answer = transmit(request, received.partial());
        CompletableFuture<Void> response = answer.thenApply(result -> {
            try {
                received.finish(result);
                return null;
            } catch (IOException mismatch) {
                throw new CompletionException(mismatch);
            }
        });
        return new SendHandle(localSend(), response);
    }

    private SendHandle sendGroup(Operation operation, TraceOperation stored) throws IOException {
        if (!stored.hasReadGroup() || stored.getReadGroup().getRequestsCount() != operation.expansionSize())
            throw new IOException("Incomplete stored read group");
        if (groupRequest != null && groupRequest != stored) throw new IOException("Stored read group interrupted");
        if (operation.expansionPosition() != groupResults.size()) throw new IOException("Read group request out of order");
        if (groupRequest == null) {
            groupRequest = stored;
            groupResponse = new CompletableFuture<>();
        }
        var checked = groupResponse;
        var request = TraceRequests.read(stored.getReadGroup().getRequests(operation.expansionPosition()));
        var items = new ArrayList<ByteString>();
        var answer = transmit(request, chunk -> {
            if (!chunk.more() || chunk.status() != IndexStatus.OK)
                throw new IllegalArgumentException("Invalid read-group result chunk");
            items.addAll(chunk.values());
        }).thenApply(result -> {
            if (result.more() || result.status() != IndexStatus.OK)
                throw new CompletionException(new IOException("Read-group request failed"));
            items.addAll(result.values());
            return List.copyOf(items);
        });
        answer.whenComplete((result, error) -> {
            if (error != null) checked.completeExceptionally(error);
        });
        groupResults.add(answer);
        var sent = localSend();
        if (groupResults.size() == operation.expansionSize()) {
            var answers = List.copyOf(groupResults);
            var source = groupRequest.getReadGroup();
            requests.recordReadGroup(source.getRequestsCount());
            groupResults.clear();
            groupRequest = null;
            groupResponse = null;
            CompletableFuture.allOf(answers.toArray(CompletableFuture[]::new)).whenComplete((unused, error) -> {
                try {
                    if (error != null) {
                        checked.completeExceptionally(error);
                        return;
                    }
                    var results = TraceRequests.combine(source, answers.stream().map(CompletableFuture::join).toList());
                    requests.check(operation, IndexResponse.ok(results));
                    checked.complete(null);
                } catch (IOException | RuntimeException failure) {
                    checked.completeExceptionally(failure);
                }
            });
        }
        return new SendHandle(sent, checked, answer.thenApply(result -> null));
    }
}
