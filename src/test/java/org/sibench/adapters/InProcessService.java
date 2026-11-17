package org.sibench.adapters;

import com.google.protobuf.Message;
import org.sibench.adapters.proto.*;
import org.sibench.replay.ReplayTarget;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;

/** Test clients of an in-process executor; like gRPC, a non-OK response fails its call. */
public final class InProcessService {
    private InProcessService() {}

    @FunctionalInterface
    public interface Executor {
        CompletableFuture<IndexResponse> submit(Message request, Consumer<IndexResponse> partial);
    }

    public static ReplayTarget.Factory factory(Executor executor, TraceRequests requests) {
        return clientId -> new ServiceClient(requests) {
            @Override
            CompletableFuture<IndexResponse> transmit(Message request, Consumer<IndexResponse> partial) {
                String method;
                if (request instanceof PutRequest) method = "Put";
                else if (request instanceof DeleteRequest) method = "Delete";
                else if (request instanceof GetRequest) method = "Get";
                else if (request instanceof ScanRequest) method = "Scan";
                else if (request instanceof WriteBatchRequest) method = "WriteBatch";
                else throw new IllegalArgumentException("Expected a data operation");
                requests.rpcRequest(request, method);
                return executor.submit(request, chunk -> { requests.rpcResults(chunk.values()); partial.accept(chunk); })
                        .thenApply(response -> {
                            if (response.status() != IndexStatus.OK)
                                throw new CompletionException(new IOException("Service " + response.status() + ": " + response.message()));
                            requests.rpcResults(response.values());
                            return response;
                        });
            }

            @Override
            CompletableFuture<Void> localSend() { return CompletableFuture.completedFuture(null); }
        };
    }
}
