package org.sibench.adapters;

import com.google.protobuf.Message;
import org.sibench.adapters.proto.*;
import org.sibench.replay.ReplayTarget;

import java.util.concurrent.CompletableFuture;

/** Clients of an in-process executor, with the same asynchronous execution as gRPC. */
public final class InProcessService {
    private InProcessService() {}

    /** An executor completes responses after backend execution. */
    public static ReplayTarget.Factory factory(
            IndexExecution.Executor executor, TraceRequests requests) {
        return clientId -> new ServiceClient(requests) {
            @Override
            CompletableFuture<IndexResponse> transmit(
                    Message request, java.util.function.Consumer<IndexResponse> partial) {
                String method;
                if (request instanceof PutRequest) method = "Put";
                else if (request instanceof DeleteRequest) method = "Delete";
                else if (request instanceof GetRequest) method = "Get";
                else if (request instanceof ScanRequest) method = "Scan";
                else if (request instanceof WriteBatchRequest) method = "WriteBatch";
                else throw new IllegalArgumentException("Expected a data operation");
                requests.rpcRequest(request, method);
                return executor.submit(request, chunk -> { requests.rpcResults(chunk.values()); partial.accept(chunk); })
                        .thenApply(response -> { requests.rpcResults(response.values()); return response; });
            }
        };
    }
}
