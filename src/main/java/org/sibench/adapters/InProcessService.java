package org.sibench.adapters;

import org.sibench.adapters.proto.Request;
import org.sibench.adapters.proto.Response;
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
            CompletableFuture<Response> transmit(
                    Request request, java.util.function.Consumer<Response> partial) {
                switch (request.getOperationCase()) {
                    case PUT -> requests.rpcRequest(request.getPut(), "Put");
                    case DELETE -> requests.rpcRequest(request.getDelete(), "Delete");
                    case GET -> requests.rpcRequest(request.getGet(), "Get");
                    case SCAN -> requests.rpcRequest(request.getScan(), "Scan");
                    case BATCH -> requests.rpcRequest(request.getBatch(), "BatchWrite");
                    default -> throw new IllegalArgumentException("Expected a data operation");
                }
                return executor.submit(request, chunk -> { requests.rpcResults(chunk.getResultsList()); partial.accept(chunk); })
                        .thenApply(response -> { requests.rpcResults(response.getResultsList()); return response; });
            }
        };
    }
}
