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
                if (request.hasBatch()) requests.rpcRequest(IndexRpc.batch(request.getBatch()), "BatchWrite");
                else if (request.hasWrite()) {
                    var write = request.getWrite();
                    if (write.getKind() == org.sibench.adapters.proto.Write.Kind.LOGICAL_DELETE)
                        requests.rpcRequest(IndexRpc.delete(write), "Delete");
                    else requests.rpcRequest(IndexRpc.put(write), "Put");
                } else {
                    var get = IndexRpc.get(request.getSearch());
                    if (get != null) requests.rpcRequest(get, "Get");
                    else requests.rpcRequest(IndexRpc.scan(request.getSearch()), "Scan");
                }
                return executor.submit(request, chunk -> { requests.rpcResults(chunk.getResultsList()); partial.accept(chunk); })
                        .thenApply(response -> { requests.rpcResults(response.getResultsList()); return response; });
            }
        };
    }
}
