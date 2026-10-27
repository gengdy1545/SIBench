package org.sibench.adapters;

import org.sibench.adapters.proto.Request;
import org.sibench.adapters.proto.Response;
import org.sibench.replay.ReplayTarget;

import java.util.concurrent.CompletableFuture;

/**
 * Clients that call a backend in the driver's process, on the sending thread: the cost of the
 * index itself without protocol or network, executed one request at a time in send order.
 */
public final class InProcessService {
    private InProcessService() {}

    public static ReplayTarget.Factory factory(IndexBackend backend, TraceRequests requests) {
        return factory(request -> IndexExecution.execute(backend, request), requests);
    }

    public static ReplayTarget.Factory factory(
            IndexExecution.Executor executor, TraceRequests requests) {
        return factory(executor, requests, null);
    }

    /**
     * A published in-process service must run requests off the sending thread: a read may wait
     * for a commit marker that the same single sender still has to send.
     */
    public static ReplayTarget.Factory factory(
            IndexExecution.Executor executor,
            TraceRequests requests,
            java.util.concurrent.ExecutorService background) {
        return clientId -> new ServiceClient(requests) {
            @Override
            CompletableFuture<Response> transmit(Request request) {
                if (background == null)
                    return CompletableFuture.completedFuture(executor.execute(request));
                return CompletableFuture.supplyAsync(() -> executor.execute(request), background);
            }
        };
    }
}
