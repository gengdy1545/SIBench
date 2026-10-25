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
        return clientId -> new ServiceClient(requests) {
            @Override
            CompletableFuture<Response> transmit(Request request) {
                return CompletableFuture.completedFuture(IndexExecution.execute(backend, request));
            }
        };
    }
}
