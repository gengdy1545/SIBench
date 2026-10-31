package org.sibench.adapters;

import org.sibench.adapters.proto.Request;
import org.sibench.adapters.proto.Response;
import org.sibench.replay.ReplayTarget;

import java.util.concurrent.CompletableFuture;

/** Clients of an in-process executor, with the same asynchronous publication as gRPC. */
public final class InProcessService {
    private InProcessService() {}

    /** A published executor answers later: a read may wait for a commit marker not yet sent. */
    public static ReplayTarget.Factory factory(
            IndexExecution.Executor executor, TraceRequests requests) {
        return clientId -> new ServiceClient(requests) {
            @Override
            CompletableFuture<Response> transmit(
                    Request request, java.util.function.Consumer<Response> partial) {
                return executor.submit(request, partial);
            }
        };
    }
}
