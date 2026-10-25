package org.sibench.adapters;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import org.sibench.adapters.proto.DescribeRequest;
import org.sibench.adapters.proto.DescribeResponse;
import org.sibench.adapters.proto.FlushRequest;
import org.sibench.adapters.proto.FlushResponse;
import org.sibench.adapters.proto.IndexServiceGrpc;
import org.sibench.adapters.proto.Request;
import org.sibench.adapters.proto.Response;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Serves one {@link IndexBackend} over the SIBench index protocol. Unordered, each client stream
 * executes its requests in arrival order on gRPC's callback for that stream, and different streams
 * run concurrently. Ordered, every request waits for its send position (Request.sequence): requests
 * that arrive early on one stream are held until every earlier position, from any stream, has
 * executed, so execution follows the driver's send order rather than network arrival.
 */
public final class IndexServer implements AutoCloseable {
    private final Server server;

    private IndexServer(Server server) {
        this.server = server;
    }

    public static IndexServer start(int port, IndexBackend backend, boolean ordered) throws IOException {
        var sequencer = ordered ? new Sequencer(backend) : null;
        var service = new IndexServiceGrpc.IndexServiceImplBase() {
            @Override
            public void describe(DescribeRequest request, StreamObserver<DescribeResponse> response) {
                response.onNext(backend.describe().toBuilder().setCrossClientOrdering(ordered).build());
                response.onCompleted();
            }

            @Override
            public void flush(FlushRequest request, StreamObserver<FlushResponse> response) {
                try {
                    backend.flush();
                    response.onNext(FlushResponse.getDefaultInstance());
                    response.onCompleted();
                } catch (IOException | RuntimeException error) {
                    response.onError(io.grpc.Status.UNAVAILABLE.withDescription(String.valueOf(error)).asRuntimeException());
                }
            }

            @Override
            public StreamObserver<Request> execute(StreamObserver<Response> responses) {
                return new StreamObserver<>() {
                    @Override
                    public void onNext(Request request) {
                        if (sequencer != null) sequencer.submit(request, responses);
                        else responses.onNext(IndexExecution.execute(backend, request));
                    }

                    @Override
                    public void onError(Throwable error) {}

                    @Override
                    public void onCompleted() {
                        // Held requests of other streams write to this observer under the lock.
                        if (sequencer == null) responses.onCompleted();
                        else synchronized (sequencer) {
                            responses.onCompleted();
                        }
                    }
                };
            }
        };
        return new IndexServer(
                ServerBuilder.forPort(port).addService(service).maxInboundMessageSize(Integer.MAX_VALUE).build().start());
    }

    /**
     * Executes requests in send position order. Positions are contiguous from 1, so a request runs
     * once every lower position has; held requests are bounded by the driver's in-flight limit.
     * Responses are written while holding the lock, which also serializes each stream's observer.
     */
    private static final class Sequencer {
        private record Held(Request request, StreamObserver<Response> responses) {}

        private final IndexBackend backend;
        private final java.util.Map<Long, Held> held = new java.util.HashMap<>();
        private long next = 1;

        Sequencer(IndexBackend backend) {
            this.backend = backend;
        }

        synchronized void submit(Request request, StreamObserver<Response> responses) {
            long position = request.getSequence();
            if (position < next || held.containsKey(position)) {
                responses.onNext(Response.newBuilder()
                        .setId(request.getId())
                        .setStatus(org.sibench.adapters.proto.Status.INVALID)
                        .setMessage("Send position " + position + " is missing or repeated")
                        .build());
                return;
            }
            held.put(position, new Held(request, responses));
            for (Held ready; (ready = held.remove(next)) != null; next++)
                ready.responses().onNext(IndexExecution.execute(backend, ready.request()));
        }
    }

    public int port() {
        return server.getPort();
    }

    public void awaitTermination() throws InterruptedException {
        server.awaitTermination();
    }

    @Override
    public void close() throws IOException {
        server.shutdown();
        try {
            if (!server.awaitTermination(30, TimeUnit.SECONDS)) server.shutdownNow();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            server.shutdownNow();
        }
    }
}
