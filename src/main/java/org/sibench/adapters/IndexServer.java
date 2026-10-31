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

/** Serves complete-commit publication, transaction-local order and versioned reads over gRPC. */
public final class IndexServer implements AutoCloseable {
    private final Server server;
    private final PublishedIndex published;

    private IndexServer(Server server, PublishedIndex published) {
        this.server = server;
        this.published = published;
    }

    public static IndexServer start(int port, IndexBackend backend) throws IOException {
        var published = new PublishedIndex(backend);
        var service = new IndexServiceGrpc.IndexServiceImplBase() {
            @Override
            public void describe(DescribeRequest request, StreamObserver<DescribeResponse> response) {
                response.onNext(
                        backend.describe().toBuilder()
                                .setCompleteCommitPublication(true)
                                .build());
                response.onCompleted();
            }

            @Override
            public void flush(FlushRequest request, StreamObserver<FlushResponse> response) {
                try {
                    backend.bind(request.getIndexBinding());
                    long version = published.flush();
                    response.onNext(
                            FlushResponse.newBuilder()
                                    .setPublishedVersion(version)
                                    .setIndexBinding(request.getIndexBinding())
                                    .setIndexLocation(backend.location())
                                    .build());
                    response.onCompleted();
                } catch (IllegalArgumentException invalid) {
                    response.onError(io.grpc.Status.INVALID_ARGUMENT.withDescription(invalid.getMessage()).asRuntimeException());
                } catch (IOException | RuntimeException error) {
                    response.onError(io.grpc.Status.UNAVAILABLE.withDescription(String.valueOf(error)).asRuntimeException());
                }
            }

            @Override
            public StreamObserver<Request> execute(StreamObserver<Response> responses) {
                var publicationPending = new java.util.concurrent.atomic.AtomicInteger();
                // Responses respect gRPC flow control: a sender waits while the client has not
                // read earlier messages, so chunks of large results are not buffered unboundedly.
                var call = (io.grpc.stub.ServerCallStreamObserver<Response>) responses;
                var ready = new Object();
                call.setOnReadyHandler(() -> {
                    synchronized (ready) {
                        ready.notifyAll();
                    }
                });
                java.util.function.Consumer<Response> send = response -> {
                    synchronized (ready) {
                        while (!call.isReady() && !call.isCancelled()) {
                            try {
                                ready.wait(10);
                            } catch (InterruptedException interrupted) {
                                // Shutting down: send now, so the client fails fast, not by timeout.
                                Thread.currentThread().interrupt();
                                break;
                            }
                        }
                    }
                    synchronized (responses) {
                        responses.onNext(response);
                    }
                };
                return new StreamObserver<>() {
                    @Override
                    public void onNext(Request request) {
                        publicationPending.incrementAndGet();
                        published.submit(request, send).thenAccept(response -> {
                            try {
                                send.accept(response);
                            } finally {
                                if (publicationPending.decrementAndGet() == 0)
                                    synchronized (publicationPending) {
                                        publicationPending.notifyAll();
                                    }
                            }
                        });
                    }

                    @Override
                    public void onError(Throwable error) {}

                    @Override
                    public void onCompleted() {
                        synchronized (publicationPending) {
                            while (publicationPending.get() != 0) {
                                try {
                                    publicationPending.wait(1000);
                                } catch (InterruptedException interrupted) {
                                    Thread.currentThread().interrupt();
                                    break;
                                }
                            }
                        }
                        synchronized (responses) {
                            responses.onCompleted();
                        }
                    }
                };
            }
        };
        var bound = io.grpc.ServerInterceptors.intercept(service, new io.grpc.ServerInterceptor() {
            @Override
            public <Q, S> io.grpc.ServerCall.Listener<Q> interceptCall(
                    io.grpc.ServerCall<Q, S> call, io.grpc.Metadata headers,
                    io.grpc.ServerCallHandler<Q, S> next) {
                if (call.getMethodDescriptor().getFullMethodName()
                        .equals(IndexServiceGrpc.getExecuteMethod().getFullMethodName())) {
                    try {
                        backend.bind(headers.get(GrpcService.INDEX_BINDING));
                    } catch (IOException | RuntimeException error) {
                        call.close(io.grpc.Status.FAILED_PRECONDITION
                                .withDescription(error.getMessage()), new io.grpc.Metadata());
                        return new io.grpc.ServerCall.Listener<>() {};
                    }
                }
                return next.startCall(call, headers);
            }
        });
        return new IndexServer(
                ServerBuilder.forPort(port).addService(bound).maxInboundMessageSize(Integer.MAX_VALUE).build().start(),
                published);
    }

    public int port() {
        return server.getPort();
    }

    public void awaitTermination() throws InterruptedException {
        server.awaitTermination();
    }

    @Override
    public void close() throws IOException {
        published.close();
        server.shutdown();
        try {
            if (!server.awaitTermination(30, TimeUnit.SECONDS)) server.shutdownNow();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            server.shutdownNow();
        }
    }
}
