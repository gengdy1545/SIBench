package org.sibench.adapters;

import com.google.protobuf.Message;
import io.grpc.*;
import io.grpc.stub.*;
import org.sibench.adapters.proto.*;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/** Five versioned data RPCs; persistence belongs to the service's internal executor. */
public final class IndexServer implements AutoCloseable {
    private final Server server;
    private final IndexExecutor executor;

    private IndexServer(Server server, IndexExecutor executor) { this.server = server; this.executor = executor; }

    public static IndexServer start(int port, IndexBackend backend) throws IOException {
        var description = backend.describe();
        description.require(description.valueKind());
        byte[] metadata = new com.google.gson.Gson().toJson(description).getBytes(java.nio.charset.StandardCharsets.UTF_8);
        var executor = new IndexExecutor(backend);
        var service = new IndexServiceGrpc.IndexServiceImplBase() {
            private <T> void submit(Message request, StreamObserver<T> out, Function<IndexResponse,T> result) {
                executor.submit(request).thenAccept(response -> finish(response, out, result));
            }
            @Override public void put(PutRequest put, StreamObserver<PutResponse> out) {
                try { submit(put, out, r -> PutResponse.getDefaultInstance()); }
                catch (IllegalArgumentException error) { invalid(out, error); }
            }
            @Override public void delete(DeleteRequest delete, StreamObserver<DeleteResponse> out) {
                try { submit(delete, out, r -> DeleteResponse.getDefaultInstance()); }
                catch (IllegalArgumentException error) { invalid(out, error); }
            }
            @Override public void writeBatch(WriteBatchRequest batch, StreamObserver<WriteBatchResponse> out) {
                try {
                    submit(batch, out, r -> WriteBatchResponse.getDefaultInstance());
                } catch (IllegalArgumentException error) { invalid(out, error); }
            }
            @Override public void get(GetRequest get, StreamObserver<GetResponse> out) {
                try {
                    submit(get, out, response -> {
                        if (response.values().size() > 1) throw new IllegalArgumentException("Get returned multiple entries");
                        var result = GetResponse.newBuilder();
                        if (response.values().size() == 1) result.setValue(response.values().get(0));
                        return result.build();
                    });
                } catch (IllegalArgumentException error) { invalid(out, error); }
            }
            @Override public void scan(ScanRequest scan, StreamObserver<ScanResponse> out) {
                try {
                    var call = (ServerCallStreamObserver<ScanResponse>) out;
                    var ready = new Object();
                    call.setOnReadyHandler(() -> { synchronized (ready) { ready.notifyAll(); } });
                    java.util.function.Consumer<IndexResponse> send = response -> {
                        synchronized (ready) {
                            while (!call.isReady() && !call.isCancelled()) {
                                try { ready.wait(10); }
                                catch (InterruptedException error) {
                                    Thread.currentThread().interrupt(); throw new IllegalStateException("Scan interrupted", error);
                                }
                            }
                        }
                        if (call.isCancelled()) throw new IllegalStateException("Scan cancelled");
                        out.onNext(ScanResponse.newBuilder().addAllValues(response.values()).build());
                    };
                    executor.submit(scan, send).thenAccept(response -> {
                        if (response.status() != IndexStatus.OK) { failed(out, response); return; }
                        try { if (!response.values().isEmpty()) send.accept(response); out.onCompleted(); }
                        catch (RuntimeException error) { out.onError(io.grpc.Status.UNAVAILABLE.withDescription(error.toString()).asRuntimeException()); }
                    });
                } catch (IllegalArgumentException error) { invalid(out, error); }
            }
        };
        // The benchmark harness checks target identity before application, outside the data schema.
        var bound = ServerInterceptors.intercept(service, new ServerInterceptor() {
            @Override public <Q,S> ServerCall.Listener<Q> interceptCall(ServerCall<Q,S> call, Metadata headers, ServerCallHandler<Q,S> next) {
                var described = new ForwardingServerCall.SimpleForwardingServerCall<Q,S>(call) {
                    private boolean headersSent;
                    @Override public void sendHeaders(Metadata responseHeaders) {
                        responseHeaders.put(GrpcService.DESCRIPTION, metadata);
                        headersSent = true;
                        super.sendHeaders(responseHeaders);
                    }
                    @Override public void close(io.grpc.Status status, Metadata trailers) {
                        // Empty streams and rejected calls may close without sending a message.
                        if (!headersSent) sendHeaders(new Metadata());
                        super.close(status, trailers);
                    }
                };
                try {
                    executor.requireHealthy();
                    String binding = headers.get(GrpcService.INDEX_BINDING);
                    String kind = headers.get(GrpcService.VALUE_KIND);
                    if (binding != null || kind != null) {
                        if (!description.valueKind().name().equals(kind)) throw new IOException("Service value mode differs from the client");
                        backend.bind(binding);
                    }
                } catch (IOException | RuntimeException error) {
                    described.close(io.grpc.Status.FAILED_PRECONDITION.withDescription(error.getMessage()), new Metadata());
                    return new ServerCall.Listener<>() {};
                }
                return next.startCall(described, headers);
            }
        });
        try {
            return new IndexServer(ServerBuilder.forPort(port).addService(bound)
                    .maxInboundMessageSize(IndexExecution.MAX_MESSAGE_BYTES).build().start(), executor);
        } catch (IOException | RuntimeException failure) {
            try { executor.close(); } catch (IOException close) { failure.addSuppressed(close); }
            throw failure;
        }
    }

    private static <T> void finish(IndexResponse response, StreamObserver<T> out, Function<IndexResponse,T> result) {
        if (response.status() != IndexStatus.OK) { failed(out, response); return; }
        try { out.onNext(result.apply(response)); out.onCompleted(); }
        catch (RuntimeException error) { out.onError(io.grpc.Status.UNAVAILABLE.withDescription(error.toString()).asRuntimeException()); }
    }
    private static void failed(StreamObserver<?> out, IndexResponse response) {
        var status = response.status() == IndexStatus.INVALID
                ? io.grpc.Status.INVALID_ARGUMENT : io.grpc.Status.UNAVAILABLE;
        out.onError(status.withDescription(response.message()).asRuntimeException());
    }
    private static void invalid(StreamObserver<?> out, IllegalArgumentException error) {
        out.onError(io.grpc.Status.INVALID_ARGUMENT.withDescription(error.getMessage()).asRuntimeException());
    }
    public int port() { return server.getPort(); }
    public void awaitTermination() throws InterruptedException { server.awaitTermination(); }
    @Override public void close() throws IOException {
        server.shutdownNow();
        try {
            if (!server.awaitTermination(30, TimeUnit.SECONDS)) throw new IOException("Index server shutdown timed out; backend must remain open");
            executor.close();
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt(); throw new IOException("Index server shutdown interrupted; backend must remain open", error);
        }
    }
}
