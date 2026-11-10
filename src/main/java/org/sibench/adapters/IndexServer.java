package org.sibench.adapters;

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
            private <T> void submit(Request request, StreamObserver<T> out, Function<Response,T> result) {
                executor.submit(request).thenAccept(response -> finish(response, out, result));
            }
            @Override public void put(PutRequest put, StreamObserver<WriteResponse> out) {
                try { submit(Request.newBuilder().setWrite(IndexRpc.write(put)).build(), out, r -> WriteResponse.getDefaultInstance()); }
                catch (IllegalArgumentException error) { invalid(out, error); }
            }
            @Override public void delete(DeleteRequest delete, StreamObserver<WriteResponse> out) {
                try { submit(Request.newBuilder().setWrite(IndexRpc.write(delete)).build(), out, r -> WriteResponse.getDefaultInstance()); }
                catch (IllegalArgumentException error) { invalid(out, error); }
            }
            @Override public void batchWrite(BatchWriteRequest batch, StreamObserver<WriteResponse> out) {
                try {
                    if (!batch.getUnknownFields().asMap().isEmpty()) throw new IllegalArgumentException("Unsupported batch fields");
                    var writes = WriteBatch.newBuilder();
                    for (var mutation : batch.getMutationsList()) {
                        if (!mutation.getUnknownFields().asMap().isEmpty()) throw new IllegalArgumentException("Unsupported mutation fields");
                        if (mutation.hasPut()) writes.addWrites(IndexRpc.write(mutation.getPut()));
                        else if (mutation.hasDelete()) writes.addWrites(IndexRpc.write(mutation.getDelete()));
                        else throw new IllegalArgumentException("Missing mutation");
                    }
                    submit(Request.newBuilder().setBatch(writes).build(), out, r -> WriteResponse.getDefaultInstance());
                } catch (IllegalArgumentException error) { invalid(out, error); }
            }
            @Override public void get(GetRequest get, StreamObserver<GetResponse> out) {
                try {
                    submit(Request.newBuilder().setSearch(IndexRpc.search(get)).build(), out, response -> {
                        if (response.getResultsCount() > 1) throw new IllegalArgumentException("Get returned multiple entries");
                        var result = GetResponse.newBuilder();
                        if (response.getResultsCount() == 1) result.setResult(response.getResults(0));
                        return result.build();
                    });
                } catch (IllegalArgumentException error) { invalid(out, error); }
            }
            @Override public void scan(ScanRequest scan, StreamObserver<ScanResponse> out) {
                try {
                    var call = (ServerCallStreamObserver<ScanResponse>) out;
                    var ready = new Object();
                    call.setOnReadyHandler(() -> { synchronized (ready) { ready.notifyAll(); } });
                    java.util.function.Consumer<Response> send = response -> {
                        synchronized (ready) {
                            while (!call.isReady() && !call.isCancelled()) {
                                try { ready.wait(10); }
                                catch (InterruptedException error) {
                                    Thread.currentThread().interrupt(); throw new IllegalStateException("Scan interrupted", error);
                                }
                            }
                        }
                        if (call.isCancelled()) throw new IllegalStateException("Scan cancelled");
                        out.onNext(ScanResponse.newBuilder().addAllResults(response.getResultsList()).build());
                    };
                    executor.submit(Request.newBuilder().setSearch(IndexRpc.search(scan)).build(), send).thenAccept(response -> {
                        if (response.getStatus() != org.sibench.adapters.proto.Status.OK) { failed(out, response); return; }
                        try { if (response.getResultsCount() > 0) send.accept(response); out.onCompleted(); }
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

    private static <T> void finish(Response response, StreamObserver<T> out, Function<Response,T> result) {
        if (response.getStatus() != org.sibench.adapters.proto.Status.OK) { failed(out, response); return; }
        try { out.onNext(result.apply(response)); out.onCompleted(); }
        catch (RuntimeException error) { out.onError(io.grpc.Status.UNAVAILABLE.withDescription(error.toString()).asRuntimeException()); }
    }
    private static void failed(StreamObserver<?> out, Response response) {
        var status = response.getStatus() == org.sibench.adapters.proto.Status.INVALID
                ? io.grpc.Status.INVALID_ARGUMENT : io.grpc.Status.UNAVAILABLE;
        out.onError(status.withDescription(response.getMessage()).asRuntimeException());
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
