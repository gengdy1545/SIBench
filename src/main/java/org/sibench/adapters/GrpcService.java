package org.sibench.adapters;

import com.google.protobuf.MessageLite;
import io.grpc.*;
import io.grpc.stub.*;
import org.sibench.adapters.proto.*;
import org.sibench.replay.ReplayTarget;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.*;
import java.util.function.Consumer;

/** Concurrent single-operation RPCs over persistent per-client channels. */
public final class GrpcService implements AutoCloseable {
    static final Metadata.Key<String> INDEX_BINDING = Metadata.Key.of("sibench-index-binding", Metadata.ASCII_STRING_MARSHALLER);
    static final Metadata.Key<String> VALUE_KIND = Metadata.Key.of("sibench-value-kind", Metadata.ASCII_STRING_MARSHALLER);
    static final Metadata.Key<byte[]> DESCRIPTION = Metadata.Key.of("sibench-service-bin", Metadata.BINARY_BYTE_MARSHALLER);
    private final String endpoint, binding;
    private final TraceRequests requests;
    private final long timeoutSeconds;
    private final Set<Client> clients = ConcurrentHashMap.newKeySet();
    private volatile IndexDescription description;

    public GrpcService(String endpoint, TraceRequests requests, String binding, long timeoutSeconds) {
        if (binding == null || binding.isBlank()) throw new IllegalArgumentException("A replay needs its trace binding");
        this.endpoint=endpoint; this.requests=requests; this.binding=binding; this.timeoutSeconds=timeoutSeconds;
    }

    /** Description returned by the harness on actual data calls; no extra RPC or probe read. */
    public IndexDescription description() throws IOException {
        var found=description;
        if (found==null) throw new IOException("No index service deployment description was received");
        return found;
    }

    public ReplayTarget.Factory factory() {
        return clientId -> { var client=new Client(); clients.add(client); return client; };
    }
    @Override public void close() { clients.forEach(Client::close); clients.clear(); }

    private final class Client extends ServiceClient {
        final ManagedChannel channel=ManagedChannelBuilder.forTarget(endpoint).usePlaintext().disableRetry()
                .maxInboundMessageSize(IndexExecution.MAX_MESSAGE_BYTES).build();
        final ConcurrentHashMap<CompletableFuture<Response>,ClientCall<?,?>> pending=new ConcurrentHashMap<>();
        private CompletableFuture<Void> lastSend=CompletableFuture.completedFuture(null);
        private volatile Throwable failure;

        Client() { super(requests); }

        private synchronized void fail(Throwable error) {
            if (failure!=null) return;
            failure=error;
            pending.forEach((done,call) -> { done.completeExceptionally(error); call.cancel("Index client failed",error); });
            pending.clear();
        }

        @Override synchronized CompletableFuture<Response> transmit(Request request, Consumer<Response> partial) throws IOException {
            if (failure!=null) throw new IOException("Index client failed",failure);
            try {
                return switch (request.getOperationCase()) {
                    case PUT -> call(IndexServiceGrpc.getPutMethod(), request.getPut(), partial, false);
                    case DELETE -> call(IndexServiceGrpc.getDeleteMethod(), request.getDelete(), partial, false);
                    case GET -> call(IndexServiceGrpc.getGetMethod(), request.getGet(), partial, false);
                    case SCAN -> call(IndexServiceGrpc.getScanMethod(), request.getScan(), partial, true);
                    case BATCH -> call(IndexServiceGrpc.getBatchWriteMethod(), request.getBatch(), partial, false);
                    default -> throw new IOException("Only data operations reach the transport");
                };
            } catch (IllegalArgumentException error) { throw new IOException("Unsupported index operation",error); }
        }

        private <Q extends MessageLite,S> CompletableFuture<Response> call(MethodDescriptor<Q,S> method,Q input,
                Consumer<Response> partial,boolean scan) throws IOException {
            if (input.getSerializedSize()>IndexExecution.MAX_MESSAGE_BYTES) throw new IOException("Index request exceeds the RPC message limit");
            var done=new CompletableFuture<Response>();
            var sent=new CompletableFuture<Void>(); lastSend=sent;
            var options=CallOptions.DEFAULT.withDeadlineAfter(timeoutSeconds,TimeUnit.SECONDS)
                    .withStreamTracerFactory(new ClientStreamTracer.Factory() {
                        @Override public ClientStreamTracer newClientStreamTracer(ClientStreamTracer.StreamInfo info,Metadata headers) {
                            return new ClientStreamTracer() {
                                @Override public void outboundMessageSent(int sequence,long wire,long plain) { sent.complete(null); }
                                @Override public void streamClosed(io.grpc.Status status) {
                                    if (!status.isOk()) sent.completeExceptionally(status.asRuntimeException());
                                }
                            };
                        }
                    });
            var raw=channel.newCall(method,options);
            ClientCall<Q,S> call=new ForwardingClientCall.SimpleForwardingClientCall<Q,S>(raw) {
                @Override public void start(Listener<S> listener,Metadata headers) {
                    headers.put(INDEX_BINDING,binding); headers.put(VALUE_KIND,requests.valueKind().name());
                    super.start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<S>(listener) {
                        private boolean described;
                        @Override public void onHeaders(Metadata headers) {
                            try {
                                byte[] value=headers.get(DESCRIPTION);
                                if (value==null) throw new IOException("Missing index service deployment description");
                                var actual=new com.google.gson.Gson().fromJson(new String(value,java.nio.charset.StandardCharsets.UTF_8),IndexDescription.class);
                                if (actual==null) throw new IOException("Empty index service deployment description");
                                actual.require(requests.valueKind());
                                synchronized (GrpcService.this) {
                                    if (description!=null && !description.equals(actual)) throw new IOException("Index service description changed during replay");
                                    description=actual;
                                }
                                described=true;
                                super.onHeaders(headers);
                            } catch (IOException | RuntimeException error) { fail(error); }
                        }
                        @Override public void onClose(io.grpc.Status status, Metadata trailers) {
                            if (status.isOk() && !described) fail(new IOException("Missing index service deployment description"));
                            super.onClose(status,trailers);
                        }
                    },headers);
                }
            };
            pending.put(done,call);
            requests.rpcRequest(input,method.getBareMethodName());
            var answer=Response.newBuilder().setStatus(org.sibench.adapters.proto.Status.OK);
            StreamObserver<S> observer=new StreamObserver<>() {
                int messages;
                @Override public void onNext(S value) {
                    try {
                        if (!scan && messages++!=0) throw new IOException("Multiple responses to a unary index call");
                        if (value instanceof GetResponse get && get.hasResult()) {
                            answer.addResults(get.getResult()); requests.rpcResults(java.util.List.of(get.getResult()));
                        } else if (value instanceof ScanResponse chunk) {
                            requests.rpcResults(chunk.getResultsList());
                            partial.accept(Response.newBuilder().setStatus(org.sibench.adapters.proto.Status.OK)
                                    .setMore(true).addAllResults(chunk.getResultsList()).build());
                        }
                    } catch (IOException | RuntimeException error) { fail(error); }
                }
                @Override public void onError(Throwable error) { fail(error); }
                @Override public void onCompleted() {
                    if (!scan && messages!=1) { fail(new IOException("Missing unary index response")); return; }
                    pending.remove(done); done.complete(answer.build());
                }
            };
            try {
                if (scan) ClientCalls.asyncServerStreamingCall(call,input,observer);
                else ClientCalls.asyncUnaryCall(call,input,observer);
            } catch (RuntimeException error) { fail(error); sent.completeExceptionally(error); }
            done.whenComplete((result,error) -> { if (error!=null) sent.completeExceptionally(error); });
            return done;
        }
        @Override CompletableFuture<Void> localSend() { return lastSend; }
        @Override public void close() { fail(new IOException("Index client closed")); channel.shutdownNow(); }
    }
}
