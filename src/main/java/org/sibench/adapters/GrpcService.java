package org.sibench.adapters;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import org.sibench.adapters.proto.DescribeRequest;
import org.sibench.adapters.proto.DescribeResponse;
import org.sibench.adapters.proto.FlushRequest;
import org.sibench.adapters.proto.IndexServiceGrpc;
import org.sibench.adapters.proto.Request;
import org.sibench.adapters.proto.Response;
import org.sibench.replay.ReplayTarget;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Clients of a remote index service over gRPC: each benchmark client has its own connection and
 * one ordered request stream, responses are matched by request id. A request counts as sent once
 * the gRPC stream accepted it for transmission; that is the local send the driver waits for, not
 * a delivery or execution acknowledgement.
 */
public final class GrpcService implements AutoCloseable {
    private final String endpoint;
    private final TraceRequests requests;
    private final Map<Integer, Client> clients = new ConcurrentHashMap<>();

    public GrpcService(String endpoint, TraceRequests requests) {
        this.endpoint = endpoint;
        this.requests = requests;
    }

    private static ManagedChannel channel(String endpoint) {
        return ManagedChannelBuilder.forTarget(endpoint).usePlaintext().maxInboundMessageSize(Integer.MAX_VALUE).build();
    }

    public DescribeResponse describe() {
        ManagedChannel channel = channel(endpoint);
        try {
            return IndexServiceGrpc.newBlockingStub(channel).describe(DescribeRequest.getDefaultInstance());
        } finally {
            channel.shutdownNow();
        }
    }

    /** Waits until every request the service received is durable to its declared level. */
    public void flush() {
        ManagedChannel channel = channel(endpoint);
        try {
            IndexServiceGrpc.newBlockingStub(channel).flush(FlushRequest.getDefaultInstance());
        } finally {
            channel.shutdownNow();
        }
    }

    public ReplayTarget.Factory factory() {
        return clientId -> {
            var client = new Client(channel(endpoint));
            clients.put(clientId, client);
            return client;
        };
    }

    @Override
    public void close() {
        for (Client client : clients.values()) client.close();
        clients.clear();
    }

    private final class Client extends ServiceClient {
        private final ManagedChannel channel;
        private final StreamObserver<Request> stream;
        private final Map<Long, CompletableFuture<Response>> pending = new ConcurrentHashMap<>();
        private volatile Throwable failure;
        private boolean closed;

        Client(ManagedChannel channel) {
            super(requests);
            this.channel = channel;
            this.stream = IndexServiceGrpc.newStub(channel).execute(new StreamObserver<>() {
                @Override
                public void onNext(Response response) {
                    var waiting = pending.remove(response.getId());
                    if (waiting != null) waiting.complete(response);
                    else fail(new IOException("Response for an unknown request " + response.getId()));
                }

                @Override
                public void onError(Throwable error) {
                    fail(error);
                }

                @Override
                public void onCompleted() {
                    fail(new IOException("Service closed the stream"));
                }
            });
        }

        private void fail(Throwable error) {
            failure = error;
            for (var waiting : pending.values()) waiting.completeExceptionally(error);
            pending.clear();
        }

        @Override
        CompletableFuture<Response> transmit(Request request) throws IOException {
            if (failure != null) throw new IOException("Service stream failed", failure);
            var response = new CompletableFuture<Response>();
            if (pending.putIfAbsent(request.getId(), response) != null)
                throw new IOException("Duplicate request id " + request.getId());
            stream.onNext(request);
            return response;
        }

        @Override
        public void close() {
            if (closed) return;
            closed = true;
            if (failure == null) stream.onCompleted();
            channel.shutdown();
            try {
                if (!channel.awaitTermination(30, TimeUnit.SECONDS)) channel.shutdownNow();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                channel.shutdownNow();
            }
        }
    }
}
