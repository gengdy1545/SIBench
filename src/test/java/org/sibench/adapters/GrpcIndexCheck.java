package org.sibench.adapters;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.sibench.adapters.proto.*;

/** Acceptance-only backend view of the ordinary gRPC transport; never interprets Trace. */
final class GrpcIndexCheck implements IndexTestSupport.BlockingBackend {
    private final GrpcService remote;
    private final ServiceClient client;
    private final String binding;
    private final IndexDescription description;

    GrpcIndexCheck(ServiceTarget config, String binding, long timeoutSeconds) throws IOException {
        this.binding = binding;
        remote = new GrpcService(config.endpoint(), new TraceRequests(config.value()), binding, timeoutSeconds);
        try {
            client = (ServiceClient) remote.factory().open(0);
            // An empty range establishes binding and validates metadata without reading index entries.
            scan(ScanRequest.newBuilder().setStartKey(ByteString.EMPTY).setEndKey(ByteString.EMPTY).build(),
                    value -> { throw new IOException("Empty range returned a value"); });
            description = remote.description();
        } catch (IOException | RuntimeException error) { remote.close(); throw error; }
    }

    private IndexResponse call(Message request, ValueSink sink) throws IOException {
        try {
            var result = client.transmit(request, chunk -> {
                try {
                    if (!chunk.more() || chunk.status() != IndexStatus.OK) throw new IOException("Invalid result chunk");
                    for (var value : chunk.values()) sink.accept(value);
                } catch (IOException error) { throw new UncheckedIOException(error); }
            }).get();
            if (result.more() || result.status() != IndexStatus.OK) throw new IOException("Incomplete index call");
            return result;
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt(); remote.close();
            throw new IOException("Index acceptance interrupted; write outcome may be unknown", error);
        } catch (ExecutionException error) { throw new IOException("Index acceptance RPC failed", error.getCause()); }
    }

    private static void noValue(ByteString value) throws IOException { throw new IOException("Unexpected result value"); }
    @Override public IndexDescription describe() { return description; }
    @Override public void write(Mutation mutation) throws IOException {
        IndexExecution.validateMutation(mutation);
        var result = call(mutation.hasPut() ? mutation.getPut() : mutation.getDelete(), GrpcIndexCheck::noValue);
        if (!result.values().isEmpty()) throw new IOException("Write returned values");
    }
    @Override public void writeBatch(List<Mutation> mutations) throws IOException {
        var result = call(WriteBatchRequest.newBuilder().addAllMutations(mutations).build(), GrpcIndexCheck::noValue);
        if (!result.values().isEmpty()) throw new IOException("Write batch returned values");
    }
    @Override public ByteString get(GetRequest request) throws IOException {
        var result = call(request, GrpcIndexCheck::noValue);
        if (result.values().size() > 1) throw new IOException("Get returned multiple values");
        return result.values().isEmpty() ? null : result.values().get(0);
    }
    @Override public void scan(ScanRequest request, ValueSink sink) throws IOException {
        var result = call(request, sink);
        for (var value : result.values()) sink.accept(value);
    }
    @Override public void bind(String supplied) throws IOException {
        if (!binding.equals(supplied)) throw new IOException("Acceptance client belongs to another Trace");
    }
    // The data protocol has no Flush RPC; remote maintenance stays with the service deployment.
    @Override public void flush() {}
    @Override public void close() { remote.close(); }
}
