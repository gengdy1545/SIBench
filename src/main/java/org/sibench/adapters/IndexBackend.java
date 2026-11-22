package org.sibench.adapters;

import com.google.protobuf.ByteString;
import org.sibench.adapters.proto.GetRequest;
import org.sibench.adapters.proto.ScanRequest;
import org.sibench.adapters.proto.Mutation;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * A KV index service implementation behind the SIBench index protocol (proto). It owns
 * its physical layout; the semantics are docs/index-service.md. Implementations may be
 * called concurrently and must be thread-safe; replay establishes required write/read
 * dependencies before dispatch. Requests arrive validated by {@link IndexExecution}.
 */
public interface IndexBackend extends AutoCloseable {
    IndexDescription describe();

    /** Later reads at applicable versions see the write after successful completion. */
    default CompletableFuture<Void> writeAsync(Mutation write) throws IOException {
        return writeBatchAsync(List.of(write));
    }

    /**
     * Success acknowledges every write at the declared durability, without an atomicity promise.
     * A synchronous binding may perform the work before returning a completed future; a remote binding
     * may return while transport is pending. Only successful completion establishes visibility.
     */
    CompletableFuture<Void> writeBatchAsync(List<Mutation> writes) throws IOException;

    /** Receives search results one at a time, in result order. */
    @FunctionalInterface
    interface ValueSink {
        void accept(ByteString result) throws IOException;
    }

    /** One visible value, or null when absent. */
    ByteString get(GetRequest get) throws IOException;

    /** The visible entries of a half-open byte range, at most its limit. */
    void scan(ScanRequest scan, ValueSink sink) throws IOException;

    /** Persists completed writes; the caller must drain outstanding write futures before final persistence. */
    void flush() throws IOException;

    /** Optional engine measurements. The service owns call timings and statistics output for every backend. */
    default java.util.Map<String, Object> statistics() throws IOException { return java.util.Map.of(); }

    /**
     * Binds the index to the trace it holds: recorded on first use, durable after the next
     * {@link #flush()}; any other binding is refused.
     */
    void bind(String binding) throws IOException;

    @Override
    void close() throws IOException;
}
