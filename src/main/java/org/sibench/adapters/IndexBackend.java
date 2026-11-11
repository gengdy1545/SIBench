package org.sibench.adapters;

import com.google.protobuf.ByteString;
import org.sibench.adapters.proto.GetRequest;
import org.sibench.adapters.proto.ScanRequest;
import org.sibench.adapters.proto.Mutation;

import java.io.IOException;
import java.util.List;

/**
 * A KV index service implementation behind the SIBench index protocol (proto). It owns
 * its physical layout; the semantics are docs/index-service.md. Implementations may be
 * called concurrently and must be thread-safe; replay establishes required write/read
 * dependencies before dispatch.
 */
public interface IndexBackend extends AutoCloseable {
    IndexDescription describe();

    /** Applies one write; later normal reads at applicable versions see its effect once this returns. */
    void write(Mutation write) throws IOException;

    /** Applies validated writes; all are visible once this returns, without an atomicity promise. */
    default void writeBatch(List<Mutation> writes) throws IOException {
        for (var write : writes) write(write);
    }

    /** Receives search results one at a time, in result order. */
    @FunctionalInterface
    interface ValueSink {
        void accept(ByteString result) throws IOException;
    }

    /** One visible value, or null when absent. */
    ByteString get(GetRequest get) throws IOException;

    /** The visible entries of a half-open byte range, at most its limit. */
    void scan(ScanRequest scan, ValueSink sink) throws IOException;

    /** The visible entries of one search, collected; for searches known to be small. */
    default List<ByteString> scan(ScanRequest scan) throws IOException {
        var results = new java.util.ArrayList<ByteString>();
        scan(scan, results::add);
        return results;
    }

    /** Every write that returned is durable to the level {@link #describe()} declares. */
    void flush() throws IOException;

    /**
     * Binds the index to the trace it holds: recorded on first use, durable after the next
     * {@link #flush()}; any other binding is refused.
     */
    void bind(String binding) throws IOException;

    /** Where the index lives: a local directory or an object-storage location. */
    String location();

    @Override
    void close() throws IOException;
}
