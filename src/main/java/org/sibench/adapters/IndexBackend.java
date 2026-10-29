package org.sibench.adapters;

import org.sibench.adapters.proto.DescribeResponse;
import org.sibench.adapters.proto.Result;
import org.sibench.adapters.proto.Search;
import org.sibench.adapters.proto.Write;

import java.io.IOException;
import java.util.List;

/**
 * A KV index service implementation behind the SIBench index protocol (src/main/proto). It owns
 * its physical layout; the semantics are docs/contracts/index-service.md. Implementations may be
 * called concurrently and must be thread-safe; the order requests run in is the caller's
 * (IndexServer, or the driver's sending thread in process).
 */
public interface IndexBackend extends AutoCloseable {
    DescribeResponse describe();

    /** Applies one write; it is visible to every later request once this returns. */
    void write(Write write) throws IOException;

    /** Receives search results one at a time, in result order. */
    @FunctionalInterface
    interface ResultSink {
        void accept(Result result) throws IOException;
    }

    /** The visible entries of one search, in result order, at most its limit. */
    void search(Search search, ResultSink sink) throws IOException;

    /** The visible entries of one search, collected; for searches known to be small. */
    default List<Result> search(Search search) throws IOException {
        var results = new java.util.ArrayList<Result>();
        search(search, results::add);
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

    /** The complete-commit watermark last recorded in this index, 0 for none. */
    long publishedVersion() throws IOException;

    /** Records the watermark in the index; it is durable after the next {@link #flush()}. */
    void recordPublishedVersion(long version) throws IOException;

    @Override
    void close() throws IOException;
}
