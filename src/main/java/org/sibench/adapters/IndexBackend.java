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

    /** The visible entries of one search, in result order, at most its limit. */
    List<Result> search(Search search) throws IOException;

    /** Every write that returned is durable to the level {@link #describe()} declares. */
    void flush() throws IOException;

    @Override
    void close() throws IOException;
}
