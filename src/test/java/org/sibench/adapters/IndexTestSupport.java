package org.sibench.adapters;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import org.sibench.adapters.proto.Mutation;
import org.sibench.adapters.proto.ScanRequest;

/** Collection and replay helpers for tests; production execution stays streaming. */
public final class IndexTestSupport {
    private IndexTestSupport() {}

    public static ServiceConfig.Execution execution(int readThreads, int writeThreads) throws IOException {
        var directory = java.nio.file.Files.createTempDirectory("sibench-service-test-");
        var statistics = directory.resolve("statistics.jsonl");
        directory.toFile().deleteOnExit(); statistics.toFile().deleteOnExit();
        return new ServiceConfig.Execution(readThreads, writeThreads, statistics);
    }

    public static ServiceConfig.Execution execution() throws IOException { return execution(4, 2); }

    public static IndexServer start(int port, IndexBackend backend) throws IOException {
        return IndexServer.start(port, backend, execution());
    }

    public static IndexExecutor executor(IndexBackend backend) throws IOException {
        return new IndexExecutor(backend, execution());
    }

    public static List<ByteString> scan(IndexBackend backend, ScanRequest request) throws IOException {
        var results = new ArrayList<ByteString>();
        backend.scan(request, results::add);
        return results;
    }

    /** The ordinary gRPC replay against a fresh local RocksDB index served in this process. */
    public static java.util.Map<String, Object> replay(java.nio.file.Path trace, org.sibench.replay.ReplayConfig config,
            java.nio.file.Path index, ValueKind value, java.nio.file.Path report) throws IOException {
        try (var backend = VersionedIndexBackend.create(index, value); var server = start(0, backend)) {
            return ServiceReplay.run(trace, config, new ServiceTarget("127.0.0.1:" + server.port(), value), report);
        }
    }

    public static IndexResponse execute(IndexBackend backend, Message request) {
        return execute(backend, request, chunk -> {
            throw new IllegalStateException("Unexpected partial response");
        });
    }

    public static IndexResponse execute(IndexBackend backend, Message request, Consumer<IndexResponse> partial) {
        return IndexExecution.executeAsync(backend, request, partial).join();
    }

    static void awaitWrite(CompletableFuture<Void> result) throws IOException {
        try { result.get(); }
        catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("Index call interrupted; write outcome may be unknown", interrupted);
        } catch (ExecutionException error) {
            Throwable cause = error.getCause();
            while (cause instanceof CompletionException && cause.getCause() != null) cause = cause.getCause();
            if (cause instanceof IOException io) throw io;
            if (cause instanceof RuntimeException runtime) throw runtime;
            throw new IOException(cause);
        }
    }

    interface BlockingBackend extends IndexBackend {
        void write(Mutation write) throws IOException;

        default void writeBatch(List<Mutation> writes) throws IOException {
            for (var write : writes) write(write);
        }

        @Override default CompletableFuture<Void> writeAsync(Mutation write) throws IOException {
            write(write);
            return CompletableFuture.completedFuture(null);
        }

        @Override default CompletableFuture<Void> writeBatchAsync(List<Mutation> writes) throws IOException {
            writeBatch(writes);
            return CompletableFuture.completedFuture(null);
        }
    }
}
