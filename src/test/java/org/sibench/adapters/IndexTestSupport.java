package org.sibench.adapters;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.sibench.adapters.proto.ScanRequest;

/** Collection and replay helpers for tests; production execution stays streaming. */
public final class IndexTestSupport {
    private IndexTestSupport() {}

    public static List<ByteString> scan(IndexBackend backend, ScanRequest request) throws IOException {
        var results = new ArrayList<ByteString>();
        backend.scan(request, results::add);
        return results;
    }

    /** The ordinary gRPC replay against a fresh local RocksDB index served in this process. */
    public static java.util.Map<String, Object> replay(java.nio.file.Path trace, org.sibench.replay.ReplayConfig config,
            java.nio.file.Path index, ValueKind value, java.nio.file.Path report) throws IOException {
        try (var backend = VersionedIndexBackend.create(index, value); var server = IndexServer.start(0, backend)) {
            return ServiceReplay.run(trace, config, new ServiceTarget("127.0.0.1:" + server.port(), value), report);
        }
    }

    public static IndexResponse execute(IndexBackend backend, Message request) {
        return IndexExecution.execute(backend, request, chunk -> {
            throw new IllegalStateException("Unexpected partial response");
        });
    }
}
