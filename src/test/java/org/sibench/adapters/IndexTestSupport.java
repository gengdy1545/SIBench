package org.sibench.adapters;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.sibench.adapters.proto.ScanRequest;

/** Collection helpers for bounded test answers; production execution stays streaming. */
public final class IndexTestSupport {
    private IndexTestSupport() {}

    public static List<ByteString> scan(IndexBackend backend, ScanRequest request) throws IOException {
        var results = new ArrayList<ByteString>();
        backend.scan(request, results::add);
        return results;
    }

    public static IndexResponse execute(IndexBackend backend, Message request) {
        return IndexExecution.execute(backend, request, chunk -> {
            throw new IllegalStateException("Unexpected partial response");
        });
    }
}
