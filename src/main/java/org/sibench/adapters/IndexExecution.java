package org.sibench.adapters;

import com.google.protobuf.Message;
import org.sibench.adapters.proto.*;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Validates and executes opaque versioned KV requests: the service trust boundary, ahead of every backend. */
public final class IndexExecution {
    static final int CHUNK_RESULTS = 16_384;
    static final int MAX_MESSAGE_BYTES = 32 * 1024 * 1024;
    private IndexExecution() {}

    public static IndexResponse execute(IndexBackend backend, Message request,
            java.util.function.Consumer<IndexResponse> partial) {
        try {
            if (request instanceof PutRequest put) {
                var mutation = Mutation.newBuilder().setPut(put).build();
                validateMutation(mutation); backend.write(mutation);
            } else if (request instanceof DeleteRequest delete) {
                var mutation = Mutation.newBuilder().setDelete(delete).build();
                validateMutation(mutation); backend.write(mutation);
            } else if (request instanceof WriteBatchRequest batch) {
                known(batch);
                if (batch.getMutationsCount() == 0) throw new IllegalArgumentException("Empty write batch");
                for (var mutation : batch.getMutationsList()) validateMutation(mutation);
                backend.writeBatch(batch.getMutationsList());
            } else if (request instanceof GetRequest get) {
                validateGet(get);
                var result = backend.get(get);
                return IndexResponse.ok(result == null ? List.of() : List.of(result));
            } else if (request instanceof ScanRequest scan) {
                validateScan(scan); return scan(backend, scan, partial);
            } else throw new IllegalArgumentException("Expected an individual data operation");
            return IndexResponse.ok(List.of());
        } catch (IllegalArgumentException invalid) {
            return IndexResponse.error(IndexStatus.INVALID, String.valueOf(invalid.getMessage()));
        } catch (IOException | RuntimeException error) {
            return IndexResponse.error(IndexStatus.UNAVAILABLE, String.valueOf(error));
        }
    }
    public static void known(Message message) {
        if (!message.getUnknownFields().asMap().isEmpty()) throw new IllegalArgumentException("Unsupported fields");
    }
    public static void validateMutation(Mutation mutation) {
        known(mutation);
        if (mutation.hasPut()) { known(mutation.getPut()); ts(mutation.getPut().getTs()); }
        else if (mutation.hasDelete()) { known(mutation.getDelete()); ts(mutation.getDelete().getTs()); }
        else throw new IllegalArgumentException("Missing mutation");
    }
    public static void validateGet(GetRequest get) {
        known(get); ts(get.getReadTs());
        validateLocalWrites(get.getLocalWritesList(), get.getKey(), null, true);
    }
    public static void validateScan(ScanRequest scan) {
        known(scan); ts(scan.getReadTs());
        if (scan.getLimit() < 0) throw new IllegalArgumentException("Limit exceeds int64");
        validateLocalWrites(scan.getLocalWritesList(), scan.hasStartKey() ? scan.getStartKey() : null,
                scan.hasEndKey() ? scan.getEndKey() : null, false);
    }
    private static void validateLocalWrites(List<Mutation> writes, ByteString start, ByteString end, boolean point) {
        if (writes.size() > org.sibench.capture.ProtoTrace.MAX_TRANSACTION_OPERATIONS) throw new IllegalArgumentException("Private write context exceeds operation limit");
        var order = ByteString.unsignedLexicographicalComparator();
        ByteString previous = null;
        for (var write : writes) {
            validateMutation(write);
            var key = write.hasPut() ? write.getPut().getKey() : write.getDelete().getKey();
            long timestamp = write.hasPut() ? write.getPut().getTs() : write.getDelete().getTs();
            if (timestamp != 0 || previous != null && order.compare(previous, key) >= 0)
                throw new IllegalArgumentException("Private writes require timestamp zero and unique ordered keys");
            if (point ? !key.equals(start) : start != null && order.compare(key, start) < 0 || end != null && order.compare(key, end) >= 0)
                throw new IllegalArgumentException("Private write outside read bounds");
            previous = key;
        }
    }
    private static void ts(long ts) {
        if (ts < 0) throw new IllegalArgumentException("Timestamp exceeds int64");
    }
    private static IndexResponse scan(IndexBackend backend, ScanRequest scan,
            java.util.function.Consumer<IndexResponse> partial) throws IOException {
        var chunk = new ArrayList<ByteString>();
        int[] bytes = {0};
        backend.scan(scan, result -> {
            int size = result.size() + 10;
            if (size > MAX_MESSAGE_BYTES - 64) throw new IOException("One index result exceeds the RPC message limit");
            if (!chunk.isEmpty() && bytes[0] + size > 1024 * 1024) {
                partial.accept(new IndexResponse(IndexStatus.OK, "", chunk, true));
                chunk.clear(); bytes[0] = 0;
            }
            chunk.add(result); bytes[0] += size;
            if (chunk.size() == CHUNK_RESULTS) {
                partial.accept(new IndexResponse(IndexStatus.OK, "", chunk, true));
                chunk.clear(); bytes[0] = 0;
            }
        });
        return IndexResponse.ok(chunk);
    }
}
