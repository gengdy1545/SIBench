package org.sibench.adapters;

import com.google.protobuf.ByteString;

import org.sibench.adapters.proto.Request;
import org.sibench.adapters.proto.Response;
import org.sibench.adapters.proto.Result;
import org.sibench.adapters.proto.Search;
import org.sibench.adapters.proto.SearchGroup;
import org.sibench.adapters.proto.Status;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;

/** Executes one protocol request on a backend; shared by the server and in-process clients. */
public final class IndexExecution {
    /**
     * One request executor behind a stream; the response may complete later. A large search
     * result goes to {@code partial} in chunks ({@code more = true}) before the final response.
     */
    @FunctionalInterface
    public interface Executor {
        java.util.concurrent.CompletableFuture<Response> submit(
                Request request, java.util.function.Consumer<Response> partial);
    }

    /**
     * Results per response message; a larger search result is sent in several chunks. About
     * 1 MB, so a chunk fits gRPC's default 4 MiB inbound message limit.
     */
    static final int CHUNK_RESULTS = 16_384;
    static final int MAX_MESSAGE_BYTES = 32 * 1024 * 1024;

    private IndexExecution() {}

    /** Executes a request whose results, if any, fit one message. */
    public static Response execute(IndexBackend backend, Request request) {
        return execute(backend, request, chunk -> {
            throw new IllegalStateException("Unexpected partial response");
        });
    }

    public static Response execute(
            IndexBackend backend, Request request, java.util.function.Consumer<Response> partial) {
        var response = Response.newBuilder().setId(request.getId());
        try {
            if (!request.getUnknownFields().asMap().isEmpty())
                throw new IllegalArgumentException("Unsupported request fields");
            switch (request.getOperationCase()) {
                case WRITE -> backend.write(request.getWrite());
                case BATCH -> batch(backend, request.getBatch());
                case SEARCH -> response = search(backend, request, partial);
                case GROUP -> throw new IllegalArgumentException("Search groups execute on the client");
                default -> throw new IllegalArgumentException("Request without an operation");
            }
            return response.setStatus(Status.OK).build();
        } catch (IllegalArgumentException invalid) {
            return response.setStatus(Status.INVALID).setMessage(String.valueOf(invalid.getMessage())).build();
        } catch (IOException | RuntimeException error) {
            return response.setStatus(Status.UNAVAILABLE).setMessage(String.valueOf(error)).build();
        }
    }

    /** Validate every member before applying a batch; execution failures may be partial. */
    static void batch(IndexBackend backend, org.sibench.adapters.proto.WriteBatch batch) throws IOException {
        if (batch.getWritesCount() == 0) throw new IllegalArgumentException("Empty write batch");
        var kind = backend.describe().valueKind();
        for (var write : batch.getWritesList()) {
            validateWrite(write, kind);
            if (write.getKind() == org.sibench.adapters.proto.Write.Kind.RELOCATE)
                throw new IllegalArgumentException("Relocate is not a batch mutation");
        }
        backend.writeBatch(batch.getWritesList());
    }

    static void validateKey(org.sibench.adapters.proto.EntryKey key) {
        if (!key.getUnknownFields().asMap().isEmpty() || key.getTable() < 1 || key.getIndex() < 0
                || (key.getIndex() == 0) != key.getIndexedValue().isEmpty() || key.getPrimaryKey().isEmpty())
            throw new IllegalArgumentException("Invalid entry key");
        if (component(key.getPrimaryKey()).isNull()) throw new IllegalArgumentException("Primary key is NULL");
        if (key.getIndex() != 0) component(key.getIndexedValue());
    }

    static org.sibench.trace.IndexKey component(ByteString bytes) {
        try { return org.sibench.trace.IndexKey.decode(bytes.toByteArray()); }
        catch (IOException invalid) { throw new IllegalArgumentException("Invalid index key component", invalid); }
    }

    static void validateWrite(org.sibench.adapters.proto.Write write, org.sibench.adapters.proto.ValueKind kind) {
        validateKey(write.getKey());
        if (!write.getUnknownFields().asMap().isEmpty() || write.getVersion() < 0)
            throw new IllegalArgumentException("Unsupported write fields or version");
        switch (write.getKind()) {
            case INSERT -> { if (write.getVersion() != 0) throw new IllegalArgumentException("Insert outside the baseline"); }
            case UPSERT, LOGICAL_DELETE, RELOCATE -> { }
            default -> throw new IllegalArgumentException("Unknown write kind");
        }
        if (write.getKind() == org.sibench.adapters.proto.Write.Kind.LOGICAL_DELETE) {
            if (write.hasValue()) throw new IllegalArgumentException("Delete carries no value");
        } else {
            if (!write.hasValue() || !write.getValue().getUnknownFields().asMap().isEmpty())
                throw new IllegalArgumentException("Missing or unsupported write value");
            var value = write.getValue();
            if (kind == org.sibench.adapters.proto.ValueKind.ROW_ID && (value.getFile() != 0 || value.getPosition() != 0)
                    || kind == org.sibench.adapters.proto.ValueKind.LOCATOR && value.getRowId() != 0)
                throw new IllegalArgumentException("Write value differs from the service value mode");
        }
    }

    /**
     * Runs a search, handing every full chunk of results to {@code partial} as a response with
     * {@code more = true}; the returned builder holds the remaining results.
     */
    static Response.Builder search(
            IndexBackend backend, Request request, java.util.function.Consumer<Response> partial)
            throws IOException {
        var chunk = new Response.Builder[] {Response.newBuilder().setId(request.getId())};
        int[] bytes = {0};
        backend.search(
                request.getSearch(),
                result -> {
                    int size = result.getSerializedSize() + 10;
                    if (size > MAX_MESSAGE_BYTES - 64) throw new IOException("One index result exceeds the RPC message limit");
                    if (chunk[0].getResultsCount() > 0 && bytes[0] + size > 1024 * 1024) {
                        partial.accept(chunk[0].setStatus(Status.OK).setMore(true).build());
                        chunk[0] = Response.newBuilder().setId(request.getId());
                        bytes[0] = 0;
                    }
                    chunk[0].addResults(result);
                    bytes[0] += size;
                    if (chunk[0].getResultsCount() == CHUNK_RESULTS) {
                        partial.accept(chunk[0].setStatus(Status.OK).setMore(true).build());
                        chunk[0] = Response.newBuilder().setId(request.getId());
                        bytes[0] = 0;
                    }
                });
        return chunk[0];
    }

    /**
     * The members' results combined by primary key: an intersection keeps the first member's
     * entries every other member also found, a union adds the entries later members found.
     */
    static List<Result> group(SearchGroup group, List<List<Result>> results) {
        if (group.getMembersCount() < 2) throw new IllegalArgumentException("A group has at least two members");
        boolean intersect = switch (group.getMode()) {
            case INTERSECTION -> true;
            case UNION -> false;
            default -> throw new IllegalArgumentException("Unknown group mode");
        };
        Search first = group.getMembers(0);
        var combined = new LinkedHashMap<ByteString, Result>();
        for (Result result : results.get(0)) combined.put(result.getPrimaryKey(), result);
        for (int i = 1; i < group.getMembersCount(); i++) {
            Search member = group.getMembers(i);
            if (member.getTable() != first.getTable()
                    || member.getReadVersion() != first.getReadVersion())
                throw new IllegalArgumentException("Group members span tables or read versions");
            var keys = new HashSet<ByteString>();
            for (Result result : results.get(i)) {
                keys.add(result.getPrimaryKey());
                if (!intersect) combined.putIfAbsent(result.getPrimaryKey(), result);
            }
            if (intersect) combined.keySet().retainAll(keys);
        }
        return List.copyOf(combined.values());
    }
}
