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
            switch (request.getOperationCase()) {
                case WRITE -> backend.write(request.getWrite());
                case BATCH -> batch(backend, request.getBatch());
                case SEARCH -> response = search(backend, request, partial);
                case GROUP -> response.addAllResults(group(backend, request.getGroup()));
                default -> throw new IllegalArgumentException("Request without an operation");
            }
            return response.setStatus(Status.OK).build();
        } catch (IllegalArgumentException invalid) {
            return response.setStatus(Status.INVALID).setMessage(String.valueOf(invalid.getMessage())).build();
        } catch (IOException | RuntimeException error) {
            return response.setStatus(Status.UNAVAILABLE).setMessage(String.valueOf(error)).build();
        }
    }

    /** A batch of Baseline INSERTs; any other write refuses the whole batch before applying it. */
    static void batch(IndexBackend backend, org.sibench.adapters.proto.WriteBatch batch) throws IOException {
        for (var write : batch.getWritesList())
            if (write.getKind() != org.sibench.adapters.proto.Write.Kind.INSERT || write.getVersion() != 0)
                throw new IllegalArgumentException("A write batch holds Baseline INSERTs only");
        for (var write : batch.getWritesList()) backend.write(write);
    }

    /**
     * Runs a search, handing every full chunk of results to {@code partial} as a response with
     * {@code more = true}; the returned builder holds the remaining results.
     */
    static Response.Builder search(
            IndexBackend backend, Request request, java.util.function.Consumer<Response> partial)
            throws IOException {
        var chunk = new Response.Builder[] {Response.newBuilder().setId(request.getId())};
        backend.search(
                request.getSearch(),
                result -> {
                    chunk[0].addResults(result);
                    if (chunk[0].getResultsCount() == CHUNK_RESULTS) {
                        partial.accept(chunk[0].setStatus(Status.OK).setMore(true).build());
                        chunk[0] = Response.newBuilder().setId(request.getId());
                    }
                });
        return chunk[0];
    }

    /**
     * The members' results combined by primary key: an intersection keeps the first member's
     * entries every other member also found, a union adds the entries later members found.
     */
    static List<Result> group(IndexBackend backend, SearchGroup group) throws IOException {
        if (group.getMembersCount() < 2) throw new IllegalArgumentException("A group has at least two members");
        boolean intersect = switch (group.getMode()) {
            case INTERSECTION -> true;
            case UNION -> false;
            default -> throw new IllegalArgumentException("Unknown group mode");
        };
        Search first = group.getMembers(0);
        var combined = new LinkedHashMap<ByteString, Result>();
        for (Result result : backend.search(first)) combined.put(result.getPrimaryKey(), result);
        for (int i = 1; i < group.getMembersCount(); i++) {
            Search member = group.getMembers(i);
            if (member.getTable() != first.getTable()
                    || member.getReadVersion() != first.getReadVersion()
                    || member.getLocalVersion() != first.getLocalVersion())
                throw new IllegalArgumentException("Group members span tables or read versions");
            var keys = new HashSet<ByteString>();
            for (Result result : backend.search(member)) {
                keys.add(result.getPrimaryKey());
                if (!intersect) combined.putIfAbsent(result.getPrimaryKey(), result);
            }
            if (intersect) combined.keySet().retainAll(keys);
        }
        return List.copyOf(combined.values());
    }
}
