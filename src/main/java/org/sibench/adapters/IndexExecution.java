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
    /** One request executor behind a stream; the ordered and published modes provide one. */
    @FunctionalInterface
    public interface Executor {
        Response execute(Request request);
    }

    private IndexExecution() {}

    public static Response execute(IndexBackend backend, Request request) {
        var response = Response.newBuilder().setId(request.getId());
        try {
            switch (request.getOperationCase()) {
                case WRITE -> backend.write(request.getWrite());
                case SEARCH -> response.addAllResults(backend.search(request.getSearch()));
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
