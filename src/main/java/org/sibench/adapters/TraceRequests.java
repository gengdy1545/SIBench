package org.sibench.adapters;

import com.google.protobuf.ByteString;

import org.sibench.adapters.proto.Bound;
import org.sibench.adapters.proto.EntryKey;
import org.sibench.adapters.proto.Request;
import org.sibench.adapters.proto.Response;
import org.sibench.adapters.proto.Result;
import org.sibench.adapters.proto.Search;
import org.sibench.adapters.proto.SearchGroup;
import org.sibench.adapters.proto.Status;
import org.sibench.adapters.proto.Value;
import org.sibench.adapters.proto.ValueKind;
import org.sibench.adapters.proto.Write;
import org.sibench.capture.IndexOperationTrace;
import org.sibench.capture.IndexOperationTrace.Operation;
import org.sibench.trace.IndexKey;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/** Maps Trace operations to requests and checks response status, shape and native counts. */
public final class TraceRequests {
    private final ValueKind valueKind;
    private final AtomicLong results = new AtomicLong(), queries = new AtomicLong();
    private final AtomicLong requestBytes = new AtomicLong(), resultBytes = new AtomicLong();
    private final AtomicLong executeRequests = new AtomicLong();
    private final AtomicLong maxResults = new AtomicLong(), maxGroupMembers = new AtomicLong();
    private volatile int baselineBatch = 1;
    public TraceRequests(ValueKind valueKind) {
        this.valueKind = valueKind;
    }

    public ValueKind valueKind() {
        return valueKind;
    }

    /** A write or a search that is not part of a group. */
    public Request request(long id, Operation operation) throws IOException {
        var request = Request.newBuilder().setId(id);
        if (operation.kind() == IndexOperationTrace.SEARCH) {
            if (operation.grouped()) throw new IOException("Group members are sent as one group");
            return countRequest(request.setSearch(search(operation)).build());
        }
        return countRequest(request.setWrite(write(operation)).build());
    }

    /** Reuses a validated Trace payload; only replay IDs and value representation change. */
    public Request forward(long id, Request stored, List<Operation> members) throws IOException {
        Operation first = members.get(0);
        if (first.grouped()) {
            if (!stored.hasGroup() || stored.getGroup().getMembersCount() != members.size()
                    || members.size() != first.expansionSize()) throw new IOException("Incomplete stored search group");
            maxGroupMembers.accumulateAndGet(members.size(), Math::max);
        } else if (members.size() != 1 || (first.kind() == IndexOperationTrace.SEARCH
                ? !stored.hasSearch() : !stored.hasWrite())) {
            throw new IOException("Stored service payload disagrees with operation view");
        }
        var request = stored.toBuilder().setId(id);
        if (stored.hasWrite() && stored.getWrite().getKind() != Write.Kind.LOGICAL_DELETE)
            request.setWrite(forwardWrite(stored.getWrite(), first));
        return countRequest(request.build());
    }

    /** Baseline INSERTs sent as one request; a batch takes one send position. */
    public Request batch(long id, List<Operation> inserts) throws IOException {
        return batch(id, inserts, List.of());
    }

    Request batch(long id, List<Operation> inserts, List<Write> stored) throws IOException {
        if (!stored.isEmpty() && stored.size() != inserts.size())
            throw new IOException("Invalid stored Baseline batch");
        var batch = org.sibench.adapters.proto.WriteBatch.newBuilder();
        for (int i = 0; i < inserts.size(); i++) {
            Operation insert = inserts.get(i);
            if (!baselineInsert(insert)) throw new IOException("Only Baseline INSERTs travel in a batch");
            if (stored.isEmpty()) batch.addWrites(write(insert));
            else batch.addWrites(forwardWrite(stored.get(i), insert));
        }
        return countRequest(Request.newBuilder().setId(id).setBatch(batch).build());
    }

    private Write forwardWrite(Write stored, Operation operation) {
        if (valueKind == ValueKind.ROW_ID && stored.getValue().getFile() == 0
                && stored.getValue().getPosition() == 0) return stored;
        return stored.toBuilder().setValue(value(operation)).build();
    }

    static boolean baselineInsert(Operation operation) {
        return operation.kind() == IndexOperationTrace.INSERT
                && operation.version() == 0
                && operation.transactionId() == 0;
    }

    /** A write batch response carries no results. */
    public void checkBatch(Response response, long id) throws IOException {
        if (response.getId() != id || response.getMore())
            throw new IOException("Invalid final response for write batch " + id);
        if (response.getStatus() != Status.OK)
            throw new IOException("Service " + response.getStatus() + " for a write batch: " + response.getMessage());
        if (response.getResultsCount() != 0) throw new IOException("Write batch returned results");
    }

    /** Baseline INSERTs per batch request; 1 sends each on its own. */
    public void baselineBatch(int writes) {
        if (writes < 1) throw new IllegalArgumentException("A batch holds at least one write");
        baselineBatch = writes;
    }

    int baselineBatch() {
        return baselineBatch;
    }

    private Write.Builder write(Operation operation) throws IOException {
        Write.Kind kind = switch (operation.kind()) {
            case IndexOperationTrace.INSERT -> Write.Kind.INSERT;
            case IndexOperationTrace.UPSERT -> Write.Kind.UPSERT;
            case IndexOperationTrace.LOGICAL_DELETE -> Write.Kind.LOGICAL_DELETE;
            case IndexOperationTrace.RELOCATE -> Write.Kind.RELOCATE;
            default -> throw new IOException("Not an index operation: " + IndexOperationTrace.kindName(operation.kind()));
        };
        var write = Write.newBuilder()
                .setKind(kind)
                .setKey(EntryKey.newBuilder()
                        .setTable(operation.table())
                        .setIndex(operation.index())
                        .setIndexedValue(operation.index() == 0 ? ByteString.EMPTY : bytes(operation.indexedValue()))
                        .setPrimaryKey(bytes(operation.key())))
                .setVersion(operation.version());
        if (kind != Write.Kind.LOGICAL_DELETE) write.setValue(value(operation));
        return write;
    }

    /** The members of one search group, in trace order. */
    public Request group(long id, List<Operation> members) throws IOException {
        Operation first = members.get(0);
        if (members.size() != first.expansionSize()) throw new IOException("Incomplete search group");
        var group = SearchGroup.newBuilder()
                .setMode((first.flags() & IndexOperationTrace.INTERSECT_GROUP) != 0
                        ? SearchGroup.Mode.INTERSECTION
                        : SearchGroup.Mode.UNION);
        for (Operation member : members) group.addMembers(search(member));
        var request = Request.newBuilder().setId(id);
        maxGroupMembers.accumulateAndGet(members.size(), Math::max);
        return countRequest(request.setGroup(group).build());
    }

    private Search search(Operation operation) throws IOException {
        if ((operation.flags() & IndexOperationTrace.TRANSACTION_LOCAL_SEARCH) != 0)
            throw new IOException("Only committed-snapshot searches may reach the service");
        var search = Search.newBuilder()
                .setTable(operation.table())
                .setIndex(operation.index())
                .setDescending((operation.flags() & IndexOperationTrace.DESCENDING) != 0)
                .setLimit(operation.limit())
                .setReadVersion(operation.readTimestamp());
        if (operation.lower() != null)
            search.setLower(Bound.newBuilder().setKey(bytes(operation.lower()))
                    .setInclusive((operation.flags() & IndexOperationTrace.LOWER_INCLUSIVE) != 0));
        if (operation.upper() != null)
            search.setUpper(Bound.newBuilder().setKey(bytes(operation.upper()))
                    .setInclusive((operation.flags() & IndexOperationTrace.UPPER_INCLUSIVE) != 0));
        // A secondary search that names a primary key is pinned to that one entry.
        if (operation.key() != null) search.setPinnedPrimaryKey(bytes(operation.key()));
        return search.build();
    }

    private Value value(Operation operation) {
        return valueKind == ValueKind.ROW_ID
                ? Value.newBuilder().setRowId(operation.rowId()).build()
                : Value.newBuilder().setFile(operation.file()).setPosition(operation.position()).build();
    }

    private static ByteString bytes(IndexKey key) {
        return ByteString.copyFrom(key.bytes());
    }

    /** Checks one complete response against its Trace operation. */
    public void check(Operation operation, Response response) throws IOException {
        receive(operation, response.getId()).finish(response);
    }

    /** The response to request {@code id}, received as one or more chunks. */
    public Received receive(Operation operation, long id) {
        return new Received(operation, id);
    }

    /** Counts streamed results without retaining a complete query result. */
    public final class Received {
        private final Operation operation;
        private final long id;
        private long count, bytes;
        private IOException failure;

        private Received(Operation operation, long id) {
            this.operation = operation;
            this.id = id;
        }

        /** Chunks as a transport delivers them; a failure is reported by {@link #finish}. */
        public java.util.function.Consumer<Response> partial() {
            return chunk -> {
                if (failure != null) return;
                try {
                    add(chunk);
                } catch (IOException error) {
                    failure = error;
                }
            };
        }

        /** A chunk with {@code more = true}: results only. */
        public void add(Response chunk) throws IOException {
            if (chunk.getId() != id || !chunk.getMore() || chunk.getStatus() != Status.OK)
                throw new IOException("Invalid partial response for request " + id);
            if (operation.kind() != IndexOperationTrace.SEARCH)
                throw new IOException("Partial response to a write");
            accept(chunk);
        }

        private void accept(Response chunk) throws IOException {
            count += chunk.getResultsCount();
            if (count > operation.expectedCount())
                throw new IOException("Search count mismatch at source event " + operation.sourceEvent()
                        + ": expected " + operation.expectedCount() + ", found more");
            for (Result result : chunk.getResultsList()) {
                bytes += result.getSerializedSize();
            }
        }

        /** The final response (more = false): status, the remaining results and every check. */
        public void finish(Response response) throws IOException {
            if (failure != null) throw failure;
            if (response.getId() != id) throw new IOException("Response ID differs from request " + id);
            if (response.getMore()) throw new IOException("Final response marked partial");
            if (response.getStatus() != Status.OK)
                throw new IOException("Service " + response.getStatus() + " at source event "
                        + operation.sourceEvent() + ": " + response.getMessage());
            if (operation.kind() != IndexOperationTrace.SEARCH) {
                if (count + response.getResultsCount() != 0) throw new IOException("Write returned results");
                return;
            }
            accept(response);
            if (count != operation.expectedCount())
                throw new IOException("Search count mismatch at source event " + operation.sourceEvent()
                        + ": expected " + operation.expectedCount() + ", found " + count);
            results.addAndGet(count);
            queries.incrementAndGet();
            resultBytes.addAndGet(bytes);
            maxResults.accumulateAndGet(count, Math::max);
        }
    }

    public long checkedResults() {
        return results.get();
    }

    public long queries() {
        return queries.get();
    }

    public long requestBytes() {
        return requestBytes.get();
    }

    public long resultBytes() {
        return resultBytes.get();
    }

    public long maxResults() {
        return maxResults.get();
    }

    public long maxGroupMembers() {
        return maxGroupMembers.get();
    }

    private Request countRequest(Request request) {
        executeRequests.incrementAndGet();
        requestBytes.addAndGet(request.getSerializedSize());
        return request;
    }

    /** Execute request messages; a stream contains many messages. */
    public long executeRequests() {
        return executeRequests.get();
    }
}
