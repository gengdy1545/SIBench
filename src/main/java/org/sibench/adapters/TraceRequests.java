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

/**
 * Maps canonical trace operations to protocol requests and checks each response against the
 * trace: a search returns exactly its native expected count, a group its combined count. The
 * results also fold into the reference replay's order-independent checksum.
 */
public final class TraceRequests {
    private final ValueKind valueKind;
    private final AtomicLong checksum = new AtomicLong(), results = new AtomicLong(), queries = new AtomicLong();
    /** The last send position handed out; requests are built on the single sending thread. */
    private long sequence;

    public TraceRequests(ValueKind valueKind) {
        this.valueKind = valueKind;
    }

    public ValueKind valueKind() {
        return valueKind;
    }

    /** A write or a search that is not part of a group. */
    public Request request(long id, Operation operation) throws IOException {
        var request = Request.newBuilder().setId(id).setSequence(++sequence);
        if (operation.kind() == IndexOperationTrace.SEARCH) {
            if (operation.grouped()) throw new IOException("Group members are sent as one group");
            return request.setSearch(search(operation)).build();
        }
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
        return request.setWrite(write).build();
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
        return Request.newBuilder().setId(id).setSequence(++sequence).setGroup(group).build();
    }

    private Search search(Operation operation) {
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

    /**
     * Checks one response; {@code operation} is the search (a group's last member) or the write
     * it answers.
     */
    public void check(Operation operation, Response response) throws IOException {
        if (response.getStatus() != Status.OK)
            throw new IOException("Service " + response.getStatus() + " at source event "
                    + operation.sourceEvent() + ": " + response.getMessage());
        if (operation.kind() != IndexOperationTrace.SEARCH) {
            if (response.getResultsCount() != 0) throw new IOException("Write returned results");
            return;
        }
        if (response.getResultsCount() != operation.expectedCount())
            throw new IOException("Search count mismatch at source event " + operation.sourceEvent()
                    + ": expected " + operation.expectedCount() + ", found " + response.getResultsCount());
        long words = 0;
        for (Result result : response.getResultsList()) words ^= word(result);
        checksum.getAndAccumulate(words, (a, b) -> a ^ b);
        results.addAndGet(response.getResultsCount());
        queries.incrementAndGet();
    }

    /** The reference replay's word: the row id, or the Locator packed as file << 32 | position. */
    private long word(Result result) {
        return valueKind == ValueKind.ROW_ID
                ? result.getValue().getRowId()
                : (result.getValue().getFile() << 32) | result.getValue().getPosition();
    }

    public long checksum() {
        return checksum.get();
    }

    public long checkedResults() {
        return results.get();
    }

    public long queries() {
        return queries.get();
    }
}
