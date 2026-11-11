package org.sibench.adapters;

import com.google.protobuf.ByteString;

import org.sibench.adapters.proto.*;
import org.sibench.trace.IndexEntry;
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
    private final AtomicLong returnedResults = new AtomicLong();
    private final AtomicLong rpcRequests = new AtomicLong();
    private final java.util.concurrent.ConcurrentHashMap<String, AtomicLong> rpcKinds = new java.util.concurrent.ConcurrentHashMap<>();
    private final AtomicLong maxResults = new AtomicLong(), maxGroupMembers = new AtomicLong();
    private volatile int baselineBatch = 1;
    public TraceRequests(ValueKind valueKind) {
        this.valueKind = valueKind;
    }

    public ValueKind valueKind() {
        return valueKind;
    }

    public Request request(Operation operation) throws IOException {
        if (operation.grouped()) throw new IOException("Grouped queries require member metadata");
        if (operation.kind() == IndexOperationTrace.SEARCH) return query(operation);
        var key = new IndexEntry(operation.table(), operation.index(), operation.indexedValue(), operation.key()).bytes();
        if (operation.kind() == IndexOperationTrace.LOGICAL_DELETE)
            return Request.newBuilder().setDelete(DeleteRequest.newBuilder().setKey(key).setTs(operation.version())).build();
        if (operation.kind() != IndexOperationTrace.INSERT && operation.kind() != IndexOperationTrace.UPSERT)
            throw new IOException("Not a data operation");
        return Request.newBuilder().setPut(PutRequest.newBuilder().setKey(key).setTs(operation.version())
                .setValue(value(operation))).build();
    }

    /** Stored operations already contain the data RPC payload; only the selected value encoding changes. */
    public Request forward(Request stored, List<Operation> members) throws IOException {
        Operation first = members.get(0);
        if (first.grouped()) {
            if (!stored.hasGroup() || stored.getGroup().getMembersCount() != members.size())
                throw new IOException("Incomplete stored query group");
            maxGroupMembers.accumulateAndGet(members.size(), Math::max);
        } else if (members.size() != 1 || (first.kind() == IndexOperationTrace.SEARCH
                ? !(stored.hasGet() || stored.hasScan()) : first.kind() == IndexOperationTrace.LOGICAL_DELETE
                ? !stored.hasDelete() : !stored.hasPut())) throw new IOException("Stored operation shape mismatch");
        if (stored.hasPut() && valueKind == ValueKind.LOCATOR)
            return stored.toBuilder().setPut(stored.getPut().toBuilder().setValue(value(first))).build();
        return stored;
    }

    public Request batch(List<Operation> inserts) throws IOException { return batch(inserts, List.of()); }
    Request batch(List<Operation> inserts, List<PutRequest> stored) throws IOException {
        if (!stored.isEmpty() && stored.size() != inserts.size()) throw new IOException("Invalid stored baseline batch");
        var batch = BatchWriteRequest.newBuilder();
        for (int i = 0; i < inserts.size(); i++) {
            var insert = inserts.get(i);
            if (!baselineInsert(insert)) throw new IOException("Only baseline puts are batched by replay");
            PutRequest put = stored.isEmpty() ? request(insert).getPut() : stored.get(i);
            if (valueKind == ValueKind.LOCATOR) put = put.toBuilder().setValue(value(insert)).build();
            batch.addMutations(Mutation.newBuilder().setPut(put));
        }
        return Request.newBuilder().setBatch(batch).build();
    }
    static boolean baselineInsert(Operation operation) {
        return operation.kind() == IndexOperationTrace.INSERT && operation.version() == 0 && operation.transactionId() == 0;
    }
    public void checkBatch(Response response) throws IOException {
        if (response.getMore() || response.getStatus() != Status.OK || response.getResultsCount() != 0)
            throw new IOException("Invalid write batch response: " + response.getMessage());
    }
    public void baselineBatch(int writes) {
        if (writes < 1) throw new IllegalArgumentException("A batch holds at least one write");
        baselineBatch = writes;
    }
    int baselineBatch() { return baselineBatch; }

    public Request group(List<Operation> members) throws IOException {
        Operation first = members.get(0);
        if (members.size() != first.expansionSize()) throw new IOException("Incomplete query group");
        var group = SearchGroup.newBuilder().setMode((first.flags() & IndexOperationTrace.INTERSECT_GROUP) != 0
                ? SearchGroup.Mode.INTERSECTION : SearchGroup.Mode.UNION);
        for (var member : members) {
            var request = query(member);
            var read = ReadRequest.newBuilder();
            if (request.hasGet()) read.setGet(request.getGet()); else read.setScan(request.getScan());
            group.addMembers(read);
        }
        maxGroupMembers.accumulateAndGet(members.size(), Math::max);
        return Request.newBuilder().setGroup(group).build();
    }
    static Request read(ReadRequest read) throws IOException {
        if (read.hasGet()) return Request.newBuilder().setGet(read.getGet()).build();
        if (read.hasScan()) return Request.newBuilder().setScan(read.getScan()).build();
        throw new IOException("Missing query member");
    }
    Request query(Operation operation) throws IOException {
        if ((operation.flags() & IndexOperationTrace.TRANSACTION_LOCAL_SEARCH) != 0)
            throw new IOException("Transaction-local read cannot reach the service");
        boolean exact = operation.range().point() && (operation.index() == 0 || operation.key() != null);
        if (exact) {
            var key = new IndexEntry(operation.table(), operation.index(), operation.index() == 0 ? null : operation.lower(),
                    operation.index() == 0 ? operation.lower() : operation.key()).bytes();
            return Request.newBuilder().setGet(GetRequest.newBuilder().setKey(key).setReadTs(operation.readTimestamp())).build();
        }
        var scan = ScanRequest.newBuilder().setReadTs(operation.readTimestamp()).setLimit(operation.limit())
                .setReverse((operation.flags() & IndexOperationTrace.DESCENDING) != 0)
                .setStartKey(IndexEntry.bound(operation.table(), operation.index(), operation.lower(),
                        operation.lower() != null && !operation.range().lowerInclusive()))
                .setEndKey(IndexEntry.bound(operation.table(), operation.index(), operation.upper(),
                        operation.upper() == null || operation.range().upperInclusive()));
        return Request.newBuilder().setScan(scan).build();
    }
    private ByteString value(Operation operation) {
        return valueKind == ValueKind.ROW_ID ? IndexEntry.rowId(operation.rowId())
                : IndexEntry.locator(operation.file(), operation.position());
    }

    /** Groups are a replay concern; full index keys differ between indexes, so combine by table/PK. */
    public static List<Result> combine(SearchGroup group, List<List<Result>> answers) throws IOException {
        if (group.getMembersCount() < 2 || answers.size() != group.getMembersCount()) throw new IOException("Invalid query group");
        boolean intersect = group.getMode() == SearchGroup.Mode.INTERSECTION;
        if (!intersect && group.getMode() != SearchGroup.Mode.UNION) throw new IOException("Unknown group mode");
        var combined = new java.util.LinkedHashMap<IndexKey, Result>();
        int table = -1; long ts = -1;
        for (int i = 0; i < answers.size(); i++) {
            var member = group.getMembers(i);
            ByteString key = member.hasGet() ? member.getGet().getKey() : member.getScan().getStartKey();
            if (key.size() < 8) throw new IOException("Missing group namespace");
            int current = key.asReadOnlyByteBuffer().getInt();
            long readTs = member.hasGet() ? member.getGet().getReadTs() : member.getScan().getReadTs();
            if (i > 0 && (table != current || ts != readTs)) throw new IOException("Group spans tables or read points");
            table = current; ts = readTs;
            var keys = new java.util.HashSet<IndexKey>();
            for (Result result : answers.get(i)) {
                var entry = IndexEntry.decode(result.getKey());
                if (entry.table() != table) throw new IOException("Result outside query table");
                keys.add(entry.primaryKey());
                if (i == 0 || !intersect) combined.putIfAbsent(entry.primaryKey(), result);
            }
            if (i > 0 && intersect) combined.keySet().retainAll(keys);
        }
        return List.copyOf(combined.values());
    }

    /** Checks one complete response against its Trace operation. */
    public void check(Operation operation, Response response) throws IOException {
        receive(operation, operation.sourceEvent()).finish(response);
    }

    /** The response to request {@code id}, received as one or more chunks. */
    public Received receive(Operation operation, long id) {
        return new Received(operation, id);
    }

    /** Counts streamed results without retaining a complete query result. */
    public final class Received {
        private final Operation operation;
        private final long id;
        private long count;
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
            if (!chunk.getMore() || chunk.getStatus() != Status.OK)
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
        }

        /** The final response (more = false): status, the remaining results and every check. */
        public void finish(Response response) throws IOException {
            if (failure != null) throw failure;
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

    void rpcRequest(com.google.protobuf.MessageLite request, String kind) {
        rpcRequests.incrementAndGet();
        requestBytes.addAndGet(request.getSerializedSize());
        rpcKinds.computeIfAbsent(kind, key -> new AtomicLong()).incrementAndGet();
    }

    void rpcResults(List<Result> items) {
        returnedResults.addAndGet(items.size());
        for (var item : items) resultBytes.addAndGet(item.getSerializedSize());
    }

    public long returnedResults() { return returnedResults.get(); }

    public java.util.Map<String, Long> rpcKinds() {
        var result = new java.util.TreeMap<String, Long>();
        rpcKinds.forEach((kind, count) -> result.put(kind, count.get()));
        return result;
    }

    /** Actual data RPC calls, including one call per batch or search-group member. */
    public long rpcRequests() {
        return rpcRequests.get();
    }
}
