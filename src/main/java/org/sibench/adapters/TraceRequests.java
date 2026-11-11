package org.sibench.adapters;

import com.google.protobuf.ByteString;

import org.sibench.adapters.proto.*;
import com.google.protobuf.Message;
import org.sibench.trace.IndexEntry;
import org.sibench.capture.IndexOperationTrace;
import org.sibench.capture.IndexOperationTrace.Operation;

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

    /** Encodes a source operation for Trace export. */
    public Message request(Operation operation) throws IOException {
        if (operation.grouped()) throw new IOException("Grouped queries require member metadata");
        if (operation.kind() == IndexOperationTrace.SEARCH) return query(operation);
        var key = new IndexEntry(operation.table(), operation.index(), operation.indexedValue(), operation.key()).bytes();
        if (operation.kind() == IndexOperationTrace.LOGICAL_DELETE)
            return DeleteRequest.newBuilder().setKey(key).setTs(operation.version()).build();
        if (operation.kind() != IndexOperationTrace.INSERT && operation.kind() != IndexOperationTrace.UPSERT)
            throw new IOException("Not a data operation");
        return PutRequest.newBuilder().setKey(key).setTs(operation.version()).setValue(value(operation)).build();
    }

    /** Stored operations already contain the data RPC payload; only the selected value encoding changes. */
    public Message forward(TraceOperation stored, Operation operation) throws IOException {
        boolean matches = switch (operation.kind()) {
            case IndexOperationTrace.SEARCH -> stored.hasGet() || stored.hasScan();
            case IndexOperationTrace.LOGICAL_DELETE -> stored.hasDelete();
            case IndexOperationTrace.INSERT, IndexOperationTrace.UPSERT -> stored.hasPut();
            default -> false;
        };
        if (operation.grouped() || !matches) throw new IOException("Stored operation shape mismatch");
        return switch (stored.getRequestCase()) {
            case GET -> stored.getGet();
            case SCAN -> stored.getScan();
            case DELETE -> stored.getDelete();
            case PUT -> valueKind == ValueKind.LOCATOR
                    ? stored.getPut().toBuilder().setValue(value(operation)).build() : stored.getPut();
            default -> throw new IOException("Expected a stored data operation");
        };
    }
    static boolean baselineInsert(Operation operation) {
        return operation.kind() == IndexOperationTrace.INSERT && operation.version() == 0 && operation.transactionId() == 0;
    }
    public void checkBatch(IndexResponse response) throws IOException {
        if (response.more() || response.status() != IndexStatus.OK || !response.values().isEmpty())
            throw new IOException("Invalid write batch response: " + response.message());
    }
    public void baselineBatch(int writes) {
        if (writes < 1) throw new IllegalArgumentException("A batch holds at least one write");
        baselineBatch = writes;
    }
    int baselineBatch() { return baselineBatch; }

    /** Encodes a source query group for Trace export. */
    public ReadGroup group(List<Operation> members) throws IOException {
        Operation first = members.get(0);
        if (members.size() != first.expansionSize()) throw new IOException("Incomplete query group");
        var group = ReadGroup.newBuilder().setCombineMode((first.flags() & IndexOperationTrace.INTERSECT_GROUP) != 0
                ? ReadGroup.CombineMode.INTERSECTION : ReadGroup.CombineMode.UNION);
        for (var member : members) {
            var request = query(member);
            var read = ReadRequest.newBuilder();
            if (request instanceof GetRequest get) read.setGet(get); else read.setScan((ScanRequest) request);
            group.addRequests(read);
        }
        return group.build();
    }
    public static Message read(ReadRequest read) throws IOException {
        if (read.hasGet()) return read.getGet();
        if (read.hasScan()) return read.getScan();
        throw new IOException("Missing query member");
    }
    private Message query(Operation operation) throws IOException {
        if ((operation.flags() & IndexOperationTrace.TRANSACTION_LOCAL_SEARCH) != 0)
            throw new IOException("Transaction-local read cannot reach the service");
        boolean exact = operation.range().point() && (operation.index() == 0 || operation.key() != null);
        if (exact) {
            var key = new IndexEntry(operation.table(), operation.index(), operation.index() == 0 ? null : operation.lower(),
                    operation.index() == 0 ? operation.lower() : operation.key()).bytes();
            return GetRequest.newBuilder().setKey(key).setReadTs(operation.readTimestamp()).build();
        }
        var scan = ScanRequest.newBuilder().setReadTs(operation.readTimestamp()).setLimit(operation.limit())
                .setReverse((operation.flags() & IndexOperationTrace.DESCENDING) != 0)
                .setStartKey(IndexEntry.bound(operation.table(), operation.index(), operation.lower(),
                        operation.lower() != null && !operation.range().lowerInclusive()))
                .setEndKey(IndexEntry.bound(operation.table(), operation.index(), operation.upper(),
                        operation.upper() == null || operation.range().upperInclusive()));
        return scan.build();
    }
    private ByteString value(Operation operation) {
        return valueKind == ValueKind.ROW_ID ? IndexEntry.rowId(operation.rowId())
                : IndexEntry.locator(operation.file(), operation.position());
    }

    /** At one table/snapshot, complete index maintenance gives each row the same value in every member. */
    public static List<ByteString> combine(ReadGroup group, List<List<ByteString>> answers) throws IOException {
        if (group.getRequestsCount() < 2 || answers.size() != group.getRequestsCount()) throw new IOException("Invalid query group");
        boolean intersect = group.getCombineMode() == ReadGroup.CombineMode.INTERSECTION;
        if (!intersect && group.getCombineMode() != ReadGroup.CombineMode.UNION) throw new IOException("Unknown group mode");
        var combined = new java.util.LinkedHashSet<ByteString>();
        int table = -1; long ts = -1;
        for (int i = 0; i < answers.size(); i++) {
            var member = group.getRequests(i);
            ByteString key = member.hasGet() ? member.getGet().getKey() : member.getScan().getStartKey();
            if (key.size() < 8) throw new IOException("Missing group namespace");
            int current = key.asReadOnlyByteBuffer().getInt();
            long readTs = member.hasGet() ? member.getGet().getReadTs() : member.getScan().getReadTs();
            if (i > 0 && (table != current || ts != readTs)) throw new IOException("Group spans tables or read points");
            table = current; ts = readTs;
            if (i == 0 || !intersect) combined.addAll(answers.get(i));
            else combined.retainAll(new java.util.HashSet<>(answers.get(i)));
        }
        return List.copyOf(combined);
    }

    /** Checks one complete response against its Trace operation. */
    public void check(Operation operation, IndexResponse response) throws IOException {
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
        public java.util.function.Consumer<IndexResponse> partial() {
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
        public void add(IndexResponse chunk) throws IOException {
            if (!chunk.more() || chunk.status() != IndexStatus.OK)
                throw new IOException("Invalid partial response for request " + id);
            if (operation.kind() != IndexOperationTrace.SEARCH)
                throw new IOException("Partial response to a write");
            accept(chunk);
        }

        private void accept(IndexResponse chunk) throws IOException {
            count += chunk.values().size();
            if (count > operation.expectedCount())
                throw new IOException("Search count mismatch at source event " + operation.sourceEvent()
                        + ": expected " + operation.expectedCount() + ", found more");
        }

        /** The final response (more = false): status, the remaining results and every check. */
        public void finish(IndexResponse response) throws IOException {
            if (failure != null) throw failure;
            if (response.more()) throw new IOException("Final response marked partial");
            if (response.status() != IndexStatus.OK)
                throw new IOException("Service " + response.status() + " at source event "
                        + operation.sourceEvent() + ": " + response.message());
            if (operation.kind() != IndexOperationTrace.SEARCH) {
                if (count + response.values().size() != 0) throw new IOException("Write returned results");
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

    void recordReadGroup(int requests) {
        maxGroupMembers.accumulateAndGet(requests, Math::max);
    }

    public long maxGroupMembers() {
        return maxGroupMembers.get();
    }

    void rpcRequest(com.google.protobuf.MessageLite request, String kind) {
        rpcRequests.incrementAndGet();
        requestBytes.addAndGet(request.getSerializedSize());
        rpcKinds.computeIfAbsent(kind, key -> new AtomicLong()).incrementAndGet();
    }

    void rpcResults(List<ByteString> items) {
        returnedResults.addAndGet(items.size());
        for (var item : items) resultBytes.addAndGet(item.size());
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
