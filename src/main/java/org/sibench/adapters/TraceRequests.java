package org.sibench.adapters;

import com.google.protobuf.ByteString;

import org.sibench.adapters.proto.Bound;
import org.sibench.adapters.proto.Commit;
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
import org.sibench.replay.QueryDigest;
import org.sibench.trace.IndexKey;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Maps canonical trace operations to protocol requests and checks each response against the
 * trace. A search's native count remains the source fact; when an expected-result stream is
 * supplied, its ordered {@code (PK, version, value)} digest must also match, so equal counts with
 * a wrong key, version, Locator, order or a query swapped with another cannot pass.
 */
public final class TraceRequests {
    private record Expected(
            long sourceEvent, long workUnitId, String digest, List<Operation> members) {}

    private final ValueKind valueKind;
    private final AtomicLong results = new AtomicLong(), queries = new AtomicLong();
    private final BufferedReader expectedInput;
    private String pendingExpectedLine;
    private final Map<Long, Expected> expected = new ConcurrentHashMap<>();
    private final AtomicLong skippedQueries = new AtomicLong();
    private boolean snapshotReads;
    private final AtomicLong digestChecks = new AtomicLong(), digestMismatches = new AtomicLong();
    private final AtomicLong requestBytes = new AtomicLong(), resultBytes = new AtomicLong();
    private final AtomicLong maxResults = new AtomicLong(), maxExpected = new AtomicLong(), maxGroupMembers = new AtomicLong();
    private volatile int baselineBatch = 1;
    /** The source transaction of the requests being built and its sent-operation counters. */
    private long currentTransaction, currentOperation, currentWrites;

    public TraceRequests(ValueKind valueKind) {
        this.valueKind = valueKind;
        this.expectedInput = null;
    }

    /** {@code expectedResults}: a {@link QueryDigest} stream in trace order, or null. */
    public TraceRequests(ValueKind valueKind, Path expectedResults) throws IOException {
        this(valueKind, expectedResults, null);
    }

    /**
     * {@code binding} must equal the digest stream's header: trace / baseline identity, operation
     * value mode and secondary-index coverage. It stops a digest stream generated
     * for another trace or filter setting from being accepted by query order alone.
     */
    public TraceRequests(ValueKind valueKind, Path expectedResults, String binding)
            throws IOException {
        this.valueKind = valueKind;
        if (expectedResults == null) {
            this.expectedInput = null;
            return;
        }
        this.expectedInput =
                Files.newBufferedReader(expectedResults, StandardCharsets.UTF_8);
        String first = expectedInput.readLine();
        if (binding != null) {
            if (first == null || !first.equals("# " + binding)) {
                expectedInput.close();
                throw new IOException(
                        "Expected query digest header does not match this trace/replay: "
                                + first);
            }
        } else if (first != null && !first.startsWith("# ")) {
            pendingExpectedLine = first;
        }
    }

    /**
     * Searches of an index that already holds the whole trace: they go out as plain snapshot reads
     * at their read version, without the transaction position a live replay sends.
     */
    public void snapshotReads() {
        snapshotReads = true;
    }

    /** Consumes the expected digest of a search that is deliberately not sent, keeping order. */
    public void skipExpected(List<Operation> members) throws IOException {
        if (expectedInput == null) return;
        expect(-1, members);
        expected.remove(-1L);
        skippedQueries.incrementAndGet();
    }

    public long skippedQueries() {
        return skippedQueries.get();
    }

    public ValueKind valueKind() {
        return valueKind;
    }

    /** A write or a search that is not part of a group. */
    public Request request(long id, Operation operation) throws IOException {
        var request = Request.newBuilder().setId(id);
        if (operation.kind() == IndexOperationTrace.SEARCH) {
            if (operation.grouped()) throw new IOException("Group members are sent as one group");
            expect(id, List.of(operation));
            transaction(request, operation);
            return countRequest(request.setSearch(search(operation)).build());
        }
        transaction(request, operation);
        return countRequest(request.setWrite(write(operation)).build());
    }

    /** Baseline INSERTs sent as one request; a batch takes one send position. */
    public Request batch(long id, List<Operation> inserts) throws IOException {
        var batch = org.sibench.adapters.proto.WriteBatch.newBuilder();
        for (Operation insert : inserts) {
            if (!baselineInsert(insert)) throw new IOException("Only Baseline INSERTs travel in a batch");
            batch.addWrites(write(insert));
        }
        return countRequest(Request.newBuilder().setId(id).setBatch(batch).build());
    }

    static boolean baselineInsert(Operation operation) {
        return operation.kind() == IndexOperationTrace.INSERT
                && operation.version() == 0
                && operation.transactionId() == 0;
    }

    /** A write batch response carries no results. */
    public void checkBatch(Response response) throws IOException {
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
        expect(id, members);
        var group = SearchGroup.newBuilder()
                .setMode((first.flags() & IndexOperationTrace.INTERSECT_GROUP) != 0
                        ? SearchGroup.Mode.INTERSECTION
                        : SearchGroup.Mode.UNION);
        for (Operation member : members) group.addMembers(search(member));
        var request = Request.newBuilder().setId(id);
        transaction(request, first);
        maxGroupMembers.accumulateAndGet(members.size(), Math::max);
        return countRequest(request.setGroup(group).build());
    }

    /** Marks the request with its source transaction and sent-operation position. */
    private void transaction(Request.Builder request, Operation operation) throws IOException {
        long transaction = operation.transactionId();
        if (transaction <= 0 || snapshotReads) return;
        if (currentTransaction != transaction) {
            currentTransaction = transaction;
            currentOperation = 0;
            currentWrites = 0;
        }
        currentOperation++;
        if (operation.kind() == IndexOperationTrace.UPSERT
                || operation.kind() == IndexOperationTrace.LOGICAL_DELETE
                || operation.kind() == IndexOperationTrace.RELOCATE) currentWrites++;
        request.setTransactionId(transaction).setTransactionOperation(currentOperation);
    }

    /**
     * The complete-commit marker of one source transaction. {@code writes} and {@code operations}
     * count the requests actually sent, after secondary filtering; a group is one operation.
     */
    public Request commit(long id, long transactionId, long version) throws IOException {
        if (transactionId <= 0 || version <= 0)
            throw new IOException("Invalid transaction commit marker");
        if (currentTransaction != 0 && currentTransaction != transactionId)
            throw new IOException("Commit marker does not follow its transaction");
        var request =
                Request.newBuilder()
                        .setId(id)

                        .setTransactionId(transactionId)
                        .setTransactionOperation(0)
                        .setCommit(
                                Commit.newBuilder()
                                        .setVersion(version)
                                        .setWrites(currentWrites)
                                        .setOperations(currentOperation));
        currentTransaction = 0;
        currentOperation = 0;
        currentWrites = 0;
        return countRequest(request.build());
    }

    private Search search(Operation operation) {
        var search = Search.newBuilder()
                .setTable(operation.table())
                .setIndex(operation.index())
                .setDescending((operation.flags() & IndexOperationTrace.DESCENDING) != 0)
                .setLimit(operation.limit())
                .setReadVersion(operation.readTimestamp())
                .setLocalVersion(
                        (operation.flags() & IndexOperationTrace.TRANSACTION_LOCAL_SEARCH) != 0
                                ? operation.version()
                                : 0);
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

    /** Reads the next expected digest, bound to the request's source event and work unit. */
    private void expect(long id, List<Operation> members) throws IOException {
        if (expectedInput == null) return;
        String line = pendingExpectedLine;
        pendingExpectedLine = null;
        if (line == null) line = expectedInput.readLine();
        Operation first = members.get(0);
        if (line == null)
            throw new IOException(
                    "Expected query digests exhausted at source event "
                            + first.sourceEvent()
                            + " (work unit "
                            + first.workUnitId()
                            + ")");
        String[] fields = line.split("\t", -1);
        if (fields.length != 3)
            throw new IOException("Invalid expected query digest record: " + line);
        long event = Long.parseLong(fields[0]), unit = Long.parseLong(fields[1]);
        if (event != first.sourceEvent() || unit != first.workUnitId())
            throw new IOException(
                    "Expected query digest order mismatch at request "
                            + id
                            + ": trace "
                            + first.sourceEvent()
                            + "/"
                            + first.workUnitId()
                            + ", expected stream "
                            + event
                            + "/"
                            + unit);
        expected.put(id, new Expected(event, unit, fields[2], members));
        maxExpected.accumulateAndGet(expected.size(), Math::max);
    }

    /**
     * Checks one response; {@code operation} is the search (a group's last member) or the write
     * it answers. Writes and requests without an expected stream check only the native shape.
     */
    public void check(Operation operation, Response response) throws IOException {
        receive(operation, response.getId()).finish(response);
    }

    /** The response to request {@code id}, received as one or more chunks. */
    public Received receive(Operation operation, long id) {
        return new Received(operation, id);
    }

    /**
     * One response being received: counts and digest accumulate per chunk, so a large
     * result is never held whole. The digest starts from the native expected count; a different
     * number of results fails the count check before the digest is compared.
     */
    public final class Received {
        private final Operation operation;
        private final long id;
        private final Expected wanted;
        private final QueryDigest.Incremental digest;
        private long count, bytes;
        private IOException failure;

        private Received(Operation operation, long id) {
            this.operation = operation;
            this.id = id;
            this.wanted = operation.kind() == IndexOperationTrace.SEARCH ? expected.get(id) : null;
            this.digest =
                    wanted == null
                            ? null
                            : new QueryDigest.Incremental(
                                    wanted.members(),
                                    operation.expectedCount(),
                                    valueKind == ValueKind.ROW_ID);
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
                if (digest != null) digest.add(result);
            }
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
                if (expected.remove(response.getId()) != null)
                    throw new IOException("Expected a search result but the request was a write");
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
            if (wanted != null) {
                expected.remove(id);
                String actual = digest.hex();
                if (!actual.equals(wanted.digest())) {
                    digestMismatches.incrementAndGet();
                    throw new IOException(
                            "Search results differ at source event "
                                    + wanted.sourceEvent()
                                    + " (work unit "
                                    + wanted.workUnitId()
                                    + "): expected digest "
                                    + wanted.digest()
                                    + ", got "
                                    + actual);
                }
                digestChecks.incrementAndGet();
            }
        }
    }

    /** A commit marker response carries no results. */
    public void checkCommit(Response response) throws IOException {
        if (response.getStatus() != Status.OK)
            throw new IOException(
                    "Service " + response.getStatus() + " for commit marker: "
                            + response.getMessage());
        if (response.getResultsCount() != 0)
            throw new IOException("Commit marker returned results");
    }

    public long checkedResults() {
        return results.get();
    }

    public long queries() {
        return queries.get();
    }

    public long digestChecks() {
        return digestChecks.get();
    }

    public long digestMismatches() {
        return digestMismatches.get();
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

    public long maxExpected() {
        return maxExpected.get();
    }

    public long maxGroupMembers() {
        return maxGroupMembers.get();
    }

    /**
     * Ends an expected-result comparison: no line may remain and every expected response must
     * have been checked.
     */
    public void finish() throws IOException {
        if (expectedInput == null) return;
        if (pendingExpectedLine != null || expectedInput.readLine() != null)
            throw new IOException("Expected query digests have unused records");
        if (!expected.isEmpty())
            throw new IOException(
                    "Expected query digests have no response for request " + expected.keySet());
        if (digestChecks.get() != queries.get())
            throw new IOException(
                    "Query digest coverage mismatch: "
                            + digestChecks
                            + " checked of "
                            + queries
                            + " queries");
        expectedInput.close();
    }

    private Request countRequest(Request request) {
        requestBytes.addAndGet(request.getSerializedSize());
        return request;
    }
}
