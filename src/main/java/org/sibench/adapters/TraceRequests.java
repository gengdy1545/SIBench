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
    private final AtomicLong checksum = new AtomicLong(), results = new AtomicLong(), queries = new AtomicLong();
    private final BufferedReader expectedInput;
    private String pendingExpectedLine;
    private final Map<Long, Expected> expected = new ConcurrentHashMap<>();
    private final AtomicLong digestChecks = new AtomicLong(), digestMismatches = new AtomicLong();
    /** The last send position handed out; requests are built on the single sending thread. */
    private long sequence;
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
     * trace version, value mode and secondary-index coverage. It stops a digest stream generated
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

    public ValueKind valueKind() {
        return valueKind;
    }

    /** A write or a search that is not part of a group. */
    public Request request(long id, Operation operation) throws IOException {
        var request = Request.newBuilder().setId(id).setSequence(++sequence);
        if (operation.kind() == IndexOperationTrace.SEARCH) {
            if (operation.grouped()) throw new IOException("Group members are sent as one group");
            expect(id, List.of(operation));
            transaction(request, operation);
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
        transaction(request, operation);
        return request.setWrite(write).build();
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
        var request = Request.newBuilder().setId(id).setSequence(++sequence);
        transaction(request, first);
        return request.setGroup(group).build();
    }

    /** Marks the request with its source transaction and sent-operation position. */
    private void transaction(Request.Builder request, Operation operation) throws IOException {
        long transaction = operation.transactionId();
        if (transaction <= 0) return;
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
                        .setSequence(++sequence)
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
        return request.build();
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
    }

    /**
     * Checks one response; {@code operation} is the search (a group's last member) or the write
     * it answers. Writes and requests without an expected stream check only the native shape.
     */
    public void check(Operation operation, Response response) throws IOException {
        if (response.getStatus() != Status.OK)
            throw new IOException("Service " + response.getStatus() + " at source event "
                    + operation.sourceEvent() + ": " + response.getMessage());
        if (operation.kind() == IndexOperationTrace.SEARCH) {
            if (response.getResultsCount() != operation.expectedCount())
                throw new IOException("Search count mismatch at source event " + operation.sourceEvent()
                        + ": expected " + operation.expectedCount() + ", found " + response.getResultsCount());
            long words = 0;
            for (Result result : response.getResultsList()) words ^= word(result);
            checksum.getAndAccumulate(words, (a, b) -> a ^ b);
            results.addAndGet(response.getResultsCount());
            queries.incrementAndGet();
            Expected wanted = expected.remove(response.getId());
            if (wanted != null) {
                String actual =
                        QueryDigest.ofResponse(
                                wanted.members(),
                                response.getResultsList(),
                                valueKind == ValueKind.ROW_ID);
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
            return;
        }
        if (response.getResultsCount() != 0) throw new IOException("Write returned results");
        if (expected.remove(response.getId()) != null)
            throw new IOException("Expected a search result but the request was a write");
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

    public long digestChecks() {
        return digestChecks.get();
    }

    public long digestMismatches() {
        return digestMismatches.get();
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
}