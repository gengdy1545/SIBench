package org.sibench.adapters;

import static org.junit.Assert.*;

import com.google.protobuf.ByteString;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.adapters.proto.Bound;
import org.sibench.adapters.proto.Request;
import org.sibench.adapters.proto.Result;
import org.sibench.adapters.proto.Search;
import org.sibench.adapters.proto.SearchGroup;
import org.sibench.adapters.proto.Status;
import org.sibench.adapters.proto.ValueKind;
import org.sibench.capture.IndexOperationTrace;
import org.sibench.capture.IndexOperationTrace.Operation;
import org.sibench.capture.IndexOperationTrace.Range;
import org.sibench.capture.IndexReferenceReplay;
import org.sibench.capture.IndexReferenceReplay.LookupResult;
import org.sibench.capture.IndexReferenceSpillTest;
import org.sibench.trace.IndexKey;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/** The RocksDB backend answers every search exactly as the reference model does. */
public class RocksDbBackendTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    private static final int TABLE = IndexReferenceSpillTest.TABLE, ROWS = IndexReferenceSpillTest.ROWS;

    private RocksDbBackend load(List<Operation> operations, ValueKind kind) throws Exception {
        var backend = RocksDbBackend.create(temporary.getRoot().toPath().resolve("db-" + kind + "-" + System.nanoTime()), kind);
        var requests = new TraceRequests(kind);
        long id = 0;
        for (Operation operation : operations) {
            var response = IndexExecution.execute(backend, requests.request(++id, operation));
            assertEquals(response.getMessage(), Status.OK, response.getStatus());
        }
        return backend;
    }

    private static Search search(int index, Range range, long time) {
        var search = Search.newBuilder().setTable(TABLE).setIndex(index).setReadVersion(time)
                .setDescending(range.descending()).setLimit(range.limit());
        if (range.lower() != null)
            search.setLower(Bound.newBuilder().setKey(ByteString.copyFrom(range.lower().bytes())).setInclusive(range.lowerInclusive()));
        if (range.upper() != null)
            search.setUpper(Bound.newBuilder().setKey(ByteString.copyFrom(range.upper().bytes())).setInclusive(range.upperInclusive()));
        return search.build();
    }

    private static void same(String what, List<LookupResult> expected, List<Result> actual, boolean rowIds) {
        assertEquals(what + " size", expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            LookupResult e = expected.get(i);
            Result a = actual.get(i);
            assertArrayEquals(what + " key " + i, e.key().bytes(), a.getPrimaryKey().toByteArray());
            assertEquals(what + " version " + i, e.commitVersion(), a.getVersion());
            if (rowIds) assertEquals(what + " row id " + i, e.value(), a.getValue().getRowId());
            else {
                assertEquals(what + " file " + i, e.value(), a.getValue().getFile());
                assertEquals(what + " position " + i, e.position(), a.getValue().getPosition());
            }
        }
    }

    @Test
    public void answersLikeTheReferenceModel() throws Exception {
        for (boolean rowIds : new boolean[] {false, true}) {
            for (long seed : new long[] {1, 2, 3}) {
                var operations = IndexReferenceSpillTest.stream(rowIds, seed);
                long last = operations.stream().mapToLong(Operation::version).max().orElse(0);
                var kind = rowIds ? ValueKind.ROW_ID : ValueKind.LOCATOR;
                try (var model = new IndexReferenceReplay(rowIds, true); var backend = load(operations, kind)) {
                    for (Operation operation : operations) model.accept(operation);
                    var random = new Random(seed);
                    for (long time = 0; time <= last; time += 1 + random.nextInt(7)) {
                        for (int index = 0; index <= 2; index++) {
                            int top = index == 0 ? ROWS + 40 : index == 1 ? IndexReferenceSpillTest.VALUES : 5;
                            for (int trial = 0; trial < 12; trial++) {
                                IndexKey lower = random.nextInt(4) == 0 ? null : IndexKey.ofInt(random.nextInt(top + 4) - 2);
                                IndexKey upper = random.nextInt(4) == 0 ? null : IndexKey.ofInt(random.nextInt(top + 4) - 2);
                                if (trial % 3 == 0) upper = lower;
                                Range range = new Range(lower, random.nextBoolean() || trial % 3 == 0, upper,
                                        random.nextBoolean() || trial % 3 == 0, random.nextBoolean(),
                                        random.nextBoolean() ? 0 : 1 + random.nextInt(9));
                                same("index " + index + " at " + time + " " + range,
                                        model.search(TABLE, index, range, time), backend.search(search(index, range, time)), rowIds);
                            }
                        }
                        // Pinned entries: one (value, PK) of a secondary index.
                        for (int id = 1; id <= ROWS + 40; id += 1 + random.nextInt(20)) {
                            IndexKey value = IndexKey.ofInt(random.nextInt(IndexReferenceSpillTest.VALUES));
                            var pinned = Search.newBuilder(search(1, Range.equal(value), time))
                                    .setPinnedPrimaryKey(ByteString.copyFrom(IndexKey.ofInt(id).bytes())).build();
                            LookupResult entry = model.lookupEntry(TABLE, 1, IndexKey.ofInt(id), value, time);
                            same("pinned " + id + " at " + time, entry == null ? List.of() : List.of(entry),
                                    backend.search(pinned), rowIds);
                        }
                    }
                }
            }
        }
    }

    @Test
    public void transactionLocalReadSeesOwnWritesAndKeepsTheStatementSnapshot() throws Exception {
        int table = 1;
        var operations = new ArrayList<Operation>();
        for (int id = 1; id <= 2; id++) {
            Operation row =
                    IndexOperationTrace.write(
                            1,
                            IndexOperationTrace.INSERT,
                            table,
                            0,
                            IndexKey.ofInt(id),
                            0,
                            1,
                            id - 1,
                            id);
            operations.add(row);
            operations.add(row.withIndex(1, IndexKey.ofInt(1), IndexOperationTrace.INSERT));
        }
        // Another transaction moves row 2 from value 1 to value 2 at version 1.
        Operation other =
                IndexOperationTrace.write(
                                2,
                                IndexOperationTrace.UPSERT,
                                table,
                                0,
                                IndexKey.ofInt(2),
                                1,
                                1,
                                2,
                                2)
                        .withTransaction(1, 1);
        operations.add(other);
        operations.add(
                other.withIndex(1, IndexKey.ofInt(1), IndexOperationTrace.LOGICAL_DELETE));
        operations.add(other.withIndex(1, IndexKey.ofInt(2), IndexOperationTrace.UPSERT));
        // Our transaction moves row 1 from value 1 to value 2 at version 2.
        Operation own =
                IndexOperationTrace.write(
                                3,
                                IndexOperationTrace.UPSERT,
                                table,
                                0,
                                IndexKey.ofInt(1),
                                2,
                                1,
                                2,
                                1)
                        .withTransaction(2, 1);
        operations.add(own);
        operations.add(
                own.withIndex(1, IndexKey.ofInt(1), IndexOperationTrace.LOGICAL_DELETE));
        operations.add(own.withIndex(1, IndexKey.ofInt(2), IndexOperationTrace.UPSERT));

        Operation local =
                IndexOperationTrace.search(
                                4,
                                table,
                                1,
                                Range.equal(IndexKey.ofInt(2)),
                                0,
                                1,
                                IndexOperationTrace.TRANSACTION_LOCAL_SEARCH)
                        .withTransaction(2, 1)
                        .withVersion(2);
        Operation localPrimary =
                IndexOperationTrace.search(
                                5,
                                table,
                                0,
                                Range.equal(IndexKey.ofInt(1)),
                                0,
                                1,
                                IndexOperationTrace.TRANSACTION_LOCAL_SEARCH)
                        .withTransaction(2, 1)
                        .withVersion(2);
        try (var backend = load(operations, ValueKind.ROW_ID)) {
            var requests = new TraceRequests(ValueKind.ROW_ID);
            var request = requests.request(1, local);
            assertEquals(2, request.getSearch().getLocalVersion());
            assertEquals(0, request.getSearch().getReadVersion());
            var response = IndexExecution.execute(backend, request);
            assertEquals(response.getMessage(), Status.OK, response.getStatus());
            // Hand-derived oracle: own row 1 moved to value 2; other row 2 is still at value 1.
            var ownResult = List.of(new LookupResult(IndexKey.ofInt(1), 2, 1, 0));
            same("own secondary", ownResult, response.getResultsList(), true);
            same("own primary", ownResult,
                    IndexExecution.execute(backend, requests.request(2, localPrimary)).getResultsList(), true);
            Search oldSecondary = search(1, Range.equal(IndexKey.ofInt(1)), 0).toBuilder()
                    .setTable(table).setLocalVersion(2).build();
            same("other transaction remains at snapshot", List.of(new LookupResult(IndexKey.ofInt(2), 0, 2, 0)),
                    backend.search(oldSecondary), true);

            // Same transaction, later prefix: delete then reinsert the same PK with a new RowID.
            Operation deleted = IndexOperationTrace.write(6, IndexOperationTrace.LOGICAL_DELETE,
                    table, 0, IndexKey.ofInt(1), 2, 0, 0, 1).withTransaction(2, 2);
            assertEquals(Status.OK, IndexExecution.execute(backend, requests.request(3, deleted)).getStatus());
            assertEquals(Status.OK, IndexExecution.execute(backend, requests.request(4,
                    deleted.withIndex(1, IndexKey.ofInt(2), IndexOperationTrace.LOGICAL_DELETE))).getStatus());
            assertTrue(backend.search(request.getSearch()).isEmpty());
            assertTrue(backend.search(requests.request(5, localPrimary).getSearch()).isEmpty());
            same("deletion does not erase the snapshot", List.of(new LookupResult(IndexKey.ofInt(1), 0, 1, 0)),
                    backend.search(search(0, Range.equal(IndexKey.ofInt(1)), 0).toBuilder().setTable(table).build()), true);

            Operation reinserted = IndexOperationTrace.write(7, IndexOperationTrace.UPSERT,
                    table, 0, IndexKey.ofInt(1), 2, 1, 3, 101).withTransaction(2, 3);
            assertEquals(Status.OK, IndexExecution.execute(backend, requests.request(6, reinserted)).getStatus());
            assertEquals(Status.OK, IndexExecution.execute(backend, requests.request(7,
                    reinserted.withIndex(1, IndexKey.ofInt(2), IndexOperationTrace.UPSERT))).getStatus());
            var reinsertedResult = List.of(new LookupResult(IndexKey.ofInt(1), 2, 101, 0));
            same("new identity in secondary", reinsertedResult, backend.search(request.getSearch()), true);
            same("new identity in primary", reinsertedResult,
                    backend.search(requests.request(8, localPrimary).getSearch()), true);
        }
    }

    @Test
    public void groupsCombineMembersByPrimaryKey() throws Exception {
        var operations = IndexReferenceSpillTest.stream(true, 4);
        long last = operations.stream().mapToLong(Operation::version).max().orElse(0);
        try (var model = new IndexReferenceReplay(true, true); var backend = load(operations, ValueKind.ROW_ID)) {
            for (Operation operation : operations) model.accept(operation);
            for (int a = 0; a < IndexReferenceSpillTest.VALUES; a += 3)
                for (int b = 0; b < 5; b++) {
                    var first = model.search(TABLE, 1, Range.equal(IndexKey.ofInt(a)), last);
                    var second = model.search(TABLE, 2, Range.equal(IndexKey.ofInt(b)), last);
                    var keys = new ArrayList<IndexKey>();
                    for (var r : second) keys.add(r.key());
                    long intersection = first.stream().filter(r -> keys.contains(r.key())).count();
                    var group = SearchGroup.newBuilder().setMode(SearchGroup.Mode.INTERSECTION)
                            .addMembers(search(1, Range.equal(IndexKey.ofInt(a)), last))
                            .addMembers(search(2, Range.equal(IndexKey.ofInt(b)), last)).build();
                    var response = IndexExecution.execute(backend, Request.newBuilder().setId(1).setGroup(group).build());
                    assertEquals(Status.OK, response.getStatus());
                    assertEquals(intersection, response.getResultsCount());
                    var union = IndexExecution.execute(backend, Request.newBuilder().setId(2).setGroup(
                            group.toBuilder().setMode(SearchGroup.Mode.UNION).build()).build());
                    assertEquals(first.size() + second.size() - intersection, union.getResultsCount());
                }
        }
    }

    @Test
    public void rejectsWhatTheProtocolForbids() throws Exception {
        var operations = IndexReferenceSpillTest.stream(true, 5);
        try (var backend = load(operations, ValueKind.ROW_ID)) {
            var requests = new TraceRequests(ValueKind.ROW_ID);
            // Relocating a version that does not exist.
            var relocate = IndexOperationTrace.write(1, IndexOperationTrace.RELOCATE, TABLE, 0, IndexKey.ofInt(1), 9_999, 0, 0, 1);
            assertEquals(Status.INVALID, IndexExecution.execute(backend, requests.request(1, relocate)).getStatus());
            // An insert after the baseline.
            var insert = IndexOperationTrace.write(2, IndexOperationTrace.INSERT, TABLE, 0, IndexKey.ofInt(1), 3, 0, 0, 1);
            assertEquals(Status.INVALID, IndexExecution.execute(backend, requests.request(2, insert)).getStatus());
        }
        // An index directory is never reused.
        var path = temporary.getRoot().toPath().resolve("once");
        RocksDbBackend.create(path, ValueKind.LOCATOR).close();
        assertThrows(java.io.IOException.class, () -> RocksDbBackend.create(path, ValueKind.LOCATOR));
    }

    @Test
    public void reopenedIndexKeepsItsWatermarkAndRefusesAnotherValueKind() throws Exception {
        java.nio.file.Path path = temporary.getRoot().toPath().resolve("reopen");
        try (var backend = RocksDbBackend.create(path, ValueKind.ROW_ID)) {
            assertEquals(0, backend.publishedVersion());
            backend.bind("trace-a");
            backend.bind("trace-a");
            assertThrows(java.io.IOException.class, () -> backend.bind("trace-b"));
            assertThrows(IllegalArgumentException.class, () -> backend.bind(""));
            backend.recordPublishedVersion(42);
            backend.flush();
        }
        try (var backend = RocksDbBackend.open(path, ValueKind.ROW_ID)) {
            assertEquals(42, backend.publishedVersion());
            // The binding survives the reopen: the index still refuses another trace.
            backend.bind("trace-a");
            assertThrows(java.io.IOException.class, () -> backend.bind("trace-b"));
            assertEquals(path.toAbsolutePath().toString(), backend.location());
        }
        var stats = java.nio.file.Files.readAllLines(path.resolveSibling("reopen.engine-stats.jsonl"));
        assertEquals(1, stats.size());
        var line = com.google.gson.JsonParser.parseString(stats.get(0)).getAsJsonObject();
        assertEquals(1, line.get("flush").getAsLong());
        assertEquals("N/A", line.get("object_requests").getAsString());
        assertTrue(line.get("rocksdb.stats").getAsString().contains("Compaction Stats"));
        assertThrows(java.io.IOException.class, () -> RocksDbBackend.open(path, ValueKind.LOCATOR));
        assertThrows(
                java.io.IOException.class,
                () -> RocksDbBackend.open(temporary.getRoot().toPath().resolve("missing"), ValueKind.ROW_ID));
        assertThrows(java.io.IOException.class, () -> RocksDbBackend.create(path, ValueKind.ROW_ID));
    }
}
