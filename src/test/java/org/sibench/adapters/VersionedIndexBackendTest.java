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
import org.sibench.adapters.proto.EntryKey;
import org.sibench.adapters.proto.Value;
import org.sibench.adapters.proto.ValueKind;
import org.sibench.adapters.proto.Write;
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
public class VersionedIndexBackendTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    private static final int TABLE = IndexReferenceSpillTest.TABLE, ROWS = IndexReferenceSpillTest.ROWS;

    private VersionedIndexBackend load(List<Operation> operations, ValueKind kind) throws Exception {
        var backend = VersionedIndexBackend.create(temporary.getRoot().toPath().resolve("db-" + kind + "-" + System.nanoTime()), kind);
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

    private static void put(VersionedIndexBackend backend, int index, int value, int pk,
            long version, long rowId, boolean deleted) throws Exception {
        var key = EntryKey.newBuilder().setTable(TABLE).setIndex(index)
                .setPrimaryKey(ByteString.copyFrom(IndexKey.ofInt(pk).bytes()));
        if (index != 0) key.setIndexedValue(ByteString.copyFrom(IndexKey.ofInt(value).bytes()));
        var write = Write.newBuilder().setKey(key).setVersion(version)
                .setKind(deleted ? Write.Kind.LOGICAL_DELETE : version == 0 ? Write.Kind.INSERT : Write.Kind.UPSERT);
        if (!deleted) write.setValue(Value.newBuilder().setRowId(rowId));
        backend.write(write.build());
    }

    private static LookupResult row(int pk, long version, long rowId) {
        return new LookupResult(IndexKey.ofInt(pk), version, rowId, 0);
    }

    @Test
    public void rangesSkipInvisibleEntriesWithoutSkippingTheirVisibleSuccessors() throws Exception {
        try (var backend = load(List.of(), ValueKind.ROW_ID)) {
            put(backend, 0, 0, 1, 0, 11, false);
            put(backend, 0, 0, 1, 2, 12, false);
            put(backend, 0, 0, 1, 4, 12, true);
            put(backend, 0, 0, 1, 7, 17, false);
            put(backend, 0, 0, 2, 5, 25, false); // Entire entry is invisible at version zero.
            put(backend, 0, 0, 3, 0, 30, false);
            put(backend, 0, 0, 3, 6, 36, false);
            put(backend, 0, 0, 4, 0, 40, false);
            put(backend, 0, 0, 4, 5, 45, false);
            for (boolean descending : new boolean[] {false, true}) {
                Range bounds = new Range(IndexKey.ofInt(1), true, IndexKey.ofInt(4), true, descending, 0);
                var initial = new ArrayList<>(List.of(row(1, 0, 11), row(3, 0, 30), row(4, 0, 40)));
                var deleted = new ArrayList<>(List.of(row(3, 0, 30), row(4, 0, 40)));
                var reinserted = new ArrayList<>(List.of(row(1, 7, 17), row(2, 5, 25), row(3, 6, 36), row(4, 5, 45)));
                if (descending) {
                    java.util.Collections.reverse(initial); java.util.Collections.reverse(deleted);
                    java.util.Collections.reverse(reinserted);
                }
                same("initial", initial, backend.search(search(0, bounds, 0)), true);
                same("tombstone hides older rows", deleted, backend.search(search(0, bounds, 4)), true);
                same("reinserted identity", reinserted, backend.search(search(0, bounds, 7)), true);
                same("limit counts visible rows", initial.subList(0, 2),
                        backend.search(search(0, bounds, 0).toBuilder().setLimit(2).build()), true);
                Range exclusive = new Range(IndexKey.ofInt(1), false, IndexKey.ofInt(4), false, descending, 0);
                same("invisible lower entry then visible successor", List.of(row(3, 0, 30)),
                        backend.search(search(0, exclusive, 0)), true);
            }
        }
    }

    @Test
    public void secondaryVersionSkippingKeepsValueAndPrimaryKeyTogether() throws Exception {
        try (var backend = load(List.of(), ValueKind.ROW_ID)) {
            put(backend, 1, 10, 1, 0, 11, false);
            put(backend, 1, 10, 1, 4, 11, true);
            put(backend, 1, 20, 1, 7, 17, false);
            put(backend, 1, 10, 2, 5, 25, false);
            put(backend, 1, 10, 3, 0, 30, false);
            put(backend, 1, 20, 4, 0, 40, false);
            for (boolean descending : new boolean[] {false, true}) {
                Range all = new Range(IndexKey.ofInt(10), true, IndexKey.ofInt(20), true, descending, 0);
                var initial = new ArrayList<>(List.of(row(1, 0, 11), row(3, 0, 30), row(4, 0, 40)));
                var latest = new ArrayList<>(List.of(row(2, 5, 25), row(3, 0, 30), row(1, 7, 17), row(4, 0, 40)));
                if (descending) { java.util.Collections.reverse(initial); java.util.Collections.reverse(latest); }
                same("initial secondary", initial, backend.search(search(1, all, 0)), true);
                same("same PK moves between secondary entries", latest, backend.search(search(1, all, 7)), true);
                same("secondary limit", latest.subList(0, 2),
                        backend.search(search(1, all, 7).toBuilder().setLimit(2).build()), true);
            }
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
                    var results = List.of(backend.search(group.getMembers(0)),backend.search(group.getMembers(1)));
                    var response = IndexExecution.group(group,results);
                    assertEquals(intersection, response.size());
                    var union = IndexExecution.group(group.toBuilder().setMode(SearchGroup.Mode.UNION).build(),results);
                    assertEquals(first.size() + second.size() - intersection, union.size());
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
        VersionedIndexBackend.create(path, ValueKind.LOCATOR).close();
        assertThrows(java.io.IOException.class, () -> VersionedIndexBackend.create(path, ValueKind.LOCATOR));
    }

    @Test
    public void reopenRejectsUnrecognizedMetadataInsteadOfIgnoringItsSemantics() throws Exception {
        var path = temporary.getRoot().toPath().resolve("unknown-metadata");
        VersionedIndexBackend.create(path, ValueKind.ROW_ID).close();
        try (var store = LocalRocksStore.open(path)) {
            byte[] name = "unsupported_state".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
            byte[] key = java.nio.ByteBuffer.allocate(8 + name.length).putLong(0).put(name).array();
            store.put(key, new byte[] {1}, true);
            store.flush();
        }
        var error = assertThrows(java.io.IOException.class, () -> VersionedIndexBackend.open(path, ValueKind.ROW_ID));
        assertTrue(error.getMessage(), error.getMessage().contains("Unsupported index metadata"));
    }

    @Test
    public void reopenedIndexKeepsHistoricalValuesAndRefusesAnotherValueKind() throws Exception {
        java.nio.file.Path path = temporary.getRoot().toPath().resolve("reopen");
        try (var backend = VersionedIndexBackend.create(path, ValueKind.ROW_ID)) {
            backend.bind("trace-a");
            backend.bind("trace-a");
            assertThrows(java.io.IOException.class, () -> backend.bind("trace-b"));
            assertThrows(IllegalArgumentException.class, () -> backend.bind(""));
            put(backend, 0, 0, 7, 0, 70, false);
            put(backend, 0, 0, 7, 42, 71, false);
            backend.flush();
        }
        try (var backend = VersionedIndexBackend.open(path, ValueKind.ROW_ID)) {
            same("retained old value", List.of(row(7, 0, 70)), backend.search(search(0, Range.equal(IndexKey.ofInt(7)), 41)), true);
            same("retained latest value", List.of(row(7, 42, 71)), backend.search(search(0, Range.equal(IndexKey.ofInt(7)), 42)), true);
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
        assertThrows(java.io.IOException.class, () -> VersionedIndexBackend.open(path, ValueKind.LOCATOR));
        assertThrows(
                java.io.IOException.class,
                () -> VersionedIndexBackend.open(temporary.getRoot().toPath().resolve("missing"), ValueKind.ROW_ID));
        assertThrows(java.io.IOException.class, () -> VersionedIndexBackend.create(path, ValueKind.ROW_ID));
    }
}
