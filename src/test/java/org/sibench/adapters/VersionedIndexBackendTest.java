package org.sibench.adapters;

import static org.junit.Assert.*;

import com.google.protobuf.ByteString;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.adapters.proto.*;
import org.sibench.trace.IndexEntry;
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
        for (Operation operation : operations) {
            var response = IndexExecution.execute(backend, requests.request(operation));
            assertEquals(response.getMessage(), Status.OK, response.getStatus());
        }
        return backend;
    }

    private static ScanRequest search(int index, Range range, long time) {
        return ScanRequest.newBuilder().setReadTs(time).setReverse(range.descending()).setLimit(range.limit())
                .setStartKey(IndexEntry.bound(TABLE,index,range.lower(),range.lower()!=null && !range.lowerInclusive()))
                .setEndKey(IndexEntry.bound(TABLE,index,range.upper(),range.upper()==null || range.upperInclusive())).build();
    }

    private static void same(String what, List<LookupResult> expected, List<Result> actual, boolean rowIds) throws Exception {
        assertEquals(what + " size", expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            LookupResult e = expected.get(i);
            Result a = actual.get(i);
            assertArrayEquals(what + " key " + i, e.key().bytes(), IndexEntry.decode(a.getKey()).primaryKey().bytes());
            assertEquals(what + " version " + i, e.commitVersion(), a.getTs());
            if (rowIds) assertEquals(what + " row id " + i, e.value(), a.getValue().asReadOnlyByteBuffer().getLong());
            else {
                assertEquals(what + " file " + i, e.value(), a.getValue().asReadOnlyByteBuffer().getLong());
                assertEquals(what + " position " + i, e.position(), a.getValue().asReadOnlyByteBuffer().getLong(8));
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
                                        model.search(TABLE, index, range, time), backend.scan(search(index, range, time)), rowIds);
                            }
                        }
                        // Pinned entries: one (value, PK) of a secondary index.
                        for (int id = 1; id <= ROWS + 40; id += 1 + random.nextInt(20)) {
                            IndexKey value = IndexKey.ofInt(random.nextInt(IndexReferenceSpillTest.VALUES));
                            var pinned = GetRequest.newBuilder().setKey(new IndexEntry(TABLE,1,value,IndexKey.ofInt(id)).bytes()).setReadTs(time).build();
                            LookupResult entry = model.lookupEntry(TABLE, 1, IndexKey.ofInt(id), value, time);
                            var actual = backend.get(pinned);
                            same("pinned " + id + " at " + time, entry == null ? List.of() : List.of(entry),
                                    actual == null ? List.of() : List.of(actual), rowIds);
                        }
                    }
                }
            }
        }
    }

    private static void put(VersionedIndexBackend backend, int index, int value, int pk,
            long version, long rowId, boolean deleted) throws Exception {
        var key = new IndexEntry(TABLE,index,index==0?null:IndexKey.ofInt(value),IndexKey.ofInt(pk)).bytes();
        var mutation = Mutation.newBuilder();
        if (deleted) mutation.setDelete(DeleteRequest.newBuilder().setKey(key).setTs(version));
        else mutation.setPut(PutRequest.newBuilder().setKey(key).setTs(version).setValue(IndexEntry.rowId(rowId)));
        backend.write(mutation.build());
    }

    private static LookupResult row(int pk, long version, long rowId) {
        return new LookupResult(IndexKey.ofInt(pk), version, rowId, 0);
    }

    @Test
    public void opaqueKeysKeepSeparateHistoriesAndHalfOpenScanBounds() throws Exception {
        var keys = List.of(ByteString.EMPTY, ByteString.copyFrom(new byte[] {0}),
                ByteString.copyFrom(new byte[] {0, (byte) 255}), ByteString.copyFromUtf8("a"),
                ByteString.copyFrom(new byte[] {'a', 0}), ByteString.copyFromUtf8("ab"),
                ByteString.copyFrom(new byte[] {(byte) 255}),
                ByteString.copyFrom(new byte[] {(byte) 255, 0}));
        var order = ByteString.unsignedLexicographicalComparator();
        ByteString a = keys.get(3), ab = keys.get(5), updated = ByteString.copyFromUtf8("new");
        try (var backend = load(List.of(), ValueKind.ROW_ID)) {
            // Arrival order differs from timestamp order; values have no RowID/Locator shape.
            backend.write(Mutation.newBuilder().setPut(PutRequest.newBuilder()
                    .setKey(a).setTs(4).setValue(updated)).build());
            for (var key : keys) backend.write(Mutation.newBuilder().setPut(PutRequest.newBuilder()
                    .setKey(key).setTs(1).setValue(key)).build());
            backend.write(Mutation.newBuilder().setDelete(DeleteRequest.newBuilder().setKey(a).setTs(3)).build());
            backend.write(Mutation.newBuilder().setDelete(DeleteRequest.newBuilder().setKey(ab).setTs(5)).build());
            for (long ts : new long[] {0, 1, 3, 4, 5, Long.MAX_VALUE}) {
                var expected = new ArrayList<Result>();
                for (var key : keys) {
                    Result result = ts == 0 || key.equals(a) && ts == 3 || key.equals(ab) && ts >= 5 ? null
                            : Result.newBuilder().setKey(key).setTs(key.equals(a) && ts >= 4 ? 4 : 1)
                                    .setValue(key.equals(a) && ts >= 4 ? updated : key).build();
                    assertEquals(result, backend.get(GetRequest.newBuilder().setKey(key).setReadTs(ts).build()));
                    if (result != null) expected.add(result);
                }
                for (boolean reverse : new boolean[] {false, true}) {
                    var wanted = new ArrayList<>(expected);
                    if (reverse) java.util.Collections.reverse(wanted);
                    var scan = ScanRequest.newBuilder().setReadTs(ts).setReverse(reverse);
                    assertEquals(wanted, backend.scan(scan.build()));
                    assertEquals(wanted.subList(0, Math.min(2, wanted.size())),
                            backend.scan(scan.clone().setLimit(2).build()));
                    var end = ByteString.copyFromUtf8("b");
                    var bounded = wanted.stream().filter(r -> order.compare(r.getKey(), a) >= 0
                            && order.compare(r.getKey(), end) < 0).toList();
                    assertEquals(bounded, backend.scan(scan.clone().setStartKey(a).setEndKey(end).build()));
                    assertTrue(backend.scan(scan.clone().setEndKey(ByteString.EMPTY).build()).isEmpty());
                    assertEquals(wanted, backend.scan(scan.clone().setStartKey(ByteString.EMPTY).build()));
                }
            }
        }
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
                same("initial", initial, backend.scan(search(0, bounds, 0)), true);
                same("tombstone hides older rows", deleted, backend.scan(search(0, bounds, 4)), true);
                same("reinserted identity", reinserted, backend.scan(search(0, bounds, 7)), true);
                same("limit counts visible rows", initial.subList(0, 2),
                        backend.scan(search(0, bounds, 0).toBuilder().setLimit(2).build()), true);
                Range exclusive = new Range(IndexKey.ofInt(1), false, IndexKey.ofInt(4), false, descending, 0);
                same("invisible lower entry then visible successor", List.of(row(3, 0, 30)),
                        backend.scan(search(0, exclusive, 0)), true);
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
                same("initial secondary", initial, backend.scan(search(1, all, 0)), true);
                same("same PK moves between secondary entries", latest, backend.scan(search(1, all, 7)), true);
                same("secondary limit", latest.subList(0, 2),
                        backend.scan(search(1, all, 7).toBuilder().setLimit(2).build()), true);
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
                            .addMembers(ReadRequest.newBuilder().setScan(search(1, Range.equal(IndexKey.ofInt(a)), last)))
                            .addMembers(ReadRequest.newBuilder().setScan(search(2, Range.equal(IndexKey.ofInt(b)), last))).build();
                    var results = List.of(backend.scan(group.getMembers(0).getScan()),backend.scan(group.getMembers(1).getScan()));
                    var response = TraceRequests.combine(group,results);
                    assertEquals(intersection, response.size());
                    var union = TraceRequests.combine(group.toBuilder().setMode(SearchGroup.Mode.UNION).build(),results);
                    assertEquals(first.size() + second.size() - intersection, union.size());
                }
        }
    }

    @Test
    public void rejectsWhatTheProtocolForbids() throws Exception {
        try (var backend = load(List.of(), ValueKind.ROW_ID)) {
            assertEquals(Status.INVALID, IndexExecution.execute(backend, Request.newBuilder()
                    .setPut(PutRequest.newBuilder().setTs(-1)).build()).getStatus());
            assertEquals(Status.INVALID, IndexExecution.execute(backend, Request.newBuilder()
                    .setGet(GetRequest.newBuilder().setReadTs(-1)).build()).getStatus());
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
            same("retained old value", List.of(row(7, 0, 70)), backend.scan(search(0, Range.equal(IndexKey.ofInt(7)), 41)), true);
            same("retained latest value", List.of(row(7, 42, 71)), backend.scan(search(0, Range.equal(IndexKey.ofInt(7)), 42)), true);
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
