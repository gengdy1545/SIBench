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
}
