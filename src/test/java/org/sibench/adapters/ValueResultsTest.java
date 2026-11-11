package org.sibench.adapters;

import static org.junit.Assert.*;

import com.google.protobuf.ByteString;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.adapters.proto.*;
import org.sibench.capture.IndexOperationTrace.Range;
import org.sibench.capture.IndexReferenceReplay;
import org.sibench.capture.IndexReferenceReplay.LookupResult;
import org.sibench.capture.IndexReferenceSpillTest;
import org.sibench.trace.IndexEntry;
import org.sibench.trace.IndexKey;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/** Value-only results against hand-derived answers and the independent PK reference model. */
public class ValueResultsTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    private static ByteString bytes(String value) { return ByteString.copyFromUtf8(value); }

    private static List<ByteString> scan(IndexServiceGrpc.IndexServiceBlockingStub api, ScanRequest request) {
        var result = new ArrayList<ByteString>();
        api.scan(request).forEachRemaining(chunk -> result.addAll(chunk.getValuesList()));
        return result;
    }

    @Test(timeout = 30000) public void getAndScanPreservePresenceOrderDuplicatesAndHistoricalValues() throws Exception {
        var keys = List.of(bytes("a"), bytes("b"), bytes("c"), bytes("d"));
        var initial = List.of(bytes("z"), bytes("a"), bytes("z"), ByteString.EMPTY);
        try (var backend = VersionedIndexBackend.create(temporary.getRoot().toPath().resolve("rpc"), ValueKind.ROW_ID);
             var server = IndexServer.start(0, backend)) {
            var channel = IndexServerTest.channel(server.port());
            try {
                var api = IndexServerTest.stub(channel);
                for (int i = 0; i < keys.size(); i++)
                    api.put(PutRequest.newBuilder().setKey(keys.get(i)).setValue(initial.get(i)).build());
                api.put(PutRequest.newBuilder().setKey(keys.get(1)).setTs(1).setValue(bytes("updated")).build());
                api.delete(DeleteRequest.newBuilder().setKey(keys.get(0)).setTs(2).build());
                api.put(PutRequest.newBuilder().setKey(keys.get(0)).setTs(3).setValue(bytes("reinserted")).build());
                for (long ts : new long[] {0, 1, 2, 3, Long.MAX_VALUE}) {
                    var expected = new ArrayList<ByteString>();
                    for (int i = 0; i < keys.size(); i++) {
                        ByteString value = i == 0 && ts == 2 ? null : i == 0 && ts >= 3 ? bytes("reinserted")
                                : i == 1 && ts >= 1 ? bytes("updated") : initial.get(i);
                        var get = api.get(GetRequest.newBuilder().setKey(keys.get(i)).setReadTs(ts).build());
                        assertEquals(value, get.hasValue() ? get.getValue() : null);
                        if (value != null) expected.add(value);
                    }
                    for (boolean reverse : new boolean[] {false, true}) {
                        var ordered = new ArrayList<>(expected);
                        if (reverse) java.util.Collections.reverse(ordered);
                        var request = ScanRequest.newBuilder().setReadTs(ts).setReverse(reverse);
                        assertEquals(ordered, scan(api, request.build()));
                        assertEquals(ordered.subList(0, Math.min(2, ordered.size())), scan(api, request.setLimit(2).build()));
                    }
                    assertEquals(List.of(ts == 0 ? bytes("a") : bytes("updated"), bytes("z")),
                            scan(api, ScanRequest.newBuilder().setReadTs(ts).setStartKey(keys.get(1)).setEndKey(keys.get(3)).build()));
                }
                assertFalse(api.get(GetRequest.newBuilder().setKey(bytes("missing")).build()).hasValue());
            } finally { channel.shutdownNow(); }
        }
    }

    private static ScanRequest query(int index, Range range, long timestamp) {
        return ScanRequest.newBuilder().setReadTs(timestamp).setReverse(range.descending()).setLimit(range.limit())
                .setStartKey(IndexEntry.bound(IndexReferenceSpillTest.TABLE, index, range.lower(), range.lower() != null && !range.lowerInclusive()))
                .setEndKey(IndexEntry.bound(IndexReferenceSpillTest.TABLE, index, range.upper(), range.upper() == null || range.upperInclusive())).build();
    }

    private static ByteString value(LookupResult row, boolean rowIds) {
        return rowIds ? IndexEntry.rowId(row.value()) : IndexEntry.locator(row.value(), row.position());
    }

    @Test public void mixedGroupsKeepFirstOccurrenceOrderAndRequireOneTableAndSnapshot() throws Exception {
        var first = ReadRequest.newBuilder().setGet(GetRequest.newBuilder().setReadTs(9)
                .setKey(new IndexEntry(5, 0, null, IndexKey.ofInt(2)).bytes())).build();
        var scan = ScanRequest.newBuilder().setReadTs(9).setStartKey(IndexEntry.bound(5, 1, null, false));
        var group = ReadGroup.newBuilder().addRequests(first).addRequests(ReadRequest.newBuilder().setScan(scan));
        var one = IndexEntry.rowId(1); var two = IndexEntry.rowId(2); var three = IndexEntry.rowId(3);
        var answers = List.of(List.of(two), List.of(three, two, one));
        assertEquals(List.of(two), TraceRequests.combine(group.setCombineMode(ReadGroup.CombineMode.INTERSECTION).build(), answers));
        assertEquals(List.of(two, three, one), TraceRequests.combine(group.setCombineMode(ReadGroup.CombineMode.UNION).build(), answers));
        for (var invalid : List.of(scan.clone().setReadTs(10), scan.clone().setStartKey(IndexEntry.bound(6, 1, null, false)))) {
            var bad = group.clone().setRequests(1, ReadRequest.newBuilder().setScan(invalid)).build();
            assertThrows(java.io.IOException.class, () -> TraceRequests.combine(bad, answers));
        }
    }

    private static void checkGroup(ReadGroup group, List<LookupResult> first, List<LookupResult> second,
            List<List<ByteString>> answers, boolean rowIds) throws Exception {
        // This oracle combines original PK identities, without using returned values as identities.
        var expected = new LinkedHashMap<IndexKey, ByteString>();
        for (var row : first) expected.putIfAbsent(row.key(), value(row, rowIds));
        if (group.getCombineMode() == ReadGroup.CombineMode.INTERSECTION) {
            var keys = second.stream().map(LookupResult::key).collect(java.util.stream.Collectors.toSet());
            expected.keySet().retainAll(keys);
        } else for (var row : second) expected.putIfAbsent(row.key(), value(row, rowIds));
        assertEquals(List.copyOf(expected.values()), TraceRequests.combine(group, answers));
    }

    @Test(timeout = 60000) public void rowIdAndLocatorGroupsMatchThePkOracleAcrossSnapshots() throws Exception {
        for (boolean rowIds : new boolean[] {true, false}) for (long seed : new long[] {1, 2, 3}) {
            var kind = rowIds ? ValueKind.ROW_ID : ValueKind.LOCATOR;
            var operations = IndexReferenceSpillTest.stream(rowIds, seed);
            var requests = new TraceRequests(kind);
            try (var model = new IndexReferenceReplay(rowIds, true);
                 var backend = VersionedIndexBackend.create(temporary.getRoot().toPath().resolve(kind + "-" + seed), kind)) {
                for (var operation : operations) {
                    model.accept(operation);
                    assertEquals(IndexStatus.OK, IndexExecution.execute(backend, requests.request(operation)).status());
                }
                for (long ts : new long[] {0, 1, 17, 60, 120}) for (boolean reverse : new boolean[] {false, true}) {
                    for (int a = 0; a < IndexReferenceSpillTest.VALUES; a += 3) for (int b = 0; b < 5; b++) {
                        var firstRange = new Range(IndexKey.ofInt(a), true, IndexKey.ofInt(a + 2), true, reverse, 7);
                        var secondRange = new Range(IndexKey.ofInt(b), true, IndexKey.ofInt(b), true, !reverse, 0);
                        var first = model.search(IndexReferenceSpillTest.TABLE, 1, firstRange, ts);
                        var second = model.search(IndexReferenceSpillTest.TABLE, 2, secondRange, ts);
                        var firstQuery = query(1, firstRange, ts);
                        var secondQuery = query(2, secondRange, ts);
                        var answers = List.of(backend.scan(firstQuery), backend.scan(secondQuery));
                        assertEquals(first.stream().map(row -> value(row, rowIds)).toList(), answers.get(0));
                        assertEquals(second.stream().map(row -> value(row, rowIds)).toList(), answers.get(1));
                        var group = ReadGroup.newBuilder().addRequests(ReadRequest.newBuilder().setScan(firstQuery))
                                .addRequests(ReadRequest.newBuilder().setScan(secondQuery));
                        for (var mode : List.of(ReadGroup.CombineMode.INTERSECTION, ReadGroup.CombineMode.UNION))
                            checkGroup(group.setCombineMode(mode).build(), first, second, answers, rowIds);
                    }
                }
            }
        }
    }
}
