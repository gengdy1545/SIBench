package org.sibench.replay;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.sibench.capture.IndexOperationTrace;
import org.sibench.capture.IndexOperationTrace.Range;
import org.sibench.capture.IndexReferenceReplay;
import org.sibench.replay.SnapshotLookupValidation.Entry;
import org.sibench.replay.SnapshotLookupValidation.History;
import org.sibench.replay.SnapshotLookupValidation.RangeProbe;
import org.sibench.replay.SnapshotLookupValidation.Row;
import org.sibench.trace.IndexKey;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.LongStream;

public class SnapshotLookupValidationTest {
    private static final int TABLE = 3;

    private static IndexKey key(long value) {
        return IndexKey.ofInt(value);
    }

    private static List<IndexKey> keys(long... values) {
        return LongStream.of(values).mapToObj(SnapshotLookupValidationTest::key).toList();
    }

    @Test
    public void spreadKeepsBothEndsAndSpacesTheRestEvenly() {
        var sorted = keys(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        assertEquals(keys(0, 5, 10), SnapshotLookupValidation.spread(sorted, 3));
        assertEquals(keys(0, 2, 5, 7, 10), SnapshotLookupValidation.spread(sorted, 5));
        assertEquals(sorted, SnapshotLookupValidation.spread(sorted, 64));
        assertEquals(keys(0), SnapshotLookupValidation.spread(sorted, 1));
        assertEquals(List.of(), SnapshotLookupValidation.spread(sorted, 0));
        assertEquals(List.of(), SnapshotLookupValidation.spread(List.of(), 4));
    }

    @Test
    public void rangesTestEachBoundInclusiveAndExclusiveFromItsOwnSide() {
        var ranges = SnapshotLookupValidation.ranges(keys(1, 5, 9), keys(4));
        // Two unbounded, four per boundary pair, two per anchor.
        assertEquals(2 + 2 * 4 + 2, ranges.size());
        for (Range range : ranges) assertEquals(32, range.limit());
        // An ascending search reads from its lower bound, a descending one from its upper bound.
        for (Range range : ranges.subList(2, 10)) {
            if (range.descending()) assertTrue(range.lowerInclusive());
            else assertTrue(range.upperInclusive());
        }
        var lowerFlags = new TreeSet<Boolean>();
        var upperFlags = new TreeSet<Boolean>();
        for (Range range : ranges.subList(2, 6)) {
            if (range.descending()) upperFlags.add(range.upperInclusive());
            else lowerFlags.add(range.lowerInclusive());
        }
        assertEquals(2, lowerFlags.size());
        assertEquals(2, upperFlags.size());
        Range anchoredUp = ranges.get(10), anchoredDown = ranges.get(11);
        assertEquals(key(4), anchoredUp.lower());
        assertEquals(null, anchoredUp.upper());
        assertFalse(anchoredUp.descending());
        assertEquals(key(4), anchoredDown.upper());
        assertTrue(anchoredDown.descending());
    }

    /** A baseline of even keys with a low-cardinality secondary value, and its later changes. */
    private static final class Fixture {
        final Map<Long, Long> baseline = new LinkedHashMap<>();
        final Map<IndexKey, History> histories = new LinkedHashMap<>();
        final List<long[]> changes = new ArrayList<>();

        Fixture() {
            for (long id = 2; id <= 200; id += 2) baseline.put(id, id % 7);
            // version, id, new value (-1 deletes), rowId of the new version
            changes.add(new long[] {1, 10, 5, 10});
            changes.add(new long[] {2, 20, -1, 0});
            changes.add(new long[] {3, 21, 6, 201});
            changes.add(new long[] {4, 10, 0, 10});
            changes.add(new long[] {5, 21, -1, 0});
            changes.add(new long[] {6, 20, 1, 202});
            changes.add(new long[] {7, 2, 6, 2});
            for (long[] change : changes) {
                History history =
                        histories.computeIfAbsent(
                                key(change[1]),
                                ignored -> {
                                    var created = new History();
                                    Long value = baseline.get(change[1]);
                                    if (value != null)
                                        created.baseline =
                                                new Row(0, change[1], 0, 0, Map.of(1, key(value)));
                                    return created;
                                });
                history.commits.put(
                        change[0],
                        change[2] < 0
                                ? null
                                : new Row(change[0], change[3], 0, 0, Map.of(1, key(change[2]))));
            }
        }

        IndexReferenceReplay model(long skippedVersion) throws IOException {
            var model = new IndexReferenceReplay(true, true);
            long event = 0;
            for (var row : baseline.entrySet()) {
                var primary =
                        IndexOperationTrace.write(
                                ++event, IndexOperationTrace.INSERT, TABLE, 0, key(row.getKey()),
                                0, 1, 0, row.getKey());
                model.accept(primary);
                model.accept(
                        primary.withIndex(1, key(row.getValue()), IndexOperationTrace.INSERT));
            }
            Map<Long, Long> current = new LinkedHashMap<>(baseline);
            Map<Long, Long> rowIds = new LinkedHashMap<>();
            for (long id : baseline.keySet()) rowIds.put(id, id);
            for (long[] change : changes) {
                long version = change[0], id = change[1], value = change[2], rowId = change[3];
                if (version == skippedVersion) continue;
                Long previous = current.get(id);
                if (value < 0) {
                    var delete =
                            IndexOperationTrace.write(
                                            ++event, IndexOperationTrace.LOGICAL_DELETE, TABLE,
                                            0, key(id), version, 0, 0, rowIds.get(id))
                                    .withTransaction(version, 1);
                    model.accept(delete);
                    model.accept(
                            delete.withIndex(
                                    1, key(previous), IndexOperationTrace.LOGICAL_DELETE));
                    current.remove(id);
                    continue;
                }
                var upsert =
                        IndexOperationTrace.write(
                                        ++event, IndexOperationTrace.UPSERT, TABLE, 0, key(id),
                                        version, 2, version, rowId)
                                .withTransaction(version, 1);
                model.accept(upsert);
                if (previous != null && previous != value)
                    model.accept(
                            upsert.withIndex(1, key(previous), IndexOperationTrace.LOGICAL_DELETE));
                model.accept(upsert.withIndex(1, key(value), IndexOperationTrace.UPSERT));
                current.put(id, value);
                rowIds.put(id, rowId);
            }
            return model;
        }

        /** Probes as the oracle derives them, filled from the rows no change ever touched. */
        List<RangeProbe> probes() {
            var probes = new ArrayList<RangeProbe>();
            var primaryPoints = new ArrayList<IndexKey>();
            var secondaryPoints = new TreeSet<IndexKey>();
            for (var row : baseline.entrySet()) {
                primaryPoints.add(key(row.getKey()));
                secondaryPoints.add(key(row.getValue()));
            }
            var changedKeys = new TreeSet<>(histories.keySet());
            var changedValues = new TreeSet<IndexKey>();
            for (History history : histories.values())
                for (Row row : history.commits.values())
                    if (row != null) changedValues.add(row.indexes().get(1));
            for (Range range :
                    SnapshotLookupValidation.ranges(
                            SnapshotLookupValidation.spread(primaryPoints, 9),
                            SnapshotLookupValidation.spread(new ArrayList<>(changedKeys), 4)))
                probes.add(new RangeProbe(TABLE, 0, range));
            for (Range range :
                    SnapshotLookupValidation.ranges(
                            SnapshotLookupValidation.spread(new ArrayList<>(secondaryPoints), 9),
                            SnapshotLookupValidation.spread(new ArrayList<>(changedValues), 4)))
                probes.add(new RangeProbe(TABLE, 1, range));
            for (RangeProbe probe : probes) {
                probe.changed.putAll(histories);
                for (var row : baseline.entrySet()) {
                    IndexKey id = key(row.getKey());
                    if (histories.containsKey(id)) continue;
                    probe.offer(
                            new Entry(
                                    probe.index == 0 ? id : key(row.getValue()),
                                    id,
                                    row.getKey(),
                                    0,
                                    0));
                }
            }
            return probes;
        }
    }

    @Test
    public void rangeProbesAgreeWithTheReferenceModelAtEveryVersion() throws Exception {
        var fixture = new Fixture();
        var model = fixture.model(-1);
        var probes = fixture.probes();
        long checked = 0, results = 0, truncated = 0;
        for (RangeProbe probe : probes)
            for (long time = 0; time <= 7; time++) {
                var actual = model.search(probe.table, probe.index, probe.range, time);
                assertEquals(probe + " at " + time, probe.expected(time, true), actual);
                checked++;
                results += actual.size();
                if (actual.size() == probe.range.limit()) truncated++;
            }
        // Primary: 8 boundary pairs, 4 changed keys; secondary: values 0..6, 4 changed values.
        assertEquals(2 + 8 * 4 + 4 * 2 + 2 + 6 * 4 + 4 * 2, probes.size());
        assertEquals(probes.size() * 8L, checked);
        // Wide ranges stop at the limit; narrow ones end at their bound.
        assertTrue(truncated > 0);
        assertTrue(results > truncated * 32);
    }

    @Test
    public void rangeProbesDetectAMissingWrite() throws Exception {
        var fixture = new Fixture();
        var model = fixture.model(4);
        long mismatches = 0;
        for (RangeProbe probe : fixture.probes()) {
            for (long time = 0; time < 4; time++)
                assertEquals(
                        probe.expected(time, true),
                        model.search(probe.table, probe.index, probe.range, time));
            if (!probe.expected(7, true)
                    .equals(model.search(probe.table, probe.index, probe.range, 7)))
                mismatches++;
        }
        assertNotEquals(0, mismatches);
    }

    @Test
    public void aDeletedRowLeavesEveryRangeAndAReinsertReturnsWithItsNewIdentity()
            throws Exception {
        var fixture = new Fixture();
        var probe =
                new RangeProbe(TABLE, 0, new Range(key(18), true, key(22), true, false, 32));
        probe.changed.putAll(fixture.histories);
        probe.offer(new Entry(key(18), key(18), 18, 0, 0));
        probe.offer(new Entry(key(22), key(22), 22, 0, 0));
        // 20 is deleted at 2 and reinserted at 6; 21 lives from 3 to 5.
        assertEquals(
                List.of(key(18), key(20), key(22)),
                probe.expected(1, true).stream().map(r -> r.key()).toList());
        assertEquals(
                List.of(key(18), key(22)),
                probe.expected(2, true).stream().map(r -> r.key()).toList());
        assertEquals(
                List.of(key(18), key(21), key(22)),
                probe.expected(3, true).stream().map(r -> r.key()).toList());
        var reinserted = probe.expected(6, true);
        assertEquals(new IndexReferenceReplay.LookupResult(key(20), 6, 202), reinserted.get(1));
        assertEquals(probe.expected(6, true), fixture.model(-1).search(TABLE, 0, probe.range, 6));
    }
}
