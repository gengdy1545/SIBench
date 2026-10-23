package org.sibench.replay;

import org.sibench.capture.IndexOperationTrace.Range;
import org.sibench.trace.IndexKey;
import static org.junit.Assert.*;

import com.google.gson.Gson;

import org.junit.Test;
import org.sibench.capture.IndexOperationTrace;
import org.sibench.capture.TestOps;
import org.sibench.capture.IndexOperationTrace.Operation;
import org.sibench.capture.IndexReferenceReplay;
import org.sibench.generation.BaselineGenerator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class VersionedLookupTest {
    private static Operation row(
            int table, long key, long version, long position, long rowId, int kind, long ordinal) {
        return TestOps.op(
                ordinal,
                kind,
                table,
                0,
                kind == 3 ? 2 : 0,
                key,
                version,
                kind == 5 ? 0 : 9,
                position,
                rowId,
                10,
                20,
                0,
                version,
                ordinal);
    }

    private static void commit(ReplayProjection target, long version, Operation... rows)
            throws Exception {
        target.accept(IndexOperationTrace.transaction(version, 6, version, version, 0));
        for (Operation row : rows) org.sibench.capture.TransferIndexFixtures.expandRow(row, target);
        target.accept(IndexOperationTrace.transaction(version, 7, version, version, rows.length));
    }

    private static Operation snapshot(int table, int index, long key, long timestamp, long count) {
        return IndexOperationTrace.search(
                1000,
                table,
                index,
                Range.equal(IndexKey.ofInt(key)),
                timestamp,
                count,
                IndexOperationTrace.SNAPSHOT_SEARCH);
    }

    @Test
    public void selectsCommittedVersionNotReaderCommitAndPreservesBaselineDeletionAndReinsert()
            throws Exception {
        for (boolean rowIds : new boolean[] {false, true}) {
            for (String dispatchMode : new String[] {"operation", "transaction"}) {
                var model = new IndexReferenceReplay(rowIds, false);
                var replay =
                        new ReplayProjection(
                                new ReplayConfig(false, "none", 0, 0, 0, 0, dispatchMode),
                                true,
                                model::accept);
                model.accept(TestOps.op(0, 1, 3, 0, 0, 2, 0, 3, 0, 52, 0, 0, 0));
                commit(replay, 100, row(3, 1, 100, 0, 50, 3, 1));
                commit(replay, 110, row(3, 1, 110, 1, 50, 3, 1));
                commit(replay, 120, row(3, 1, 120, 2, 50, 3, 1), row(3, 1, 120, 3, 50, 3, 2));
                commit(replay, 130);
                replay.accept(snapshot(3, 0, 1, 90, 0));
                replay.accept(snapshot(3, 0, 1, 105, 1));
                replay.accept(snapshot(3, 0, 1, 110, 1));
                replay.accept(snapshot(3, 0, 1, 130, 1));
                assertNull(model.lookupPrimary(3, TestOps.key(1), 90));
                assertEquals(100, model.lookupPrimary(3, TestOps.key(1), 105).commitVersion());
                assertEquals(110, model.lookupPrimary(3, TestOps.key(1), 110).commitVersion());
                assertEquals(120, model.lookupPrimary(3, TestOps.key(1), 130).commitVersion());
                assertEquals(
                        rowIds ? 50 : IndexReferenceReplay.locator(9, 3),
                        model.lookupPrimary(3, TestOps.key(1), 120).value());
                assertEquals(0, model.lookupPrimary(3, TestOps.key(2), 90).commitVersion());
                assertEquals(4, model.retainedVersions());
                assertEquals(4, model.snapshotQueries());
                commit(replay, 140, row(3, 1, 140, 0, 50, 5, 1));
                assertNull(model.lookupPrimary(3, TestOps.key(1), 140));
                assertEquals(120, model.lookupPrimary(3, TestOps.key(1), 139).commitVersion());
                commit(replay, 150, row(3, 1, 150, 4, 51, 3, 1));
                assertNull(model.lookupPrimary(3, TestOps.key(1), 149));
                assertEquals(150, model.lookupPrimary(3, TestOps.key(1), 150).commitVersion());
                assertEquals(
                        rowIds ? 51 : IndexReferenceReplay.locator(9, 4),
                        model.lookupPrimary(3, TestOps.key(1), 150).value());
                assertEquals(6, model.retainedVersions());
                replay.finish();
            }
        }
    }

    @Test
    public void
            secondaryHistoryIncludesDeletedEntriesExcludesFutureInsertsAndRelocatesExactVersion()
                    throws Exception {
        for (boolean rowIds : new boolean[] {false, true}) {
            var model = new IndexReferenceReplay(rowIds, true);
            var replay = new ReplayProjection(ReplayConfig.defaults(), true, model::accept);
            org.sibench.capture.TransferIndexFixtures.expandRow(
                    TestOps.op(0, 1, 5, 0, 0, 1, 0, 5, 0, 50, 10, 20, 0), model::accept);
            commit(replay, 100, row(5, 1, 100, 1, 50, 3, 1), row(5, 2, 100, 2, 51, 3, 2));
            commit(replay, 110, row(5, 1, 110, 0, 50, 5, 1));
            commit(replay, 130);
            assertEquals(1, model.lookupSecondary(5, 1, TestOps.key(10), 0).size());
            assertEquals(2, model.lookupSecondary(5, 1, TestOps.key(10), 105).size());
            assertEquals(
                    List.of(TestOps.key(2)),
                    model.lookupSecondary(5, 1, TestOps.key(10), 130).stream()
                            .map(IndexReferenceReplay.LookupResult::key)
                            .toList());
            replay.accept(snapshot(5, 1, 10, 105, 2));
            replay.accept(snapshot(5, 2, 20, 0, 1));
            org.sibench.capture.TransferIndexFixtures.expandRow(
                    TestOps.op(200, 4, 5, 0, 0, 1, 100, 11, 7, 50, 10, 20, 0), model::accept);
            assertEquals(rowIds ? 0 : 3, model.relocationWrites());
            assertEquals(
                    rowIds ? 50 : IndexReferenceReplay.locator(11, 7),
                    model.lookupPrimary(5, TestOps.key(1), 105).value());
            assertEquals(
                    rowIds ? 50 : IndexReferenceReplay.locator(11, 7),
                    model.lookupSecondary(5, 1, TestOps.key(10), 105).stream()
                            .filter(o -> o.key().equals(TestOps.key(1)))
                            .findFirst()
                            .orElseThrow()
                            .value());
            assertNull(model.lookupPrimary(5, TestOps.key(1), 130));
            assertThrows(
                    IOException.class,
                    () ->
                            model.accept(
                                    TestOps.op(201, 4, 5, 0, 0, 1, 110, 11, 8, 50, 10, 20, 0)));
            assertThrows(
                    IOException.class,
                    () ->
                            model.accept(
                                    TestOps.op(201, 4, 5, 0, 0, 1, 99, 11, 8, 50, 10, 20, 0)));
            if (rowIds)
                assertThrows(
                        IOException.class,
                        () ->
                                model.accept(
                                        TestOps.op(
                                                201, 4, 5, 0, 0, 1, 100, 11, 8, 99, 10, 20, 0)));
        }
    }

    @Test
    public void rejectsFutureReadsAndUnmarkedHistoricalQueriesEvenWhenFiltered() throws Exception {
        for (Operation invalid :
                List.of(
                        snapshot(5, 1, 10, 101, 0),
                        IndexOperationTrace.search(0, 5, 1, Range.equal(IndexKey.ofInt(10)), 99, 0, 0),
                        snapshot(5, 1, 10, -1, 0))) {
            var rejected =
                    new ReplayProjection(
                            new ReplayConfig(false, "none", 0, 0, 0, 0), true, o -> {});
            commit(rejected, 100);
            assertThrows(IOException.class, () -> rejected.accept(invalid));
        }
        var replay =
                new ReplayProjection(new ReplayConfig(false, "none", 0, 0, 0, 0), true, o -> {});
        commit(replay, 100);
        replay.accept(snapshot(5, 1, 10, 99, 0));
        replay.accept(IndexOperationTrace.search(0, 3, 0, Range.equal(IndexKey.ofInt(1)), 100, 0, 0));
        assertEquals(1, replay.dispatched());
        var model = new IndexReferenceReplay(true, false);
        assertNull(model.lookupPrimary(3, TestOps.key(1), 1));
    }

    @Test
    public void snapshotTraceReplaysAllDispatchVariantsWithoutLosingHistoricalVersions()
            throws Exception {
        Path directory = Files.createTempDirectory("sibench-snapshots-");
        try {
            Path baseline = directory.resolve("baseline.ops"),
                    workload = directory.resolve("workload.ops");
            try (var writer = new IndexOperationTrace.Writer(baseline)) {
                org.sibench.capture.TransferIndexFixtures.appendRow(
                        writer, TestOps.op(0, 1, 5, 0, 0, 1, 0, 5, 0, 50, 10, 20, 0));
            }
            try (var writer = new IndexOperationTrace.Writer(workload)) {
                writer.append(IndexOperationTrace.transaction(100, 6, 100, 100, 0));
                org.sibench.capture.TransferIndexFixtures.appendRow(
                        writer, row(5, 1, 100, 1, 50, 3, 1));
                org.sibench.capture.TransferIndexFixtures.appendRow(
                        writer, row(5, 1, 100, 2, 50, 3, 2));
                writer.append(IndexOperationTrace.transaction(100, 7, 100, 100, 2));
                writer.append(IndexOperationTrace.transaction(110, 6, 110, 110, 0));
                org.sibench.capture.TransferIndexFixtures.appendRow(
                        writer, row(5, 1, 110, 0, 50, 5, 1));
                writer.append(IndexOperationTrace.transaction(110, 7, 110, 110, 1));
                writer.append(snapshot(5, 0, 1, 105, 1));
                writer.append(snapshot(5, 1, 10, 105, 1));
                writer.append(snapshot(5, 2, 20, 0, 1));
                writer.append(snapshot(5, 0, 1, 110, 0));
            }
            Files.writeString(directory.resolve("work-units.jsonl"), "");
            Files.writeString(
                    directory.resolve("manifest.json"),
                    new Gson()
                            .toJson(
                                    Map.of(
                                            "baseline_operations",
                                            3,
                                            "workload_operations",
                                            17,
                                            "baseline_trace_sha256",
                                            BaselineGenerator.sha256(baseline),
                                            "workload_trace_sha256",
                                            BaselineGenerator.sha256(workload),
                                            "work_units_sha256",
                                            BaselineGenerator.sha256(
                                                    directory.resolve("work-units.jsonl")))));
            for (boolean secondary : new boolean[] {false, true}) {
                List<?> previous = null;
                for (String dispatchMode : new String[] {"operation", "transaction"}) {
                    var result =
                            ReferenceReplay.runFiles(
                                    baseline,
                                    workload,
                                    new ReplayConfig(secondary, "none", 0, 0, 0, 0, dispatchMode),
                                    directory.resolve(
                                            "report-" + secondary + "-" + dispatchMode + ".json"));
                    var modes = (List<?>) result.get("results");
                    for (int mode = 0; mode < 2; mode++) {
                        var report = (Map<?, ?>) modes.get(mode);
                        assertEquals(secondary ? 4L : 2L, report.get("snapshot_queries"));
                        assertEquals(secondary ? 9L : 3L, report.get("retained_index_versions"));
                        if (previous != null) {
                            var before = (Map<?, ?>) previous.get(mode);
                            assertEquals(
                                    before.get("result_checksum"), report.get("result_checksum"));
                            assertEquals(
                                    before.get("selected_version_checksum"),
                                    report.get("selected_version_checksum"));
                        }
                    }
                    previous = modes;
                }
            }
        } finally {
            try (var files = Files.walk(directory)) {
                for (Path file : files.sorted(Comparator.reverseOrder()).toList())
                    Files.delete(file);
            }
        }
    }
}
