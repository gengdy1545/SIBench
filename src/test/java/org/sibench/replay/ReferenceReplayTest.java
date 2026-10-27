package org.sibench.replay;

import static org.junit.Assert.*;


import org.junit.Test;
import org.sibench.capture.IndexOperationTrace;
import org.sibench.capture.TestOps;
import org.sibench.capture.IndexOperationTrace.Operation;
import org.sibench.capture.IndexReferenceReplay;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public class ReferenceReplayTest {
    private static Operation row(int kind, long version, long file, long rowId) {
        return TestOps.op(1, kind, 5, 0, 0, 1, version, file, 0, rowId, 10, 20, 0);
    }

    private static Operation search(int index, long key, long count) {
        return TestOps.op(
                2, IndexOperationTrace.SEARCH, 5, index, 0, key, 0, 0, 0, 0, 0, 0, count);
    }

    @Test
    public void preflightCountsEffectiveInputAndRejectsShortTraceBeforeReportCreation()
            throws Exception {
        Path directory = Files.createTempDirectory("sibench-replay-");
        Path baseline = directory.resolve("baseline.ops"),
                workload = directory.resolve("workload.ops");
        Path report = directory.resolve("report.json");
        try {
            try (var writer = new IndexOperationTrace.Writer(baseline)) {
                org.sibench.capture.TransferIndexFixtures.appendRow(writer, row(1, 0, 5, 50));
            }
            try (var writer = new IndexOperationTrace.Writer(workload)) {
                writer.append(search(0, 1, 1));
                writer.append(search(1, 10, 1));
                writer.append(search(2, 20, 1));
                writer.append(IndexOperationTrace.transaction(3, 6, 3, 1, 0));
                org.sibench.capture.TransferIndexFixtures.appendRow(
                        writer, TestOps.op(3, 5, 5, 0, 0, 1, 1, 0, 0, 50, 10, 20, 0, 3, 1));
                writer.append(IndexOperationTrace.transaction(3, 7, 3, 1, 1));
            }
            assertThrows(
                    IOException.class,
                    () ->
                            ReferenceReplay.runFiles(
                                    baseline,
                                    workload,
                                    new ReplayConfig(false, "none", 1, 3, 0, 0),
                                    report));
            assertFalse(Files.exists(report));
            var config = new ReplayConfig(false, "none", 1, 2, 0, 0);
            var checked = ReferenceReplay.checkFiles(baseline, workload, config);
            assertEquals("trace_integrity_and_supply_valid", checked.get("status"));
            var result = ReferenceReplay.runFiles(baseline, workload, config, report);
            assertEquals("passed", result.get("status"));
            assertThrows(
                    IOException.class,
                    () -> ReferenceReplay.runFiles(baseline, workload, config, report));
        } finally {
            Files.deleteIfExists(report);
            Files.deleteIfExists(baseline);
            Files.deleteIfExists(workload);
            Files.delete(directory);
        }
    }

    @Test
    public void rejectsInvalidKeysVersionsAndDeletionOfAbsentEntries() throws Exception {
        var model = new IndexReferenceReplay(true, true);
        assertThrows(IOException.class, () -> model.accept(row(5, 1, 0, 50)));
        assertThrows(IOException.class, () -> model.accept(row(3, 0, 5, 50)));
        assertThrows(IOException.class, () -> model.accept(row(1, -1, 5, 50)));
        // The empty model cannot return the one row the source observed.
        assertThrows(IOException.class, () -> model.accept(search(0, 1, 1)));
        assertThrows(
                IOException.class,
                () -> model.accept(TestOps.op(1, 5, 5, 1, 0, 1, 1, 0, 0, 50, 10, 20, 0)));
    }

    @Test
    public void dispatchModesKeepIdenticalSupplyStateAndInsertDeleteOrder() throws Exception {
        Path directory = Files.createTempDirectory("sibench-tx-replay-");
        Path baseline = directory.resolve("baseline.ops"),
                workload = directory.resolve("workload.ops");
        try {
            try (var writer = new IndexOperationTrace.Writer(baseline)) {
                org.sibench.capture.TransferIndexFixtures.appendRow(writer, row(1, 0, 5, 50));
            }
            try (var writer = new IndexOperationTrace.Writer(workload)) {
                writer.append(IndexOperationTrace.transaction(10, 6, 10, 1, 0));
                org.sibench.capture.TransferIndexFixtures.appendRow(
                        writer, TestOps.op(1, 3, 5, 0, 2, 1, 1, 9, 0, 50, 10, 20, 0, 10, 1));
                org.sibench.capture.TransferIndexFixtures.appendRow(
                        writer, TestOps.op(2, 3, 5, 0, 2, 1, 1, 9, 1, 50, 10, 20, 0, 10, 2));
                org.sibench.capture.TransferIndexFixtures.appendRow(
                        writer, TestOps.op(3, 3, 5, 0, 1, 2, 1, 9, 2, 51, 10, 20, 0, 10, 3));
                org.sibench.capture.TransferIndexFixtures.appendRow(
                        writer, TestOps.op(4, 5, 5, 0, 0, 2, 1, 0, 0, 51, 10, 20, 0, 10, 4));
                writer.append(IndexOperationTrace.transaction(10, 7, 10, 1, 4));
                writer.append(TestOps.op(11, 2, 5, 0, 0, 1, 1, 0, 0, 0, 0, 0, 1));
                writer.append(TestOps.op(12, 2, 5, 0, 0, 2, 1, 0, 0, 0, 0, 0, 0));
            }
            for (boolean secondary : new boolean[] {true, false}) {
                var groupedConfig = new ReplayConfig(secondary, "none", 0, 0, 0, 0, "transaction");
                var rawConfig = new ReplayConfig(secondary, "none", 0, 0, 0, 0, "operation");
                var grouped =
                        ReferenceReplay.runFiles(
                                baseline,
                                workload,
                                groupedConfig,
                                directory.resolve("grouped-" + secondary + ".json"));
                var raw =
                        ReferenceReplay.runFiles(
                                baseline,
                                workload,
                                rawConfig,
                                directory.resolve("raw-" + secondary + ".json"));
                var a = (java.util.List<?>) grouped.get("results");
                var b = (java.util.List<?>) raw.get("results");
                for (int mode = 0; mode < 2; mode++) {
                    var x = (Map<?, ?>) a.get(mode);
                    var y = (Map<?, ?>) b.get(mode);
                    assertEquals(x.get("result_checksum"), y.get("result_checksum"));
                    assertEquals(x.get("primary_state_checksum"), y.get("primary_state_checksum"));
                    assertEquals(secondary ? 14L : 6L, x.get("workload_operations"));
                    assertEquals(secondary ? 14L : 6L, y.get("workload_operations"));
                }
                assertThrows(
                        IOException.class,
                        () ->
                                ReferenceReplay.checkFiles(
                                        baseline,
                                        workload,
                                        new ReplayConfig(
                                                secondary,
                                                "none",
                                                1,
                                                secondary ? 15 : 7,
                                                0,
                                                0,
                                                "transaction")));
            }
        } finally {
            try (var paths = Files.walk(directory)) {
                for (Path path : paths.sorted(java.util.Comparator.reverseOrder()).toList())
                    Files.delete(path);
            }
        }
    }
}
