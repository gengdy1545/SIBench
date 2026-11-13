package org.sibench.replay;

import static org.junit.Assert.*;


import org.junit.Test;
import org.sibench.capture.IndexOperationTrace;
import org.sibench.capture.ProtoTrace;
import org.sibench.capture.TestOps;
import org.sibench.capture.IndexOperationTrace.Operation;
import org.sibench.capture.IndexReferenceReplay;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
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
        Path baseline = directory.resolve("baseline.pb.zst"),
                workload = directory.resolve("workload.pb.zst");
        Path report = directory.resolve("report.json");
        try {
            try (var writer = new ProtoTrace.Writer(baseline, true)) {
                org.sibench.capture.TransferIndexFixtures.appendRow(writer, row(1, 0, 5, 50));
            }
            try (var writer = new ProtoTrace.Writer(workload, true)) {
                writer.append(search(0, 1, 1));
                writer.append(search(1, 10, 1));
                writer.append(search(2, 20, 1));
                writer.append(IndexOperationTrace.transaction(0, 6, 7, 7, 0));
                org.sibench.capture.TransferIndexFixtures.appendRow(
                        writer, TestOps.op(3, 5, 5, 0, 0, 1, 7, 0, 0, 50, 10, 20, 0, 7, 1));
                writer.append(IndexOperationTrace.transaction(0, 7, 7, 7, 1));
            }
            assertThrows(
                    IOException.class,
                    () ->
                            ReferenceReplayFixture.run(
                                    baseline,
                                    workload,
                                    new ReplayConfig(false, 1, 3, 0, 0),
                                    report));
            assertFalse(Files.exists(report));
            var config = new ReplayConfig(false, 1, 2, 0, 0);
            var checked = ReferenceReplayFixture.check(baseline, workload, config);
            assertEquals("trace_integrity_and_supply_valid", checked.get("status"));
            assertEquals(7L, ((Map<?, ?>) checked.get("workload")).get("last_resolved_version"));
            var result = ReferenceReplayFixture.run(baseline, workload, config, report);
            assertEquals("passed", result.get("status"));
            assertThrows(
                    IOException.class,
                    () -> ReferenceReplayFixture.run(baseline, workload, config, report));
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
    public void dispatchModesKeepIdenticalFinalCommittedStateAndSupply() throws Exception {
        Path directory = Files.createTempDirectory("sibench-tx-replay-");
        Path baseline = directory.resolve("baseline.pb.zst"),
                workload = directory.resolve("workload.pb.zst");
        try {
            try (var writer = new ProtoTrace.Writer(baseline, true)) {
                org.sibench.capture.TransferIndexFixtures.appendRow(writer, row(1, 0, 5, 50));
            }
            try (var writer = new ProtoTrace.Writer(workload, true)) {
                writer.append(IndexOperationTrace.transaction(0, 6, 1, 1, 0));
                org.sibench.capture.TransferIndexFixtures.appendRow(
                        writer, TestOps.op(1, 3, 5, 0, 2, 1, 1, 9, 0, 50, 10, 20, 0, 1, 1));
                org.sibench.capture.TransferIndexFixtures.appendRow(
                        writer, TestOps.op(2, 3, 5, 0, 2, 1, 1, 9, 1, 50, 10, 20, 0, 1, 2));
                org.sibench.capture.TransferIndexFixtures.appendRow(
                        writer, TestOps.op(3, 3, 5, 0, 1, 2, 1, 9, 2, 51, 10, 20, 0, 1, 3));
                org.sibench.capture.TransferIndexFixtures.appendRow(
                        writer, TestOps.op(4, 5, 5, 0, 0, 2, 1, 0, 0, 51, 10, 20, 0, 1, 4));
                writer.append(IndexOperationTrace.transaction(0, 7, 1, 1, 4));
                writer.append(TestOps.op(11, 2, 5, 0, 0, 1, 1, 0, 0, 0, 0, 0, 1));
                writer.append(TestOps.op(12, 2, 5, 0, 0, 2, 1, 0, 0, 0, 0, 0, 0));
            }
            for (boolean secondary : new boolean[] {true, false}) {
                var groupedConfig = new ReplayConfig(secondary, 0, 0, 0, 0, "transaction");
                var rawConfig = new ReplayConfig(secondary, 0, 0, 0, 0, "operation");
                var grouped =
                        ReferenceReplayFixture.run(
                                baseline,
                                workload,
                                groupedConfig,
                                directory.resolve("grouped-" + secondary + ".json"));
                var raw =
                        ReferenceReplayFixture.run(
                                baseline,
                                workload,
                                rawConfig,
                                directory.resolve("raw-" + secondary + ".json"));
                var a = (java.util.List<?>) grouped.get("results");
                var b = (java.util.List<?>) raw.get("results");
                for (int mode = 0; mode < 2; mode++) {
                    var x = (Map<?, ?>) a.get(mode);
                    var y = (Map<?, ?>) b.get(mode);
                    assertNotNull(x.get("primary_state_checksum"));
                    assertEquals(x.get("primary_state_checksum"), y.get("primary_state_checksum"));
                    // Key 1 retains its last update; key 2 retains its final tombstone.
                    // Two final entries per included index, followed by two snapshot searches.
                    assertEquals(secondary ? 8L : 4L, x.get("workload_operations"));
                    assertEquals(secondary ? 8L : 4L, y.get("workload_operations"));
                }
                assertThrows(
                        IOException.class,
                        () ->
                                ReferenceReplayFixture.check(
                                        baseline,
                                        workload,
                                        new ReplayConfig(
                                                secondary, 1,
                                                secondary ? 9 : 5,
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
