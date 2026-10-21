package org.sibench.replay;

import static org.junit.Assert.*;

import com.google.gson.Gson;

import org.junit.Test;
import org.sibench.capture.IndexOperationTrace;
import org.sibench.capture.IndexOperationTrace.Operation;
import org.sibench.capture.IndexReferenceReplay;
import org.sibench.generation.BaselineGenerator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public class ReferenceReplayTest {
    private static Operation row(int kind, long version, long file, long rowId) {
        return new Operation(1, kind, 5, 0, 0, 1, version, file, 0, rowId, 10, 20, 0);
    }

    private static Operation search(int index, long key, long count) {
        return new Operation(
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
                        writer, new Operation(3, 5, 5, 0, 0, 1, 1, 0, 0, 50, 10, 20, 0, 3, 1));
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
        assertThrows(IOException.class, () -> model.accept(search(0, 0, 0)));
        assertThrows(
                IOException.class,
                () -> model.accept(new Operation(1, 5, 5, 1, 0, 1, 1, 0, 0, 50, 10, 20, 0)));
    }

    @Test
    public void checksManifestAgainstEncodedRatherThanExpandedRecordCount() throws Exception {
        Path directory = Files.createTempDirectory("sibench-manifest-");
        Path baseline = directory.resolve("baseline.ops"),
                workload = directory.resolve("workload.ops");
        Path units = directory.resolve("work-units.jsonl");
        try {
            try (var writer = new IndexOperationTrace.Writer(baseline)) {
                org.sibench.capture.IndexMutation.emit(
                        row(1, 0, 5, 50).withWorkUnitId(1),
                        Map.of(),
                        Map.of(1, "10", 2, "20", 3, "type"),
                        writer::append);
            }
            try (var writer = new IndexOperationTrace.Writer(workload)) {
                writer.append(search(0, 1, 1).withWorkUnitId(1));
            }
            var manifest =
                    new java.util.LinkedHashMap<String, Object>(
                            Map.of(
                                    "baseline_operations",
                                    4L,
                                    "workload_operations",
                                    1L,
                                    "baseline_trace_sha256",
                                    BaselineGenerator.sha256(baseline),
                                    "workload_trace_sha256",
                                    BaselineGenerator.sha256(workload)));
            String identities =
                    "{\"work_unit_id\":1,\"source_event\":1,\"pid\":1,\"application\":\"request\"}\n";
            Files.writeString(units, identities);
            manifest.put("work_units_sha256", BaselineGenerator.sha256(units));
            manifest.put("secondary_index_coverage", "all_source_definitions");
            Path source = Path.of("benchmark/HyBench-2024").toAbsolutePath();
            manifest.put("source_directory", source.toString());
            manifest.put(
                    "source_schema_sha256", org.sibench.capture.IndexCatalog.fingerprints(source));
            manifest.put(
                    "index_definitions", org.sibench.capture.IndexCatalog.read(source).describe());
            Path manifestPath = directory.resolve("manifest.json");
            Files.writeString(manifestPath, new Gson().toJson(manifest));
            assertEquals(
                    "trace_integrity_and_supply_valid",
                    ReferenceReplay.check(directory, ReplayConfig.defaults()).get("status"));
            Files.writeString(units, "tampered");
            assertThrows(
                    IOException.class,
                    () -> ReferenceReplay.check(directory, ReplayConfig.defaults()));
            Files.writeString(units, identities);
            manifest.put("baseline_operations", 1L);
            Files.writeString(manifestPath, new Gson().toJson(manifest));
            assertThrows(
                    IOException.class,
                    () -> ReferenceReplay.check(directory, ReplayConfig.defaults()));
        } finally {
            Files.deleteIfExists(directory.resolve("manifest.json"));
            Files.deleteIfExists(units);
            Files.deleteIfExists(baseline);
            Files.deleteIfExists(workload);
            Files.delete(directory);
        }
    }

    @Test
    public void indexAppliesSingleOperationsAndReadsIntermediateSameVersionWithoutTransactionApi()
            throws Exception {
        for (boolean rowId : new boolean[] {false, true}) {
            var model = new IndexReferenceReplay(rowId, false);
            model.accept(new Operation(0, 1, 3, 0, 0, 1, 0, 3, 0, 50, 0, 0, 0));
            long old = model.value(3, 1);
            assertThrows(
                    IOException.class,
                    () -> model.accept(IndexOperationTrace.transaction(10, 6, 10, 1, 0)));
            model.accept(new Operation(1, 3, 3, 0, 2, 1, 1, 9, 0, 50, 0, 0, 0, 10, 1));
            assertEquals(
                    rowId ? 50 : IndexReferenceReplay.locator(9, 0),
                    model.lookupPrimary(3, 1, 1).value());
            model.accept(new Operation(2, 3, 3, 0, 2, 1, 1, 9, 1, 50, 0, 0, 0, 10, 2));
            assertEquals(0, model.lookupPrimary(3, 1, 0).commitVersion());
            assertEquals(rowId ? 50 : IndexReferenceReplay.locator(9, 1), model.value(3, 1));
            assertEquals(old, model.lookupPrimary(3, 1, 0).value());
            assertEquals(1, model.lookupPrimary(3, 1, 1).commitVersion());
            assertEquals(2, model.retainedVersions());
            assertThrows(
                    IOException.class,
                    () -> model.accept(new Operation(3, 3, 3, 0, 2, 1, 1, 9, 2, 50, 0, 0, 0)));
        }
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
                        writer, new Operation(1, 3, 5, 0, 2, 1, 1, 9, 0, 50, 10, 20, 0, 10, 1));
                org.sibench.capture.TransferIndexFixtures.appendRow(
                        writer, new Operation(2, 3, 5, 0, 2, 1, 1, 9, 1, 50, 10, 20, 0, 10, 2));
                org.sibench.capture.TransferIndexFixtures.appendRow(
                        writer, new Operation(3, 3, 5, 0, 1, 2, 1, 9, 2, 51, 10, 20, 0, 10, 3));
                org.sibench.capture.TransferIndexFixtures.appendRow(
                        writer, new Operation(4, 5, 5, 0, 0, 2, 1, 0, 0, 51, 10, 20, 0, 10, 4));
                writer.append(IndexOperationTrace.transaction(10, 7, 10, 1, 4));
                writer.append(new Operation(11, 2, 5, 0, 0, 1, 1, 0, 0, 0, 0, 0, 1));
                writer.append(new Operation(12, 2, 5, 0, 0, 2, 1, 0, 0, 0, 0, 0, 0));
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
