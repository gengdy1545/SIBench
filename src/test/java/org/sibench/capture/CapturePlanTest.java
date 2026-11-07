package org.sibench.capture;

import static org.junit.Assert.*;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/** One factor scales every native knob of a source, so the workload mix is kept. */
public class CapturePlanTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void scalesEachSourcesOwnKnobs() throws Exception {
        var root = temporary.getRoot().toPath();
        var trace = Files.createDirectory(root.resolve("trace"));
        var manifest = calibration(trace, false);
        Files.writeString(root.resolve("sysbench.yaml"),
                "benchmark: sysbench\nnative:\n  workloads:\n    oltp_insert: {events: 100}\n    oltp_point_select: {events: 300}\n");
        Path replay = root.resolve("replay.yaml");
        Files.writeString(replay, "schema_version: 1\npurpose: sibench\ntarget:\n  secondary_indexes: {enabled: false}\n"
                + "replay:\n  trace_budget: {duration_seconds: 1, planned_operations_per_second: 25, warmup_operations: 0, reserve_percent: 0}\n");
        var plan = CapturePlan.operations(root, Path.of("sysbench.yaml"), trace, replay);
        assertEquals(12.5, (Double) plan.get("factor"), 1e-9);
        assertEquals(Map.of("native.workloads.oltp_insert.events", 1250L, "native.workloads.oltp_point_select.events", 3750L),
                plan.get("parameters"));
        Files.writeString(root.resolve("hybench.yaml"),
                "benchmark: hybench\nnative:\n  duration_minutes: 1\n  phase_duration_minutes: {tp: 2}\n");
        var hybench = CapturePlan.operations(root, Path.of("hybench.yaml"), trace, replay);
        // TP keeps its own duration, XP the default; both scale by the same factor of 12.5, rounding each duration up.
        assertEquals(Map.of("native.phase_duration_minutes.tp", 25L, "native.phase_duration_minutes.xp", 13L),
                hybench.get("parameters"));
        var aborted = Files.createDirectory(root.resolve("aborted"));
        calibration(aborted, true);
        // Only the first transaction's two primary writes survive; all-aborted input has no supply.
        assertTrue(assertThrows(java.io.IOException.class,
                () -> CapturePlan.operations(root, Path.of("sysbench.yaml"), aborted, replay))
                .getMessage().contains("no effective workload operations"));
        assertEquals(2L, plan.get("calibration_effective_operations"));
        manifest.put("workload_operations", 999L);
        Files.writeString(trace.resolve("manifest.json"), new com.google.gson.Gson().toJson(manifest));
        assertThrows(java.io.IOException.class,
                () -> CapturePlan.operations(root, Path.of("sysbench.yaml"), trace, replay));
    }

    private static Map<String, Object> calibration(Path directory, boolean abortAll) throws Exception {
        var writer = new ProtoTrace.Writer(directory.resolve("workload.pb.zst"));
        try (writer) {
            for (long version = 1; version <= 2; version++) {
                writer.accept(IndexOperationTrace.transaction(version, IndexOperationTrace.TRANSACTION_BEGIN, version, version, 0));
                for (long ordinal = 1; ordinal <= version + 1; ordinal++) {
                    int kind = ordinal == 1 ? IndexOperationTrace.UPSERT : IndexOperationTrace.LOGICAL_DELETE;
                    var operation = IndexOperationTrace.write(version, kind, 1, 0,
                            org.sibench.trace.IndexKey.ofInt(ordinal), version, 0, 0, ordinal)
                            .withTransaction(version, ordinal).withWorkUnitId(version);
                    var keys = Map.of(1, org.sibench.trace.IndexKey.ofInt(10));
                    IndexMutation.emit(operation, kind == IndexOperationTrace.UPSERT ? Map.of() : keys,
                            kind == IndexOperationTrace.UPSERT ? keys : Map.of(), writer);
                }
                writer.accept(IndexOperationTrace.transaction(version,
                        abortAll || version == 2 ? IndexOperationTrace.TRANSACTION_ABORT : IndexOperationTrace.TRANSACTION_COMMIT,
                        version, version, version + 1));
            }
        }
        var manifest = new java.util.LinkedHashMap<String, Object>();
        manifest.put("value_kinds", java.util.List.of("row_id"));
        manifest.put("output", "proto");
        manifest.put("secondary_index_coverage", "all_source_definitions");
        manifest.put("baseline_rows", 100);
        manifest.put("baseline_operations", 200);
        manifest.put("baseline_trace_sha256", "0".repeat(64));
        manifest.put("source_work_units", 2);
        manifest.put("last_resolved_version", 2);
        manifest.put("workload_operations", writer.count());
        var files = new com.google.gson.JsonArray();
        files.add(new com.google.gson.Gson().toJsonTree(ProtoTrace.file("workload.pb.zst", writer)));
        manifest.put("proto_files", Map.of("workload", files));
        manifest.put("workload_trace_sha256", ProtoTrace.identity(files));
        Files.writeString(directory.resolve("manifest.json"), new com.google.gson.Gson().toJson(manifest));
        return manifest;
    }

    /** The replay budget is reached by scaling with the effective operations replay itself counts. */
    @Test
    public void operationsScaleToTheReplayBudgetIncludingGroupedSearches() throws Exception {
        var root = temporary.getRoot().toPath();
        var trace = Files.createDirectory(root.resolve("ops-trace"));
        var protocol = new ProtoTrace.Writer(trace.resolve("workload.pb.zst"));
        try (protocol) {
            for (int i = 0; i < 1000; i++) {
                var operation = i < 3
                        ? IndexOperationTrace.search(0, 1, 0,
                                IndexOperationTrace.Range.equal(org.sibench.trace.IndexKey.ofInt(i)),
                                null, 0, 3, IndexOperationTrace.UNION_GROUP, i, 3).withWorkUnitId(1)
                        : TestOps.op(i, IndexOperationTrace.SEARCH, 1, 0, 0, i, 0, 0, 0, 0, 0, 0, 1);
                protocol.append(operation);
            }
        }
        var manifest = new java.util.LinkedHashMap<String, Object>();
        manifest.put("value_kinds", java.util.List.of("row_id", "locator"));
        manifest.put("baseline_rows", 0);
        manifest.put("source_work_units", 1);
        manifest.put("last_resolved_version", 0);
        manifest.put("baseline_operations", 0);
        manifest.put("workload_operations", 1000);
        manifest.put("baseline_trace_sha256", "0".repeat(64));
        Files.writeString(root.resolve("replay.yaml"),
                "schema_version: 1\npurpose: sibench\ntarget:\n  secondary_indexes:\n    enabled: true\n"
                        + "replay:\n  trace_budget:\n"
                        + "    duration_seconds: 10\n    planned_operations_per_second: 300\n    warmup_operations: 0\n    reserve_percent: 0\n"
                       );
        Files.writeString(root.resolve("sysbench.yaml"),
                "benchmark: sysbench\nnative:\n  workloads:\n    oltp_point_select: {events: 100}\n");
        manifest.put("output", "proto");
        manifest.put("secondary_index_coverage", "all_source_definitions");
        var files = new com.google.gson.JsonArray();
        files.add(new com.google.gson.Gson().toJsonTree(ProtoTrace.file("workload.pb.zst", protocol)));
        manifest.put("proto_files", Map.of("workload", files));
        manifest.put("workload_trace_sha256", ProtoTrace.identity(files));
        Files.writeString(trace.resolve("manifest.json"), new com.google.gson.Gson().toJson(manifest));
        var plan = CapturePlan.operations(root, Path.of("sysbench.yaml"), trace, Path.of("replay.yaml"));
        assertEquals(1000L, plan.get("calibration_effective_operations"));
        assertEquals(3000L, plan.get("target_effective_operations"));
        assertEquals(Map.of("native.workloads.oltp_point_select.events", 300L), plan.get("parameters"));
        manifest.put("secondary_index_coverage", "primary_only");
        Files.writeString(trace.resolve("manifest.json"), new com.google.gson.Gson().toJson(manifest));
        assertThrows(java.io.IOException.class,
                () -> CapturePlan.operations(root, Path.of("sysbench.yaml"), trace, Path.of("replay.yaml")));
    }
}
