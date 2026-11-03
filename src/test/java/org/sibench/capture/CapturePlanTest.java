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
        Files.writeString(trace.resolve("manifest.json"), "{\"baseline_rows\":1000000,\"logical_row_version_writes\":10000}");
        Files.writeString(root.resolve("sysbench.yaml"),
                "benchmark: sysbench\nnative:\n  workloads:\n    oltp_insert: {events: 100}\n    oltp_point_select: {events: 300}\n");
        var plan = CapturePlan.writeRatio(root, root.relativize(root.resolve("sysbench.yaml")), trace, 0.05);
        assertEquals(5.0, (Double) plan.get("factor"), 1e-9);
        assertEquals(Map.of("native.workloads.oltp_insert.events", 500L, "native.workloads.oltp_point_select.events", 1500L),
                plan.get("parameters"));
        Files.writeString(root.resolve("hybench.yaml"),
                "benchmark: hybench\nnative:\n  duration_minutes: 1\n  phase_duration_minutes: {tp: 2}\n");
        var hybench = CapturePlan.writeRatio(root, root.relativize(root.resolve("hybench.yaml")), trace, 0.02);
        // TP keeps its own duration, XP the default; both scale by the same factor of 2.
        assertEquals(Map.of("native.phase_duration_minutes.tp", 4L, "native.phase_duration_minutes.xp", 2L),
                hybench.get("parameters"));
        Files.writeString(trace.resolve("manifest.json"), "{\"baseline_rows\":1000000,\"logical_row_version_writes\":0}");
        assertThrows(java.io.IOException.class,
                () -> CapturePlan.writeRatio(root, root.relativize(root.resolve("sysbench.yaml")), trace, 0.05));
    }

    /** The replay budget is reached by scaling with the effective operations replay itself counts. */
    @Test
    public void operationsScaleToTheReplayBudget() throws Exception {
        var root = temporary.getRoot().toPath();
        var trace = Files.createDirectory(root.resolve("ops-trace"));
        var protocol = new ProtoTrace.Writer(trace.resolve("workload.pb.zst"));
        try (protocol) {
            for (int i = 0; i < 1000; i++) {
                var operation = TestOps.op(i, IndexOperationTrace.SEARCH, 1, 0, 0, i, 0, 0, 0, 0, 0, 0, 1);
                protocol.append(operation);
            }
        }
        var manifest = new java.util.LinkedHashMap<String, Object>();
        manifest.put("baseline_rows", 0);
        manifest.put("source_work_units", 1);
        manifest.put("baseline_operations", 0);
        manifest.put("workload_operations", 1000);
        manifest.put("baseline_trace_sha256", "0".repeat(64));
        Files.writeString(root.resolve("replay.yaml"),
                "schema_version: 1\npurpose: sibench\ntarget:\n  secondary_indexes:\n    enabled: true\n"
                        + "replay:\n  strategy: single_pass\n  trace_budget:\n"
                        + "    duration_seconds: 10\n    planned_operations_per_second: 300\n    warmup_operations: 0\n    reserve_percent: 0\n"
                        + "versions:\n  gc:\n    owner: none\n");
        Files.writeString(root.resolve("sysbench.yaml"),
                "benchmark: sysbench\nnative:\n  workloads:\n    oltp_point_select: {events: 100}\n");
        for (String strategy : java.util.List.of("full", "proto")) {
            manifest.put("output", strategy);
            var files = new com.google.gson.JsonArray();
            files.add(new com.google.gson.Gson().toJsonTree(ProtoTrace.file("workload.pb.zst", protocol)));
            manifest.put("proto_files", Map.of("workload", files));
            manifest.put("workload_trace_sha256", ProtoTrace.identity(files));
            Files.writeString(trace.resolve("manifest.json"), new com.google.gson.Gson().toJson(manifest));
            var plan = CapturePlan.operations(root, Path.of("sysbench.yaml"), trace, Path.of("replay.yaml"));
            assertEquals(1000L, plan.get("calibration_effective_operations"));
            assertEquals(3000L, plan.get("target_effective_operations"));
            assertEquals(Map.of("native.workloads.oltp_point_select.events", 300L), plan.get("parameters"));
        }
    }
}
