package org.sibench.replay;

import static org.sibench.replay.ReplayInput.*;

import com.google.gson.GsonBuilder;

import org.sibench.capture.IndexOperationTrace;
import org.sibench.capture.IndexReferenceReplay;
import org.sibench.capture.RunTiming;
import org.sibench.trace.TraceManifest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** A single-pass correctness reference, not a timed network/backend benchmark. */
public final class ReferenceReplay {
    private ReferenceReplay() {}

    public static Map<String, Object> run(Path directory, ReplayConfig config, Path report)
            throws IOException {
        return run(directory, config, modes(directory), report);
    }

    /** {@code rowIdModes}: the value modes to replay, as row-id flags, in report order. */
    public static Map<String, Object> run(
            Path directory, ReplayConfig config, List<Boolean> rowIdModes, Path report)
            throws IOException {
        return runDirectory(directory, config, rowIdModes, report, false);
    }

    /** Explicit matrix diagnostics also scan the complete final primary state. */
    static Map<String, Object> audit(Path directory, ReplayConfig config, Path report)
            throws IOException {
        return runDirectory(directory, config, modes(directory), report, true);
    }

    private static List<Boolean> modes(Path directory) throws IOException {
        var declared = TraceManifest.read(directory).getAsJsonArray("value_kinds");
        var modes = new ArrayList<Boolean>();
        for (boolean rowIds : new boolean[]{false, true})
            if (declared.contains(new com.google.gson.JsonPrimitive(rowIds ? "row_id" : "locator"))) modes.add(rowIds);
        return List.copyOf(modes);
    }

    private static Map<String, Object> runDirectory(
            Path directory, ReplayConfig config, List<Boolean> rowIdModes, Path report,
            boolean auditState) throws IOException {
        if (Files.exists(report)) throw new IOException("Replay report already exists: " + report);
        var manifest = TraceManifest.read(directory);
        for (boolean rowIds : rowIdModes)
            TraceManifest.requireValue(directory, rowIds ? org.sibench.adapters.proto.ValueKind.ROW_ID
                    : org.sibench.adapters.proto.ValueKind.LOCATOR);
        var input = prepare(directory, config);
        return runChecked(
                input.baseline(),
                input.workload(),
                config,
                rowIdModes,
                input.preflight(),
                report,
                auditState);
    }

    static Map<String, Object> runChecked(
            IndexOperationTrace.Source baseline,
            IndexOperationTrace.Source workload,
            ReplayConfig config,
            List<Boolean> rowIdModes,
            Map<String, Object> preflight,
            Path report,
            boolean auditState)
            throws IOException {
        if (rowIdModes.isEmpty()) throw new IOException("No value mode to replay");
        RunTiming.current().measure(report.toAbsolutePath().getParent());
        List<Map<String, Object>> results = new ArrayList<>();
        for (boolean rowId : rowIdModes) {
            String mode = rowId ? "rowid" : "locator";
            try (var index = new IndexReferenceReplay(rowId, config.secondaryIndexes());
                    var load = new ReplayProjection(config, false, index::acceptCommitted);
                    var runtime = new ReplayProjection(config, true, index::acceptCommitted)) {
                RunTiming.current().phase("load");
                long start = System.nanoTime();
                baseline.read(load);
                load.finish();
                double loadSeconds = (System.nanoTime() - start) / 1e9;
                RunTiming.current().phase("runtime");
                start = System.nanoTime();
                workload.read(runtime);
                runtime.finish();
                double seconds = (System.nanoTime() - start) / 1e9;
                Map<String, Object> result = new LinkedHashMap<>();
                result.put("mode", mode);
                result.put("baseline_operations", load.dispatched());
                result.put("workload_operations", runtime.dispatched());
                result.put("baseline_seconds", loadSeconds);
                result.put("workload_seconds", seconds);
                result.put("operations_per_second", runtime.dispatched() / seconds);
                result.put("baseline_dispatch", load.describe());
                result.put("workload_dispatch", runtime.describe());
                result.put("queries", index.queries());
                result.put("checked_result_items", index.checkedResults());
                if (auditState) result.put("primary_state_checksum", index.stateChecksum());
                result.put("selected_version_checksum", index.selectedVersionChecksum());
                result.put("snapshot_queries", index.snapshotQueries());
                result.put("retained_index_versions", index.retainedVersions());
                result.put("skipped_entry_histories", index.skippedHistories());
                result.put("read_model", "committed_snapshot");
                results.add(result);
            }
        }
        Map<String, Object> output = new LinkedHashMap<>();
        output.put("status", "passed");
        output.put("preflight", preflight);
        output.put("results", results);
        output.put(
                "scope",
                "single-sender multi-client in-process correctness reference; clients share"
                    + " reference service state; not a network or cross-client visibility"
                    + " implementation; resolved-source version history and timestamp lookup,"
                    + " ordered point/range/limited scans, pinned entries and intersection/union"
                    + " search groups over order-preserving keys of the complete declared source"
                    + " indexes, including reads before Abort and exclusion of aborted writes;"
                    + " no RPC, persistence, concurrent MVCC, physical GC, rate scheduling or sustained-duration proof");
        Files.writeString(
                report,
                new GsonBuilder().setPrettyPrinting().create().toJson(output),
                StandardOpenOption.CREATE_NEW);
        return output;
    }
}
