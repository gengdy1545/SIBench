package org.sibench.replay;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;

import org.sibench.capture.IndexOperationTrace;
import org.sibench.capture.IndexReferenceReplay;
import org.sibench.generation.BaselineGenerator;

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

    public static Map<String, Object> check(Path directory, ReplayConfig config)
            throws IOException {
        if (Files.exists(directory.resolve("INCOMPLETE"))
                || !Files.isRegularFile(directory.resolve("manifest.json")))
            throw new IOException("Incomplete normalized dataset");
        var manifest =
                JsonParser.parseString(Files.readString(directory.resolve("manifest.json")))
                        .getAsJsonObject();
        for (String name : List.of("baseline", "workload")) {
            String checksum = name + "_trace_sha256";
            if (!manifest.has(checksum)
                    || !BaselineGenerator.sha256(directory.resolve(name + ".ops"))
                            .equals(manifest.get(checksum).getAsString()))
                throw new IOException("Trace checksum mismatch: " + name);
        }
        if (IndexOperationTrace.formatVersion(directory.resolve("workload.ops")) >= 6) {
            if (!manifest.has("work_units_sha256")
                    || !BaselineGenerator.sha256(directory.resolve("work-units.jsonl"))
                            .equals(manifest.get("work_units_sha256").getAsString()))
                throw new IOException("Source work-unit identity checksum mismatch");
        }
        var checked =
                checkFiles(
                        directory.resolve("baseline.ops"),
                        directory.resolve("workload.ops"),
                        config);
        for (String name : List.of("baseline", "workload")) {
            var counts = (Map<?, ?>) checked.get(name);
            String countKey = name + "_operations";
            if (!manifest.has(countKey)
                    || manifest.get(countKey).getAsLong() != (long) counts.get("encoded_records"))
                throw new IOException("Trace record count mismatch: " + name);
        }
        return checked;
    }

    public static Map<String, Object> checkFiles(Path baseline, Path workload, ReplayConfig config)
            throws IOException {
        config.requireReferenceCapabilities();
        boolean transactional = IndexOperationTrace.formatVersion(workload) >= 3;
        try (var load = new ReplayProjection(config, false, operation -> {});
                var runtime = new ReplayProjection(config, transactional, operation -> {})) {
            long loadRecords = IndexOperationTrace.readExpanded(baseline, load);
            load.finish();
            long runtimeRecords = IndexOperationTrace.readExpanded(workload, runtime);
            runtime.finish();
            runtime.requireClientCoverage();
            if (runtime.dispatched() < config.requiredOperations())
                throw new IOException(
                        "Insufficient single-pass trace: requires "
                                + config.requiredOperations()
                                + " effective workload operations, found "
                                + runtime.dispatched()
                                + "; continue native capture, trace repetition is not supported");
            Map<String, Object> report = new LinkedHashMap<>();
            report.put("status", "trace_integrity_and_supply_valid");
            report.put("configuration", config.describe());
            var loadCounts = load.describe();
            loadCounts.put("encoded_records", loadRecords);
            var runtimeCounts = runtime.describe();
            runtimeCounts.put("encoded_records", runtimeRecords);
            report.put("baseline", loadCounts);
            report.put("workload", runtimeCounts);
            report.put(
                    "budget_scope", "input supply only; no rate scheduling or duration guarantee");
            report.put(
                    "validation_scope",
                    "binary integrity and input counts; state/result semantics checked during"
                        + " replay");
            return report;
        }
    }

    public static Map<String, Object> run(Path directory, ReplayConfig config, Path report)
            throws IOException {
        if (Files.exists(report)) throw new IOException("Replay report already exists: " + report);
        var preflight = check(directory, config);
        return runChecked(
                directory.resolve("baseline.ops"),
                directory.resolve("workload.ops"),
                config,
                preflight,
                report);
    }

    public static Map<String, Object> runFiles(
            Path baseline, Path workload, ReplayConfig config, Path report) throws IOException {
        if (Files.exists(report)) throw new IOException("Replay report already exists: " + report);
        return runChecked(
                baseline, workload, config, checkFiles(baseline, workload, config), report);
    }

    private static Map<String, Object> runChecked(
            Path baseline,
            Path workload,
            ReplayConfig config,
            Map<String, Object> preflight,
            Path report)
            throws IOException {
        List<Map<String, Object>> results = new ArrayList<>();
        boolean versioned = IndexOperationTrace.formatVersion(workload) >= 3;
        for (boolean rowId : new boolean[] {false, true}) {
            var index = new IndexReferenceReplay(rowId, config.secondaryIndexes(), versioned);
            try (var load = new ReplayProjection(config, false, index::accept);
                    var runtime = new ReplayProjection(config, versioned, index::accept)) {
                long start = System.nanoTime();
                IndexOperationTrace.readExpanded(baseline, load);
                load.finish();
                double loadSeconds = (System.nanoTime() - start) / 1e9;
                start = System.nanoTime();
                IndexOperationTrace.readExpanded(workload, runtime);
                runtime.finish();
                double seconds = (System.nanoTime() - start) / 1e9;
                Map<String, Object> result = new LinkedHashMap<>();
                result.put("mode", rowId ? "rowid" : "locator");
                result.put("baseline_operations", load.dispatched());
                result.put("workload_operations", runtime.dispatched());
                result.put("baseline_seconds", loadSeconds);
                result.put("workload_seconds", seconds);
                result.put("operations_per_second", runtime.dispatched() / seconds);
                result.put("baseline_dispatch", load.describe());
                result.put("workload_dispatch", runtime.describe());
                result.put("queries", index.queries());
                result.put("checked_result_items", index.checkedResults());
                result.put("relocation_writes", index.relocationWrites());
                result.put("result_checksum", index.checksum());
                result.put("primary_state_checksum", index.stateChecksum());
                result.put(
                        "selected_version_checksum",
                        versioned ? index.selectedVersionChecksum() : null);
                result.put("snapshot_queries", index.snapshotQueries());
                result.put("retained_index_versions", versioned ? index.retainedVersions() : null);
                result.put(
                        "read_model",
                        versioned ? "committed_version_at_read_timestamp" : "legacy_latest_state");
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
                    + " implementation; v3-v6 committed-source version history and timestamp"
                    + " lookup, v1/v2 latest-state compatibility; supported primary/transfer"
                    + " indexes only; no RPC, persistence, concurrent PG MVCC, range query,"
                    + " physical GC, rate scheduling or sustained-duration proof");
        Files.writeString(
                report,
                new GsonBuilder().setPrettyPrinting().create().toJson(output),
                StandardOpenOption.CREATE_NEW);
        return output;
    }
}
