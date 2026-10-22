package org.sibench.replay;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;

import org.sibench.capture.IndexCatalog;
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
        if (!manifest.has("work_units_sha256")
                || !BaselineGenerator.sha256(directory.resolve("work-units.jsonl"))
                        .equals(manifest.get("work_units_sha256").getAsString()))
            throw new IOException("Source work-unit identity checksum mismatch");
        if (!manifest.has("secondary_index_coverage")
                || !manifest.get("secondary_index_coverage")
                        .getAsString()
                        .equals("all_source_definitions"))
            throw new IOException("Trace must declare complete source index coverage");
        validateIndexCoverage(directory, manifest);
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

    private static void validateIndexCoverage(Path directory, com.google.gson.JsonObject manifest)
            throws IOException {
        IndexCatalog.fromManifest(manifest);
        Map<Long, Long> units = new LinkedHashMap<>();
        long lastUnit = 0, lastEvent = 0;
        for (String line : Files.readAllLines(directory.resolve("work-units.jsonl"))) {
            var unit = JsonParser.parseString(line).getAsJsonObject();
            long id = unit.get("work_unit_id").getAsLong(),
                    event = unit.get("source_event").getAsLong();
            if (id != lastUnit + 1
                    || event <= lastEvent
                    || unit.get("pid").getAsInt() < 1
                    || unit.get("application").getAsString().isEmpty())
                throw new IOException("Invalid source work-unit catalog order/identity");
            units.put(id, event);
            lastUnit = id;
            lastEvent = event;
        }
        if (manifest.has("source_work_units")
                && manifest.get("source_work_units").getAsLong() != units.size())
            throw new IOException("Source work-unit count mismatch");
        Map<Integer, java.util.Set<Integer>> catalog = new LinkedHashMap<>();
        for (var item : manifest.getAsJsonArray("index_definitions")) {
            var definition = item.getAsJsonObject();
            int table = definition.get("table_id").getAsInt(),
                    index = definition.get("index_id").getAsInt();
            if (!catalog.computeIfAbsent(table, ignored -> new java.util.TreeSet<>()).add(index))
                throw new IOException("Duplicate index catalog identity");
        }
        for (String name : List.of("baseline", "workload")) {
            java.util.Set<Integer> seen = new java.util.HashSet<>();
            IndexOperationTrace.Operation[] primary = {null};
            IndexOperationTrace.Operation[] previous = {null};
            int[] position = {0};
            long[] rows = {0};
            java.util.Set<Integer> loadedTables = new java.util.HashSet<>();
            IndexOperationTrace.read(
                    directory.resolve(name + ".ops"),
                    operation -> {
                        if (!IndexOperationTrace.isControl(operation) && name.equals("workload")) {
                            Long start = units.get(operation.workUnitId());
                            Long next = units.get(operation.workUnitId() + 1);
                            if (start == null
                                    || operation.sourceEvent() < start
                                    || next != null && operation.sourceEvent() >= next)
                                throw new IOException(
                                        "Operation not bound to its source SQL request");
                        }
                        if (operation.kind() == IndexOperationTrace.SEARCH
                                || IndexOperationTrace.isControl(operation)) {
                            if (primary[0] != null)
                                throw new IOException("Interrupted index maintenance unit");
                            if (operation.expansionSize() != 0
                                    || operation.expansionPosition() != 0)
                                throw new IOException("Unexpected search/control expansion");
                        }
                        if (operation.kind() == IndexOperationTrace.SEARCH) {
                            if (!catalog.getOrDefault(operation.table(), java.util.Set.of())
                                    .contains(operation.index()))
                                throw new IOException("Search outside source catalog");
                            return;
                        }
                        if (IndexOperationTrace.isControl(operation)) return;
                        if (operation.expansionSize() < 1)
                            throw new IOException(
                                    "Current index write missing complete maintenance unit");
                        if (operation.expansionPosition() == 0) {
                            if (primary[0] != null)
                                throw new IOException("Truncated index maintenance unit");
                            if (operation.index() != 0)
                                throw new IOException("Maintenance unit missing primary");
                            seen.clear();
                            primary[0] = operation;
                            previous[0] = null;
                            position[0] = 0;
                            if (name.equals("baseline")) {
                                if (operation.workUnitId()
                                        != 1 + rows[0]++ / IndexOperationTrace.BLOCK_RECORDS)
                                    throw new IOException(
                                            "Invalid baseline source-row work-unit assignment");
                                loadedTables.add(operation.table());
                            }
                        }
                        if (primary[0] == null
                                || operation.table() != primary[0].table()
                                || !operation.key().equals(primary[0].key())
                                || operation.expansionSize() != primary[0].expansionSize()
                                || operation.expansionPosition() != position[0]++
                                || operation.rowId() != primary[0].rowId()
                                || operation.version() != primary[0].version()
                                || operation.transactionId() != primary[0].transactionId()
                                || operation.mutationOrdinal() != primary[0].mutationOrdinal()
                                || operation.workUnitId() != primary[0].workUnitId()
                                || operation.sourceEvent() != primary[0].sourceEvent()
                                || operation.flags() != primary[0].flags()
                                || (operation.index() == 0 && operation.indexedValue() != null)
                                || (name.equals("baseline")
                                        && operation.kind() != IndexOperationTrace.INSERT)
                                || (operation.kind() != primary[0].kind()
                                        && operation.kind() != IndexOperationTrace.LOGICAL_DELETE))
                            throw new IOException("Invalid maintenance unit identity");
                        if (operation.index() > 0
                                && operation.kind() != IndexOperationTrace.LOGICAL_DELETE
                                && (operation.file() != primary[0].file()
                                        || operation.position() != primary[0].position()))
                            throw new IOException("Secondary value location differs from primary");
                        if (operation.kind() == IndexOperationTrace.LOGICAL_DELETE
                                && (operation.file() != 0 || operation.position() != 0))
                            throw new IOException("Logical deletion has a physical location");
                        if (!catalog.getOrDefault(operation.table(), java.util.Set.of())
                                .contains(operation.index()))
                            throw new IOException("Write outside source catalog");
                        if (previous[0] != null) {
                            var prior = previous[0];
                            if (operation.index() < prior.index()
                                    || operation.index() > prior.index() + 1
                                    || (operation.index() != prior.index()
                                            && prior.kind() != primary[0].kind())
                                    || (operation.index() == prior.index()
                                            && (prior.kind() != IndexOperationTrace.LOGICAL_DELETE
                                                    || operation.kind()
                                                            != IndexOperationTrace.UPSERT
                                                    || java.util.Objects.equals(
                                                            prior.indexedValue(),
                                                            operation.indexedValue()))))
                                throw new IOException("Invalid source index maintenance order");
                        }
                        previous[0] = operation;
                        seen.add(operation.index());
                        if (operation.expansionPosition() == operation.expansionSize() - 1) {
                            if (!seen.equals(catalog.get(operation.table()))
                                    || operation.kind() != primary[0].kind())
                                throw new IOException(
                                        "Partial source index maintenance is not supported");
                            primary[0] = null;
                        }
                    });
            if (primary[0] != null) throw new IOException("Unfinished index maintenance unit");
            if (name.equals("baseline")
                    && manifest.has("baseline_rows")
                    && (manifest.get("baseline_rows").getAsLong() != rows[0]
                            || !loadedTables.equals(catalog.keySet())))
                throw new IOException("Baseline row/table coverage mismatch");
        }
    }

    public static Map<String, Object> checkFiles(Path baseline, Path workload, ReplayConfig config)
            throws IOException {
        config.requireReferenceCapabilities();
        try (var load = new ReplayProjection(config, false, operation -> {});
                var runtime = new ReplayProjection(config, true, operation -> {})) {
            long loadRecords = IndexOperationTrace.read(baseline, load);
            load.finish();
            long runtimeRecords = IndexOperationTrace.read(workload, runtime);
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
                    "bare-file binary integrity and supply only; no source catalog/provenance"
                        + " guarantee; directory check additionally validates complete source"
                        + " catalog and maintenance units; state/results checked during replay");
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
        for (boolean rowId : new boolean[] {false, true}) {
            var index = new IndexReferenceReplay(rowId, config.secondaryIndexes());
            try (var load = new ReplayProjection(config, false, index::accept);
                    var runtime = new ReplayProjection(config, true, index::accept)) {
                long start = System.nanoTime();
                IndexOperationTrace.read(baseline, load);
                load.finish();
                double loadSeconds = (System.nanoTime() - start) / 1e9;
                start = System.nanoTime();
                IndexOperationTrace.read(workload, runtime);
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
                result.put("selected_version_checksum", index.selectedVersionChecksum());
                result.put("snapshot_queries", index.snapshotQueries());
                result.put("retained_index_versions", index.retainedVersions());
                result.put("read_model", "committed_version_at_read_timestamp");
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
                    + " implementation; v8 committed-source version history and timestamp"
                    + " lookup, ordered point/range/limited scans over order-preserving keys of"
                    + " the complete declared source indexes; no RPC, persistence, concurrent PG"
                    + " MVCC, physical GC, rate scheduling or sustained-duration proof");
        Files.writeString(
                report,
                new GsonBuilder().setPrettyPrinting().create().toJson(output),
                StandardOpenOption.CREATE_NEW);
        return output;
    }
}
