package org.sibench.replay;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;

import org.sibench.capture.IndexCatalog;
import org.sibench.capture.IndexOperationTrace;
import org.sibench.capture.IndexReferenceReplay;
import org.sibench.capture.RunTiming;
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
            Path file = directory.resolve(name + ".ops");
            // A derived baseline load is checked against its digest as it is read.
            if (!manifest.has(checksum)
                    || (Files.exists(file) || name.equals("workload"))
                            && !IndexOperationTrace.sha256(file).equals(manifest.get(checksum).getAsString()))
                throw new IOException("Trace checksum mismatch: " + name);
        }
        if (!manifest.has("secondary_index_coverage")
                || !manifest.get("secondary_index_coverage")
                        .getAsString()
                        .equals("all_source_definitions"))
            throw new IOException("Trace must declare complete source index coverage");
        var units = org.sibench.capture.WorkUnits.read(directory);
        if (!manifest.has("work_units_sha256")
                || !units.sha256().equals(manifest.get("work_units_sha256").getAsString()))
            throw new IOException("Source work-unit identity checksum mismatch");
        var catalog = catalog(manifest, units);
        var baselineCoverage = new Coverage("baseline", catalog, units);
        var workloadCoverage = new Coverage("workload", catalog, units);
        var checked =
                checkFiles(
                        tee(org.sibench.capture.BaselineOperations.of(directory), baselineCoverage),
                        tee(
                                IndexOperationTrace.Source.file(directory.resolve("workload.ops")),
                                workloadCoverage),
                        config);
        baselineCoverage.finish(manifest);
        workloadCoverage.finish(manifest);
        for (String name : List.of("baseline", "workload")) {
            var counts = (Map<?, ?>) checked.get(name);
            String countKey = name + "_operations";
            if (!manifest.has(countKey)
                    || manifest.get(countKey).getAsLong() != (long) counts.get("encoded_records"))
                throw new IOException("Trace record count mismatch: " + name);
        }
        return checked;
    }

    /**
     * Validates source catalog coverage, maintenance units and source-request binding of one
     * stream while it is read for the supply check, so the trace is decoded once.
     */
    private static final class Coverage implements IndexOperationTrace.Consumer {
        private final String name;
        private final Map<Integer, java.util.Set<Integer>> catalog;
        private final org.sibench.capture.WorkUnits.Catalog units;
        private final java.util.Set<Integer> seen = new java.util.HashSet<>();
        private final java.util.Set<Integer> loadedTables = new java.util.HashSet<>();
        private IndexOperationTrace.Operation primary, previous;
        private int position, group;
        private long rows, groupEvent;

        Coverage(
                String name,
                Map<Integer, java.util.Set<Integer>> catalog,
                org.sibench.capture.WorkUnits.Catalog units) {
            this.name = name;
            this.catalog = catalog;
            this.units = units;
        }

        @Override
        public void accept(IndexOperationTrace.Operation operation) throws IOException {
                if (!IndexOperationTrace.isControl(operation) && name.equals("workload")) {
                    long start = units.firstEvent(operation.workUnitId());
                    long next = units.firstEvent(operation.workUnitId() + 1);
                    if (start < 0
                            || operation.sourceEvent() < start
                            || next >= 0 && operation.sourceEvent() >= next)
                        throw new IOException(
                                "Operation not bound to its source SQL request");
                }
                if (operation.kind() == IndexOperationTrace.SEARCH
                        || IndexOperationTrace.isControl(operation)) {
                    if (primary != null)
                        throw new IOException("Interrupted index maintenance unit");
                    if (!operation.grouped()
                            && (operation.expansionSize() != 0
                                    || operation.expansionPosition() != 0))
                        throw new IOException("Unexpected search/control expansion");
                    if (group > 0 && !operation.grouped())
                        throw new IOException("Interrupted search group");
                }
                if (operation.kind() == IndexOperationTrace.SEARCH) {
                    if (!catalog.getOrDefault(operation.table(), java.util.Set.of())
                            .contains(operation.index()))
                        throw new IOException("Search outside source catalog");
                    // Group members are consecutive: positions 0..size-1, one source access.
                    if (operation.grouped()) {
                        if (operation.expansionPosition() != group
                                || group > 0 && operation.sourceEvent() != groupEvent)
                            throw new IOException("Search group out of order");
                        groupEvent = operation.sourceEvent();
                        group = operation.expansionPosition() + 1 == operation.expansionSize()
                                ? 0
                                : group + 1;
                    }
                    return;
                }
                if (group > 0) throw new IOException("Interrupted search group");
                if (IndexOperationTrace.isControl(operation)) return;
                if (operation.expansionSize() < 1)
                    throw new IOException(
                            "Current index write missing complete maintenance unit");
                if (operation.expansionPosition() == 0) {
                    if (primary != null)
                        throw new IOException("Truncated index maintenance unit");
                    if (operation.index() != 0)
                        throw new IOException("Maintenance unit missing primary");
                    seen.clear();
                    primary = operation;
                    previous = null;
                    position = 0;
                    if (name.equals("baseline")) {
                        if (operation.workUnitId()
                                != 1 + rows++ / IndexOperationTrace.BLOCK_RECORDS)
                            throw new IOException(
                                    "Invalid baseline source-row work-unit assignment");
                        loadedTables.add(operation.table());
                    }
                }
                if (primary == null
                        || operation.table() != primary.table()
                        || !operation.key().equals(primary.key())
                        || operation.expansionSize() != primary.expansionSize()
                        || operation.expansionPosition() != position++
                        || operation.rowId() != primary.rowId()
                        || operation.version() != primary.version()
                        || operation.transactionId() != primary.transactionId()
                        || operation.mutationOrdinal() != primary.mutationOrdinal()
                        || operation.workUnitId() != primary.workUnitId()
                        || operation.sourceEvent() != primary.sourceEvent()
                        || operation.flags() != primary.flags()
                        || (operation.index() == 0 && operation.indexedValue() != null)
                        || (name.equals("baseline")
                                && operation.kind() != IndexOperationTrace.INSERT)
                        || (operation.kind() != primary.kind()
                                && operation.kind() != IndexOperationTrace.LOGICAL_DELETE))
                    throw new IOException("Invalid maintenance unit identity");
                if (operation.index() > 0
                        && operation.kind() != IndexOperationTrace.LOGICAL_DELETE
                        && (operation.file() != primary.file()
                                || operation.position() != primary.position()))
                    throw new IOException("Secondary value location differs from primary");
                if (operation.kind() == IndexOperationTrace.LOGICAL_DELETE
                        && (operation.file() != 0 || operation.position() != 0))
                    throw new IOException("Logical deletion has a physical location");
                if (!catalog.getOrDefault(operation.table(), java.util.Set.of())
                        .contains(operation.index()))
                    throw new IOException("Write outside source catalog");
                if (previous != null) {
                    var prior = previous;
                    if (operation.index() < prior.index()
                            || operation.index() > prior.index() + 1
                            || (operation.index() != prior.index()
                                    && prior.kind() != primary.kind())
                            || (operation.index() == prior.index()
                                    && (prior.kind() != IndexOperationTrace.LOGICAL_DELETE
                                            || operation.kind()
                                                    != IndexOperationTrace.UPSERT
                                            || java.util.Objects.equals(
                                                    prior.indexedValue(),
                                                    operation.indexedValue()))))
                        throw new IOException("Invalid source index maintenance order");
                }
                previous = operation;
                seen.add(operation.index());
                if (operation.expansionPosition() == operation.expansionSize() - 1) {
                    if (!seen.equals(catalog.get(operation.table()))
                            || operation.kind() != primary.kind())
                        throw new IOException(
                                "Partial source index maintenance is not supported");
                    primary = null;
                }
        }

        void finish(com.google.gson.JsonObject manifest) throws IOException {
            if (primary != null) throw new IOException("Unfinished index maintenance unit");
            if (group > 0) throw new IOException("Unfinished search group");
            if (name.equals("baseline")
                    && manifest.has("baseline_rows")
                    && (manifest.get("baseline_rows").getAsLong() != rows
                            || !loadedTables.equals(catalog.keySet())))
                throw new IOException("Baseline row/table coverage mismatch");
        }
    }

    private static Map<Integer, java.util.Set<Integer>> catalog(
            com.google.gson.JsonObject manifest, org.sibench.capture.WorkUnits.Catalog units)
            throws IOException {
        IndexCatalog.fromManifest(manifest);
        if (manifest.has("source_work_units")
                && manifest.get("source_work_units").getAsLong() != units.count())
            throw new IOException("Source work-unit count mismatch");
        Map<Integer, java.util.Set<Integer>> catalog = new LinkedHashMap<>();
        for (var item : manifest.getAsJsonArray("index_definitions")) {
            var definition = item.getAsJsonObject();
            int table = definition.get("table_id").getAsInt(),
                    index = definition.get("index_id").getAsInt();
            if (!catalog.computeIfAbsent(table, ignored -> new java.util.TreeSet<>()).add(index))
                throw new IOException("Duplicate index catalog identity");
        }
        return catalog;
    }

    /** The stream with every operation also shown to {@code also}, first. */
    private static IndexOperationTrace.Source tee(
            IndexOperationTrace.Source source, IndexOperationTrace.Consumer also) {
        return consumer ->
                source.read(
                        operation -> {
                            also.accept(operation);
                            consumer.accept(operation);
                        });
    }

    public static Map<String, Object> checkFiles(Path baseline, Path workload, ReplayConfig config)
            throws IOException {
        return checkFiles(IndexOperationTrace.Source.file(baseline), IndexOperationTrace.Source.file(workload), config);
    }

    public static Map<String, Object> checkFiles(
            IndexOperationTrace.Source baseline, IndexOperationTrace.Source workload, ReplayConfig config)
            throws IOException {
        config.requireReferenceCapabilities();
        try (var load = new ReplayProjection(config, false, operation -> {});
                var runtime = new ReplayProjection(config, true, operation -> {})) {
            RunTiming.current().phase("load");
            long loadRecords = baseline.read(load);
            load.finish();
            RunTiming.current().phase("runtime");
            long runtimeRecords = workload.read(runtime);
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
        return run(directory, config, List.of(false, true), report);
    }

    /** {@code rowIdModes}: the value modes to replay, as row-id flags, in report order. */
    public static Map<String, Object> run(
            Path directory, ReplayConfig config, List<Boolean> rowIdModes, Path report)
            throws IOException {
        if (Files.exists(report)) throw new IOException("Replay report already exists: " + report);
        var preflight = check(directory, config);
        return runChecked(
                binding(directory, config),
                org.sibench.capture.BaselineOperations.of(directory),
                IndexOperationTrace.Source.file(directory.resolve("workload.ops")),
                config,
                rowIdModes,
                preflight,
                report);
    }

    public static Map<String, Object> runFiles(Path baseline, Path workload, ReplayConfig config, Path report)
            throws IOException {
        return runFiles(IndexOperationTrace.Source.file(baseline), IndexOperationTrace.Source.file(workload), config, report);
    }

    public static Map<String, Object> runFiles(
            IndexOperationTrace.Source baseline, IndexOperationTrace.Source workload, ReplayConfig config, Path report)
            throws IOException {
        if (Files.exists(report)) throw new IOException("Replay report already exists: " + report);
        return runChecked(
                null,
                baseline,
                workload,
                config,
                List.of(false, true),
                checkFiles(baseline, workload, config),
                report);
    }

    /**
     * A one-time expected-result stream for a service replay. It is generated from the same
     * canonical trace and value mode, and can be reused by any number of service runs.
     */
    public static Path writeQueryDigests(
            Path directory, ReplayConfig config, boolean rowIds, Path output, String binding)
            throws IOException {
        if (Files.exists(output))
            throw new IOException("Query digest file already exists: " + output);
        try (var digest = new QueryDigest.File(output, rowIds, binding);
                var index =
                        new IndexReferenceReplay(
                                rowIds,
                                config.secondaryIndexes(),
                                IndexReferenceReplay.Settings.defaults(),
                                digest);
                var load = new ReplayProjection(config, false, index::accept);
                var runtime = new ReplayProjection(config, true, index::accept)) {
            org.sibench.capture.BaselineOperations.of(directory).read(load);
            load.finish();
            IndexOperationTrace.read(directory.resolve("workload.ops"), runtime);
            runtime.finish();
        }
        return output;
    }

    /** Binds a digest stream to the trace and replay settings that produced it. */
    public static String binding(Path directory, ReplayConfig config) throws IOException {
        var manifest =
                JsonParser.parseString(
                                Files.readString(directory.resolve("manifest.json")))
                        .getAsJsonObject();
        return "workload="
                + manifest.get("workload_trace_sha256").getAsString()
                + " baseline="
                + manifest.get("baseline_trace_sha256").getAsString()
                + " version="
                + manifest.get("operation_trace_version").getAsInt()
                + " secondary="
                + config.secondaryIndexes();
    }

    /** The per-query digest file recorded in a reference report, requiring the current digest metadata. */
    public static Path queryDigestFile(Path report, String mode) throws IOException {
        var document = JsonParser.parseString(Files.readString(report)).getAsJsonObject();
        if (!document.has("results")) throw new IOException("Reference report has no query digests");
        for (var item : document.getAsJsonArray("results")) {
            var result = item.getAsJsonObject();
            if (!result.get("mode").getAsString().equals(mode)) continue;
            if (!result.has("query_digest_file") || !result.has("query_digest_sha256"))
                throw new IOException("Reference report has no query digests for " + mode);
            Path file = Path.of(result.get("query_digest_file").getAsString());
            if (!Files.isRegularFile(file)
                    || !BaselineGenerator.sha256(file)
                            .equals(result.get("query_digest_sha256").getAsString()))
                throw new IOException("Reference query digest file changed or is missing: " + file);
            return file;
        }
        throw new IOException("Reference report has no " + mode + " result");
    }

    private static Map<String, Object> runChecked(
            String binding,
            IndexOperationTrace.Source baseline,
            IndexOperationTrace.Source workload,
            ReplayConfig config,
            List<Boolean> rowIdModes,
            Map<String, Object> preflight,
            Path report)
            throws IOException {
        if (rowIdModes.isEmpty()) throw new IOException("No value mode to replay");
        RunTiming.current().measure(report.toAbsolutePath().getParent());
        List<Map<String, Object>> results = new ArrayList<>();
        for (boolean rowId : rowIdModes) {
            String mode = rowId ? "rowid" : "locator";
            Path digests =
                    report.toAbsolutePath()
                            .resolveSibling(report.getFileName() + "." + mode + ".queries");
            long[] digestCount = {0};
            try (var digest =
                            new QueryDigest.File(
                                    digests,
                                    rowId,
                                    binding == null ? null : binding + " mode=" + mode);
                    var index =
                            new IndexReferenceReplay(
                                    rowId,
                                    config.secondaryIndexes(),
                                    IndexReferenceReplay.Settings.defaults(),
                                    digest);
                    var load = new ReplayProjection(config, false, index::accept);
                    var runtime = new ReplayProjection(config, true, index::accept)) {
                RunTiming.current().phase("load");
                long start = System.nanoTime();
                baseline.read(load);
                load.finish();
                double loadSeconds = (System.nanoTime() - start) / 1e9;
                RunTiming.current().phase("runtime");
                start = System.nanoTime();
                workload.read(runtime);
                runtime.finish();
                digestCount[0] = digest.count();
                digest.close();
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
                result.put("relocation_writes", index.relocationWrites());
                result.put("primary_state_checksum", index.stateChecksum());
                result.put("selected_version_checksum", index.selectedVersionChecksum());
                result.put("snapshot_queries", index.snapshotQueries());
                result.put("retained_index_versions", index.retainedVersions());
                result.put("skipped_entry_histories", index.skippedHistories());
                result.put("read_model", "statement_snapshot_plus_transaction_local_prefix");
                result.put("query_digests", digestCount[0]);
                result.put("query_digest_file", digests.toString());
                result.put("query_digest_sha256", BaselineGenerator.sha256(digests));
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
                    + " implementation; committed-source version history and timestamp lookup,"
                    + " ordered point/range/limited scans, pinned entries and intersection/union"
                    + " search groups over order-preserving keys of the complete declared source"
                    + " indexes; no RPC, persistence, concurrent PG"
                    + " MVCC, physical GC, rate scheduling or sustained-duration proof");
        Files.writeString(
                report,
                new GsonBuilder().setPrettyPrinting().create().toJson(output),
                StandardOpenOption.CREATE_NEW);
        return output;
    }
}
