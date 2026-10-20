package org.sibench.replay;

import com.google.gson.GsonBuilder;

import org.sibench.capture.IndexOperationTrace;
import org.sibench.capture.IndexOperationTrace.Operation;
import org.sibench.capture.IndexReferenceReplay;
import org.sibench.capture.IndexReferenceReplay.LookupResult;
import org.sibench.generation.BaselineGenerator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;

/** Out-of-band derived probes, never counted as native workload or long-replay input supply. */
public final class SnapshotLookupValidation {
    private static final int SECONDARY_PREDICATES = 64;

    private record Key(int table, long key) implements Comparable<Key> {
        @Override
        public int compareTo(Key other) {
            int tableOrder = Integer.compare(table, other.table);
            return tableOrder == 0 ? Long.compare(key, other.key) : tableOrder;
        }
    }

    private record ExpectedRow(Operation baseline, List<Operation> commits) {}

    private SnapshotLookupValidation() {}

    public static Map<String, Object> run(Path directory, ReplayConfig config, Path report)
            throws IOException {
        if (Files.exists(report))
            throw new IOException("Snapshot report already exists: " + report);
        Path baseline = directory.resolve("baseline.ops"),
                workload = directory.resolve("workload.ops");
        if (IndexOperationTrace.formatVersion(workload) < 3)
            throw new IOException(
                    "Snapshot validation requires captured transaction commit versions (v3-v5)");
        var preflight = ReferenceReplay.check(directory, config);
        Map<Key, List<Operation>> changes = new TreeMap<>();
        Map<Key, Operation> pending = new LinkedHashMap<>();
        long[] published = {0};
        IndexOperationTrace.read(
                workload,
                o -> {
                    if (o.kind() == IndexOperationTrace.RELOCATE)
                        throw new IOException(
                                "Derived source-row oracle requires a workload without relocation"
                                    + " events");
                    if (IndexOperationTrace.isMutation(o) && o.index() == 0)
                        pending.put(new Key(o.table(), o.key()), o);
                    else if (o.kind() == IndexOperationTrace.TRANSACTION_COMMIT) {
                        for (var entry : pending.entrySet())
                            changes.computeIfAbsent(entry.getKey(), key -> new ArrayList<>())
                                    .add(entry.getValue());
                        pending.clear();
                        published[0] = o.version();
                    }
                });
        if (changes.isEmpty())
            throw new IOException("No committed row changes available for derived history probes");

        Map<Integer, SortedSet<Long>> predicates = new TreeMap<>();
        for (int index = 1; index <= 2; index++) {
            var candidates = new TreeSet<Long>();
            for (var entry : changes.entrySet())
                if (entry.getKey().table() == 5)
                    for (Operation o : entry.getValue()) candidates.add(indexed(o, index));
            var selected = new TreeSet<Long>();
            if (config.secondaryIndexes())
                candidates.stream().limit(SECONDARY_PREDICATES).forEach(selected::add);
            predicates.put(index, selected);
        }
        Map<Key, ExpectedRow> oracle = new TreeMap<>();
        changes.forEach((key, versions) -> oracle.put(key, new ExpectedRow(null, versions)));
        IndexOperationTrace.readExpanded(
                baseline,
                o -> {
                    if (o.index() != 0) return;
                    Key key = new Key(o.table(), o.key());
                    if (changes.containsKey(key)
                            || (o.table() == 5
                                    && (predicates.get(1).contains(o.secondarySource())
                                            || predicates.get(2).contains(o.secondaryTarget()))))
                        oracle.put(key, new ExpectedRow(o, changes.getOrDefault(key, List.of())));
                });
        List<Map<String, Object>> modes = new ArrayList<>();
        for (boolean rowIds : new boolean[] {false, true}) {
            var model = new IndexReferenceReplay(rowIds, config.secondaryIndexes());
            var load = new ReplayProjection(config.secondaryIndexes(), model::accept);
            var runtime = new ReplayProjection(config, true, model::accept);
            IndexOperationTrace.readExpanded(baseline, load);
            IndexOperationTrace.readExpanded(workload, runtime);
            runtime.finish();
            long primaryProbes = 0, secondaryProbes = 0, items = 0;
            long initialQueries = model.queries();
            for (var entry : changes.entrySet()) {
                Key key = entry.getKey();
                var times = new TreeSet<Long>(List.of(0L, published[0]));
                for (Operation o : entry.getValue()) {
                    times.add(o.version() - 1);
                    times.add(o.version());
                }
                for (long time : times) {
                    LookupResult expected = expected(key, oracle.get(key), time, rowIds);
                    LookupResult actual =
                            model.lookupPrimary(key.table(), Math.toIntExact(key.key()), time);
                    if (!Objects.equals(expected, actual))
                        throw new IOException(
                                "Historical primary result mismatch: " + key + " at " + time);
                    model.accept(probe(key.table(), 0, key.key(), time, expected == null ? 0 : 1));
                    primaryProbes++;
                    if (expected != null) items++;
                }
            }
            for (var indexEntry : predicates.entrySet()) {
                int index = indexEntry.getKey();
                for (long predicate : indexEntry.getValue()) {
                    Map<Key, ExpectedRow> bucket = new TreeMap<>();
                    long firstChange = Long.MAX_VALUE;
                    for (var entry : oracle.entrySet()) {
                        if (entry.getKey().table() != 5) continue;
                        ExpectedRow row = entry.getValue();
                        boolean matches =
                                row.baseline() != null
                                        && indexed(row.baseline(), index) == predicate;
                        for (Operation o : row.commits())
                            if (indexed(o, index) == predicate) {
                                matches = true;
                                firstChange = Math.min(firstChange, o.version());
                            }
                        if (matches) bucket.put(entry.getKey(), row);
                    }
                    var times = new TreeSet<Long>(List.of(0L, published[0]));
                    if (firstChange != Long.MAX_VALUE) {
                        times.add(firstChange - 1);
                        times.add(firstChange);
                    }
                    for (long time : times) {
                        Set<LookupResult> expected = new HashSet<>();
                        for (var entry : bucket.entrySet()) {
                            Operation selected = select(entry.getValue(), time);
                            if (selected != null && indexed(selected, index) == predicate) {
                                LookupResult value =
                                        expected(entry.getKey(), entry.getValue(), time, rowIds);
                                if (value != null) expected.add(value);
                            }
                        }
                        var actual = model.lookupSecondary(index, Math.toIntExact(predicate), time);
                        if (actual.size() != expected.size()
                                || !new HashSet<>(actual).equals(expected))
                            throw new IOException(
                                    "Historical secondary result mismatch: index "
                                            + index
                                            + ", predicate "
                                            + predicate
                                            + " at "
                                            + time);
                        model.accept(probe(5, index, predicate, time, expected.size()));
                        secondaryProbes++;
                        items += expected.size();
                    }
                }
            }
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("mode", rowIds ? "rowid" : "locator");
            result.put("native_queries", initialQueries);
            result.put("derived_primary_probes", primaryProbes);
            result.put("derived_secondary_probes", secondaryProbes);
            result.put("derived_checked_result_items", items);
            result.put("selected_version_checksum", model.selectedVersionChecksum());
            result.put("retained_index_versions", model.retainedVersions());
            result.put("workload_dispatch", runtime.describe());
            modes.add(result);
        }
        Map<String, Object> output = new LinkedHashMap<>();
        output.put("status", "passed");
        output.put(
                "provenance", "explicitly_derived_history_validation_not_native_hybench_queries");
        output.put("preflight", preflight);
        output.put("baseline_trace_sha256", BaselineGenerator.sha256(baseline));
        output.put("workload_trace_sha256", BaselineGenerator.sha256(workload));
        output.put("changed_primary_keys", changes.size());
        output.put("secondary_predicate_sample_limit_per_index", SECONDARY_PREDICATES);
        output.put("last_published_commit_version", published[0]);
        output.put("results", modes);
        output.put(
                "scope",
                "all changed primary keys at baseline, before/at each commit and final publication;"
                    + " deterministic sampled secondary equality predicates at baseline/first"
                    + " change/final; exact key/version/value comparison against independent"
                    + " source-row history; no native SQL snapshot claim, Parquet payload readback,"
                    + " GC, range queries or timed performance");
        Files.writeString(
                report,
                new GsonBuilder().setPrettyPrinting().create().toJson(output),
                StandardOpenOption.CREATE_NEW);
        return output;
    }

    private static Operation probe(int table, int index, long key, long timestamp, long count) {
        return IndexOperationTrace.search(
                0,
                table,
                index,
                key,
                timestamp,
                count,
                IndexOperationTrace.SNAPSHOT_SEARCH | IndexOperationTrace.DERIVED_SEARCH);
    }

    private static long indexed(Operation o, int index) {
        return index == 1 ? o.secondarySource() : o.secondaryTarget();
    }

    private static Operation select(ExpectedRow row, long timestamp) {
        Operation selected = row.baseline();
        for (Operation o : row.commits()) {
            if (o.version() > timestamp) break;
            selected = o;
        }
        return selected;
    }

    private static LookupResult expected(Key key, ExpectedRow row, long timestamp, boolean rowIds)
            throws IOException {
        Operation selected = select(row, timestamp);
        if (selected == null || selected.kind() == IndexOperationTrace.LOGICAL_DELETE) return null;
        return new LookupResult(
                key.key(),
                selected.version(),
                rowIds
                        ? selected.rowId()
                        : IndexReferenceReplay.locator(selected.file(), selected.position()));
    }
}
