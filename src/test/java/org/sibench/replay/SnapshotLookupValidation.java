package org.sibench.replay;

import com.google.gson.*;

import org.sibench.capture.*;
import org.sibench.capture.IndexOperationTrace.Range;
import org.sibench.capture.IndexReferenceReplay.LookupResult;
import org.sibench.trace.IndexKey;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import org.sibench.generation.DuckDbDatabase;
import org.sibench.trace.TraceManifest;
import java.nio.file.*;
import java.util.*;

/** Independent native row oracle; derived probes are not native workload or timed performance. */
public final class SnapshotLookupValidation {
    private static final int SECONDARY_PREDICATES = 64;
    /** Baseline rows sampled per table, evenly in PK order, to describe each index's domain. */
    private static final int VALUE_SAMPLE = 1024;
    /** Range boundaries per index: the domain's minimum, evenly spaced sample values, maximum. */
    private static final int RANGE_POINTS = 9;
    /** Changed values per index that also anchor a range, so baseline and overlay entries mix. */
    private static final int RANGE_ANCHORS = 4;
    private static final int RANGE_LIMIT = 32;

    /**
     * A row by table and primary key, hashed with {@link org.sibench.trace.KeyHash}: the key's
     * own hash collides heavily on integer keys and turned this oracle's maps into trees.
     */
    private static final class Key {
        private final int table;
        private final IndexKey key;
        private final int hash;

        Key(int table, IndexKey key) {
            this.table = table;
            this.key = key;
            this.hash = 31 * table + mixed(key);
        }

        int table() {
            return table;
        }

        IndexKey key() {
            return key;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof Key that && table == that.table && key.equals(that.key);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public String toString() {
            return "Key[table=" + table + ", key=" + key + "]";
        }
    }

    static int mixed(IndexKey key) {
        return org.sibench.trace.KeyHash.of(key.bytes());
    }

    private record Predicate(int table, int index, IndexKey value) {}

    record Row(long version, long rowId, long file, long position, Map<Integer, IndexKey> indexes) {
        LookupResult result(IndexKey key, boolean rowIds) {
            return rowIds
                    ? new LookupResult(key, version, rowId)
                    : new LookupResult(key, version, file, position);
        }
    }

    static final class History {
        Row baseline;
        final NavigableMap<Long, Row> commits = new TreeMap<>();
        final Map<Integer, Set<IndexKey>> indexedValues = new LinkedHashMap<>();

        void remember(Row row) {
            if (row != null)
                row.indexes()
                        .forEach(
                                (index, value) ->
                                        indexedValues
                                                .computeIfAbsent(index, ignored -> new HashSet<>())
                                                .add(value));
        }

        Row at(long timestamp) {
            var entry = commits.floorEntry(timestamp);
            return entry == null ? baseline : entry.getValue();
        }
    }

    /** Baseline PKs of one predicate, appended in baseline (PK) order. */
    private static final class Keys {
        final List<IndexKey> keys = new ArrayList<>();
        int size;

        void add(IndexKey key) throws IOException {
            if (size > 0 && keys.get(size - 1).compareTo(key) >= 0)
                throw new IOException("Baseline predicate keys must be strictly ordered");
            keys.add(key);
            size++;
        }

        boolean contains(IndexKey key) {
            return Collections.binarySearch(keys, key) >= 0;
        }
    }

    private static final class Locations {
        private final Map<Key, long[]> rows = new HashMap<>();

        void add(int table, IndexKey key, long rowId, long file, long position) {
            rows.put(new Key(table, key), new long[] {rowId, file, position});
        }

        LookupResult get(int table, IndexKey key, boolean rowIds) {
            long[] row = rows.get(new Key(table, key));
            return rowIds
                    ? new LookupResult(key, 0, row[0])
                    : new LookupResult(key, 0, row[1], row[2]);
        }
    }

    /** One baseline index entry; ranges order entries by indexed value, then PK. */
    record Entry(IndexKey value, IndexKey key, long rowId, long file, long position) {
        LookupResult result(boolean rowIds) {
            return rowIds
                    ? new LookupResult(key, 0, rowId)
                    : new LookupResult(key, 0, file, position);
        }
    }

    private static final Comparator<Entry> ENTRY_ORDER =
            Comparator.comparing(Entry::value).thenComparing(Entry::key);

    /** A changed row under one value it holds in an index at some version (its PK for index 0). */
    record Changed(IndexKey value, IndexKey key, History history) {
        Entry entry() {
            return new Entry(value, key, 0, 0, 0);
        }
    }

    private static final Comparator<Changed> CHANGED_ORDER =
            Comparator.comparing(Changed::value).thenComparing(Changed::key);

    /** One table's changed rows under every value they hold in the index at some version. */
    static List<Changed> changed(Map<IndexKey, History> rows, int index) {
        var changed = new ArrayList<Changed>();
        rows.forEach(
                (key, history) -> {
                    if (index == 0) {
                        if (history.baseline != null
                                || history.commits.values().stream().anyMatch(Objects::nonNull))
                            changed.add(new Changed(key, key, history));
                    } else
                        for (IndexKey value : history.indexedValues.getOrDefault(index, Set.of()))
                            changed.add(new Changed(value, key, history));
                });
        changed.sort(CHANGED_ORDER);
        return changed;
    }

    /** The values of one index over the whole baseline: its extremes and an even sample. */
    private static final class Domain {
        final List<IndexKey> sample = new ArrayList<>();
        IndexKey min, max;

        void observe(IndexKey value, boolean sampled) {
            if (min == null || value.compareTo(min) < 0) min = value;
            if (max == null || value.compareTo(max) > 0) max = value;
            if (sampled) sample.add(value);
        }

        List<IndexKey> sorted() {
            var values = new TreeSet<>(sample);
            if (min != null) {
                values.add(min);
                values.add(max);
            }
            return new ArrayList<>(values);
        }
    }

    /**
     * One derived range search. Unchanged baseline entries never change, so only the first
     * {@code limit} of them in the search's direction can ever be returned; changed rows are
     * evaluated from their history at each probed time.
     */
    static final class RangeProbe {
        final int table, index;
        final Range range;
        final TreeSet<Entry> unchanged;
        /** Changed rows under each value in the range they ever hold, in the search's order. */
        final List<Changed> changed = new ArrayList<>();
        final SortedSet<Long> times = new TreeSet<>();

        RangeProbe(int table, int index, Range range) {
            this.table = table;
            this.index = index;
            this.range = range;
            this.unchanged = new TreeSet<>(range.descending() ? ENTRY_ORDER.reversed() : ENTRY_ORDER);
        }

        void offer(Entry entry) {
            if (!range.contains(entry.value())) return;
            if (unchanged.size() >= range.limit()
                    && unchanged.comparator().compare(entry, unchanged.last()) >= 0) return;
            unchanged.add(entry);
            if (unchanged.size() > range.limit()) unchanged.pollLast();
        }

        /** Keeps the changed entries in the range, in the search's order. */
        void select(List<Changed> sorted) {
            for (Changed change : sorted) if (range.contains(change.value())) changed.add(change);
            if (range.descending()) Collections.reverse(changed);
        }

        IndexKey value(IndexKey key, Row row) {
            return index == 0 ? key : row.indexes().get(index);
        }

        /**
         * The first {@code limit} entries at the time: unchanged and changed rows are disjoint and
         * both in search order, so the first {@code limit} changed rows holding their listed value
         * at the time, merged with the unchanged ones, are the answer.
         */
        List<LookupResult> expected(long time, boolean rowIds) {
            var order = unchanged.comparator();
            var current = new ArrayList<Changed>();
            for (Changed change : changed) {
                if (current.size() == range.limit()) break;
                Row row = change.history().at(time);
                if (row != null && change.value().equals(value(change.key(), row)))
                    current.add(change);
            }
            var results = new ArrayList<LookupResult>();
            Iterator<Entry> base = unchanged.iterator();
            Entry nextBase = base.hasNext() ? base.next() : null;
            int nextChanged = 0;
            while (results.size() < range.limit() && (nextBase != null || nextChanged < current.size())) {
                Changed change = nextChanged < current.size() ? current.get(nextChanged) : null;
                if (change == null
                        || nextBase != null && order.compare(nextBase, change.entry()) < 0) {
                    results.add(nextBase.result(rowIds));
                    nextBase = base.hasNext() ? base.next() : null;
                } else {
                    results.add(change.history().at(time).result(change.key(), rowIds));
                    nextChanged++;
                }
            }
            return results;
        }

        @Override
        public String toString() {
            return "table "
                    + table
                    + " index "
                    + index
                    + (range.lowerInclusive() ? " [" : " (")
                    + range.lower()
                    + ", "
                    + range.upper()
                    + (range.upperInclusive() ? "]" : ")")
                    + (range.descending() ? " desc" : " asc")
                    + " limit "
                    + range.limit();
        }
    }

    /** {@code count} values evenly spaced over a sorted list, including both ends. */
    static List<IndexKey> spread(List<IndexKey> sorted, int count) {
        if (count <= 0 || sorted.isEmpty()) return List.of();
        if (sorted.size() <= count) return sorted;
        var result = new ArrayList<IndexKey>();
        for (int i = 0; i < count; i++)
            result.add(
                    sorted.get(count == 1 ? 0 : (int) ((long) i * (sorted.size() - 1) / (count - 1))));
        return result;
    }

    /** Both ends of each boundary pair are tested inclusive and exclusive from their own side. */
    static List<Range> ranges(List<IndexKey> points, Collection<IndexKey> anchors) {
        var ranges = new ArrayList<Range>();
        ranges.add(new Range(null, false, null, false, false, RANGE_LIMIT));
        ranges.add(new Range(null, false, null, false, true, RANGE_LIMIT));
        for (int i = 1; i < points.size(); i++) {
            IndexKey lower = points.get(i - 1), upper = points.get(i);
            ranges.add(new Range(lower, true, upper, true, false, RANGE_LIMIT));
            ranges.add(new Range(lower, false, upper, true, false, RANGE_LIMIT));
            ranges.add(new Range(lower, true, upper, true, true, RANGE_LIMIT));
            ranges.add(new Range(lower, true, upper, false, true, RANGE_LIMIT));
        }
        for (IndexKey anchor : anchors) {
            ranges.add(new Range(anchor, true, null, false, false, RANGE_LIMIT));
            ranges.add(new Range(null, false, anchor, true, true, RANGE_LIMIT));
        }
        return ranges;
    }

    @FunctionalInterface
    private interface RowConsumer {
        void accept(int table, IndexKey key, Row row) throws IOException;
    }

    private SnapshotLookupValidation() {}

    public static Map<String, Object> run(Path directory, ReplayConfig config, Path report)
            throws IOException {
        return run(directory, config, List.of(true), report);
    }

    /** {@code rowIdModes}: the value modes whose reference model is checked, as row-id flags. */
    public static Map<String, Object> run(
            Path directory, ReplayConfig config, List<Boolean> rowIdModes, Path report)
            throws IOException {
        if (rowIdModes.isEmpty()) throw new IOException("No value mode to check");
        if (Files.exists(report))
            throw new IOException("Snapshot report already exists: " + report);
        for (boolean rowIds : rowIdModes) {
            TraceManifest.requireValue(directory, rowIds ? org.sibench.adapters.proto.ValueKind.ROW_ID
                    : org.sibench.adapters.proto.ValueKind.LOCATOR);
            if (!rowIds) throw new IOException("Native row oracle requires RowID values");
        }
        var input = ReplayInput.prepare(directory, config);
        var baseline = input.baseline();
        var preflight = input.preflight();
        JsonObject manifest =
                JsonParser.parseString(Files.readString(directory.resolve("manifest.json")))
                        .getAsJsonObject();
        IndexCatalog catalog = IndexCatalog.fromManifest(manifest);
        var workload = org.sibench.capture.ProtoTrace.source(directory, manifest, "workload");
        Map<Integer, NavigableMap<IndexKey, History>> nativeRows;
        try (var verified = TraceManifest.openData(directory);
                var database = DuckDbDatabase.openReadOnly(directory.resolve("data"))) {
            nativeRows = nativeHistories(database, catalog);
        } catch (SQLException failure) { throw new IOException("Cannot read native oracle rows", failure); }
        Map<Key, History> histories = new LinkedHashMap<>();
        nativeRows.forEach((table, rows) -> rows.forEach((key, history) -> {
            if (!history.commits.isEmpty()) histories.put(new Key(table, key), history);
        }));
        long[] lastCommit = {manifest.get("last_resolved_version").getAsLong()};
        for (History history : histories.values())
            if (history.commits.lastKey() > lastCommit[0]) throw new IOException("History exceeds resolution watermark");
        long rowCount = nativeRows.values().stream().flatMap(rows -> rows.values().stream())
                .filter(history -> history.baseline != null).count();
        if (rowCount != manifest.get("baseline_rows").getAsLong()) throw new IOException("Native baseline row count mismatch");
        Map<String, SortedSet<IndexKey>> changedValues = new TreeMap<>();
        Map<String, Domain> domains = new TreeMap<>();
        for (var definition : catalog.definitions()) {
            String id = definition.tableId() + "/" + definition.indexId();
            changedValues.put(id, new TreeSet<>());
            domains.put(id, new Domain());
        }
        for (Key key : histories.keySet()) changedValues.get(key.table() + "/0").add(key.key());
        for (var entry : histories.entrySet())
            entry.getValue()
                    .indexedValues
                    .forEach(
                            (index, valuesForIndex) ->
                                    changedValues
                                            .get(entry.getKey().table() + "/" + index)
                                            .addAll(valuesForIndex));
        Map<Integer, Long> tableRows = new HashMap<>();
        nativeRows.forEach((table, rows) -> tableRows.put(table,
                rows.values().stream().filter(history -> history.baseline != null).count()));
        Map<Integer, long[]> seen = new HashMap<>();
        readBaseline(nativeRows, (table, key, row) -> {
                        long position = seen.computeIfAbsent(table, ignored -> new long[1])[0]++;
                        boolean sampled =
                                position % Math.max(1, tableRows.get(table) / VALUE_SAMPLE) == 0;
                        domains.get(table + "/0").observe(key, sampled);
                        row.indexes()
                                .forEach(
                                        (index, value) ->
                                                domains.get(table + "/" + index)
                                                        .observe(value, sampled));
                        History history = histories.get(new Key(table, key));
                        if (history != null) {
                            history.baseline = row;
                            history.remember(row);
                            row.indexes()
                                    .forEach(
                                            (index, value) ->
                                                    changedValues
                                                            .get(table + "/" + index)
                                                            .add(value));
                        }
                    });
        Map<Predicate, Keys> buckets = new LinkedHashMap<>();
        if (config.secondaryIndexes())
            for (var definition : catalog.definitions()) {
                if (definition.indexId() == 0) continue;
                String id = definition.tableId() + "/" + definition.indexId();
                // Half changed values, the rest spread over the baseline domain, ends included.
                var selected =
                        new LinkedHashSet<IndexKey>(
                                spread(
                                        new ArrayList<>(changedValues.get(id)),
                                        SECONDARY_PREDICATES / 2));
                selected.addAll(
                        spread(domains.get(id).sorted(), SECONDARY_PREDICATES - selected.size()));
                for (IndexKey value : selected)
                    buckets.put(
                            new Predicate(definition.tableId(), definition.indexId(), value),
                            new Keys());
            }
        List<RangeProbe> probes = new ArrayList<>();
        Map<String, List<RangeProbe>> indexProbes = new HashMap<>();
        for (var definition : catalog.definitions()) {
            if (definition.indexId() > 0 && !config.secondaryIndexes()) continue;
            String id = definition.tableId() + "/" + definition.indexId();
            List<IndexKey> points = spread(domains.get(id).sorted(), RANGE_POINTS);
            if (points.isEmpty() && changedValues.get(id).isEmpty()) continue;
            for (Range range :
                    ranges(points, spread(new ArrayList<>(changedValues.get(id)), RANGE_ANCHORS))) {
                var probe = new RangeProbe(definition.tableId(), definition.indexId(), range);
                probes.add(probe);
                indexProbes.computeIfAbsent(id, ignored -> new ArrayList<>()).add(probe);
            }
        }
        Locations locations = new Locations();
        readBaseline(nativeRows, (table, key, row) -> {
                        if (!histories.containsKey(new Key(table, key))) {
                            for (RangeProbe probe : indexProbes.getOrDefault(table + "/0", List.of()))
                                probe.offer(
                                        new Entry(key, key, row.rowId(), row.file(), row.position()));
                            for (var indexed : row.indexes().entrySet())
                                for (RangeProbe probe :
                                        indexProbes.getOrDefault(
                                                table + "/" + indexed.getKey(), List.of()))
                                    probe.offer(
                                            new Entry(
                                                    indexed.getValue(),
                                                    key,
                                                    row.rowId(),
                                                    row.file(),
                                                    row.position()));
                        }
                        boolean retained = false;
                        for (var indexed : row.indexes().entrySet()) {
                            Keys keys =
                                    buckets.get(
                                            new Predicate(
                                                    table, indexed.getKey(), indexed.getValue()));
                            if (keys != null) {
                                keys.add(key);
                                retained = true;
                            }
                        }
                        if (retained) locations.add(table, key, row.rowId(), row.file(), row.position());
                    });
        // Every changed row under every value it holds, sorted once per index: probes and
        // predicates select from it instead of scanning every history.
        Map<Integer, Map<IndexKey, History>> changedRows = new HashMap<>();
        histories.forEach(
                (key, history) ->
                        changedRows
                                .computeIfAbsent(key.table(), ignored -> new LinkedHashMap<>())
                                .put(key.key(), history));
        Map<String, List<Changed>> changedByIndex = new HashMap<>();
        for (var definition : catalog.definitions())
            changedByIndex.put(
                    definition.tableId() + "/" + definition.indexId(),
                    changed(
                            changedRows.getOrDefault(definition.tableId(), Map.of()),
                            definition.indexId()));
        Map<Predicate, List<Changed>> relevantByPredicate = new HashMap<>();
        for (Predicate predicate : buckets.keySet())
            relevantByPredicate.put(
                    predicate,
                    changedByIndex.get(predicate.table() + "/" + predicate.index()).stream()
                            .filter(change -> change.value().equals(predicate.value()))
                            .toList());
        for (RangeProbe probe : probes) {
            probe.times.addAll(List.of(0L, lastCommit[0]));
            probe.select(changedByIndex.get(probe.table + "/" + probe.index));
            long first = Long.MAX_VALUE;
            for (Changed change : probe.changed)
                first = Math.min(first, change.history().commits.firstKey());
            if (first != Long.MAX_VALUE) {
                probe.times.add(first - 1);
                probe.times.add(first);
            }
        }
        List<Map<String, Object>> modes = new ArrayList<>();
        for (boolean rowIds : rowIdModes) {
            String mode = rowIds ? "rowid" : "locator";
            try (var model = new IndexReferenceReplay(rowIds, config.secondaryIndexes());
                    var load = new ReplayProjection(config, false, model::accept);
                    var runtime = new ReplayProjection(config, true, model::accept)) {
                baseline.read(load);
                load.finish();
                workload.read(runtime);
                runtime.finish();
                long primaryProbes = 0, secondaryProbes = 0, items = 0;
                for (var entry : histories.entrySet()) {
                    Key key = entry.getKey();
                    var indexedValues = entry.getValue().indexedValues;
                    for (long time : times(entry.getValue(), lastCommit[0])) {
                        Row row = entry.getValue().at(time);
                        LookupResult expected = row == null ? null : row.result(key.key(), rowIds);
                        if (!Objects.equals(
                                expected, model.lookupPrimary(key.table(), key.key(), time)))
                            throw new IOException(
                                    "Historical primary mismatch: " + key + " at " + time);
                        primaryProbes++;
                        if (expected != null) items++;
                        if (config.secondaryIndexes())
                            for (var index : indexedValues.entrySet())
                                for (IndexKey value : index.getValue()) {
                                    LookupResult expectedEntry =
                                            row != null
                                                            && Objects.equals(
                                                                    row.indexes()
                                                                            .get(index.getKey()),
                                                                    value)
                                                    ? row.result(key.key(), rowIds)
                                                    : null;
                                    LookupResult actualEntry =
                                            model.lookupEntry(
                                                    key.table(),
                                                    index.getKey(),
                                                    key.key(),
                                                    value,
                                                    time);
                                    if (!Objects.equals(expectedEntry, actualEntry))
                                        throw new IOException(
                                                "Historical secondary entry mismatch: "
                                                        + key
                                                        + " at "
                                                        + time);
                                    secondaryProbes++;
                                    if (expectedEntry != null) items++;
                                }
                    }
                }
                for (var entry : buckets.entrySet()) {
                    Predicate predicate = entry.getKey();
                    Keys keys = entry.getValue();
                    var relevant = relevantByPredicate.getOrDefault(predicate, List.of());
                    var times = new TreeSet<Long>(List.of(0L, lastCommit[0]));
                    long first = Long.MAX_VALUE;
                    for (Changed change : relevant)
                        first = Math.min(first, change.history().commits.firstKey());
                    if (first != Long.MAX_VALUE) {
                        times.add(first - 1);
                        times.add(first);
                    }
                    for (long time : times) {
                        long expectedCount = keys.size;
                        for (Changed change : relevant) {
                            if (keys.contains(change.key())) expectedCount--;
                            if (matches(change.history().at(time), predicate)) expectedCount++;
                        }
                        var actual =
                                model.lookupSecondary(
                                        predicate.table(),
                                        predicate.index(),
                                        predicate.value(),
                                        time);
                        if (actual.size() != expectedCount)
                            throw new IOException(
                                    "Historical secondary count mismatch: "
                                            + predicate
                                            + " at "
                                            + time);
                        IndexKey previous = null;
                        for (LookupResult result : actual) {
                            if (previous != null && result.key().compareTo(previous) <= 0)
                                throw new IOException("Duplicate/unordered secondary results");
                            previous = result.key();
                            History history =
                                    histories.get(new Key(predicate.table(), result.key()));
                            LookupResult expected;
                            if (history != null) {
                                Row row = history.at(time);
                                if (!matches(row, predicate))
                                    throw new IOException("Unexpected historical secondary key");
                                expected = row.result(result.key(), rowIds);
                            } else {
                                if (!keys.contains(result.key()))
                                    throw new IOException("Unexpected baseline secondary key");
                                expected = locations.get(predicate.table(), result.key(), rowIds);
                            }
                            if (!expected.equals(result))
                                throw new IOException("Secondary source value/version mismatch");
                        }
                        secondaryProbes++;
                        items += actual.size();
                    }
                }
                long rangeProbes = 0;
                for (RangeProbe probe : probes)
                    for (long time : probe.times) {
                        var actual = model.search(probe.table, probe.index, probe.range, time);
                        if (!probe.expected(time, rowIds).equals(actual))
                            throw new IOException(
                                    "Derived range mismatch: " + probe + " at " + time);
                        rangeProbes++;
                        items += actual.size();
                    }
                var result = new LinkedHashMap<String, Object>();
                result.put("mode", mode);
                result.put("native_queries", model.queries());
                result.put("derived_primary_probes", primaryProbes);
                result.put("derived_secondary_probes", secondaryProbes);
                result.put("derived_range_probes", rangeProbes);
                result.put("derived_checked_result_items", items);
                result.put("retained_index_versions", model.retainedVersions());
                result.put("workload_dispatch", runtime.describe());
                modes.add(result);
            }
        }
        Map<String, Object> output = new LinkedHashMap<>();
        output.put("status", "passed");
        output.put(
                "provenance",
                "derived_history_validation_from_native_before_images_not_trace_mutations");
        output.put("preflight", preflight);
        // A derived load was checked against this digest when it was read.
        output.put("baseline_trace_sha256", manifest.get("baseline_trace_sha256").getAsString());
        output.put("workload_trace_sha256", manifest.get("workload_trace_sha256").getAsString());
        output.put("changed_primary_keys", histories.size());
        output.put("secondary_predicate_sample_limit_per_index", SECONDARY_PREDICATES);
        output.put("sampled_secondary_predicates", buckets.size());
        output.put("value_sample_rows_per_table", VALUE_SAMPLE);
        output.put("range_points_per_index", RANGE_POINTS);
        output.put("range_anchors_per_index", RANGE_ANCHORS);
        output.put("range_probe_limit", RANGE_LIMIT);
        output.put("range_probes", probes.size());
        output.put("last_resolved_version", lastCommit[0]);
        output.put("results", modes);
        output.put(
                "scope",
                "All changed primary keys and full secondary identities at baseline, before/at"
                    + " every per-key commit and final; nonunique equality predicates for every"
                    + " source index, half changed values and half spread over the baseline"
                    + " domain; limited range probes on every primary and source index between"
                    + " domain quantiles (both ends inclusive and exclusive, ascending and"
                    + " descending), unbounded, and anchored at changed values; predicates and"
                    + " ranges at baseline, before/at first relevant change and final. Exact"
                    + " key/version/RowID and result order against independently reconstructed"
                    + " native row histories, including old/new values; search groups"
                    + " only through their member searches; committed snapshots, not aborted local row contents,"
                    + " relocation/GC or performance.");
        Files.writeString(
                report,
                new GsonBuilder().setPrettyPrinting().create().toJson(output),
                StandardOpenOption.CREATE_NEW);
        return output;
    }

    private static boolean matches(Row row, Predicate predicate) {
        return row != null
                && row.indexes().containsKey(predicate.index())
                && Objects.equals(row.indexes().get(predicate.index()), predicate.value());
    }

    private static SortedSet<Long> times(History history, long last) {
        var times = new TreeSet<Long>(List.of(0L, last));
        for (long version : history.commits.keySet()) {
            times.add(version - 1);
            times.add(version);
        }
        return times;
    }

    private static void readBaseline(Map<Integer, NavigableMap<IndexKey, History>> tables, RowConsumer consumer)
            throws IOException {
        for (var table : tables.entrySet())
            for (var row : table.getValue().entrySet())
                if (row.getValue().baseline != null) consumer.accept(table.getKey(), row.getKey(), row.getValue().baseline);
    }

    /** Small predicate/range oracle; full-data acceptance uses the streaming reader directly. */
    static Map<Integer, NavigableMap<IndexKey, History>> nativeHistories(Connection database, IndexCatalog catalog)
            throws SQLException, IOException {
        var result = new LinkedHashMap<Integer, NavigableMap<IndexKey, History>>();
        for (var index : catalog.definitions()) result.computeIfAbsent(index.tableId(), ignored -> new TreeMap<>());
        NativeRowHistories.read(database, catalog, (table, key, history) -> result.get(table).put(key, history));
        return result;
    }
}
