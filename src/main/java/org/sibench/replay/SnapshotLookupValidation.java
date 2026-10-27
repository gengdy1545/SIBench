package org.sibench.replay;

import com.google.gson.*;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.sibench.capture.*;
import org.sibench.capture.IndexOperationTrace.Operation;
import org.sibench.capture.IndexOperationTrace.Range;
import org.sibench.capture.IndexReferenceReplay.LookupResult;
import org.sibench.generation.BaselineGenerator;
import org.sibench.trace.IndexKey;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;

/** Independent Parquet row oracle; derived probes are not native workload or timed performance. */
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
        // Rows of one table, each once: only iterated, so no map (integer keys' own hash collides).
        final List<Map.Entry<IndexKey, History>> changed = new ArrayList<>();
        final SortedSet<Long> times = new TreeSet<>();

        RangeProbe(int table, int index, Range range) {
            this.table = table;
            this.index = index;
            this.range = range;
            this.unchanged = new TreeSet<>(range.descending() ? ENTRY_ORDER.reversed() : ENTRY_ORDER);
        }

        void offer(Entry entry) {
            if (!range.contains(entry.value())) return;
            unchanged.add(entry);
            if (unchanged.size() > range.limit()) unchanged.pollLast();
        }

        IndexKey value(IndexKey key, Row row) {
            return index == 0 ? key : row.indexes().get(index);
        }

        /** Whether the row's entry in this index lies in the range; a deleted row has none. */
        boolean contains(IndexKey key, Row row) {
            if (row == null) return false;
            IndexKey value = value(key, row);
            return value != null && range.contains(value);
        }

        boolean ever(IndexKey key, History history) {
            if (contains(key, history.baseline)) return true;
            for (Row row : history.commits.values()) if (contains(key, row)) return true;
            return false;
        }

        List<LookupResult> expected(long time, boolean rowIds) {
            var candidates = new TreeMap<Entry, LookupResult>(unchanged.comparator());
            for (Entry entry : unchanged) candidates.put(entry, entry.result(rowIds));
            for (var change : changed) {
                Row row = change.getValue().at(time);
                if (contains(change.getKey(), row))
                    candidates.put(
                            new Entry(value(change.getKey(), row), change.getKey(), 0, 0, 0),
                            row.result(change.getKey(), rowIds));
            }
            return candidates.values().stream().limit(range.limit()).toList();
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
        if (Files.exists(report))
            throw new IOException("Snapshot report already exists: " + report);
        var baseline = org.sibench.capture.BaselineOperations.of(directory);
        Path workload = directory.resolve("workload.ops");
        var preflight = ReferenceReplay.check(directory, config);
        JsonObject manifest =
                JsonParser.parseString(Files.readString(directory.resolve("manifest.json")))
                        .getAsJsonObject();
        IndexCatalog catalog = IndexCatalog.fromManifest(manifest);
        Path sourceBaseline = Path.of(manifest.get("baseline_directory").getAsString());
        if (!BaselineGenerator.sha256(sourceBaseline.resolve("verified-manifest.json"))
                .equals(manifest.get("baseline_verified_manifest_sha256").getAsString()))
            throw new IOException("Baseline verification provenance mismatch");
        JsonObject verification =
                JsonParser.parseString(
                                Files.readString(sourceBaseline.resolve("verified-manifest.json")))
                        .getAsJsonObject();
        if (!verification.get("status").getAsString().equals("verified_complete")
                || !BaselineGenerator.sha256(sourceBaseline.resolve("files.jsonl"))
                        .equals(verification.get("files_manifest_sha256").getAsString()))
            throw new IOException("Independent baseline manifests changed or are incomplete");
        for (var item : verification.getAsJsonArray("files"))
            verifyFile(sourceBaseline, item.getAsJsonObject());
        Map<Key, History> histories = new LinkedHashMap<>();
        Map<Key, Operation> pending = new LinkedHashMap<>();
        Map<String, Operation> payloadOperations = new HashMap<>();
        long[] lastCommit = {0};
        IndexOperationTrace.read(
                workload,
                operation -> {
                    if (operation.kind() == IndexOperationTrace.RELOCATE)
                        throw new IOException(
                                "Source-row oracle does not infer relocation history");
                    if (IndexOperationTrace.isMutation(operation) && operation.index() == 0) {
                        pending.put(new Key(operation.table(), operation.key()), operation);
                        if (operation.kind() != IndexOperationTrace.LOGICAL_DELETE
                                && payloadOperations.put(
                                                location(operation.file(), operation.position()),
                                                operation)
                                        != null)
                            throw new IOException("Duplicate incremental payload location");
                    } else if (operation.kind() == IndexOperationTrace.TRANSACTION_COMMIT) {
                        for (var entry : pending.entrySet())
                            histories
                                    .computeIfAbsent(entry.getKey(), ignored -> new History())
                                    .commits
                                    .put(operation.version(), null);
                        pending.clear();
                        lastCommit[0] = operation.version();
                    }
                });
        if (histories.isEmpty()) throw new IOException("No committed row changes to validate");
        Map<String, Row> payloadRows = new HashMap<>();
        for (var item : manifest.getAsJsonArray("incremental_files")) {
            JsonObject file = item.getAsJsonObject();
            verifyFile(directory, file);
            long[] position = {0};
            readFile(
                    directory,
                    file,
                    catalog,
                    (table, key, row) -> {
                        String identity = location(file.get("file_id").getAsLong(), position[0]++);
                        Operation operation = payloadOperations.get(identity);
                        if (operation == null
                                || operation.table() != table
                                || !operation.key().equals(key)
                                || operation.version() != row.version()
                                || operation.rowId() != row.rowId())
                            throw new IOException(
                                    "Incremental source row identity/version mismatch");
                        payloadRows.put(identity, row);
                        histories.get(new Key(table, key)).remember(row);
                    },
                    payloadOperations);
        }
        if (payloadRows.size() != payloadOperations.size())
            throw new IOException("Missing incremental Parquet payload");
        pending.clear();
        IndexOperationTrace.read(
                workload,
                operation -> {
                    if (IndexOperationTrace.isMutation(operation) && operation.index() == 0)
                        pending.put(new Key(operation.table(), operation.key()), operation);
                    else if (operation.kind() == IndexOperationTrace.TRANSACTION_COMMIT) {
                        for (var entry : pending.entrySet()) {
                            Operation write = entry.getValue();
                            histories
                                    .get(entry.getKey())
                                    .commits
                                    .put(
                                            operation.version(),
                                            write.kind() == IndexOperationTrace.LOGICAL_DELETE
                                                    ? null
                                                    : payloadRows.get(
                                                            location(
                                                                    write.file(),
                                                                    write.position())));
                        }
                        pending.clear();
                    }
                });
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
        List<JsonObject> sourceFiles =
                Files.readAllLines(sourceBaseline.resolve("files.jsonl")).stream()
                        .map(line -> JsonParser.parseString(line).getAsJsonObject())
                        .toList();
        Map<Integer, Long> tableRows = new HashMap<>();
        for (JsonObject file : sourceFiles)
            tableRows.merge(
                    catalog.tableId(file.get("table").getAsString()),
                    file.get("rows").getAsLong(),
                    Long::sum);
        Map<Integer, long[]> seen = new HashMap<>();
        for (JsonObject file : sourceFiles)
            readFile(
                    sourceBaseline,
                    file,
                    catalog,
                    (table, key, row) -> {
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
                    },
                    null);
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
        for (JsonObject file : sourceFiles)
            readFile(
                    sourceBaseline,
                    file,
                    catalog,
                    (table, key, row) -> {
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
                    },
                    null);
        for (RangeProbe probe : probes) {
            probe.times.addAll(List.of(0L, lastCommit[0]));
            long first = Long.MAX_VALUE;
            // Only a row whose entry lies in the range at some version can ever be returned, so
            // each probe keeps those alone; keeping every changed row of the table in every probe
            // made memory the probe count times the changed rows.
            for (var change : histories.entrySet())
                if (change.getKey().table() == probe.table && probe.ever(change.getKey().key(), change.getValue())) {
                    probe.changed.add(Map.entry(change.getKey().key(), change.getValue()));
                    first = Math.min(first, change.getValue().commits.firstKey());
                }
            if (first != Long.MAX_VALUE) {
                probe.times.add(first - 1);
                probe.times.add(first);
            }
        }
        List<Map<String, Object>> modes = new ArrayList<>();
        for (boolean rowIds : new boolean[] {false, true}) {
            try (var model = new IndexReferenceReplay(rowIds, config.secondaryIndexes());
                    var load = new ReplayProjection(config, false, model::accept);
                    var runtime = new ReplayProjection(config, true, model::accept)) {
                baseline.read(load);
                load.finish();
                IndexOperationTrace.read(workload, runtime);
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
                    var relevant = new ArrayList<Map.Entry<IndexKey, History>>();
                    var times = new TreeSet<Long>(List.of(0L, lastCommit[0]));
                    long first = Long.MAX_VALUE;
                    for (var change : histories.entrySet())
                        if (change.getKey().table() == predicate.table()
                                && matchesHistory(change.getValue(), predicate)) {
                            relevant.add(Map.entry(change.getKey().key(), change.getValue()));
                            first = Math.min(first, change.getValue().commits.firstKey());
                        }
                    if (first != Long.MAX_VALUE) {
                        times.add(first - 1);
                        times.add(first);
                    }
                    for (long time : times) {
                        long expectedCount = keys.size;
                        for (var change : relevant) {
                            if (keys.contains(change.getKey())) expectedCount--;
                            if (matches(change.getValue().at(time), predicate)) expectedCount++;
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
                modes.add(
                        Map.of(
                                "mode",
                                rowIds ? "rowid" : "locator",
                                "native_queries",
                                model.queries(),
                                "derived_primary_probes",
                                primaryProbes,
                                "derived_secondary_probes",
                                secondaryProbes,
                                "derived_range_probes",
                                rangeProbes,
                                "derived_checked_result_items",
                                items,
                                "retained_index_versions",
                                model.retainedVersions(),
                                "workload_dispatch",
                                runtime.describe()));
            }
        }
        Map<String, Object> output = new LinkedHashMap<>();
        output.put("status", "passed");
        output.put(
                "provenance",
                "derived_history_validation_from_independent_parquet_rows_not_native_queries");
        output.put("preflight", preflight);
        // A derived load was checked against this digest when it was read.
        output.put("baseline_trace_sha256", manifest.get("baseline_trace_sha256").getAsString());
        output.put("workload_trace_sha256", IndexOperationTrace.sha256(workload));
        output.put("changed_primary_keys", histories.size());
        output.put("secondary_predicate_sample_limit_per_index", SECONDARY_PREDICATES);
        output.put("sampled_secondary_predicates", buckets.size());
        output.put("value_sample_rows_per_table", VALUE_SAMPLE);
        output.put("range_points_per_index", RANGE_POINTS);
        output.put("range_anchors_per_index", RANGE_ANCHORS);
        output.put("range_probe_limit", RANGE_LIMIT);
        output.put("range_probes", probes.size());
        output.put("last_published_commit_version", lastCommit[0]);
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
                    + " key/version/Locator/RowID and result order against independently decoded"
                    + " baseline and incremental Parquet, including old/new values; search groups"
                    + " only through their member searches; not full HTAP snapshots,"
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

    private static boolean matchesHistory(History history, Predicate predicate) {
        return matches(history.baseline, predicate)
                || history.commits.values().stream().anyMatch(row -> matches(row, predicate));
    }

    private static SortedSet<Long> times(History history, long last) {
        var times = new TreeSet<Long>(List.of(0L, last));
        for (long version : history.commits.keySet()) {
            times.add(version - 1);
            times.add(version);
        }
        return times;
    }

    private static String location(long file, long position) {
        return file + "/" + position;
    }

    private static void verifyFile(Path directory, JsonObject file) throws IOException {
        Path path = directory.resolve(file.get("path").getAsString());
        if (!BaselineGenerator.sha256(path).equals(file.get("sha256").getAsString()))
            throw new IOException("Parquet checksum mismatch: " + path);
    }

    private static void readFile(
            Path directory,
            JsonObject file,
            IndexCatalog catalog,
            RowConsumer consumer,
            Map<String, Operation> writes)
            throws IOException {
        String table = file.get("table").getAsString();
        long fileId = file.get("file_id").getAsLong(), position = 0;
        try (ParquetReader<Group> reader =
                ParquetReader.builder(
                                new GroupReadSupport(),
                                new org.apache.hadoop.fs.Path(
                                        directory.resolve(file.get("path").getAsString()).toUri()))
                        .build()) {
            Group group;
            while ((group = reader.read()) != null) {
                if (writes != null) {
                    Operation operation = writes.get(location(fileId, position));
                    if (operation == null
                            || group.getLong("_sib_mutation_ordinal", 0)
                                    != operation.mutationOrdinal())
                        throw new IOException("Incremental mutation position mismatch");
                }
                consumer.accept(
                        catalog.tableId(table),
                        catalog.primaryKey(table, group),
                        new Row(
                                group.getLong("_sib_timestamp", 0),
                                group.getLong("_sib_row_id", 0),
                                fileId,
                                position++,
                                catalog.keys(table, group)));
            }
        }
        if (position != file.get("rows").getAsLong())
            throw new IOException("Parquet row count mismatch");
    }
}
