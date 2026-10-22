package org.sibench.replay;

import com.google.gson.*;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.sibench.capture.*;
import org.sibench.capture.IndexOperationTrace.Operation;
import org.sibench.capture.IndexReferenceReplay.LookupResult;
import org.sibench.generation.BaselineGenerator;
import org.sibench.trace.IndexKey;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;

/** Independent Parquet row oracle; derived probes are not native workload or timed performance. */
public final class SnapshotLookupValidation {
    private static final int SECONDARY_PREDICATES = 64;

    private record Key(int table, IndexKey key) {}

    private record Predicate(int table, int index, IndexKey value) {}

    private record Row(long version, long rowId, long locator, Map<Integer, IndexKey> indexes) {
        LookupResult result(IndexKey key, boolean rowIds) {
            return new LookupResult(key, version, rowIds ? rowId : locator);
        }
    }

    private static final class History {
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

        void add(int table, IndexKey key, long rowId, long locator) {
            rows.put(new Key(table, key), new long[] {rowId, locator});
        }

        LookupResult get(int table, IndexKey key, boolean rowIds) {
            long[] row = rows.get(new Key(table, key));
            return new LookupResult(key, 0, row[rowIds ? 0 : 1]);
        }
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
        Path baseline = directory.resolve("baseline.ops"),
                workload = directory.resolve("workload.ops");
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
                        .equals(verification.get("files_manifest_sha256").getAsString())
                || !BaselineGenerator.sha256(sourceBaseline.resolve("locators.tsv"))
                        .equals(verification.get("locators_sha256").getAsString()))
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
        Map<String, SortedSet<IndexKey>> changedValues = new TreeMap<>(),
                baselineValues = new TreeMap<>();
        for (var definition : catalog.definitions())
            if (definition.indexId() > 0) {
                String id = definition.tableId() + "/" + definition.indexId();
                changedValues.put(id, new TreeSet<>());
                baselineValues.put(id, new TreeSet<>());
            }
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
        for (JsonObject file : sourceFiles)
            readFile(
                    sourceBaseline,
                    file,
                    catalog,
                    (table, key, row) -> {
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
                        row.indexes()
                                .forEach(
                                        (index, value) -> {
                                            SortedSet<IndexKey> sample =
                                                    baselineValues.get(table + "/" + index);
                                            sample.add(value);
                                            if (sample.size() > SECONDARY_PREDICATES)
                                                sample.remove(sample.last());
                                        });
                    },
                    null);
        Map<Predicate, Keys> buckets = new LinkedHashMap<>();
        if (config.secondaryIndexes())
            for (var definition : catalog.definitions()) {
                if (definition.indexId() == 0) continue;
                String id = definition.tableId() + "/" + definition.indexId();
                var selected = new LinkedHashSet<IndexKey>();
                changedValues.get(id).stream().limit(SECONDARY_PREDICATES).forEach(selected::add);
                baselineValues.get(id).stream()
                        .limit(SECONDARY_PREDICATES - selected.size())
                        .forEach(selected::add);
                for (IndexKey value : selected)
                    buckets.put(
                            new Predicate(definition.tableId(), definition.indexId(), value),
                            new Keys());
            }
        Locations locations = new Locations();
        for (JsonObject file : sourceFiles)
            readFile(
                    sourceBaseline,
                    file,
                    catalog,
                    (table, key, row) -> {
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
                        if (retained) locations.add(table, key, row.rowId(), row.locator());
                    },
                    null);
        List<Map<String, Object>> modes = new ArrayList<>();
        for (boolean rowIds : new boolean[] {false, true}) {
            var model = new IndexReferenceReplay(rowIds, config.secondaryIndexes());
            try (var load = new ReplayProjection(config, false, model::accept);
                    var runtime = new ReplayProjection(config, true, model::accept)) {
                IndexOperationTrace.read(baseline, load);
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
                    var relevant = new LinkedHashMap<IndexKey, History>();
                    var times = new TreeSet<Long>(List.of(0L, lastCommit[0]));
                    long first = Long.MAX_VALUE;
                    for (var change : histories.entrySet())
                        if (change.getKey().table() == predicate.table()
                                && matchesHistory(change.getValue(), predicate)) {
                            relevant.put(change.getKey().key(), change.getValue());
                            first = Math.min(first, change.getValue().commits.firstKey());
                        }
                    if (first != Long.MAX_VALUE) {
                        times.add(first - 1);
                        times.add(first);
                    }
                    for (long time : times) {
                        long expectedCount = keys.size;
                        for (var change : relevant.entrySet()) {
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
        output.put("baseline_trace_sha256", BaselineGenerator.sha256(baseline));
        output.put("workload_trace_sha256", BaselineGenerator.sha256(workload));
        output.put("changed_primary_keys", histories.size());
        output.put("secondary_predicate_sample_limit_per_index", SECONDARY_PREDICATES);
        output.put("sampled_secondary_predicates", buckets.size());
        output.put("last_published_commit_version", lastCommit[0]);
        output.put("results", modes);
        output.put(
                "scope",
                "All changed primary keys and full secondary identities at baseline, before/at"
                    + " every per-key commit and final; sampled nonunique equality predicates for"
                    + " every source index at baseline, before/at first relevant change and final."
                    + " Exact key/version/Locator/RowID comparison against independently decoded"
                    + " baseline and incremental Parquet, including old/new values; not full HTAP"
                    + " snapshots, derived range probes, relocation/GC or performance.");
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
                                group.getLong("_sib_timestamp", 0), group.getLong("_sib_row_id", 0),
                                IndexReferenceReplay.locator(fileId, position++),
                                        catalog.keys(table, group)));
            }
        }
        if (position != file.get("rows").getAsLong())
            throw new IOException("Parquet row count mismatch");
    }
}
