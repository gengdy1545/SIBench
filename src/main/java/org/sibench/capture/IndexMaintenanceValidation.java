package org.sibench.capture;

import com.google.gson.*;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.sibench.capture.IndexOperationTrace.Operation;
import org.sibench.generation.BaselineGenerator;
import org.sibench.schema.SourceSchema;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;

/** Checks index maintenance from audited transitions, without re-auditing source row state. */
public final class IndexMaintenanceValidation {
    private record Key(String table, long key) {}

    private IndexMaintenanceValidation() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 5 && args.length != 6)
            throw new IllegalArgumentException(
                    "IndexMaintenanceValidation ROOT CONFIG BASELINE RAW REPORT [ROW_AUDIT]");
        CaptureConfig config = CaptureConfig.read(Path.of(args[0]), Path.of(args[1]));
        validate(
                IndexCatalog.read(config.source()),
                SourceSchema.hybench(config.source()),
                Path.of(args[2]),
                Path.of(args[3]),
                Path.of(args[4]),
                args.length == 6
                        ? Path.of(args[5])
                        : RowStateAudit.defaultReport(Path.of(args[3])));
    }

    public static Map<String, Object> validate(
            IndexCatalog catalog,
            SourceSchema schema,
            Path baseline,
            Path raw,
            Path report,
            Path auditReport)
            throws Exception {
        if (Files.exists(report)) throw new IOException("Validation report exists");
        RowStateAudit.reuse(schema, baseline, raw, auditReport);
        var changes = new LinkedHashMap<Key, List<JsonObject>>();
        JsonObject run = JsonParser.parseString(Files.readString(auditReport)).getAsJsonObject();
        long committed = run.get("committed_row_changes").getAsLong();
        long discarded = run.get("discarded_aborted_row_changes").getAsLong();
        try (var reader = Files.newBufferedReader(RowStateAudit.changesFile(auditReport, run))) {
            String line;
            while ((line = reader.readLine()) != null) {
                JsonObject change = JsonParser.parseString(line).getAsJsonObject();
                String table = change.get("relation").getAsString();
                JsonObject row =
                        change.get("new").isJsonNull()
                                ? change.getAsJsonObject("old")
                                : change.getAsJsonObject("new");
                Key key = new Key(table, row.get(PgBaselineExport.TABLES.get(table)).getAsLong());
                changes.computeIfAbsent(key, ignored -> new ArrayList<>()).add(change);
            }
        }
        var initial = new HashMap<Key, JsonObject>();
        long baselineRows = 0, baselineIndexEntries = 0;
        var counts = new TreeMap<String, Long>();
        for (String line : Files.readAllLines(baseline.resolve("files.jsonl"))) {
            JsonObject file = JsonParser.parseString(line).getAsJsonObject();
            String table = file.get("table").getAsString();
            try (ParquetReader<Group> reader =
                    ParquetReader.builder(
                                    new GroupReadSupport(),
                                    new org.apache.hadoop.fs.Path(
                                            baseline.resolve(file.get("path").getAsString())
                                                    .toUri()))
                            .build()) {
                Group row;
                while ((row = reader.read()) != null) {
                    Map<Integer, String> indexed = catalog.keys(table, row);
                    baselineRows++;
                    baselineIndexEntries += 1 + indexed.size();
                    counts.merge(table + "/0", 1L, Long::sum);
                    for (int index : indexed.keySet())
                        counts.merge(table + "/" + index, 1L, Long::sum);
                    Key key = new Key(table, row.getLong(PgBaselineExport.TABLES.get(table), 0));
                    if (changes.containsKey(key)) initial.put(key, SourceRow.json(row));
                }
            }
        }
        long rekeys = 0, probes = 0;
        Map<String, Long> rekeysByIndex = new TreeMap<>();
        for (var entry : changes.entrySet()) {
            Key key = entry.getKey();
            JsonObject state = initial.get(key);
            var models = List.of(new IndexReferenceReplay(false), new IndexReferenceReplay(true));
            Map<Integer, String> initialKeys =
                    state == null ? Map.of() : catalog.keys(key.table(), state);
            long rowId = 1, version = 0;
            if (state != null) {
                Operation primary =
                        new Operation(
                                0,
                                1,
                                IndexCatalog.tableId(key.table()),
                                0,
                                0,
                                key.key(),
                                0,
                                1,
                                0,
                                rowId,
                                0,
                                0,
                                0);
                for (var model : models)
                    IndexMutation.emit(primary, Map.of(), initialKeys, model::accept);
            }
            var predicates = new LinkedHashMap<Integer, Set<String>>();
            for (var definition : catalog.secondary(IndexCatalog.tableId(key.table())))
                predicates.put(definition.indexId(), new HashSet<>());
            initialKeys.forEach((index, value) -> predicates.get(index).add(value));
            for (JsonObject matched : entry.getValue()) {
                JsonObject next =
                        matched.get("new").isJsonNull() ? null : matched.getAsJsonObject("new");
                Map<Integer, String> before =
                        state == null ? Map.of() : catalog.keys(key.table(), state);
                Map<Integer, String> after =
                        next == null ? Map.of() : catalog.keys(key.table(), next);
                for (int index : before.keySet())
                    if (after.containsKey(index)
                            && !Objects.equals(before.get(index), after.get(index))) {
                        rekeys++;
                        rekeysByIndex.merge(key.table() + "/" + index, 1L, Long::sum);
                    }
                after.forEach((index, value) -> predicates.get(index).add(value));
                version++;
                if (state == null) rowId++;
                Operation primary =
                        new Operation(
                                version,
                                next == null ? 5 : 3,
                                IndexCatalog.tableId(key.table()),
                                0,
                                state == null ? 1 : 2,
                                key.key(),
                                version,
                                next == null ? 0 : 2,
                                version,
                                rowId,
                                0,
                                0,
                                0,
                                version,
                                1);
                for (int mode = 0; mode < models.size(); mode++) {
                    var model = models.get(mode);
                    IndexMutation.emit(primary, before, after, model::accept);
                    for (var predicate : predicates.entrySet())
                        for (String value : predicate.getValue()) {
                            var historical =
                                    model.lookupSecondary(
                                            primary.table(),
                                            predicate.getKey(),
                                            value,
                                            version - 1);
                            boolean previouslyPresent =
                                    before.containsKey(predicate.getKey())
                                            && Objects.equals(
                                                    before.get(predicate.getKey()), value);
                            if (historical.size() != (previouslyPresent ? 1 : 0))
                                throw new IOException("Old secondary visibility mismatch: " + key);
                            var actual =
                                    model.lookupSecondary(
                                            primary.table(), predicate.getKey(), value, version);
                            boolean present =
                                    after.containsKey(predicate.getKey())
                                            && Objects.equals(after.get(predicate.getKey()), value);
                            if (actual.size() != (present ? 1 : 0))
                                throw new IOException("New secondary visibility mismatch: " + key);
                            if (present
                                    && (actual.get(0).commitVersion() != version
                                            || actual.get(0).value()
                                                    != (mode == 1
                                                            ? rowId
                                                            : IndexReferenceReplay.locator(
                                                                    2, version))))
                                throw new IOException("Secondary value/version mismatch: " + key);
                            probes += 2;
                        }
                }
                state = next;
            }
        }
        var output = new LinkedHashMap<String, Object>();
        output.put("status", "passed");
        output.put("provenance", "native_committed_row_changes_per_key_causal_maintenance_audit");
        output.put("baseline_rows", baselineRows);
        output.put("baseline_index_entries", baselineIndexEntries);
        output.put("baseline_entries_by_index", counts);
        output.put("index_definitions", catalog.describe());
        output.put("committed_row_changes", committed);
        output.put("discarded_aborted_row_changes", discarded);
        output.put("changed_primary_keys", changes.size());
        output.put("indexed_value_changes", rekeys);
        output.put("indexed_value_changes_by_index", rekeysByIndex);
        output.put("locator_and_rowid_visibility_probes", probes);
        Path rawDirectory = Files.isDirectory(raw) ? raw : raw.toAbsolutePath().getParent();
        Path captureManifest = rawDirectory.resolve("capture-run.json");
        output.put(
                "source_manifest_sha256",
                BaselineGenerator.sha256(Files.exists(captureManifest) ? captureManifest : raw));
        output.put("row_state_audit_sha256", BaselineGenerator.sha256(auditReport));
        output.put(
                "scope",
                "All baseline indexed values decoded; real committed per-key transitions checked in"
                    + " both value modes. Derived per-key versions are not global commits or native"
                    + " queries; no concurrent HTAP snapshot projection, network performance or"
                    + " full baseline model replay.");
        Files.writeString(
                report,
                new GsonBuilder().setPrettyPrinting().create().toJson(output),
                StandardOpenOption.CREATE_NEW);
        return output;
    }
}
