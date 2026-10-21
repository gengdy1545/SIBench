package org.sibench.capture;

import com.google.gson.*;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.sibench.capture.IndexOperationTrace.Operation;
import org.sibench.generation.BaselineGenerator;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;

/** Audits real committed row changes using per-row causal chains, never a fabricated HTAP order. */
public final class IndexMaintenanceValidation {
    private record Key(String table, long key) {}

    private IndexMaintenanceValidation() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 5)
            throw new IllegalArgumentException(
                    "IndexMaintenanceValidation ROOT CONFIG BASELINE RAW REPORT");
        validate(
                IndexCatalog.read(CaptureConfig.read(Path.of(args[0]), Path.of(args[1])).source()),
                Path.of(args[2]),
                Path.of(args[3]),
                Path.of(args[4]));
    }

    public static Map<String, Object> validate(
            IndexCatalog catalog, Path baseline, Path raw, Path report) throws Exception {
        if (Files.exists(report)) throw new IOException("Validation report exists");
        CaptureRun.verifyInput(raw);
        PgBaselineVerify.verifyFiles(baseline);
        var changes = new LinkedHashMap<Key, List<JsonObject>>();
        JsonObject run =
                JsonParser.parseString(Files.readString(raw.resolve("capture-run.json")))
                        .getAsJsonObject();
        long committed = 0, discarded = 0;
        for (JsonElement session : run.getAsJsonArray("sessions")) {
            var pending = new ArrayList<JsonObject>();
            try (var reader =
                    Files.newBufferedReader(
                            raw.resolve(session.getAsJsonObject().get("pg_file").getAsString()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    JsonObject event = JsonParser.parseString(line).getAsJsonObject();
                    String type = event.get("event").getAsString();
                    if (type.equals("row_change")) pending.add(event);
                    else if (type.equals("transaction_abort")) {
                        discarded += pending.size();
                        pending.clear();
                    } else if (type.equals("transaction_commit")) {
                        for (JsonObject change : pending) {
                            String table = change.get("relation").getAsString();
                            JsonObject row =
                                    change.get("new").isJsonNull()
                                            ? change.getAsJsonObject("old")
                                            : change.getAsJsonObject("new");
                            Key key =
                                    new Key(
                                            table,
                                            row.get(PgBaselineExport.TABLES.get(table))
                                                    .getAsLong());
                            changes.computeIfAbsent(key, ignored -> new ArrayList<>()).add(change);
                            committed++;
                        }
                        pending.clear();
                    }
                }
            }
            if (!pending.isEmpty()) throw new IOException("Unfinished source row changes");
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
                    if (changes.containsKey(key)) initial.put(key, json(row));
                }
            }
        }
        long rekeys = 0, probes = 0;
        Map<String, Long> rekeysByIndex = new TreeMap<>();
        for (var entry : changes.entrySet()) {
            Key key = entry.getKey();
            JsonObject state = initial.get(key);
            var pending = new ArrayList<>(entry.getValue());
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
            while (!pending.isEmpty()) {
                JsonObject matched = null;
                for (JsonObject change : pending) {
                    JsonObject before =
                            change.get("old").isJsonNull() ? null : change.getAsJsonObject("old");
                    if ((state == null && before == null)
                            || state != null
                                    && before != null
                                    && PgCaptureAudit.equalRow(state, before)) {
                        matched = change;
                        break;
                    }
                }
                if (matched == null) throw new IOException("No causal source row chain: " + key);
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
                pending.remove(matched);
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
        output.put(
                "source_manifest_sha256",
                BaselineGenerator.sha256(raw.resolve("capture-run.json")));
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

    private static JsonObject json(Group group) {
        var row = new JsonObject();
        for (int column = 0; column < group.getType().getFieldCount(); column++) {
            String name = group.getType().getFieldName(column);
            if (name.startsWith("_sib_")) continue;
            if (group.getFieldRepetitionCount(column) == 0) row.add(name, JsonNull.INSTANCE);
            else row.addProperty(name, group.getValueToString(column, 0));
        }
        return row;
    }
}
