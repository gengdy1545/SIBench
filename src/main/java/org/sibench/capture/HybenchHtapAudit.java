package org.sibench.capture;

import com.google.gson.*;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;

import java.io.IOException;
import java.nio.file.*;
import java.sql.*;
import java.util.*;

/** Independent SF1 row-state audit; never infers a global snapshot order from per-PID files. */
public final class HybenchHtapAudit {
    private HybenchHtapAudit() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 4)
            throw new IllegalArgumentException("HybenchHtapAudit JDBC_URL BASELINE RAW_DIRECTORY REPORT");
        audit(args[0], Path.of(args[1]), Path.of(args[2]), Path.of(args[3]));
    }

    static Map<String, Object> audit(String jdbc, Path baseline, Path raw, Path report) throws Exception {
        CaptureRun.verifyInput(raw);
        PgBaselineVerify.verifyFiles(baseline);
        JsonObject capture = JsonParser.parseString(Files.readString(raw.resolve("capture-run.json")))
                .getAsJsonObject();
        Map<String, Map<Long, List<JsonObject>>> changes = new LinkedHashMap<>();
        Map<String, Long> eventCounts = new TreeMap<>(), phaseRequests = new TreeMap<>();
        Map<String, Set<String>> indexes = new TreeMap<>();
        long commits = 0, aborts = 0, discarded = 0;
        for (JsonElement session : capture.getAsJsonArray("sessions")) {
            JsonObject metadata = session.getAsJsonObject();
            String phase;
            Path client = raw.resolve(metadata.get("jdbc_file").getAsString());
            try (var input = Files.newBufferedReader(client)) {
                String first = input.readLine();
                phase = JsonParser.parseString(first).getAsJsonObject().get("phase").getAsString();
                String line;
                while ((line = input.readLine()) != null) {
                    JsonObject event = JsonParser.parseString(line).getAsJsonObject();
                    if (event.get("event").getAsString().equals("request_start"))
                        phaseRequests.merge(phase, 1L, Long::sum);
                }
            }
            List<JsonObject> pending = new ArrayList<>();
            try (var input = Files.newBufferedReader(raw.resolve(metadata.get("pg_file").getAsString()))) {
                String line;
                while ((line = input.readLine()) != null) {
                    JsonObject event = JsonParser.parseString(line).getAsJsonObject();
                    String type = event.get("event").getAsString();
                    eventCounts.merge(type, 1L, Long::sum);
                    switch (type) {
                        case "scan_rescan" -> indexes.computeIfAbsent(phase, p -> new TreeSet<>())
                                .add(event.get("relation").getAsString());
                        case "row_change" -> {
                            if (phase.equals("ap")) throw new IOException("AP phase changed business data");
                            pending.add(event);
                        }
                        case "transaction_abort" -> {
                            aborts++;
                            discarded += pending.size();
                            pending.clear();
                        }
                        case "transaction_commit" -> {
                            commits++;
                            for (JsonObject change : pending) {
                                String table = change.get("relation").getAsString();
                                String pk = PgBaselineExport.TABLES.get(table);
                                if (pk == null) throw new IOException("Unknown business table: " + table);
                                JsonObject identity = change.get("operation").getAsString().equals("delete")
                                        ? change.getAsJsonObject("old") : change.getAsJsonObject("new");
                                long key = identity.get(pk).getAsLong();
                                changes.computeIfAbsent(table, t -> new TreeMap<>())
                                        .computeIfAbsent(key, k -> new ArrayList<>()).add(change);
                            }
                            pending.clear();
                        }
                    }
                }
            }
            if (!pending.isEmpty()) throw new IOException("Unfinished row mutations");
        }
        List<JsonObject> files = Files.readAllLines(baseline.resolve("files.jsonl")).stream()
                .map(line -> JsonParser.parseString(line).getAsJsonObject()).toList();
        var tableReports = new ArrayList<Map<String, Object>>();
        long checked = 0;
        try (Connection pg = DriverManager.getConnection(jdbc, "postgres", "")) {
            pg.setAutoCommit(false);
            pg.setReadOnly(true);
            pg.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            for (var table : PgBaselineExport.TABLES.entrySet()) {
                String name = table.getKey(), pk = table.getValue();
                Map<Long, List<JsonObject>> mutations = changes.getOrDefault(name, Map.of());
                Map<Long, JsonObject> replacements = new TreeMap<>();
                Set<Long> baselineKeys = new HashSet<>();
                long baselineRows = 0, modified = 0;
                for (JsonObject file : files) {
                    if (!file.get("table").getAsString().equals(name)) continue;
                    try (ParquetReader<Group> input = reader(baseline.resolve(file.get("path").getAsString()))) {
                        Group group;
                        while ((group = input.read()) != null) {
                            baselineRows++;
                            long key = group.getLong(pk, 0);
                            if (mutations.containsKey(key)) {
                                baselineKeys.add(key);
                                replacements.put(key, applyRowChanges(row(group), mutations.get(key)));
                            }
                        }
                    }
                }
                for (var mutation : mutations.entrySet()) {
                    modified += mutation.getValue().size();
                    if (!baselineKeys.contains(mutation.getKey()))
                        replacements.put(mutation.getKey(), applyRowChanges(null, mutation.getValue()));
                }
                long finalRows = 0, unchanged = 0;
                Set<Long> seen = new HashSet<>();
                try (Statement statement = pg.createStatement()) {
                    statement.setFetchSize(8192);
                    try (ResultSet actual = statement.executeQuery(
                            "SELECT row_to_json(t)::text FROM " + name + " t ORDER BY " + pk)) {
                        for (JsonObject file : files) {
                            if (!file.get("table").getAsString().equals(name)) continue;
                            try (ParquetReader<Group> input = reader(baseline.resolve(file.get("path").getAsString()))) {
                                Group group;
                                while ((group = input.read()) != null) {
                                    long key = group.getLong(pk, 0);
                                    JsonObject expected = replacements.containsKey(key)
                                            ? replacements.get(key) : row(group);
                                    if (expected == null) continue;
                                    if (!actual.next()) throw new IOException("Missing final row " + name + "/" + key);
                                    JsonObject value = JsonParser.parseString(actual.getString(1)).getAsJsonObject();
                                    if (value.get(pk).getAsLong() != key || !PgCaptureAudit.equalRow(expected, value))
                                        throw new IOException("Final row mismatch " + name + "/" + key);
                                    if (mutations.containsKey(key)) seen.add(key);
                                    else unchanged++;
                                    finalRows++;
                                }
                            }
                        }
                        for (var entry : replacements.entrySet()) {
                            if (baselineKeys.contains(entry.getKey()) || entry.getValue() == null) continue;
                            if (!actual.next()) throw new IOException("Missing inserted row");
                            JsonObject value = JsonParser.parseString(actual.getString(1)).getAsJsonObject();
                            if (value.get(pk).getAsLong() != entry.getKey()
                                    || !PgCaptureAudit.equalRow(entry.getValue(), value))
                                throw new IOException("Inserted row mismatch " + name + "/" + entry.getKey());
                            seen.add(entry.getKey());
                            finalRows++;
                        }
                        if (actual.next()) throw new IOException("Uncaptured final row in " + name);
                    }
                }
                for (var entry : replacements.entrySet())
                    if (entry.getValue() != null && !seen.contains(entry.getKey()))
                        throw new IOException("Changed row was not verified");
                tableReports.add(Map.of("table", name, "baseline_rows", baselineRows,
                        "final_rows", finalRows, "committed_mutations", modified,
                        "unchanged_rows_checked", unchanged, "changed_keys", mutations.size()));
                checked += finalRows;
                System.out.println(name + " final rows checked=" + finalRows + " mutations=" + modified);
            }
            pg.commit();
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("status", "passed_native_htap_row_state");
        result.put("capture_manifest_sha256",
                org.sibench.generation.BaselineGenerator.sha256(raw.resolve("capture-run.json")));
        result.put("baseline_verification_sha256",
                org.sibench.generation.BaselineGenerator.sha256(baseline.resolve("verified-manifest.json")));
        result.put("phase_requests", phaseRequests);
        result.put("accessed_indexes_by_phase", indexes);
        result.put("events", eventCounts);
        result.put("commits", commits);
        result.put("aborts", aborts);
        result.put("discarded_aborted_mutations", discarded);
        result.put("final_rows_checked", checked);
        result.put("tables", tableReports);
        result.put("limitations", List.of("Not a global concurrent snapshot or KV replay audit",
                "Per-key before/after chains establish row-state causality, not global commit order",
                "Inserted IDs must follow the native sequence beyond baseline IDs"));
        Files.writeString(report, new GsonBuilder().setPrettyPrinting().create().toJson(result),
                StandardOpenOption.CREATE_NEW);
        return result;
    }

    static JsonObject applyRowChanges(JsonObject initial, List<JsonObject> input) throws IOException {
        List<JsonObject> pending = new ArrayList<>(input);
        JsonObject state = initial;
        while (!pending.isEmpty()) {
            boolean matched = false;
            for (var iterator = pending.iterator(); iterator.hasNext();) {
                JsonObject change = iterator.next();
                JsonObject before = change.get("old").isJsonNull() ? null : change.getAsJsonObject("old");
                if (!(state == null && before == null) && !PgCaptureAudit.equalRow(state, before)) continue;
                state = change.get("new").isJsonNull() ? null : change.getAsJsonObject("new");
                iterator.remove();
                matched = true;
                break;
            }
            if (!matched) throw new IOException("Committed before/after row chain does not match baseline");
        }
        return state;
    }

    private static ParquetReader<Group> reader(Path path) throws IOException {
        return ParquetReader.builder(new GroupReadSupport(),
                new org.apache.hadoop.fs.Path(path.toUri())).build();
    }

    private static JsonObject row(Group group) {
        JsonObject row = new JsonObject();
        for (int i = 0; i < group.getType().getFieldCount(); i++) {
            String name = group.getType().getFieldName(i);
            if (name.startsWith("_sib_")) continue;
            if (group.getFieldRepetitionCount(i) == 0) row.add(name, JsonNull.INSTANCE);
            else row.addProperty(name, group.getValueToString(i, 0));
        }
        return row;
    }
}
