package org.sibench.capture;

import com.google.gson.*;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;

import java.nio.file.*;
import java.sql.*;
import java.util.*;

/** Audit the single-session TP3/TP5/TP9 scope against baseline and committed changes. */
public final class PgCaptureAudit {
    private PgCaptureAudit() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 4)
            throw new IllegalArgumentException(
                    "PgCaptureAudit JDBC_URL BASELINE RAW_PG_JSONL REPORT");
        Path baseline = Path.of(args[1]);
        Map<Long, JsonObject> accounts = new HashMap<>();
        Map<Long, JsonObject> inserted = new HashMap<>();
        List<JsonObject> changes = new ArrayList<>();
        Map<String, Long> events = new TreeMap<>();
        long applied = 0, aborted = 0, commits = 0, starts = 0, ends = 0;
        List<JsonObject> fileList =
                Files.readAllLines(baseline.resolve("files.jsonl")).stream()
                        .map(l -> JsonParser.parseString(l).getAsJsonObject())
                        .toList();
        for (JsonObject file : fileList)
            if (file.get("table").getAsString().equals("savingaccount")) {
                try (ParquetReader<Group> reader =
                        ParquetReader.builder(
                                        new GroupReadSupport(),
                                        new org.apache.hadoop.fs.Path(
                                                baseline.resolve(file.get("path").getAsString())
                                                        .toUri()))
                                .build()) {
                    Group row;
                    while ((row = reader.read()) != null) {
                        JsonObject value = new JsonObject();
                        for (int i = 0; i < row.getType().getFieldCount(); i++) {
                            String name = row.getType().getFieldName(i);
                            if (name.startsWith("_sib_")) continue;
                            if (row.getFieldRepetitionCount(i) == 0)
                                value.add(name, JsonNull.INSTANCE);
                            else value.addProperty(name, row.getValueToString(i, 0));
                        }
                        accounts.put(value.get("accountid").getAsLong(), value);
                    }
                }
            }
        Map<String, Long> scanCandidates = new HashMap<>();
        long completeScans = 0, totalCandidates = 0;
        try (var reader = Files.newBufferedReader(Path.of(args[2]))) {
            String line;
            while ((line = reader.readLine()) != null) {
                JsonObject event = JsonParser.parseString(line).getAsJsonObject();
                String type = event.get("event").getAsString();
                events.merge(type, 1L, Long::sum);
                String scan =
                        event.has("scan")
                                ? event.get("application").getAsString()
                                        + "/"
                                        + event.get("scan").getAsString()
                                : "";
                switch (type) {
                    case "executor_start" -> starts++;
                    case "executor_end" -> ends++;
                    case "scan_rescan" -> scanCandidates.put(scan, 0L);
                    case "bitmap_candidate", "scan_next" -> {
                        if (event.has("found") && event.get("found").getAsBoolean()) {
                            if (!scanCandidates.containsKey(scan))
                                throw new IllegalStateException("Candidate without scan");
                            scanCandidates.merge(scan, 1L, Long::sum);
                            totalCandidates++;
                        }
                    }
                    case "scan_end" -> {
                        if (scanCandidates.remove(scan) == null)
                            throw new IllegalStateException("Unmatched scan end");
                        completeScans++;
                    }
                    case "row_change" -> changes.add(event);
                    case "transaction_abort" -> {
                        aborted += changes.size();
                        changes.clear();
                    }
                    case "transaction_commit" -> {
                        for (JsonObject change : changes) {
                            String table = change.get("relation").getAsString();
                            JsonObject next = change.getAsJsonObject("new");
                            if (table.equals("savingaccount")) {
                                long id = next.get("accountid").getAsLong();
                                if (!equalRow(accounts.get(id), change.getAsJsonObject("old")))
                                    throw new IllegalStateException(
                                            "Old account state mismatch " + id);
                                accounts.put(id, next);
                            } else if (table.equals("transfer")
                                    && change.get("operation").getAsString().equals("insert")) {
                                if (inserted.put(next.get("id").getAsLong(), next) != null)
                                    throw new IllegalStateException("Duplicate transfer");
                            } else
                                throw new IllegalStateException("Unsupported mutation: " + change);
                            applied++;
                        }
                        changes.clear();
                        commits++;
                    }
                }
            }
        }
        if (!changes.isEmpty() || !scanCandidates.isEmpty() || starts != ends)
            throw new IllegalStateException("Incomplete capture");
        try (Connection connection = DriverManager.getConnection(args[0], "postgres", "")) {
            connection.setAutoCommit(false);
            connection.setReadOnly(true);
            connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            for (String table : List.of("savingaccount", "transfer")) {
                var expected = table.equals("savingaccount") ? accounts : inserted;
                String condition = table.equals("transfer") ? " WHERE id>6000000" : "";
                long count = 0;
                try (Statement statement = connection.createStatement()) {
                    statement.setFetchSize(8192);
                    try (ResultSet rows =
                            statement.executeQuery(
                                    "SELECT row_to_json(t)::text FROM "
                                            + table
                                            + " t"
                                            + condition)) {
                        while (rows.next()) {
                            JsonObject actual =
                                    JsonParser.parseString(rows.getString(1)).getAsJsonObject();
                            long key =
                                    actual.get(table.equals("transfer") ? "id" : "accountid")
                                            .getAsLong();
                            if (!equalRow(expected.get(key), actual))
                                throw new IllegalStateException(
                                        "Final state mismatch " + table + "/" + key);
                            count++;
                        }
                    }
                }
                if (count != expected.size())
                    throw new IllegalStateException("Final count mismatch");
            }
            connection.commit();
        }
        Map<String, Object> report = new LinkedHashMap<>();
        report.put("status", "passed_for_single_session_scope");
        report.put("events", events);
        report.put("applied_row_changes", applied);
        report.put("discarded_aborted_changes", aborted);
        report.put("commits", commits);
        report.put("complete_scans", completeScans);
        report.put("candidate_events", totalCandidates);
        report.put("accounts_checked", accounts.size());
        report.put("inserted_transfers_checked", inserted.size());
        report.put(
                "limitations",
                List.of(
                        "State equality does not prove all index work captured",
                        "No multi-session visibility equivalence",
                        "No complete Index Service replay"));
        Files.writeString(
                Path.of(args[3]),
                new GsonBuilder().setPrettyPrinting().create().toJson(report),
                StandardOpenOption.CREATE_NEW);
    }

    static boolean equalRow(JsonObject left, JsonObject right) {
        if (left == null || right == null || !left.keySet().equals(right.keySet())) return false;
        for (String key : left.keySet()) {
            JsonElement a = left.get(key), b = right.get(key);
            if (a.isJsonNull() || b.isJsonNull()) {
                if (!a.equals(b)) return false;
                continue;
            }
            String x = a.getAsString(), y = b.getAsString();
            if (key.equals("balance") || key.equals("amount")) {
                if (Float.floatToIntBits(Float.parseFloat(x))
                        != Float.floatToIntBits(Float.parseFloat(y))) return false;
            } else if (key.contains("timestamp") || key.equals("fresh_ts")) {
                if (!java.time.LocalDateTime.parse(x.replace(' ', 'T'))
                        .equals(java.time.LocalDateTime.parse(y.replace(' ', 'T')))) return false;
            } else if (!x.stripTrailing().equals(y.stripTrailing())) return false;
        }
        return true;
    }
}
