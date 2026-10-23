package org.sibench.capture;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * One independent cross-check of capture completeness. The access ledger proves that the events it
 * read were partitioned exactly once; it cannot prove that an event the instrumentation never
 * emitted is missing. PostgreSQL's own index counters, read before and after a capture, are a
 * second opinion: their deltas are compared with the ledger's per-index tallies, which come from
 * the raw events alone.
 *
 * <p>The mapping is not naive equality and is reproduced in every report as
 * {@code counter_semantics}. The check is pure: it reads two snapshots and one ledger report and
 * never touches the database.
 */
public final class CaptureCompleteness {
    public static final int VERSION = 1;

    private static final Gson JSON =
            new GsonBuilder().serializeNulls().setPrettyPrinting().disableHtmlEscaping().create();

    private static final String COUNTERS =
            """
            SELECT current_database(),s.schemaname,s.relname,s.indexrelname,
                   COALESCE(s.idx_scan,0),COALESCE(s.idx_tup_read,0),COALESCE(s.idx_tup_fetch,0)
              FROM pg_stat_user_indexes s
              JOIN pg_index i ON i.indexrelid = s.indexrelid
              JOIN pg_class c ON c.oid = s.relid
             WHERE c.relkind = 'r' AND c.relname NOT LIKE 'pg_%'
             ORDER BY s.schemaname,s.relname,s.indexrelname
            """;

    private CaptureCompleteness() {}

    /** Writes a new counter snapshot; an existing snapshot path is never overwritten. */
    public static Map<String, Object> snapshot(
            String jdbc, String user, Path output, String profile)
            throws SQLException, IOException {
        if (Files.exists(output))
            throw new IOException("Index counter snapshot already exists: " + output);
        Path partial = output.resolveSibling(output.getFileName() + ".partial");
        if (Files.exists(partial))
            throw new IOException("Unfinished index counter snapshot exists; use a fresh output");
        Map<String, Object> result;
        try (Connection connection =
                DriverManager.getConnection(
                        jdbc, user, System.getenv().getOrDefault("PGPASSWORD", ""))) {
            connection.setReadOnly(true);
            result = snapshot(connection, profile);
        }
        Files.writeString(partial, JSON.toJson(result), StandardOpenOption.CREATE_NEW);
        Files.move(partial, output, StandardCopyOption.ATOMIC_MOVE);
        return result;
    }

    /** Reads the counters of every index of a user table; idx_tup_fetch is NULL when never used. */
    static Map<String, Object> snapshot(Connection connection, String profile)
            throws SQLException, IOException {
        var indexes = new ArrayList<Map<String, Object>>();
        String database = null;
        try (Statement statement = connection.createStatement();
                ResultSet rows = statement.executeQuery(COUNTERS)) {
            while (rows.next()) {
                if (database == null) database = rows.getString(1);
                var item = new LinkedHashMap<String, Object>();
                item.put("schemaname", rows.getString(2));
                item.put("relname", rows.getString(3));
                item.put("indexrelname", rows.getString(4));
                item.put("idx_scan", rows.getLong(5));
                item.put("idx_tup_read", rows.getLong(6));
                item.put("idx_tup_fetch", rows.getLong(7));
                indexes.add(item);
            }
        }
        var result = new LinkedHashMap<String, Object>();
        result.put("schema_version", VERSION);
        result.put("source", "pg_stat_user_indexes");
        result.put("profile", profile);
        result.put("database", database);
        result.put("indexes", indexes);
        return result;
    }

    private record Counters(String schema, String table, long scan, long read, long fetch) {}

    private record Ledger(
            String table, long units, long candidates, long visible, long insertAttempts) {}

    /**
     * Compares the before/after counter deltas of every index with the ledger's tallies for the
     * same index. An index missing from either side and a delta that went backwards are
     * inconsistencies; so is a delta the documented counter semantics cannot produce.
     */
    public static Map<String, Object> compare(
            JsonObject ledger, JsonObject before, JsonObject after) {
        Map<String, Counters> start = counters(before), end = counters(after);
        Map<String, Ledger> tallies = ledger(ledger);
        var names = new TreeSet<String>(start.keySet());
        names.addAll(end.keySet());
        names.addAll(tallies.keySet());
        var reports = new ArrayList<Map<String, Object>>();
        var inconsistent = new ArrayList<String>();
        for (String name : names) {
            var reasons = new ArrayList<String>();
            Counters from = start.get(name), to = end.get(name);
            Ledger tally = tallies.get(name);
            if (from == null) reasons.add("index is absent from the before snapshot");
            if (to == null) reasons.add("index is absent from the after snapshot");
            if (tally == null) reasons.add("index is absent from the access ledger");
            boolean comparable = from != null && to != null;
            Long scanDelta = comparable ? to.scan() - from.scan() : null;
            Long readDelta = comparable ? to.read() - from.read() : null;
            Long fetchDelta = comparable ? to.fetch() - from.fetch() : null;
            if (comparable) {
                if (scanDelta < 0) reasons.add("idx_scan went backwards by " + -scanDelta);
                if (readDelta < 0) reasons.add("idx_tup_read went backwards by " + -readDelta);
                if (fetchDelta < 0) reasons.add("idx_tup_fetch went backwards by " + -fetchDelta);
            }
            if (tally != null && comparable) {
                if (readDelta != tally.candidates())
                    reasons.add(
                            "idx_tup_read delta "
                                    + readDelta
                                    + " != ledger candidates "
                                    + tally.candidates());
                if (fetchDelta != tally.visible())
                    reasons.add(
                            "idx_tup_fetch delta "
                                    + fetchDelta
                                    + " != ledger visible_rows "
                                    + tally.visible());
                if (scanDelta > tally.units())
                    reasons.add(
                            "idx_scan delta "
                                    + scanDelta
                                    + " > ledger access_units "
                                    + tally.units());
            }
            var item = new LinkedHashMap<String, Object>();
            item.put("index", name);
            item.put("schemaname", to != null ? to.schema() : from == null ? null : from.schema());
            item.put("table", tally != null ? tally.table() : to != null ? to.table() : null);
            item.put("idx_scan_delta", scanDelta);
            item.put("idx_tup_read_delta", readDelta);
            item.put("idx_tup_fetch_delta", fetchDelta);
            item.put("access_units", tally == null ? null : tally.units());
            item.put("candidates", tally == null ? null : tally.candidates());
            item.put("visible_rows", tally == null ? null : tally.visible());
            item.put("index_insert_attempts", tally == null ? null : tally.insertAttempts());
            item.put(
                    "idx_scan_ratio",
                    tally == null || tally.units() == 0
                            ? 0.0
                            : scanDelta == null ? 0.0 : scanDelta / (double) tally.units());
            item.put("consistent", reasons.isEmpty());
            item.put("reasons", reasons);
            if (!reasons.isEmpty()) inconsistent.add(name);
            reports.add(item);
        }
        var result = new LinkedHashMap<String, Object>();
        result.put("schema_version", VERSION);
        result.put("status", inconsistent.isEmpty() ? "consistent" : "inconsistent");
        result.put(
                "benchmark",
                ledger.has("benchmark") ? ledger.get("benchmark").getAsString() : null);
        result.put("inconsistent_indexes", inconsistent);
        result.put("indexes", reports);
        result.put("counter_semantics", semantics());
        result.put("preconditions", preconditions());
        return result;
    }

    private static Map<String, Counters> counters(JsonObject snapshot) {
        var result = new TreeMap<String, Counters>();
        JsonElement element = snapshot.get("indexes");
        if (element == null || !element.isJsonArray()) return result;
        for (JsonElement item : element.getAsJsonArray()) {
            if (!item.isJsonObject()) continue;
            JsonObject value = item.getAsJsonObject();
            JsonElement name = value.get("indexrelname");
            if (name == null) continue;
            result.put(
                    name.getAsString(),
                    new Counters(
                            text(value, "schemaname"),
                            text(value, "relname"),
                            number(value, "idx_scan"),
                            number(value, "idx_tup_read"),
                            number(value, "idx_tup_fetch")));
        }
        return result;
    }

    private static Map<String, Ledger> ledger(JsonObject report) {
        var result = new TreeMap<String, Ledger>();
        JsonElement element = report.get("indexes");
        if (element == null || !element.isJsonArray()) return result;
        for (JsonElement item : element.getAsJsonArray()) {
            if (!item.isJsonObject()) continue;
            JsonObject value = item.getAsJsonObject();
            JsonElement name = value.get("index");
            if (name == null) continue;
            result.put(
                    name.getAsString(),
                    new Ledger(
                            text(value, "table"),
                            number(value, "access_units"),
                            number(value, "candidates"),
                            number(value, "visible_rows"),
                            number(value, "index_insert_attempts")));
        }
        return result;
    }

    private static List<String> semantics() {
        return List.of(
                "idx_scan counts descents of the index tree (PG14 nbtsearch.c), so it corresponds to"
                    + " access units that fetched at least once, not to all access units;"
                    + " idx_scan_delta is asserted <= access_units and the ratio is reported.",
                "idx_tup_read corresponds to candidate index entries and is compared with the"
                    + " ledger candidates.",
                "idx_tup_fetch corresponds to heap-visible rows and is compared with the ledger"
                    + " visible_rows.");
    }

    private static List<String> preconditions() {
        return List.of(
                "No other session accessed the indexes during the window between the two"
                    + " snapshots.",
                "The baseline export and the row-state audit ordered scans are excluded from the"
                    + " window.",
                "Lossy bitmap pages and BitmapAnd mean some candidates never reach the heap.",
                "idx_scan is not a count of all access units.");
    }

    private static long number(JsonObject value, String key) {
        JsonElement element = value.get(key);
        return element == null || element.isJsonNull() ? 0L : element.getAsLong();
    }

    private static String text(JsonObject value, String key) {
        JsonElement element = value.get(key);
        return element == null || element.isJsonNull() ? null : element.getAsString();
    }
}
