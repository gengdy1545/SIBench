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
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * One independent cross-check of capture completeness. The access ledger proves that the events it
 * read were partitioned exactly once; it cannot prove that an event the instrumentation never
 * emitted is missing. PostgreSQL's own index counters, read before and after a capture, are a
 * second opinion: their deltas are compared with the ledger's per-index tallies, which come from
 * the raw events alone.
 *
 * <p>The mapping follows where PG14 increments each counter, not the counter names, and is
 * reproduced in every report as {@code counter_semantics}. The comparison is pure; only the
 * snapshot reads the database.
 */
public final class CaptureCompleteness {
    public static final int VERSION = 2;

    /** PG14's stats collector rewrites its file at most every PGSTAT_STAT_INTERVAL (500 ms). */
    static final long READ_INTERVAL_MS = 600;

    static final long SETTLE_TIMEOUT_MS = 120_000;

    private static final Gson JSON =
            new GsonBuilder().serializeNulls().setPrettyPrinting().disableHtmlEscaping().create();

    private static final String COUNTERS =
            """
            SELECT s.schemaname,s.relname,s.indexrelname,
                   COALESCE(s.idx_scan,0),COALESCE(s.idx_tup_read,0),COALESCE(s.idx_tup_fetch,0)
              FROM pg_stat_user_indexes s
              JOIN pg_index i ON i.indexrelid = s.indexrelid
              JOIN pg_class c ON c.oid = s.relid
             WHERE c.relkind = 'r' AND c.relname NOT LIKE 'pg_%'
             ORDER BY s.schemaname,s.relname,s.indexrelname
            """;

    private static final String OTHER_SESSIONS =
            """
            SELECT count(*) FROM pg_stat_activity
             WHERE datname = current_database() AND pid <> pg_backend_pid()
               AND backend_type = 'client backend'
            """;

    private CaptureCompleteness() {}

    /** What the settling loop needs from the database, so it can be checked without one. */
    interface CounterSource {
        long otherSessions() throws SQLException;

        List<Map<String, Object>> counters() throws SQLException;

        void sleep(long millis) throws InterruptedException;

        long now();
    }

    /** Writes a new counter snapshot; an existing snapshot path is never overwritten. */
    public static Map<String, Object> snapshot(
            String jdbc, String user, Path output, String profile)
            throws SQLException, IOException, InterruptedException {
        if (Files.exists(output))
            throw new IOException("Index counter snapshot already exists: " + output);
        Path partial = output.resolveSibling(output.getFileName() + ".partial");
        if (Files.exists(partial))
            throw new IOException("Unfinished index counter snapshot exists; use a fresh output");
        Map<String, Object> result;
        try (Connection connection =
                DriverManager.getConnection(
                        jdbc, user, System.getenv().getOrDefault("PGPASSWORD", ""))) {
            connection.setAutoCommit(true);
            connection.setReadOnly(true);
            result = snapshot(connection, profile);
        }
        Files.writeString(partial, JSON.toJson(result), StandardOpenOption.CREATE_NEW);
        Files.move(partial, output, StandardCopyOption.ATOMIC_MOVE);
        return result;
    }

    static Map<String, Object> snapshot(Connection connection, String profile)
            throws SQLException, IOException, InterruptedException {
        String database = single(connection, "SELECT current_database()");
        String trackCounts = single(connection, "SHOW track_counts");
        if (!trackCounts.equals("on"))
            throw new IOException("track_counts is " + trackCounts + "; index counters do not advance");
        var source =
                new CounterSource() {
                    @Override
                    public long otherSessions() throws SQLException {
                        return Long.parseLong(single(connection, OTHER_SESSIONS));
                    }

                    @Override
                    public List<Map<String, Object>> counters() throws SQLException {
                        return read(connection);
                    }

                    @Override
                    public void sleep(long millis) throws InterruptedException {
                        Thread.sleep(millis);
                    }

                    @Override
                    public long now() {
                        return System.nanoTime() / 1_000_000;
                    }
                };
        var result = new LinkedHashMap<String, Object>();
        result.put("schema_version", VERSION);
        result.put("source", "pg_stat_user_indexes");
        result.put("profile", profile);
        result.put("database", database);
        result.put("server_version", single(connection, "SHOW server_version"));
        result.put("track_counts", trackCounts);
        result.putAll(settle(source, SETTLE_TIMEOUT_MS));
        return result;
    }

    /**
     * Waits until no other session is connected to the database, then until two reads one stats
     * interval apart agree. A closed client's backend may still be exiting, and PG14 sends its
     * counters to the collector asynchronously at exit; reading too early loses them.
     */
    static Map<String, Object> settle(CounterSource source, long timeoutMs)
            throws SQLException, IOException, InterruptedException {
        long started = source.now(), deadline = started + timeoutMs;
        long others;
        while ((others = source.otherSessions()) != 0) {
            if (source.now() >= deadline)
                throw new IOException(
                        others + " other session(s) remain connected; counters cannot settle");
            source.sleep(200);
        }
        long alone = source.now();
        List<Map<String, Object>> previous = source.counters(), current;
        int reads = 1;
        while (true) {
            source.sleep(READ_INTERVAL_MS);
            current = source.counters();
            reads++;
            if (current.equals(previous)) break;
            if (source.now() >= deadline)
                throw new IOException("Index counters did not settle within " + timeoutMs + " ms");
            previous = current;
        }
        var settled = new LinkedHashMap<String, Object>();
        settled.put("waited_for_other_sessions_ms", alone - started);
        settled.put("reads", reads);
        settled.put("read_interval_ms", READ_INTERVAL_MS);
        var result = new LinkedHashMap<String, Object>();
        result.put("settled", settled);
        result.put("indexes", current);
        return result;
    }

    /** Reads the counters of every index of a user table; idx_tup_fetch is NULL when never used. */
    private static List<Map<String, Object>> read(Connection connection) throws SQLException {
        var indexes = new ArrayList<Map<String, Object>>();
        try (Statement statement = connection.createStatement()) {
            // Each autocommit statement is its own transaction; drop any cached stats snapshot.
            statement.execute("SELECT pg_stat_clear_snapshot()");
            try (ResultSet rows = statement.executeQuery(COUNTERS)) {
                while (rows.next()) {
                    var item = new LinkedHashMap<String, Object>();
                    item.put("schemaname", rows.getString(1));
                    item.put("relname", rows.getString(2));
                    item.put("indexrelname", rows.getString(3));
                    item.put("idx_scan", rows.getLong(4));
                    item.put("idx_tup_read", rows.getLong(5));
                    item.put("idx_tup_fetch", rows.getLong(6));
                    indexes.add(item);
                }
            }
        }
        return indexes;
    }

    private static String single(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement();
                ResultSet rows = statement.executeQuery(sql)) {
            if (!rows.next()) throw new SQLException("No result for " + sql);
            return rows.getString(1);
        }
    }

    private record Counters(String schema, String table, long scan, long read, long fetch) {}

    private record Ledger(
            String table,
            long units,
            long candidates,
            long visible,
            long heapVisible,
            long descentsMin,
            long descentsMax,
            long insertAttempts) {}

    /**
     * Compares the before/after counter deltas of every index with the ledger's tallies for the
     * same index. An index missing from either side, a delta that went backwards and a delta the
     * counter semantics cannot produce from the ledger are inconsistencies.
     */
    public static Map<String, Object> compare(
            JsonObject ledger, JsonObject before, JsonObject after) {
        requireVersion(ledger, AccessLedger.VERSION, "access ledger", "rerun access-ledger");
        requireVersion(before, VERSION, "before snapshot", "retake snapshot-index-counters");
        requireVersion(after, VERSION, "after snapshot", "retake snapshot-index-counters");
        if (!Objects.equals(text(before, "database"), text(after, "database"))
                || !Objects.equals(text(before, "profile"), text(after, "profile")))
            throw new IllegalArgumentException(
                    "Counter snapshots come from different databases or profiles");
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
                if (fetchDelta != tally.heapVisible())
                    reasons.add(
                            "idx_tup_fetch delta "
                                    + fetchDelta
                                    + " != ledger heap_fetch_visible "
                                    + tally.heapVisible());
                if (scanDelta < tally.descentsMin() || scanDelta > tally.descentsMax())
                    reasons.add(
                            "idx_scan delta "
                                    + scanDelta
                                    + " outside ledger tree descents ["
                                    + tally.descentsMin()
                                    + ", "
                                    + tally.descentsMax()
                                    + "]");
            }
            var item = new LinkedHashMap<String, Object>();
            item.put("index", name);
            item.put("schemaname", to != null ? to.schema() : from == null ? null : from.schema());
            item.put("table", tally != null ? tally.table() : to != null ? to.table() : null);
            item.put("idx_scan_delta", scanDelta);
            item.put("idx_tup_read_delta", readDelta);
            item.put("idx_tup_fetch_delta", fetchDelta);
            item.put("access_units", tally == null ? null : tally.units());
            item.put("tree_descents_min", tally == null ? null : tally.descentsMin());
            item.put("tree_descents_max", tally == null ? null : tally.descentsMax());
            item.put("candidates", tally == null ? null : tally.candidates());
            item.put("visible_rows", tally == null ? null : tally.visible());
            item.put("heap_fetch_visible", tally == null ? null : tally.heapVisible());
            item.put("index_insert_attempts", tally == null ? null : tally.insertAttempts());
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
        result.put("database", text(after, "database"));
        result.put("inconsistent_indexes", inconsistent);
        result.put("indexes", reports);
        result.put("counter_semantics", semantics());
        result.put("preconditions", preconditions());
        return result;
    }

    private static void requireVersion(JsonObject value, int version, String what, String remedy) {
        long actual = number(value, "schema_version");
        if (actual != version)
            throw new IllegalArgumentException(
                    what + " schema_version " + actual + " is not " + version + "; " + remedy);
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
                            number(value, "heap_fetch_visible"),
                            number(value, "tree_descents_min"),
                            number(value, "tree_descents_max"),
                            number(value, "index_insert_attempts")));
        }
        return result;
    }

    private static List<String> semantics() {
        return List.of(
                "idx_scan counts _bt_first descents (PG14 nbtsearch.c): exactly one per pass of a"
                    + " scan without array keys, 0 to one per array element combination per pass"
                    + " with them; asserted within the ledger's tree_descents_min/max.",
                "idx_tup_read counts TIDs the access method returned (index_getnext_tid) or put"
                    + " in a bitmap (index_getbitmap); asserted equal to ledger candidates.",
                "idx_tup_fetch counts rows index_fetch_heap found visible; bitmap heap checks and"
                    + " all-visible index-only rows never reach it, so it is asserted equal to"
                    + " ledger heap_fetch_visible, not to visible_rows.");
    }

    private static List<String> preconditions() {
        return List.of(
                "Each snapshot waited until no other session was connected to the database and two"
                    + " reads one stats interval apart agreed.",
                "Only the native run lies between the two snapshots; schema checks, baseline"
                    + " export and the row-state audit are outside the window.",
                "PG14's stats collector receives counters over UDP and may drop them under load;"
                    + " a lost message shows as a delta below the ledger, never above it.");
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
