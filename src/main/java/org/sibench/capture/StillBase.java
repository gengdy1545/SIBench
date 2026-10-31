package org.sibench.capture;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.sibench.schema.SourceSchema;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The still database a capture starts from, kept in a PostgreSQL cluster of its own that is
 * stopped between captures ({@code build/services/postgresql-still/<clone_from>}). It is built once:
 * the benchmark's own load, then everything a capture needs before it runs (HyBench's upstream
 * secondary indexes, the row-capture triggers, {@code VACUUM (ANALYZE)}). Each capture runs on a
 * reflink copy of the stopped cluster, which takes no time and shares every block the capture
 * does not change, and is deleted afterwards; the still cluster itself is never written again.
 *
 * <p>Statistics are therefore taken once per still database, so every capture of it plans from the
 * same statistics. A still database of a native client source records the parameters it was
 * loaded with, and a profile whose load parameters differ cannot clone it.
 */
final class StillBase {
    static final String DIRECTORY = "build/services/postgresql-still";
    static final String LIBRARY = "build/services/postgresql-capture/row_capture.so";

    private StillBase() {}

    static Path directory(Path root, String base) {
        return root.resolve(DIRECTORY).resolve(base);
    }

    /** What the still database holds and how it was produced. */
    static Path manifest(Path root, String base) {
        return directory(root, base).resolve("base.json");
    }

    static Path library(Path root) {
        Path library = root.resolve(LIBRARY);
        if (!Files.isRegularFile(library))
            throw new IllegalStateException("Row capture library is not built: " + library);
        return library;
    }

    /**
     * Installs the row-capture triggers on every source table and sets what a loaded benchmark
     * database has by the time it is measured: hint bits, a visibility map and planner statistics.
     * Autovacuum is off in capture clusters; VACUUM moves no live tuple and changes no value.
     */
    static void captureReady(String jdbc, String user, Path library, SourceSchema schema) throws Exception {
        try (Connection connection = DriverManager.getConnection(jdbc, user, password());
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "CREATE FUNCTION public.sibench_row_capture() RETURNS trigger AS '"
                            + library.toString().replace("'", "''")
                            + "', 'sibench_row_capture' LANGUAGE C");
            for (var table : schema.tables())
                statement.execute(
                        "CREATE TRIGGER sibench_row_capture AFTER INSERT OR UPDATE OR DELETE ON \""
                                + table.name()
                                + "\" FOR EACH ROW EXECUTE FUNCTION public.sibench_row_capture()");
            statement.execute("VACUUM (ANALYZE)");
        }
    }

    /** Rows of every source table, as the still database holds them. */
    static Map<String, Long> rows(String jdbc, String user, SourceSchema schema) throws Exception {
        var result = new LinkedHashMap<String, Long>();
        try (Connection connection = DriverManager.getConnection(jdbc, user, password());
                Statement statement = connection.createStatement()) {
            for (var table : schema.tables())
                try (ResultSet rows = statement.executeQuery("SELECT count(*) FROM \"" + table.name() + "\"")) {
                    rows.next();
                    result.put(table.name(), rows.getLong(1));
                }
        }
        return result;
    }

    /** Loads a YCSB or sysbench still database with the benchmark's own client, once. */
    static void build(NativeCaptureConfig config) throws Exception {
        String base = config.cloneFrom();
        Path manifest = manifest(config.root(), base);
        if (Files.exists(manifest)) throw new IllegalStateException("Still database already built: " + manifest);
        Path library = library(config.root());
        // Fail before any database exists when the native client is not the pinned build.
        if (config.benchmark().equals("ycsb")) YcsbNativeRun.libraries(config);
        else SysbenchNativeRun.binary(config);
        var still = config.withDatabase(base);
        Path logs = Files.createDirectories(directory(config.root(), base).resolve("load"));
        long started = System.nanoTime();
        try (Connection admin = DriverManager.getConnection(config.adminJdbc(), config.user(), password());
                Statement statement = admin.createStatement()) {
            statement.execute("CREATE DATABASE " + base + " LC_COLLATE 'C' LC_CTYPE 'C' TEMPLATE template0");
        }
        if (config.benchmark().equals("ycsb")) YcsbNativeRun.load(still, logs);
        else SysbenchNativeRun.prepare(still, logs);
        var schema = SourceSchema.read(config.root(), config.document());
        captureReady(still.jdbc(), config.user(), library, schema);
        var report = new LinkedHashMap<String, Object>();
        report.put("benchmark", config.benchmark());
        report.put("database", base);
        report.put("source_commit", string(config.document(), "source", "commit"));
        report.put("load", config.loadConfig());
        report.put("loaded_rows", rows(still.jdbc(), config.user(), schema));
        report.put("load_seconds", (System.nanoTime() - started) / 1e9);
        Files.writeString(manifest, new GsonBuilder().setPrettyPrinting().create().toJson(report), StandardOpenOption.CREATE_NEW);
        System.out.println(config.benchmark() + " still database " + base + ": " + report.get("loaded_rows"));
    }

    /**
     * Turns this capture's clone of the still database into the capture database: it is renamed to
     * the profile's database, after checking that the still database is the one the profile names
     * and, for a native client source, that it was loaded with the profile's parameters.
     */
    static JsonObject attach(Path root, String adminJdbc, String user, String base, String database, Map<String, Object> load)
            throws Exception {
        Path manifest = manifest(root, base);
        if (!Files.exists(manifest)) throw new IllegalStateException("Still database has no manifest: " + manifest);
        JsonObject value = JsonParser.parseString(Files.readString(manifest)).getAsJsonObject();
        if (!value.get("database").getAsString().equals(base))
            throw new IllegalStateException("Still manifest names another database: " + manifest);
        if (load != null && !new Gson().toJsonTree(new java.util.TreeMap<>(load)).equals(value.get("load")))
            throw new IllegalStateException(
                    "The profile's load parameters differ from those the still database " + base
                            + " was loaded with; name another postgres.clone_from to build a new one");
        try (Connection admin = DriverManager.getConnection(adminJdbc, user, password());
                Statement statement = admin.createStatement()) {
            statement.execute("ALTER DATABASE " + base + " RENAME TO " + database);
        }
        return value;
    }

    private static String string(Map<String, Object> document, String object, String field) {
        return org.sibench.config.ConfigValues.string(org.sibench.config.ConfigValues.object(document, object), field);
    }

    private static String password() {
        return System.getenv().getOrDefault("PGPASSWORD", "");
    }
}
