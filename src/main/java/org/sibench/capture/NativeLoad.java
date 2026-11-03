package org.sibench.capture;

import com.google.gson.GsonBuilder;

import org.sibench.schema.SourceSchema;
import org.sibench.config.ConfigValues;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;

/** Loads one disposable database through the benchmark's native client. */
final class NativeLoad {
    static final String LIBRARY = "build/services/postgresql-capture/row_capture.so";

    private NativeLoad() {}

    /** The input identity and actual row counts retained after the database is removed. */
    static Path manifest(Path artifacts) {
        return artifacts.resolve("native/load.json");
    }

    static Path library(Path root) {
        Path library = root.resolve(LIBRARY);
        if (!Files.isRegularFile(library))
            throw new IllegalStateException("Row capture library is not built: " + library);
        return library;
    }

    /** Reject a stale native build before spending time loading the benchmark. */
    static void requireDirectCapture(String jdbc, String user, Path library) throws Exception {
        try (Connection connection = DriverManager.getConnection(jdbc, user, password());
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE FUNCTION pg_temp.sibench_capture_probe(integer,bigint,bigint) RETURNS bigint AS '"
                    + library.toString().replace("'", "''") + "', 'sibench_capture_watermark' LANGUAGE C");
            try (ResultSet result = statement.executeQuery("SELECT pg_temp.sibench_capture_probe(0,0,0)")) {
                if (!result.next() || result.getLong(1) < 0) throw new IOException("Direct capture watermark is unavailable");
            }
        }
    }

    /** Native loaders create their own tables; change persistence while those tables are empty. */
    static void unloggedTables(Connection connection) throws Exception {
        try (Statement statement = connection.createStatement()) {
            statement.execute("""
                    CREATE FUNCTION public.sibench_unlogged_tables() RETURNS event_trigger
                    LANGUAGE plpgsql AS $$
                    DECLARE command record;
                    BEGIN
                      FOR command IN
                        SELECT d.object_identity FROM pg_event_trigger_ddl_commands() d
                        JOIN pg_class c ON d.classid = 'pg_class'::regclass AND d.objid = c.oid
                        WHERE d.schema_name = 'public' AND c.relkind = 'r' AND c.relpersistence = 'p'
                      LOOP
                        EXECUTE 'ALTER TABLE ' || command.object_identity || ' SET UNLOGGED';
                      END LOOP;
                    END $$
                    """);
            statement.execute("CREATE EVENT TRIGGER sibench_unlogged_tables ON ddl_command_end"
                    + " WHEN TAG IN ('CREATE TABLE') EXECUTE FUNCTION public.sibench_unlogged_tables()");
        }
    }

    /** Installs row-capture triggers and gathers planner statistics for the loaded source. */
    static void captureReady(String jdbc, String user, Path library, SourceSchema schema) throws Exception {
        long started = System.nanoTime();
        try (Connection connection = DriverManager.getConnection(jdbc, user, password());
                Statement statement = connection.createStatement()) {
            statement.execute("DROP EVENT TRIGGER sibench_unlogged_tables");
            statement.execute("DROP FUNCTION public.sibench_unlogged_tables()");
            try (ResultSet rows = statement.executeQuery(
                    "SELECT relname FROM pg_class WHERE relnamespace = 'public'::regnamespace"
                            + " AND relkind = 'r' AND relpersistence <> 'u'")) {
                if (rows.next()) throw new IllegalStateException("Native table is not unlogged: " + rows.getString(1));
            }
            statement.execute(
                    "CREATE FUNCTION public.sibench_row_capture() RETURNS trigger AS '"
                            + library.toString().replace("'", "''")
                            + "', 'sibench_row_capture' LANGUAGE C");
            statement.execute("CREATE FUNCTION public.sibench_capture_watermark(integer DEFAULT 0, bigint DEFAULT 0, bigint DEFAULT 0) RETURNS bigint AS '"
                    + library.toString().replace("'", "''")
                    + "', 'sibench_capture_watermark' LANGUAGE C");
            for (var table : schema.tables())
                statement.execute(
                        "CREATE TRIGGER sibench_row_capture AFTER INSERT OR UPDATE OR DELETE ON \""
                                + table.name()
                                + "\" FOR EACH ROW EXECUTE FUNCTION public.sibench_row_capture()");
            RunTiming.current().note("load_triggers_millis", (System.nanoTime() - started) / 1_000_000L);
            long analyzeStarted = System.nanoTime();
            statement.execute("ANALYZE");
            RunTiming.current().note("load_analyze_millis", (System.nanoTime() - analyzeStarted) / 1_000_000L);
        }
    }

    /** Loads YCSB or sysbench into this run's database with the benchmark's own client. */
    static void build(NativeCaptureConfig config) throws Exception {
        String database = config.database();
        Path manifest = manifest(config.artifacts());
        if (Files.exists(manifest)) throw new IllegalStateException("Native load already recorded: " + manifest);
        Path library = library(config.root());
        requireDirectCapture(config.adminJdbc(), config.user(), library);
        // Fail before any database exists when the native client is not the pinned build.
        if (config.benchmark().equals("ycsb")) YcsbNativeRun.libraries(config);
        else SysbenchNativeRun.binary(config);
        Path logs = Files.createDirectories(config.artifacts().resolve("native/load"));
        long started = System.nanoTime();
        try (Connection admin = DriverManager.getConnection(config.adminJdbc(), config.user(), password());
                Statement statement = admin.createStatement()) {
            statement.execute("CREATE DATABASE " + database + " LC_COLLATE 'C' LC_CTYPE 'C' TEMPLATE template0");
        }
        try (Connection connection = DriverManager.getConnection(config.jdbc(), config.user(), password())) {
            unloggedTables(connection);
        }
        long loadStarted = System.nanoTime();
        if (config.benchmark().equals("ycsb")) YcsbNativeRun.load(config, logs);
        else SysbenchNativeRun.prepare(config, logs);
        RunTiming.current().note("load_native_load_millis", (System.nanoTime() - loadStarted) / 1_000_000L);
        var schema = SourceSchema.read(config.root(), config.document());
        captureReady(config.jdbc(), config.user(), library, schema);
        var report = new LinkedHashMap<String, Object>();
        report.put("benchmark", config.benchmark());
        report.put("database", database);
        report.put("table_persistence", "unlogged");
        report.put("source_commit", string(config.document(), "source", "commit"));
        report.put("native", config.nativeConfig());
        report.put("load_seconds", (System.nanoTime() - started) / 1e9);
        Files.writeString(manifest, new GsonBuilder().setPrettyPrinting().create().toJson(report), StandardOpenOption.CREATE_NEW);
        System.out.println(config.benchmark() + " loaded database " + database);
    }

    private static String string(Map<String, Object> document, String object, String field) {
        return org.sibench.config.ConfigValues.string(org.sibench.config.ConfigValues.object(document, object), field);
    }

    private static String password() {
        return System.getenv().getOrDefault("PGPASSWORD", "");
    }
}
