package org.sibench.capture;

import com.google.gson.GsonBuilder;

import org.sibench.schema.PgSchemaCheck;
import org.sibench.schema.SourceSchema;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.LinkedHashMap;

/**
 * Small native captures of YCSB and sysbench. Stages are explicit: nothing starts PG, and a stage
 * never reuses a database, baseline or raw directory left by an earlier one.
 */
public final class NativeCapture {
    private static final String LIBRARY = "build/services/postgresql-capture/row_capture.so";

    private NativeCapture() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 3)
            throw new IllegalArgumentException(
                    "NativeCapture paths|prepare|check|export|run ROOT CONFIG");
        var config = NativeCaptureConfig.read(Path.of(args[1]), Path.of(args[2]));
        switch (args[0]) {
            case "paths" -> {
                System.out.println("SIBENCH_RAW='" + config.raw() + "'");
                System.out.println("SIBENCH_PORT=" + config.port());
                System.out.println("SIBENCH_DATABASE='" + config.database() + "'");
                System.out.println("SIBENCH_ARTIFACTS='" + config.artifacts() + "'");
            }
            case "prepare" -> prepare(config);
            case "check" ->
                    System.out.println(
                            PgSchemaCheck.json(
                                    PgSchemaCheck.check(
                                            config.root(),
                                            config.document(),
                                            config.jdbc(),
                                            config.user())));
            case "export" -> {
                PgBaselineExport.export(config.jdbc(), config.layout().output(), config.layout());
                PgBaselineVerify.verify(
                        config.jdbc(),
                        config.user(),
                        SourceSchema.read(config.root(), config.document()),
                        config.layout().output());
            }
            case "run" -> run(config);
            default -> throw new IllegalArgumentException("Unknown stage: " + args[0]);
        }
    }

    /** New database, native schema and data, row-capture triggers, then statistics. */
    static void prepare(NativeCaptureConfig config) throws Exception {
        Path library = config.root().resolve(LIBRARY);
        if (!Files.isRegularFile(library))
            throw new IllegalStateException("Row capture library is not built: " + library);
        Path logs = config.artifacts().resolve("native");
        String password = System.getenv().getOrDefault("PGPASSWORD", "");
        // Fail before any database exists when the native client is not the pinned build.
        if (config.benchmark().equals("ycsb")) YcsbNativeRun.libraries(config);
        else SysbenchNativeRun.binary(config);
        try (Connection admin = DriverManager.getConnection(config.adminJdbc(), config.user(), password);
                Statement statement = admin.createStatement()) {
            statement.execute(
                    "CREATE DATABASE "
                            + config.database()
                            + " LC_COLLATE 'C' LC_CTYPE 'C' TEMPLATE template0");
        }
        if (config.benchmark().equals("ycsb")) YcsbNativeRun.load(config, logs);
        else SysbenchNativeRun.prepare(config, logs);
        var schema = SourceSchema.read(config.root(), config.document());
        try (Connection connection = DriverManager.getConnection(config.jdbc(), config.user(), password);
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
            // Autovacuum is off in the capture cluster. A loaded benchmark database normally has
            // hint bits, a visibility map and statistics by the time it is measured; VACUUM does
            // not move live tuples or change any value.
            statement.execute("VACUUM (ANALYZE)");
        }
    }

    static void run(NativeCaptureConfig config) throws Exception {
        Path logs = config.artifacts().resolve("native");
        boolean ycsb = config.benchmark().equals("ycsb");
        try (CaptureRun run = CaptureRun.beginExternal(config.raw(), config.benchmark())) {
            try {
                CaptureRun.requireQueryIdentity(config.jdbc(), config.user());
                run.schemaPreflight(
                        PgSchemaCheck.check(
                                config.root(), config.document(), config.jdbc(), config.user()));
                var summary = ycsb ? YcsbNativeRun.run(config, logs) : SysbenchNativeRun.run(config, logs);
                run.completeExternal(ycsb, summary);
                System.out.println(
                        new GsonBuilder().setPrettyPrinting().create().toJson(new LinkedHashMap<>(summary)));
            } catch (Exception | Error error) {
                run.fail(error, java.util.Map.of("operation", "native_run"));
                throw error;
            }
        }
    }
}
