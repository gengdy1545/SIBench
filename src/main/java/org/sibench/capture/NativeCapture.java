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
 * The stages of one native capture of HyBench, YCSB or sysbench. Stages are explicit: nothing
 * starts PG, and a stage never reuses a database, baseline or raw directory left by an earlier
 * one.
 */
public final class NativeCapture {
    private static final String LIBRARY = CaptureBuild.ROW_CAPTURE;

    private NativeCapture() {}

    /**
     * How the instrumented PG records index accesses: one summary per access ({@code summary}, the
     * default), or also every row ({@code rows}), which the access ledger checks the summaries
     * against. The script passes it to PG and the native clients as SIBENCH_CAPTURE_DETAIL.
     */
    /**
     * How backend traces are encoded: {@code zstd} (the default) writes each as one zstd stream,
     * {@code none} as plain JSON lines. The script passes it to PG as SIBENCH_CAPTURE_COMPRESS.
     */
    static String compression(java.util.Map<String, Object> document) {
        var capture = org.sibench.config.ConfigValues.object(document, "capture");
        if (!capture.containsKey("compression")) return "zstd";
        String compression = org.sibench.config.ConfigValues.string(capture, "compression");
        org.sibench.config.ConfigValues.require(
                compression.equals("zstd") || compression.equals("none"),
                "capture.compression must be zstd or none");
        return compression;
    }

    static String detail(java.util.Map<String, Object> document) {
        var capture = org.sibench.config.ConfigValues.object(document, "capture");
        if (!capture.containsKey("detail")) return "summary";
        String detail = org.sibench.config.ConfigValues.string(capture, "detail");
        org.sibench.config.ConfigValues.require(
                detail.equals("summary") || detail.equals("rows"),
                "capture.detail must be summary or rows");
        return detail;
    }

    /**
     * How a capture's row changes are vouched for: {@code full} (the default) runs the row-state
     * audit and certifies the capture; {@code certified} relies on {@code capture.certification},
     * the certificate of a full capture of the same build, see {@link Certification}.
     */
    static String verification(java.util.Map<String, Object> document) {
        var capture = org.sibench.config.ConfigValues.object(document, "capture");
        String verification =
                capture.containsKey("verification")
                        ? org.sibench.config.ConfigValues.string(capture, "verification")
                        : "full";
        org.sibench.config.ConfigValues.require(
                verification.equals("full") || verification.equals("certified"),
                "capture.verification must be full or certified");
        org.sibench.config.ConfigValues.require(
                verification.equals("certified") == capture.containsKey("certification"),
                "capture.certification is required by, and only by, capture.verification: certified");
        return verification;
    }

    /** The paths and capture settings the script reads, as shell assignments. */
    private static void paths(java.util.Map<String, Object> document) {
        System.out.println("SIBENCH_CAPTURE_DETAIL=" + detail(document));
        System.out.println("SIBENCH_CAPTURE_COMPRESS=" + compression(document));
        System.out.println("SIBENCH_VERIFICATION=" + verification(document));
        var capture = org.sibench.config.ConfigValues.object(document, "capture");
        if (capture.containsKey("certification"))
            System.out.println(
                    "SIBENCH_CERTIFICATION='" + org.sibench.config.ConfigValues.string(capture, "certification") + "'");
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3 && args.length != 4)
            throw new IllegalArgumentException(
                    "NativeCapture paths|base|prepare|check|export|run ROOT CONFIG [TIMING]");
        var timing = RunTiming.open(args.length == 4 ? Path.of(args[3]).toAbsolutePath() : null);
        try {
            timing.phase(args[0]);
            stage(args, timing);
        } finally {
            timing.close();
        }
    }

    private static void stage(String[] args, RunTiming timing) throws Exception {
        Path root = Path.of(args[1]);
        String benchmark =
                org.sibench.config.ConfigValues.string(
                        org.sibench.config.ConfigValues.parse(
                                java.nio.file.Files.readString(root.resolve(args[2]))),
                        "benchmark");
        if (benchmark.equals("hybench")) {
            var config = CaptureConfig.read(root, Path.of(args[2]));
            timing.note("benchmark", "hybench")
                    .note("database", config.database())
                    .note("capture_detail", detail(config.document()))
                    .note("capture_compression", compression(config.document()));
            hybench(args[0], root, args[2], config);
            return;
        }
        var config = NativeCaptureConfig.read(root, Path.of(args[2]));
        timing.note("benchmark", config.benchmark())
                .note("database", config.database())
                .note("capture_detail", detail(config.document()))
                .note("capture_compression", compression(config.document()));
        switch (args[0]) {
            case "paths" -> {
                System.out.println("SIBENCH_RAW='" + config.raw() + "'");
                System.out.println("SIBENCH_PORT=" + config.port());
                System.out.println("SIBENCH_DATABASE='" + config.database() + "'");
                System.out.println("SIBENCH_ARTIFACTS='" + config.artifacts() + "'");
                paths(config.document());
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

    /** HyBench stages: the same contract, against the cloned still database. */
    private static void hybench(String stage, Path root, String profile, CaptureConfig config)
            throws Exception {
        switch (stage) {
            case "paths" -> {
                System.out.println("SIBENCH_RAW='" + config.raw() + "'");
                System.out.println(
                        "SIBENCH_PORT="
                                + org.sibench.config.ConfigValues.positive(
                                        org.sibench.config.ConfigValues.object(
                                                config.document(), "postgres"),
                                        "port"));
                System.out.println("SIBENCH_DATABASE='" + config.database() + "'");
                System.out.println("SIBENCH_ARTIFACTS='" + config.artifacts() + "'");
                System.out.println("SIBENCH_WORKDIR='" + config.workDir() + "'");
                paths(config.document());
            }
            case "base" -> HybenchNativeBase.build(config);
            case "prepare" -> HybenchNativePrepare.prepare(root, config);
            case "check" ->
                    System.out.println(
                            org.sibench.schema.PgSchemaCheck.json(
                                    org.sibench.schema.PgSchemaCheck.check(
                                            root, config.document(), config.jdbc(), config.user())));
            case "export" -> {
                org.sibench.capture.PgBaselineExport.export(
                        config.jdbc(), config.layout().output(), config.layout());
                org.sibench.capture.PgBaselineVerify.verify(
                        config.jdbc(),
                        config.user(),
                        org.sibench.schema.SourceSchema.hybench(config.source()),
                        config.layout().output());
            }
            case "run" -> {
                java.nio.file.Files.createDirectories(config.artifacts().resolve("native"));
                HybenchNativeRun.run(root, Path.of(profile));
            }
            default -> throw new IllegalArgumentException("Unknown stage: " + stage);
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
                run.build(CaptureBuild.identity(config.root()));
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
