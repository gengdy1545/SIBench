package org.sibench.capture;

import com.google.gson.GsonBuilder;

import org.sibench.schema.PgSchemaCheck;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;

/**
 * The stages of one native capture of HyBench, YCSB or sysbench. Stages are explicit: nothing
 * starts PG, and a stage never reuses a database, baseline or raw directory left by an earlier
 * one.
 */
public final class NativeCapture {

    private NativeCapture() {}

    /**
     * How the instrumented PG records index accesses: one summary per access ({@code summary}, the
     * default), or also every row ({@code rows}), which the access ledger checks the summaries
     * against. The script passes it to PG and the native clients as SIBENCH_CAPTURE_DETAIL.
     */
    /** Backend traces are always zstd streams; the script passes this to PG. */
    static String compression(java.util.Map<String, Object> document) {
        var capture = org.sibench.config.ConfigValues.object(document, "capture");
        if (capture.containsKey("compression"))
            org.sibench.config.ConfigValues.require(
                    org.sibench.config.ConfigValues.string(capture, "compression").equals("zstd"),
                    "capture.compression must be zstd");
        return "zstd";
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
                    "NativeCapture paths|base|prepare|check|run ROOT CONFIG [TIMING]");
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
                System.out.println("SIBENCH_BASE='" + config.cloneFrom() + "'");
                paths(config.document());
            }
            case "base" -> StillBase.build(config);
            case "prepare" -> prepare(config);
            case "check" ->
                    System.out.println(
                            PgSchemaCheck.json(
                                    PgSchemaCheck.check(
                                            config.root(),
                                            config.document(),
                                            config.jdbc(),
                                            config.user())));
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
                System.out.println("SIBENCH_BASE='" + config.cloneFrom() + "'");
                paths(config.document());
            }
            case "base" -> HybenchNativeBase.build(config);
            case "prepare" -> HybenchNativePrepare.prepare(root, config);
            case "check" ->
                    System.out.println(
                            org.sibench.schema.PgSchemaCheck.json(
                                    org.sibench.schema.PgSchemaCheck.check(
                                            root, config.document(), config.jdbc(), config.user())));
            case "run" -> {
                java.nio.file.Files.createDirectories(config.artifacts().resolve("native"));
                HybenchNativeRun.run(root, Path.of(profile));
            }
            default -> throw new IllegalArgumentException("Unknown stage: " + stage);
        }
    }

    /** This capture's clone of the still database becomes the profile's database. */
    static void prepare(NativeCaptureConfig config) throws Exception {
        StillBase.library(config.root());
        StillBase.attach(
                config.root(), config.adminJdbc(), config.user(), config.cloneFrom(), config.database(), config.loadConfig());
    }

    static void run(NativeCaptureConfig config) throws Exception {
        Path logs = config.artifacts().resolve("native");
        boolean ycsb = config.benchmark().equals("ycsb");
        try (CaptureRun run = CaptureRun.beginExternal(config.raw(), config.benchmark())) {
            try {
                CaptureRun.requireQueryIdentity(config.jdbc(), config.user());
                var settings = PgSettings.read(config.jdbc(), config.user());
                run.build(PgSettings.identity(CaptureBuild.identity(config.root()), settings));
                run.postgresSettings(settings.all());
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
