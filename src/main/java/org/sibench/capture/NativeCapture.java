package org.sibench.capture;

import com.google.gson.GsonBuilder;

import org.sibench.schema.PgSchemaCheck;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;

/**
 * Stages of one native capture of HyBench, YCSB or sysbench. The shell manages PostgreSQL;
 * load, export and run share that capture's fresh database. Inputs from earlier captures are
 * not reused.
 */
public final class NativeCapture {

    private NativeCapture() {}

    /** Backend traces are always zstd streams; the script passes this to PG. */
    static String compression(java.util.Map<String, Object> document) {
        var capture = org.sibench.config.ConfigValues.object(document, "capture");
        if (capture.containsKey("compression"))
            org.sibench.config.ConfigValues.require(
                    org.sibench.config.ConfigValues.string(capture, "compression").equals("zstd"),
                    "capture.compression must be zstd");
        return "zstd";
    }

    /** One summary per access, or also every row to cross-check the summaries. */
    static String detail(java.util.Map<String, Object> document) {
        var capture = org.sibench.config.ConfigValues.object(document, "capture");
        if (!capture.containsKey("detail")) return "summary";
        String detail = org.sibench.config.ConfigValues.string(capture, "detail");
        org.sibench.config.ConfigValues.require(
                detail.equals("summary") || detail.equals("rows"),
                "capture.detail must be summary or rows");
        return detail;
    }

    /** The paths and capture settings the script reads, as shell assignments. */
    private static void paths(java.util.Map<String, Object> document, Path root, Path artifacts) throws Exception {
        System.out.println("SIBENCH_CAPTURE_DETAIL=" + detail(document));
        System.out.println("SIBENCH_CAPTURE_COMPRESS=" + compression(document));
        System.out.println("SIBENCH_TRACE_OUTPUT=" + TraceOutput.from(document).setting());
        var runtime = org.sibench.config.SibenchConfig.read(root.resolve("config/sibench.yaml"));
        var budget = new GenerationBudget(org.sibench.config.ConfigValues.object(runtime, "generation"),
                DirectCapture.budgetPath(artifacts));
        NativeProcess.assignment("SIBENCH_BUDGET_STOP", budget.enabled() ? DirectCapture.budgetPath(artifacts) : "");
        var capture = org.sibench.config.ConfigValues.object(document, "capture");
        org.sibench.config.ConfigValues.require(
                !capture.containsKey("verification") && !capture.containsKey("certification"),
                "Capture certification is unsupported; run independent audits explicitly");
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3 && args.length != 4)
            throw new IllegalArgumentException(
                    "NativeCapture paths|load|export|run ROOT CONFIG [TIMING]");
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
                NativeProcess.assignment("SIBENCH_RAW", config.raw());
                System.out.println("SIBENCH_PORT=" + config.port());
                NativeProcess.assignment("SIBENCH_DATABASE", config.database());
                NativeProcess.assignment("SIBENCH_ARTIFACTS", config.artifacts());
                paths(config.document(), root, config.artifacts());
            }
            case "load" -> NativeLoad.build(config);
            case "export" -> PgBaselineExport.export(config.jdbc(), config.layout().output(), config.layout());
            case "run" -> generate(root, Path.of(args[2]), config.jdbc(), config.user(), () -> run(config));
            default -> throw new IllegalArgumentException("Unknown stage: " + args[0]);
        }
    }

    /** HyBench stages follow the same disposable-database contract. */
    private static void hybench(String stage, Path root, String profile, CaptureConfig config)
            throws Exception {
        switch (stage) {
            case "paths" -> {
                NativeProcess.assignment("SIBENCH_RAW", config.raw());
                System.out.println(
                        "SIBENCH_PORT="
                                + org.sibench.config.ConfigValues.positive(
                                        org.sibench.config.ConfigValues.object(
                                                config.document(), "postgres"),
                                        "port"));
                NativeProcess.assignment("SIBENCH_DATABASE", config.database());
                NativeProcess.assignment("SIBENCH_ARTIFACTS", config.artifacts());
                NativeProcess.assignment("SIBENCH_HYBENCH", root.resolve(NativeBuildCheck.HYBENCH_DIRECTORY));
                NativeProcess.assignment("SIBENCH_WORKDIR", config.workDir());
                paths(config.document(), root, config.artifacts());
            }
            case "load" -> HybenchNativeLoad.build(config);
            case "export" -> PgBaselineExport.export(config.jdbc(), config.layout().output(), config.layout());
            case "run" -> {
                HybenchNativeLoad.requireRuntimeInputs(config.workDir().resolve("Data_" + config.scaleFactor()),
                        HybenchNativeLoad.manifest(config));
                java.nio.file.Files.createDirectories(config.artifacts().resolve("native"));
                generate(root, Path.of(profile), config.jdbc(), config.user(),
                        () -> HybenchNativeRun.run(root, Path.of(profile)));
            }
            default -> throw new IllegalArgumentException("Unknown stage: " + stage);
        }
    }

    @FunctionalInterface private interface Execution { void run() throws Exception; }

    /** Source execution and request generation share one process; replay remains independent. */
    private static void generate(Path root, Path profile, String jdbc, String user, Execution execution)
            throws Exception {
        var capture = CaptureProfile.read(root, profile);
        try (var direct = DirectCapture.begin(capture, jdbc, user)) {
            execution.run();
            direct.endSources();
            CaptureCompleteness.snapshot(jdbc, user,
                    capture.artifacts().resolve("index-counters-after.json"), profile.toString());
            var report = direct.finish();
            Files.writeString(capture.artifacts().resolve("trace-summary.json"),
                    new com.google.gson.GsonBuilder().setPrettyPrinting().create().toJson(report),
                    java.nio.file.StandardOpenOption.CREATE_NEW);
        }
    }

    static void run(NativeCaptureConfig config) throws Exception {
        Path logs = config.artifacts().resolve("native");
        boolean ycsb = config.benchmark().equals("ycsb");
        try (CaptureRun run = CaptureRun.beginExternal(config.raw(), config.benchmark())) {
            try {
                CaptureRun.requireQueryIdentity(config.jdbc(), config.user());
                var settings = PgSettings.read(config.jdbc(), config.user());
                run.schemaPreflight(
                        PgSchemaCheck.check(
                                config.root(), config.document(), config.jdbc(), config.user()));
                run.build(PgSettings.identity(CaptureBuild.identity(config.root(), NativeBuildCheck.verify(config.root(),
                        config.benchmark(), config.source(), config.root().resolve(org.sibench.config.ConfigValues.string(
                                config.nativeConfig(), "build_directory")), org.sibench.config.ConfigValues.string(
                                org.sibench.config.ConfigValues.object(config.document(), "source"), "commit"))), settings));
                run.postgresSettings(settings.all());
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
