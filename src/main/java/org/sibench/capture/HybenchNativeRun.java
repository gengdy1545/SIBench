package org.sibench.capture;

import org.sibench.schema.PgSchemaCheck;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/** Runs native HyBench runHTAP (AP, TP, then concurrent AT/IQ) through recording JDBC. */
public final class HybenchNativeRun {
    private HybenchNativeRun() {}

    static void run(Path root, Path profile) throws Exception {
        CaptureConfig config = CaptureConfig.read(root, profile);
        Path properties = config.artifacts().resolve("native/capture-resolved.props");
        try (CaptureRun run = CaptureRun.begin(config.raw())) {
            try {
                CaptureRun.requireQueryIdentity(config.jdbc(), config.user());
                run.build(CaptureBuild.identity(config.root()));
                run.schemaPreflight(
                        PgSchemaCheck.check(
                                config.root(), config.document(), config.jdbc(), config.user()));
                Files.createDirectories(properties.getParent());
                try (var output =
                        Files.newOutputStream(
                                properties, java.nio.file.StandardOpenOption.CREATE_NEW)) {
                    config.nativeProperties().store(output, "Resolved " + config.scaleFactor() + " capture configuration");
                }
                execute(
                        properties,
                        config.source().resolve("conf/stmt_postgres.toml"),
                        config.raw(),
                        run);
                run.complete();
                System.out.println("Native runHTAP capture complete: AP -> TP -> XP");
            } catch (Exception | Error error) {
                if (error instanceof PgSchemaCheck.Mismatch mismatch) {
                    try {
                        run.schemaPreflight(mismatch.report());
                    } catch (java.io.IOException logging) {
                        error.addSuppressed(logging);
                    }
                }
                run.fail(error, java.util.Map.of("operation", "native_run"));
                throw error;
            }
        }
    }

    private static void execute(Path resolved, Path statements, Path raw, CaptureRun run)
            throws Exception {
        System.setProperty("sibench.capture.directory", raw.toAbsolutePath().toString());
        Properties properties = new Properties();
        try (InputStream input = Files.newInputStream(resolved)) {
            properties.load(input);
        }
        Class<?> loader = Class.forName("com.hybench.ConfigLoader");
        loader.getField("prop").set(null, properties);
        loader.getField("confFile").set(null, resolved.toAbsolutePath().toString());
        Class.forName(CaptureDriver.class.getName());
        Class<?> hybench = Class.forName("com.hybench.HyBench");
        Object benchmark = hybench.getConstructor().newInstance();
        Object reader =
                Class.forName("com.hybench.workload.SqlReader")
                        .getConstructor(String.class)
                        .newInstance(statements.toString());
        Object sql = reader.getClass().getMethod("loader").invoke(reader);
        var setSqls =
                hybench.getDeclaredMethod(
                        "setSqls", Class.forName("com.hybench.workload.Sqlstmts"));
        setSqls.setAccessible(true);
        setSqls.invoke(benchmark, sql);
        Field verbose = hybench.getDeclaredField("verbose");
        verbose.setAccessible(true);
        verbose.set(benchmark, false);
        Object result = hybench.getMethod("getRes").invoke(benchmark);
        Object hist = result.getClass().getMethod("getHist").invoke(result);
        for (String phase : new String[] {"ap", "tp", "xp"}) {
            run.phase(phase, null);
            long started = System.nanoTime();
            try {
                if (phase.equals("xp")) hybench.getMethod("runXP", int.class).invoke(benchmark, 0);
                else hybench.getMethod(phase.equals("ap") ? "runAP" : "runTP").invoke(benchmark);
            } catch (java.lang.reflect.InvocationTargetException error) {
                if (error.getCause() instanceof Exception cause) throw cause;
                throw error;
            }
            run.drainSessions();
            CaptureRun.checkFailure();
            var summary = new java.util.LinkedHashMap<String, Object>();
            summary.put("elapsed_seconds", (System.nanoTime() - started) / 1e9);
            String method =
                    phase.equals("ap")
                            ? "getAPItem"
                            : phase.equals("tp") ? "getTPItem" : "getXPIQItem";
            int size = phase.equals("ap") ? 13 : phase.equals("tp") ? 18 : 6;
            var counts = new java.util.LinkedHashMap<String, Long>();
            for (int i = 0; i < size; i++) {
                Object item = hist.getClass().getMethod(method, int.class).invoke(hist, i);
                counts.put(
                        (phase.equals("xp") ? "iq" : phase) + (i + 1),
                        ((Number) item.getClass().getMethod("getN").invoke(item)).longValue());
            }
            summary.put("method_counts", counts);
            if (phase.equals("xp")) {
                var atCounts = new java.util.LinkedHashMap<String, Long>();
                for (int i = 0; i < 6; i++) {
                    Object item =
                            hist.getClass().getMethod("getXPATItem", int.class).invoke(hist, i);
                    atCounts.put(
                            "at" + (i + 1),
                            ((Number) item.getClass().getMethod("getN").invoke(item)).longValue());
                }
                summary.put("transaction_method_counts", atCounts);
            }
            run.phase(phase, summary);
        }
    }
}
