package org.sibench.capture;

import org.sibench.schema.PgSchemaCheck;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

/** Runs native HyBench stages, or the explicit TP diagnostic, through recording JDBC. */
public final class HybenchNativeRun {
    private HybenchNativeRun() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2) throw new IllegalArgumentException("HybenchNativeRun ROOT CONFIG");
        CaptureConfig config = CaptureConfig.read(Path.of(args[0]), Path.of(args[1]));
        Path properties = config.artifacts().resolve("native/capture-resolved.props");
        args =
                new String[] {
                    properties.toString(),
                    config.source().resolve("conf/stmt_postgres.toml").toString(),
                    Integer.toString(config.seconds()),
                    config.raw().toString()
                };
        boolean htap = config.mode().equals("runhtap");
        try (CaptureRun run = CaptureRun.begin(Path.of(args[3]), htap)) {
            try {
                CaptureRun.requireQueryIdentity(config.jdbc(), config.user());
                run.schemaPreflight(CapturePoc.checkServer(config));
                Files.createDirectories(Path.of(args[0]).getParent());
                try (var output =
                        Files.newOutputStream(
                                Path.of(args[0]), java.nio.file.StandardOpenOption.CREATE_NEW)) {
                    config.nativeProperties().store(output, "Resolved SF1 capture configuration");
                }
                long[] counts = htap ? executeHtap(args, run) : execute(args);
                run.complete(counts);
                System.out.println(
                        htap
                                ? "Native runHTAP capture complete: AP -> TP -> XP"
                                : "Native transaction counts: "
                                        + java.util.Arrays.toString(counts));
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

    private static long[] executeHtap(String[] args, CaptureRun run) throws Exception {
        System.setProperty(
                "sibench.capture.directory", Path.of(args[3]).toAbsolutePath().toString());
        Properties properties = new Properties();
        try (InputStream input = Files.newInputStream(Path.of(args[0]))) {
            properties.load(input);
        }
        Class<?> loader = Class.forName("com.hybench.ConfigLoader");
        loader.getField("prop").set(null, properties);
        loader.getField("confFile").set(null, Path.of(args[0]).toAbsolutePath().toString());
        Class.forName(CaptureDriver.class.getName());
        Class<?> hybench = Class.forName("com.hybench.HyBench");
        Object benchmark = hybench.getConstructor().newInstance();
        Object reader =
                Class.forName("com.hybench.workload.SqlReader")
                        .getConstructor(String.class)
                        .newInstance(args[1]);
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
        return new long[19];
    }

    private static long[] execute(String[] args) throws Exception {
        long seconds = Long.parseLong(args[2]);
        if (seconds <= 0) throw new IllegalArgumentException("Capture duration must be positive");
        long duration = Math.multiplyExact(seconds, 1_000_000_000L);
        Properties properties = new Properties();
        try (InputStream input = Files.newInputStream(Path.of(args[0]))) {
            properties.load(input);
        }
        System.setProperty(
                "sibench.capture.directory", Path.of(args[3]).toAbsolutePath().toString());
        Class<?> loader = Class.forName("com.hybench.ConfigLoader");
        loader.getField("prop").set(null, properties);
        loader.getField("confFile").set(null, Path.of(args[0]).toAbsolutePath().toString());
        Class<?> clientClass = Class.forName("com.hybench.workload.Client");
        Class<?> tpClass = Class.forName("com.hybench.workload.TPClient");
        Object client = tpClass.getConstructor().newInstance();
        try {
            Field prop = clientClass.getDeclaredField("prop");
            prop.setAccessible(true);
            prop.set(client, properties);
            clientClass.getMethod("setTaskType", int.class).invoke(client, 1);
            clientClass.getMethod("doInit_wrapper", String.class).invoke(client, "TPClient");
            Field histogram = clientClass.getDeclaredField("hist");
            histogram.setAccessible(true);
            histogram.set(
                    client,
                    Class.forName("com.hybench.stats.Histogram").getConstructor().newInstance());
            Object reader =
                    Class.forName("com.hybench.workload.SqlReader")
                            .getConstructor(String.class)
                            .newInstance(args[1]);
            Object statements = reader.getClass().getMethod("loader").invoke(reader);
            clientClass
                    .getMethod("setSqls", Class.forName("com.hybench.workload.Sqlstmts"))
                    .invoke(client, statements);
            Class.forName("org.sibench.capture.CaptureDriver");
            long start = System.nanoTime();
            long[] counts = new long[19];
            // Preserve the native random selector, transaction methods and result-dependent
            // branches.
            Field randomField = clientClass.getDeclaredField("rg");
            randomField.setAccessible(true);
            Object random = randomField.get(client);
            var next = random.getClass().getMethod("getRandomint", int.class);
            try (Connection connection =
                    DriverManager.getConnection(
                            properties.getProperty("url"),
                            properties.getProperty("username"),
                            properties.getProperty("password", ""))) {
                connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                do {
                    int choice = (int) next.invoke(random, 100), sum = 0, transaction = 0;
                    for (int t = 1; t <= 18; t++) {
                        sum += Integer.parseInt(properties.getProperty("tp" + t + "_percent", "0"));
                        if (choice < sum) {
                            transaction = t;
                            break;
                        }
                    }
                    if (transaction == 0)
                        throw new IllegalArgumentException(
                                "Transaction percentages must sum to 100");
                    Object result;
                    try {
                        result =
                                tpClass.getMethod("execTxn" + transaction, Connection.class)
                                        .invoke(client, connection);
                    } catch (java.lang.reflect.InvocationTargetException error) {
                        CaptureRun.checkFailure();
                        if (error.getCause() instanceof Exception cause) throw cause;
                        if (error.getCause() instanceof Error cause) throw cause;
                        throw error;
                    }
                    CaptureRun.checkFailure();
                    if (!(boolean) result.getClass().getMethod("isSucceed").invoke(result))
                        throw new IllegalStateException(
                                "Native transaction failed: " + transaction);
                    counts[transaction]++;
                } while (System.nanoTime() - start < duration);
            }
            CaptureRun.checkFailure();
            return counts;
        } finally {
            Field executor = clientClass.getDeclaredField("es");
            executor.setAccessible(true);
            Object service = executor.get(client);
            if (service != null) ((java.util.concurrent.ExecutorService) service).shutdownNow();
        }
    }
}
