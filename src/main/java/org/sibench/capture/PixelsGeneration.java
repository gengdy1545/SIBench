package org.sibench.capture;

import static org.sibench.config.ConfigValues.*;

import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.sibench.config.Digests;
import org.sibench.config.Engine;
import org.sibench.config.SibenchConfig;
import org.sibench.generation.PostgresDatabase;
import org.sibench.schema.SourceSchema;

/**
 * HyBench on an uninstrumented source PostgreSQL; pixels-sink applies its logical decoding stream
 * to Pixels Retina, and the Retina tracer's primary-index calls become the Trace. The same CSVs
 * load both databases. Each workload window ends with a marker transaction whose arrival in Pixels
 * proves every earlier source commit was applied and projected.
 */
final class PixelsGeneration {
    private static final String MARKER = "sibench_marker", SLOT = "sibench";
    private static final Duration CONNECT = Duration.ofMinutes(5);

    private PixelsGeneration() {}

    static Map<String, Object> generate(Generation.Plan plan, Generation.Input input) throws Exception {
        if (!input.selection().benchmark().equals("hybench") || Generation.secondaryIndexes(input.document()))
            throw new IOException("Pixels CDC generation supports HyBench primary indexes only");
        long started = System.nanoTime();
        var timings = new LinkedHashMap<String, Double>();
        var build = plan.engines().get(Engine.PIXELS);
        var settings = build.settings();
        Path output = input.output();
        Files.createDirectories(output.getParent());
        Files.createDirectory(output);
        String traceToken = UUID.randomUUID().toString(), reportToken = UUID.randomUUID().toString();
        Process sink = null;
        var sourceSettings = new LinkedHashMap<>(SibenchConfig.postgresSettings(object(object(settings, "source"), "settings")));
        sourceSettings.putAll(Map.of("wal_level", "logical", "max_wal_senders", "4", "max_replication_slots", "4"));
        String hostSetting = string(settings, "pixels_host");
        try (var host = hostSetting.equals("local") ? PixelsHost.local(build.directory(), output.resolve("pixels"), new com.google.gson.Gson().toJsonTree(build.identity()).getAsJsonObject())
                     : PixelsHost.Client.connect(hostSetting, token(), new com.google.gson.Gson().toJsonTree(build.identity()).getAsJsonObject());
             var database = PostgresDatabase.create(build.directory().resolve("postgres/runtime"), output.resolve("database"), sourceSettings)) {
            // A terminated generator stops its source cluster; data and logs stay for diagnosis.
            var stopSource = new Thread(() -> {
                try {
                    database.close();
                } catch (Exception ignored) {
                    // Shutdown continues; the data directory remains.
                }
            }, "sibench-pixels-source-cleanup");
            Runtime.getRuntime().addShutdownHook(stopSource);
            try {
                json(output.resolve("effective-config.json"), Generation.effectiveConfig(plan, input));
                json(output.resolve("engine-build.json"), build.identity());
                json(output.resolve("client-build.json"), input.buildIdentity());
                String url = "jdbc:postgresql://127.0.0.1:" + database.port() + "/postgres?user=sibench&ApplicationName=sibench:hybench";
                long loadStart = System.nanoTime();
                Path load = Files.createDirectory(output.resolve("load"));
                Generation.hybench(plan, input, load, true, null, Engine.POSTGRES, url, null, () -> {});
                Path data = output.resolve("native/Data_" + string(input.nativeConfig(), "sf"));
                Path script = input.source().source().resolve("conf/load_data_pg.sql"), inputs = load.resolve("inputs.json");
                HybenchInputs.record(data, script, inputs);
                timings.put("input_generation_seconds", seconds(loadStart));

                long pixelsStart = System.nanoTime();
                var address = InetAddress.getByName(host.localAddress());
                var traceListener = new ServerSocket(0, 1, address);
                var reportListener = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
                var budget = new RequestBudget(Generation.requestTarget(input.document()), input.timeout());
                var overlay = object(settings, "properties");
                var startRequest = new LinkedHashMap<String, Object>(Map.of("op", "start", "run", hostSetting.equals("local") ? "pixels" : output.getFileName().toString(), "overlay", overlay,
                        "memory", object(settings, "memory"), "trace", true, "trace_endpoint", address.getHostAddress() + ":" + traceListener.getLocalPort(),
                        "trace_token", traceToken));
                if (settings.containsKey("data_storage")) startRequest.put("data_storage", object(settings, "data_storage"));
                var pixels = host.call(startRequest);
                Path sinkRoot = Files.createDirectory(output.resolve("sink"));
                Path pixelsConfig = Files.writeString(sinkRoot.resolve("pixels.properties"), pixels.get("properties").getAsString());
                try (var trace = new PixelsTrace(output.resolve("trace"), input.source(), Map.of(), budget);
                     var capture = new PixelsCapture(trace, traceListener, traceToken, reportListener, reportToken)) {
                    capture.acceptTracer(input.timeout());
                    timings.put("pixels_start_seconds", seconds(pixelsStart));
                    long baselineStart = System.nanoTime();
                    loadBaseline(database, host, capture, trace, input.source(), data, output,
                            input.baselineRows(), object(settings, "load"), input.timeout());
                    HybenchInputs.requireRuntimeInputs(data, inputs);
                    HybenchInputs.dropLoadInputs(data, script);
                    timings.put("baseline_load_seconds", seconds(baselineStart));

                    sink = replicate(database, capture, input.source(), build.directory(), settings,
                            sinkRoot, pixelsConfig, reportListener.getLocalPort(), reportToken, input.timeout());

                    var check = checker(host, sink, capture, trace, output);
                    long workloadStart = System.nanoTime();
                    var stages = new LinkedHashMap<String, Object>();
                    long marker = 0;
                    int remaining = 2;
                    // CDC has no SQL query consumer; XP runs only its AT transaction clients.
                    for (String workload : List.of("tp", "xp")) {
                        Path stage = Files.createDirectory(output.resolve(workload));
                        Path stop = stage.resolve("budget.stop");
                        budget.begin(workload, remaining--, stop);
                        var batches = new ArrayList<Map<String, Object>>();
                        do {
                            budget.checkTime();
                            long before = budget.count();
                            Path batch = Files.createDirectory(stage.resolve("batch-" + batches.size()));
                            progress(output, workload + "_client_begin", Map.of("batch", batches.size()));
                            Generation.hybench(plan, input, batch, false, workload, Engine.POSTGRES, url, stop, check);
                            progress(output, workload + "_client_complete", Map.of("batch", batches.size()));
                            try (var control = database.connect("marker")) {
                                execute(control, "INSERT INTO " + MARKER + " VALUES (" + ++marker + ")");
                            }
                            capture.awaitMarker(marker, input.timeout());
                            capture.call(() -> progress(output, workload + "_projected", trace.progress()), false, Duration.ofSeconds(30));
                            batches.add(Map.of("phase", workload, "marker", marker, "version", trace.version(),
                                    "client", new com.google.gson.Gson().fromJson(Files.readString(batch.resolve("summary.json")), Map.class)));
                            if (budget.enabled()) budget.requireProgress(before);
                        } while (budget.enabled() && !budget.reached());
                        budget.end();
                        stages.put(workload, batches);
                    }
                    json(output.resolve("request-budget.json"), budget.finish());
                    timings.put("workload_and_export_seconds", seconds(workloadStart));

                    long closeStart = System.nanoTime();
                    capture.check();
                    capture.sinkStopping();
                    stopSink(sink);
                    sink = null;
                    capture.call(trace::finish, false, input.timeout());
                    capture.close();
                    var stopped = host.call(Map.of("op", "stop"));
                    database.close();
                    timings.put("native_close_seconds", seconds(closeStart));
                    var evidence = new LinkedHashMap<String, Object>();
                    evidence.put("engine", build.identity());
                    evidence.put("client", input.buildIdentity());
                    evidence.put("workload_scope", "HyBench TP and AT transactions; AP and IQ clients disabled");
                    evidence.put("stages", stages);
                    evidence.put("request_budget", budget.finish());
                    evidence.put("effective_config_sha256", Digests.sha256(output.resolve("effective-config.json")));
                    evidence.put("baseline_rows", input.baselineRows());
                    evidence.put("load_inputs_sha256", Digests.sha256(inputs));
                    evidence.put("parameters_sha256", Digests.sha256(output.resolve("native/parameters/parameters.toml")));
                    evidence.put("pixels_host", Map.of("deployment", hostSetting, "address", pixels.get("address").getAsString(),
                            "build", host.identity(), "stop", new com.google.gson.Gson().fromJson(stopped.get("exits"), Map.class)));
                    var manifest = Generation.publishThenDiscard(() -> trace.publish(evidence), database.data(), timings);
                    try {
                        host.call(Map.of("op", "delete"));
                    } catch (IOException failure) {
                        throw new IOException("Trace published at " + output.resolve("trace")
                                + "; Pixels cleanup failed at " + pixels.get("address").getAsString()
                                + ":" + pixels.get("directory").getAsString(), failure);
                    }
                    timings.put("total_seconds", seconds(started));
                    json(output.resolve("result.json"), Map.of("status", "complete", "trace", output.resolve("trace").toString(), "timings", timings));
                    return manifest;
                }
            } finally {
                try {
                    Runtime.getRuntime().removeShutdownHook(stopSource);
                } catch (IllegalStateException shuttingDown) {
                    // The hook is already running.
                }
                if (sink != null) {
                    sink.destroyForcibly();
                    sink.waitFor(10, TimeUnit.SECONDS);
                }
            }
        } catch (Exception | Error failure) {
            var stack = new java.io.StringWriter();
            failure.printStackTrace(new java.io.PrintWriter(stack));
            try {
                Files.writeString(output.resolve("failure.txt"), stack.toString(), StandardOpenOption.CREATE_NEW);
            } catch (Exception diagnostic) {
                failure.addSuppressed(diagnostic);
            }
            throw failure;
        }
    }

    static Process replicate(PostgresDatabase database, PixelsCapture capture, SourceSchema source,
            Path build, Map<String, Object> settings, Path sinkRoot, Path pixelsConfig, int reportPort, String reportToken,
            Duration timeout) throws Exception {
        createSlot(database, source);
        Path sinkConfig = sinkRoot.resolve("sink.properties");
        Files.writeString(sinkConfig, sinkProperties(object(settings, "sink_properties"), sinkRoot, database.port(), "retina"),
                StandardOpenOption.CREATE_NEW);
        Path logging = sinkLogging(sinkRoot);
        var command = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx" + string(object(settings, "memory"), "sink_heap"), "-Dlog4j2.configurationFile=" + logging,
                "-Dsibench.report.endpoint=127.0.0.1:" + reportPort, "-Dsibench.report.token=" + reportToken,
                "-cp", build.resolve("jars/pixels-sink.jar").toString(), "io.pixelsdb.pixels.sink.PixelsSinkApp",
                "-c", sinkConfig.toString()));
        var processBuilder = new ProcessBuilder(command).directory(sinkRoot.toFile()).redirectErrorStream(true)
                .redirectOutput(sinkRoot.resolve("sink.log").toFile());
        processBuilder.environment().put("PIXELS_CONFIG", pixelsConfig.toString());
        Process sink = processBuilder.start();
        Runtime.getRuntime().addShutdownHook(new Thread(sink::destroyForcibly, "sibench-pixels-sink-cleanup"));
        capture.acceptSink(timeout);
        try {
            awaitSlot(database, sink, capture::check);
            return sink;
        } catch (Exception | Error failure) {
            sink.destroyForcibly();
            throw failure;
        }
    }

    static void createSlot(PostgresDatabase database, SourceSchema source) throws Exception {
        try (var setup = database.connect("replication")) {
            var names = new ArrayList<String>();
            for (var table : source.tables()) names.add(quote(table.name()));
            names.add(MARKER);
            execute(setup, "CREATE PUBLICATION " + SLOT + " FOR TABLE " + String.join(",", names));
            execute(setup, "SELECT pg_create_logical_replication_slot('" + SLOT + "','pgoutput')");
        }
    }

    static Path sinkLogging(Path sinkRoot) throws IOException {
        // The bundled logging records every change event at DEBUG, twice; generation keeps INFO on stdout.
        return Files.writeString(sinkRoot.resolve("log4j2.properties"), "status=warn\n"
                + "appender.console.type=Console\nappender.console.name=STDOUT\n"
                + "appender.console.layout.type=PatternLayout\nappender.console.layout.pattern=%d [%c]-[%p] %m%n\n"
                + "rootLogger.level=info\nrootLogger.appenderRef.stdout.ref=STDOUT\n", StandardOpenOption.CREATE_NEW);
    }

    /** PostgreSQL and Pixels independently load the same bound CSVs; neither reads the other's database. */
    static void loadBaseline(PostgresDatabase database, PixelsHost.Client host, PixelsCapture capture, PixelsTrace trace,
            SourceSchema source, Path data, Path output, Map<String, Long> expected, Map<String, Object> settings,
            Duration timeout) throws Exception {
        var worker = java.util.concurrent.Executors.newSingleThreadExecutor();
        Throwable primary = null;
        long started = System.nanoTime();
        Path prepared = output.resolve("pixels-inputs");
        java.util.concurrent.Future<?> sourceLoad = null;
        try (var setup = database.connect("load")) {
            sourceLoad = worker.submit(() -> {
                try {
                    progress(output, "postgres_load_begin", Map.of());
                    Generation.requireRows(expected, Hybench.load(setup, source.source(), data));
                    prepareSource(setup, source);
                    json(output.resolve("source-load.json"), Map.of("seconds", seconds(started), "rows", expected));
                    progress(output, "postgres_load_complete", Map.of("seconds", seconds(started)));
                    return null;
                } catch (Exception | Error failure) {
                    try { host.close(); } catch (IOException close) { failure.addSuppressed(close); }
                    throw failure;
                }
            });
            progress(output, "pixels_input_begin", Map.of());
            var inputs = PixelsInputs.prepare(source, data, prepared, positive(settings, "rows_per_file"));
            Generation.requireRows(expected, inputs.rows());
            Files.writeString(output.resolve("pixels-schema.tsv"), inputs.schema().text(), StandardOpenOption.CREATE_NEW);
            double preparation = seconds(started);
            progress(output, "pixels_input_complete", Map.of("seconds", preparation));
            var created = PixelsSchema.tables(host.call(Map.of("op", "create", "schema", "postgres", "table", "baseline",
                    "spec", inputs.schema().text())).get("tables").getAsString(), source, inputs.schema().keyTypes(), MARKER);
            capture.call(() -> trace.register(created), false, timeout);
            var tables = new ArrayList<>(source.tables());
            tables.sort(java.util.Comparator.comparingLong((SourceSchema.Table table) -> expected.get(table.name())).reversed());
            for (var table : tables) {
                try (var files = Files.list(prepared.resolve(table.name()))) {
                    for (Path file : files.sorted().toList()) {
                        capture.check();
                        host.upload(table.name(), file.getFileName().toString(), file, timeout);
                        Files.delete(file);
                    }
                }
                Files.delete(prepared.resolve(table.name()));
            }
            Files.delete(prepared);
            progress(output, "pixels_load_begin", Map.of());
            host.call(Map.of("op", "load", "schema", "postgres", "tables", tables.stream().map(SourceSchema.Table::name).toList(),
                    "rows_per_file", positive(settings, "rows_per_file"), "threads", positive(settings, "threads")), timeout);
            double pixels = seconds(started);
            progress(output, "pixels_load_complete", Map.of("seconds", pixels));
            awaitLoad(sourceLoad, timeout);
            capture.synced(() -> trace.finishBaseline(expected), timeout);
            json(output.resolve("baseline-load.json"), Map.of("input_preparation_seconds", preparation,
                    "pixels_complete_seconds", pixels, "total_seconds", seconds(started), "rows", expected,
                    "pixels_schema_sha256", Digests.sha256(output.resolve("pixels-schema.tsv"))));
        } catch (Exception | Error failure) {
            primary = failure;
            try { host.close(); } catch (IOException close) { failure.addSuppressed(close); }
            if (sourceLoad != null) sourceLoad.cancel(true);
            throw failure;
        } finally {
            worker.shutdownNow();
            try {
                if (!worker.awaitTermination(10, TimeUnit.SECONDS)) throw new IOException("PostgreSQL baseline worker did not stop");
            } catch (Exception cleanup) {
                if (primary != null) primary.addSuppressed(cleanup);
                else throw cleanup;
            }
        }
    }

    static synchronized void progress(Path output, String stage, Map<String, ?> detail) throws IOException {
        var event = new LinkedHashMap<String, Object>();
        event.put("time", java.time.Instant.now().toString());
        event.put("stage", stage);
        event.putAll(detail);
        Files.writeString(output.resolve("progress.jsonl"), new com.google.gson.Gson().toJson(event) + "\n",
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    private static void awaitLoad(java.util.concurrent.Future<?> future, Duration timeout) throws Exception {
        if (future == null) return;
        try {
            future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.ExecutionException failure) {
            if (failure.getCause() instanceof Exception cause) throw cause;
            if (failure.getCause() instanceof Error cause) throw cause;
            throw failure;
        }
    }

    /** Before images for UPDATE/DELETE, a marker table for window barriers and no primary-key changes. */
    static void prepareSource(Connection database, SourceSchema source) throws Exception {
        execute(database, "CREATE TABLE " + MARKER + "(id bigint PRIMARY KEY)");
        execute(database, """
                CREATE FUNCTION public.sibench_reject_key_change() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                  RAISE EXCEPTION 'primary key assignment is unsupported';
                END $$""");
        for (var table : source.tables()) {
            String key = table.indexes().stream().filter(SourceSchema.Index::primary).findFirst().orElseThrow().column();
            execute(database, "ALTER TABLE " + quote(table.name()) + " REPLICA IDENTITY FULL");
            execute(database, "CREATE TRIGGER sibench_reject_key_change BEFORE UPDATE ON " + quote(table.name())
                    + " FOR EACH ROW WHEN (OLD." + quote(key) + " IS DISTINCT FROM NEW." + quote(key)
                    + ") EXECUTE FUNCTION public.sibench_reject_key_change()");
        }
        execute(database, "ALTER TABLE " + MARKER + " REPLICA IDENTITY FULL");
    }

    static String sinkProperties(Map<String, Object> overrides, Path root, int port, String mode) {
        var generated = new LinkedHashMap<String, String>();
        generated.put("sink.datasource", "engine");
        generated.put("sink.datasource.engine.format", "connect");
        generated.put("sink.debezium.dialect", "postgresql");
        generated.put("sink.mode", mode);
        generated.put("sink.retina.mode", "stream");
        generated.put("sink.trans.mode", "batch");
        generated.put("sink.storage.loop", "false");
        generated.put("sink.monitor.freshness.timestamp", "false");
        generated.put("sink.retina.trans.request.batch", "false");
        generated.put("sink.retina.embedded", "false");
        generated.put("debezium.name", "sibench");
        generated.put("debezium.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        generated.put("debezium.provide.transaction.metadata", "true");
        generated.put("debezium.offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        generated.put("debezium.offset.storage.file.filename", root.resolve("offsets.dat").toString());
        generated.put("debezium.offset.flush.interval.ms", "60000");
        generated.put("debezium.schema.history.internal", "io.debezium.storage.file.history.FileSchemaHistory");
        generated.put("debezium.schema.history.internal.file.filename", root.resolve("schema-history.dat").toString());
        generated.put("debezium.database.hostname", "127.0.0.1");
        generated.put("debezium.database.port", Integer.toString(port));
        generated.put("debezium.database.user", "sibench");
        generated.put("debezium.database.password", "");
        generated.put("debezium.database.dbname", "postgres");
        generated.put("debezium.plugin.name", "pgoutput");
        generated.put("debezium.slot.name", SLOT);
        generated.put("debezium.publication.name", SLOT);
        generated.put("debezium.publication.autocreate.mode", "disabled");
        generated.put("debezium.schema.include.list", "public");
        generated.put("debezium.snapshot.mode", "never");
        generated.put("debezium.topic.prefix", "sibench");
        generated.put("debezium.decimal.handling.mode", "precise");
        generated.put("debezium.time.precision.mode", "adaptive_time_microseconds");
        generated.put("debezium.key.converter", "org.apache.kafka.connect.json.JsonConverter");
        generated.put("debezium.value.converter", "org.apache.kafka.connect.json.JsonConverter");
        var text = new StringBuilder("# Generated for one SIBench run; overrides come from the engine configuration.\n");
        overrides.forEach((key, value) -> text.append(key).append('=').append(value).append('\n'));
        generated.forEach((key, value) -> text.append(key).append('=').append(value).append('\n'));
        return text.toString();
    }

    /** Debezium streams from the pre-created slot once it is active; the workload starts only then. */
    static void awaitSlot(PostgresDatabase database, Process sink, NativeProcess.Check check) throws Exception {
        long deadline = System.nanoTime() + CONNECT.toNanos();
        try (var control = database.connect("slot")) {
            while (true) {
                check.run();
                if (!sink.isAlive()) throw new IOException("pixels-sink exited with " + sink.exitValue());
                try (var query = control.createStatement();
                     var rows = query.executeQuery("SELECT active FROM pg_replication_slots WHERE slot_name = '" + SLOT + "'")) {
                    if (rows.next() && rows.getBoolean(1)) return;
                }
                if (System.nanoTime() > deadline) throw new IOException("pixels-sink did not start streaming");
                Thread.sleep(200);
            }
        }
    }

    private static NativeProcess.Check checker(PixelsHost.Client host, Process sink, PixelsCapture capture, PixelsTrace trace, Path output) {
        long[] last = {0}, sampled = {0};
        return () -> {
            capture.check();
            if (!sink.isAlive()) throw new IOException("pixels-sink exited with " + sink.exitValue());
            if (System.nanoTime() - last[0] > TimeUnit.SECONDS.toNanos(5)) {
                host.call(Map.of("op", "check"));
                last[0] = System.nanoTime();
            }
            if (System.nanoTime() - sampled[0] > TimeUnit.SECONDS.toNanos(10)) {
                capture.call(() -> progress(output, "capture", trace.progress()), false, Duration.ofSeconds(30));
                sampled[0] = System.nanoTime();
            }
        };
    }

    /** Every source commit has been applied before this point; the sink holds no further work. */
    static void stopSink(Process sink) throws Exception {
        sink.destroy();
        if (!sink.waitFor(60, TimeUnit.SECONDS)) {
            sink.destroyForcibly();
            throw new IOException("pixels-sink did not stop");
        }
        if (sink.exitValue() != 0 && sink.exitValue() != 143) throw new IOException("pixels-sink stopped with " + sink.exitValue());
    }

    private static String token() throws IOException {
        String token = System.getenv("SIBENCH_PIXELS_HOST_TOKEN");
        if (token == null || token.isBlank()) throw new IOException("Set SIBENCH_PIXELS_HOST_TOKEN for a remote Pixels host");
        return token;
    }

    private static void execute(Connection database, String sql) throws Exception {
        try (var statement = database.createStatement()) { statement.execute(sql); }
    }

    private static String quote(String name) { return "\"" + name.replace("\"", "\"\"") + "\""; }
    private static double seconds(long start) { return (System.nanoTime() - start) / 1e9; }

    private static void json(Path path, Object value) throws IOException {
        Files.writeString(path, new GsonBuilder().setPrettyPrinting().create().toJson(value), StandardOpenOption.CREATE_NEW);
    }
}
