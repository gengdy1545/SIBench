package org.sibench.capture;

import static org.sibench.config.ConfigValues.*;

import com.google.gson.*;
import java.io.IOException;
import java.net.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.sibench.config.*;
import org.sibench.query.TrinoQueries;
import org.sibench.schema.SourceSchema;

/** A fresh Pixels baseline followed by a finite CDC prefix and independently selected queries. */
public final class PixelsExperiment {
    private PixelsExperiment() {}

    public static Map<String, Object> run(Path root, Path configuration, Path input, Path baselineDirectory, Path output) throws Exception {
        root = root.toRealPath();
        Path cdcDirectory = input == null ? null : root.resolve(input);
        output = root.resolve(output).toAbsolutePath();
        var runtime = SibenchConfig.read(root.resolve(configuration));
        var experiment = PixelsExperimentConfig.settings(runtime);
        var inputs = inputs(root, cdcDirectory, baselineDirectory, experiment);
        var columnar = inputs.baseline();
        var verified = inputs.cdc();
        InputFiles.requireSeparate(output.normalize(), columnar.directory(), verified == null ? null : verified.directory());
        var checked = verified == null ? Map.<String, Object>of()
                : Map.<String, Object>of("counts", verified.counts(), "manifest_sha256", verified.manifestSha256());
        var source = inputs.source();
        var settings = SibenchConfig.engine(root, runtime, Engine.PIXELS);
        Path build = root.resolve(string(settings, "build_directory")).toRealPath();
        var identity = PixelsBuild.verify(root, build);
        PixelsBaseline.requireStorage(settings);
        PixelsBaseline.deployment(columnar, settings, identity);
        var freshness = object(experiment, "freshness");
        if (bool(freshness, "enabled") && source.tables().stream().noneMatch(t -> t.name().equals(string(freshness, "table"))
                && t.columns().stream().anyMatch(c -> c.name().equals(string(freshness, "column")) && c.type().matches("timestamp(\\(6\\))? without time zone"))))
            throw new IOException("Freshness requires the named dedicated timestamp(6) column");
        var expected = PixelsBaseline.rows(columnar.manifest());
        boolean tracing = bool(experiment, "trace");
        var timeout = Duration.ofSeconds(positive(experiment, "timeout_seconds"));
        Path sqlDirectory = root.resolve(string(object(experiment, "queries"), "directory"));
        List<Path> sql = List.of();
        if (bool(object(experiment, "queries"), "enabled")) {
            try (var files = Files.list(sqlDirectory)) {
                sql = files.filter(p -> p.getFileName().toString().endsWith(".sql")).sorted().toList();
            }
            if (sql.isEmpty()) throw new IOException("Query directory contains no .sql files");
        }
        var answers = bool(object(experiment, "queries"), "enabled") ? answers(sqlDirectory, experiment, columnar) : null;
        // Validate every option and input before creating working state or starting target services.
        Files.createDirectories(output.getParent());
        Files.createDirectory(output);
        json(output.resolve("effective-config.json"), runtime);
        if (verified != null) json(output.resolve("input.json"), checked);
        json(output.resolve("baseline-input.json"), Map.of("manifest_sha256", columnar.sha256(), "manifest", columnar.manifest()));
        json(output.resolve("engine-build.json"), identity);
        Path runtimeCode = NativeProcess.self();
        var runtimeBuild = Map.of("kind", Files.isRegularFile(runtimeCode) ? "jar" : "classes", "location", runtimeCode.toString(),
                "sha256", Files.isRegularFile(runtimeCode) ? Digests.sha256(runtimeCode) : PixelsBuild.treeSha256(runtimeCode),
                "java_version", System.getProperty("java.version"), "java_vendor", System.getProperty("java.vendor"));
        json(output.resolve("runtime-build.json"), runtimeBuild);
        var timings = new LinkedHashMap<String, Double>();
        long started = System.nanoTime();
        String deployment = string(settings, "pixels_host"), token = UUID.randomUUID().toString();
        var workers = Executors.newFixedThreadPool(2);
        Path runOutput = output;
        try (var host = deployment.equals("local") ? PixelsHost.local(build, output.resolve("pixels"), new Gson().toJsonTree(identity).getAsJsonObject())
                : PixelsHost.Client.connect(deployment, hostToken(), new Gson().toJsonTree(identity).getAsJsonObject());
             var listener = tracing ? new ServerSocket(0, 1, InetAddress.getByName(host.localAddress())) : null;
             var trace = tracing ? new PixelsTrace(output.resolve("trace"), source, Map.of(), null) : null;
             var capture = tracing ? new PixelsCapture(trace, listener, token, null, null) : null) {
            var request = new LinkedHashMap<String, Object>(Map.of("op", "start", "run", deployment.equals("local") ? "pixels" : "pixels-" + UUID.randomUUID(),
                    "overlay", object(settings, "properties"), "memory", object(settings, "memory"), "trace", tracing));
            request.put("data_storage", object(settings, "data_storage"));
            request.put("baseline_max_timestamp", columnar.manifest().getAsJsonObject("inventory").get("max_timestamp").getAsLong());
            if (tracing) request.putAll(Map.of("trace_endpoint", host.localAddress() + ":" + listener.getLocalPort(), "trace_token", token));
            var pixels = host.call(request, timeout);
            json(output.resolve("pixels-host.json"), Map.of("deployment", deployment, "address", pixels.get("address").getAsString(),
                    "directory", pixels.get("directory").getAsString(), "data_uri", pixels.get("data_uri").getAsString(), "build", host.identity()));
            Path pixelsConfig = Files.writeString(output.resolve("pixels.properties"), pixels.get("properties").getAsString());
            if (capture != null) capture.acceptTracer(timeout);
            long loadStarted = System.nanoTime();
            String bound = Files.readString(columnar.directory().resolve(columnar.manifest().getAsJsonObject("schema").get("path").getAsString()));
            var created = host.call(Map.of("op", "create", "schema", "postgres", "table", "baseline", "spec", PixelsBaseline.targetSchema(bound)), timeout);
            Files.writeString(output.resolve("pixels-tables.tsv"), created.get("tables").getAsString(), StandardOpenOption.CREATE_NEW);
            var tables = PixelsSchema.tables(created.get("tables").getAsString(), source, PixelsBaseline.keyTypes(bound), "sibench_marker");
            if (capture != null) capture.call(() -> trace.register(tables), false, timeout);
            var imported = host.call(Map.of("op", "import-baseline", "spec", bound,
                    "inventory", columnar.manifest().get("inventory")), timeout).getAsJsonObject("baseline");
            var actual = new LinkedHashMap<String, Long>();
            imported.getAsJsonObject("rows").entrySet().forEach(e -> actual.put(e.getKey(), e.getValue().getAsLong()));
            Generation.requireRows(expected, actual);
            if (capture != null) capture.synced(() -> trace.finishBaseline(expected), timeout);
            json(output.resolve("baseline-import.json"), imported);
            timings.put("baseline_import_seconds", seconds(loadStarted));

            var queries = object(experiment, "queries");
            var trino = object(experiment, "trino");
            if ((bool(queries, "enabled") || bool(object(experiment, "freshness"), "enabled"))
                    && !string(trino, "deployment_directory").isBlank()) {
                var launched = host.call(Map.of("op", "start-query", "settings", trino), timeout);
                trino.put("server", launched.get("server").getAsString());
                json(output.resolve("trino.json"), launched);
            }
            var queryFiles = sql;
            Future<?> queryRun = null;
            if (bool(queries, "enabled") && string(queries, "timing").equals("during"))
                queryRun = workers.submit(() -> { query(queryFiles, queries, trino, runOutput, answers); return null; });
            CdcInput.Counts selected = new CdcInput.Counts(0, 0, 0, 0);
            long updateStart = System.nanoTime();
            if (bool(experiment, "updates")) {
                try (var sink = PixelsApply.start(build, settings, output.resolve("sink"), pixelsConfig, capture, freshness, trino)) {
                    var update = workers.submit(() -> {
                        var counts = CdcInput.stream(verified.directory(), verified.manifest(), integer(experiment, "max_transactions"), sink::frame);
                        sink.finish(counts, timeout);
                        return counts;
                    });
                    selected = await(update, queryRun, timeout);
                }
            }
            timings.put("apply_and_drain_seconds", seconds(updateStart));
            if (queryRun != null) await(queryRun, timeout);
            if (bool(queries, "enabled") && string(queries, "timing").equals("after"))
                query(queryFiles, queries, trino, output, answers);
            if (capture != null) {
                capture.synced(trace::finish, timeout);
                capture.close();
            }
            var stopped = host.call(Map.of("op", "stop"), timeout);
            var result = new LinkedHashMap<String, Object>();
            result.putAll(Map.of("status", "complete",
                    "selected_input", selected, "committed_transactions", selected.transactions(), "committed_changes", selected.changes(),
                    "baseline_manifest_sha256", columnar.sha256(), "trace_enabled", tracing, "pixels_host", pixels.get("address").getAsString(), "stop", stopped));
            if (verified != null) result.put("input_manifest_sha256", verified.manifestSha256());
            if (trace != null) {
                var evidence = new LinkedHashMap<String, Object>(Map.of("baseline_manifest_sha256", columnar.sha256(),
                        "selected_input", selected, "engine", identity, "runtime", runtimeBuild,
                        "effective_config_sha256", Digests.sha256(output.resolve("effective-config.json"))));
                if (verified != null) evidence.put("cdc_input", checked);
                result.put("trace", trace.publish(evidence));
            }
            host.call(Map.of("op", "delete"), timeout);
            timings.put("total_seconds", seconds(started));
            json(output.resolve("timings.json"), timings);
            json(output.resolve("result.json"), result);
            return result;
        } catch (Exception | Error failure) {
            var stack = new java.io.StringWriter();
            failure.printStackTrace(new java.io.PrintWriter(stack));
            try { Files.writeString(output.resolve("failure.txt"), stack.toString(), StandardOpenOption.CREATE_NEW); }
            catch (Exception diagnostic) { failure.addSuppressed(diagnostic); }
            throw failure;
        } finally {
            workers.shutdownNow();
            workers.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    record Inputs(PixelsBaseline.Verified baseline, CdcInput.Verified cdc, SourceSchema source) {}

    /** Recorded answers apply only to the sealed baseline they were computed from, before any update. */
    static HybenchQueries.Rendered answers(Path directory, Map<String, Object> experiment, PixelsBaseline.Verified baseline) throws IOException {
        var answers = HybenchQueries.open(directory);
        if (answers != null) {
            require(!bool(experiment, "updates"), "Rendered HyBench answers describe the sealed baseline; run them without updates");
            require(answers.manifest().get("csv_manifest_sha256").equals(baseline.manifest().get("csv_manifest_sha256")),
                    "Rendered HyBench queries describe another CSV input");
        }
        return answers;
    }

    static Inputs inputs(Path root, Path input, Path baselineDirectory, Map<String, Object> experiment) throws IOException {
        boolean updates = bool(experiment, "updates");
        require(updates == (input != null), updates
                ? "Updates require a CDC input directory" : "Static experiments must omit the CDC input directory");
        var baseline = PixelsBaseline.open(root.resolve(baselineDirectory));
        CdcInput.Verified cdc = null;
        if (updates) {
            cdc = CdcInput.open(root.resolve(input));
            PixelsBaseline.match(baseline, cdc.manifest());
            require(integer(experiment, "max_transactions") <= cdc.counts().transactions(),
                    "Requested transaction prefix exceeds the published CDC input");
        }
        var manifest = baseline.manifest();
        var ddl = manifest.getAsJsonObject("source_ddl");
        String commit = manifest.getAsJsonObject("source_evidence")
                .getAsJsonObject("benchmark_source").get("commit").getAsString();
        var source = SourceSchema.captured(baseline.directory().resolve(ddl.get("path").getAsString()),
                commit, ddl.get("sha256").getAsString());
        return new Inputs(baseline, cdc, source);
    }

    private static void query(List<Path> files, Map<String, Object> queries, Map<String, Object> config, Path output,
            HybenchQueries.Rendered answers) throws Exception {
        var client = new TrinoQueries(URI.create(string(config, "server")), string(config, "user"), string(config, "catalog"),
                string(config, "schema"), Duration.ofSeconds(positive(config, "timeout_seconds")));
        Path directory = Files.createDirectory(output.resolve("queries"));
        for (long round = 0; round < positive(queries, "rounds"); round++) for (int i = 0; i < files.size(); i++) {
            Path file = files.get(i);
            String text = Files.readString(file);
            String digest = java.util.HexFormat.of().formatHex(Digests.digest().digest(text.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
            String sql = text.strip().replaceFirst(";\\s*$", "");
            String name = round + "-" + i;
            var plan = new ArrayList<JsonArray>();
            var explained = client.execute("EXPLAIN " + sql, plan::add);
            json(directory.resolve(name + "-plan.json"), Map.of("query_id", explained.queryId(), "rows", plan));
            var rows = new ArrayList<JsonArray>();
            var result = client.execute(sql, answers == null ? row -> {} : rows::add);
            var record = new LinkedHashMap<String, Object>(Map.of("sql", sql, "sql_sha256", digest, "source", file.toString(),
                    "round", round, "result", result));
            String difference = null;
            if (answers != null) {
                var entry = HybenchQueries.entry(answers, file.getFileName().toString());
                difference = HybenchQueries.difference(entry.getAsJsonArray("expected"), rows,
                        entry.has("order_column") ? entry.get("order_column").getAsInt() : null);
                record.put("oracle", difference == null ? "matched" : difference);
            }
            json(directory.resolve(name + "-result.json"), record);
            if (difference != null)
                throw new IOException("Trino result differs from the PostgreSQL answer for " + file.getFileName() + ": " + difference);
        }
    }

    private static <T> T await(Future<T> task, Duration timeout) throws Exception {
        return await(task, null, timeout);
    }

    static <T> T await(Future<T> task, Future<?> query, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        try {
            while (true) {
                if (query != null && query.isDone()) query.get();
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) throw new TimeoutException("Pixels experiment task timed out");
                try { return task.get(query == null ? remaining : Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(100)), TimeUnit.NANOSECONDS); }
                catch (TimeoutException pending) { /* Check the companion query before waiting again. */ }
            }
        }
        catch (ExecutionException error) {
            if (error.getCause() instanceof Exception cause) throw cause;
            if (error.getCause() instanceof Error cause) throw cause;
            throw error;
        } finally { if (!task.isDone()) task.cancel(true); }
    }

    static String hostToken() throws IOException {
        String token = System.getenv("SIBENCH_PIXELS_HOST_TOKEN");
        if (token == null || token.isBlank()) throw new IOException("Set SIBENCH_PIXELS_HOST_TOKEN for remote Pixels");
        return token;
    }

    private static double seconds(long start) { return (System.nanoTime() - start) / 1e9; }
    private static void json(Path path, Object value) throws IOException {
        Files.writeString(path, new GsonBuilder().setPrettyPrinting().create().toJson(value), StandardOpenOption.CREATE_NEW);
    }
}
