package org.sibench.capture;

import static org.sibench.config.ConfigValues.*;

import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.sibench.config.Digests;
import org.sibench.config.Engine;
import org.sibench.generation.PostgresDatabase;

/** One-time real HyBench/PostgreSQL CDC preparation; retains its original baseline and closed files. */
public final class CdcPreparation {
    private static final Duration DRAIN = Duration.ofMinutes(2);
    private CdcPreparation() {}

    public static Map<String, Object> run(Path root, Path configuration, Path csvDirectory) throws Exception {
        var plan = Generation.cdcPlan(root, configuration);
        return generate(plan, CsvInput.open(root.resolve(csvDirectory)));
    }

    static Map<String, Object> generate(Generation.Plan plan, CsvInput.Verified csv) throws Exception {
        var input = plan.inputs().get(0);
        var build = plan.engines().get(Engine.PIXELS);
        CsvInput.requireCompatible(csv, input);
        Path output = input.output();
        Files.createDirectories(output.getParent());
        Files.createDirectory(output);
        CsvInput.materialize(csv, output);
        long started = System.nanoTime();
        var timings = new LinkedHashMap<String, Double>();
        var settings = new LinkedHashMap<>(org.sibench.config.SibenchConfig.postgresSettings(
                object(object(build.settings(), "source"), "settings")));
        settings.putAll(Map.of("wal_level", "logical", "max_wal_senders", "4", "max_replication_slots", "4"));
        try (var database = PostgresDatabase.create(build.directory().resolve("postgres/runtime"), output.resolve("database"), settings)) {
            var cleanup = new Thread(() -> {
                try { database.close(); }
                catch (Exception ignored) { /* Failed preparation retains source diagnostics. */ }
            }, "sibench-cdc-source-cleanup");
            Runtime.getRuntime().addShutdownHook(cleanup);
            try {
                json(output.resolve("effective-config.json"), Generation.effectiveConfig(plan, input));
                json(output.resolve("engine-build.json"), build.identity());
                json(output.resolve("client-build.json"), input.buildIdentity());
                String url = "jdbc:postgresql://127.0.0.1:" + database.port() + "/postgres?user=sibench&ApplicationName=sibench:hybench";
                Path data = output.resolve("native/Data_" + string(input.nativeConfig(), "sf"));
                Path spec = output.resolve("pixels-schema.tsv");
                try (var connection = database.connect("load")) {
                    Generation.requireRows(input.baselineRows(), Hybench.load(connection, input.source().source(), data));
                    PixelsGeneration.prepareSource(connection, input.source());
                }
                timings.put("baseline_preparation_seconds", seconds(started));
                PixelsGeneration.createSlot(database, input.source());
                try (var capture = CdcCapture.start(build.directory(), build.settings(), output.resolve("sink"), spec, database.port())) {
                    PixelsGeneration.awaitSlot(database, capture.process(), capture::check);
                    var stages = new LinkedHashMap<String, Object>();
                    int marker = 0;
                    for (String phase : List.of("tp", "xp")) {
                        Path stage = Files.createDirectory(output.resolve(phase));
                        long phaseStart = System.nanoTime();
                        Generation.hybench(plan, input, stage, false, phase, Engine.POSTGRES, url, null, capture::check);
                        String id = marker(database, capture, ++marker);
                        stages.put(phase, Map.of("marker_transaction", id, "counts", capture.counts(),
                                "seconds", seconds(phaseStart), "summary_sha256", Digests.sha256(stage.resolve("summary.json"))));
                    }
                    capture.finish();
                    database.close();
                    var evidence = new LinkedHashMap<String, Object>();
                    evidence.put("csv_manifest_sha256", csv.sha256());
                    evidence.put("engine", build.identity());
                    evidence.put("client", input.buildIdentity());
                    evidence.put("benchmark_source", Map.of("commit", input.source().sourceCommit(), "files", input.source().fingerprints()));
                    Path core = NativeProcess.self();
                    evidence.put("core_sha256", Files.isDirectory(core) ? PixelsBuild.treeSha256(core) : Digests.sha256(core));
                    evidence.put("workload_scope", "HyBench TP/AT; AP and IQ disabled; configured phase durations, no Index Trace request budget");
                    evidence.put("stages", stages);
                    evidence.put("effective_config_sha256", Digests.sha256(output.resolve("effective-config.json")));
                    evidence.put("parameters_sha256", Digests.sha256(output.resolve("native/parameters/parameters.toml")));
                    var manifest = Generation.publishThenDiscard(() -> CdcInput.publish(output, input, capture.counts(), evidence), database.data(), timings);
                    timings.put("total_seconds", seconds(started));
                    json(output.resolve("result.json"), Map.of("status", "complete", "input", output.toString(), "timings", timings));
                    return manifest;
                }
            } finally {
                try { Runtime.getRuntime().removeShutdownHook(cleanup); }
                catch (IllegalStateException stopping) { /* The shutdown hook owns source cleanup. */ }
            }
        } catch (Exception | Error failure) {
            var stack = new java.io.StringWriter();
            failure.printStackTrace(new java.io.PrintWriter(stack));
            try { Files.writeString(output.resolve("failure.txt"), stack.toString(), StandardOpenOption.CREATE_NEW); }
            catch (Exception diagnostic) { failure.addSuppressed(diagnostic); }
            throw failure;
        }
    }

    static String marker(PostgresDatabase database, CdcCapture capture, int number) throws Exception {
        try (var connection = database.connect("marker")) {
            connection.setAutoCommit(false);
            String id;
            try (var statement = connection.createStatement(); var result = statement.executeQuery("SELECT txid_current()::text")) {
                if (!result.next()) throw new IOException("Missing source marker transaction ID");
                id = result.getString(1);
            }
            capture.expectMarker(id);
            try (var statement = connection.createStatement()) {
                statement.executeUpdate("INSERT INTO sibench_marker VALUES(" + number + ")");
            }
            connection.commit();
            capture.awaitMarker(id, DRAIN);
            return id;
        }
    }

    private static double seconds(long started) { return (System.nanoTime() - started) / 1e9; }
    private static void json(Path path, Object value) throws IOException {
        Files.writeString(path, new GsonBuilder().setPrettyPrinting().create().toJson(value), StandardOpenOption.CREATE_NEW);
    }
}
