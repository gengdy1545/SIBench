package org.sibench.capture;

import static org.sibench.config.ConfigValues.*;

import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.sibench.config.BenchmarkSelection;
import org.sibench.config.Digests;
import org.sibench.config.Engine;
import org.sibench.config.SibenchConfig;
import org.sibench.generation.PostgresDatabase;
import org.sibench.schema.SourceSchema;

/** One-time real HyBench/PostgreSQL CDC preparation; retains its original baseline and closed files. */
public final class CdcPreparation {
    private static final Duration DRAIN = Duration.ofMinutes(2), CONNECT = Duration.ofMinutes(5);
    private static final String MARKER = "sibench_marker";
    private CdcPreparation() {}

    public static Map<String, Object> run(Path root, Path configuration, Path csvDirectory) throws Exception {
        // PostgreSQL COPY reads the table CSVs itself; publication hashes the materialized copies afterwards.
        return generate(plan(root, configuration), CsvInput.bind(root.resolve(csvDirectory)));
    }

    /** CDC selects HyBench and only the source-side Pixels build; it consumes no target service, storage or property file. */
    static Generation.Plan plan(Path root, Path configuration) throws Exception {
        root = root.toRealPath();
        var runtime = SibenchConfig.read(root.resolve(configuration));
        var selected = Generation.hybenchSelection(root, runtime);
        var selection = new BenchmarkSelection(selected.config(), selected.benchmark(), BenchmarkSelection.Mode.CDC, Engine.PIXELS,
                selected.name(), selected.workload(), selected.directory(), selected.generate(), selected.upload());
        var settings = SibenchConfig.cdcSource(root, runtime);
        Path build = NativeBuildCheck.engineDirectory(root, settings, "pixels").toRealPath();
        return Generation.inputs(root, runtime, List.of(selection),
                Map.of(Engine.PIXELS, new Generation.NativeBuild(build, PixelsBuild.verifySource(root, build), settings)), null);
    }

    static Map<String, Object> generate(Generation.Plan plan, CsvInput.Verified csv) throws Exception {
        var input = plan.inputs().get(0);
        var build = plan.engines().get(Engine.PIXELS);
        CsvInput.requireCompatible(csv, input);
        Path output = input.output();
        InputFiles.requireSeparate(output, csv.directory());
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
                    prepareSource(connection, input.source());
                }
                timings.put("baseline_preparation_seconds", seconds(started));
                createSlot(database, input.source());
                try (var capture = CdcCapture.start(build.directory(), build.settings(), output.resolve("sink"), spec, database.port())) {
                    awaitSlot(database, capture.process(), capture::check);
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
                    evidence.put("engine", build.identity());
                    evidence.put("client", input.buildIdentity());
                    evidence.put("benchmark_source", Map.of("commit", input.source().sourceCommit(), "files", input.source().fingerprints()));
                    Path core = NativeProcess.self();
                    evidence.put("core_sha256", Files.isDirectory(core) ? PixelsBuild.treeSha256(core) : Digests.sha256(core));
                    evidence.put("workload_scope", "HyBench TP/AT; AP and IQ disabled; configured phase durations, no Index Trace request budget");
                    evidence.put("stages", stages);
                    evidence.put("effective_config_sha256", Digests.sha256(output.resolve("effective-config.json")));
                    evidence.put("parameters_sha256", Digests.sha256(output.resolve("native/parameters/parameters.toml")));
                    var manifest = Generation.publishThenDiscard(() -> CdcInput.publish(output, csv, capture.counts(), evidence), database.data(), timings);
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
                statement.executeUpdate("INSERT INTO " + MARKER + " VALUES(" + number + ")");
            }
            connection.commit();
            capture.awaitMarker(id, DRAIN);
            return id;
        }
    }

    static void createSlot(PostgresDatabase database, SourceSchema source) throws Exception {
        try (var setup = database.connect("replication")) {
            var names = new ArrayList<String>();
            for (var table : source.tables()) names.add(quote(table.name()));
            names.add(MARKER);
            execute(setup, "CREATE PUBLICATION " + PixelsSink.SLOT + " FOR TABLE " + String.join(",", names));
            execute(setup, "SELECT pg_create_logical_replication_slot('" + PixelsSink.SLOT + "','pgoutput')");
        }
    }

    /** Before images for UPDATE/DELETE, a marker table for phase barriers and no primary-key changes. */
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
            // AFTER sees the final row of every BEFORE trigger, whatever their name order.
            execute(database, "CREATE TRIGGER sibench_reject_key_change AFTER UPDATE ON " + quote(table.name())
                    + " FOR EACH ROW WHEN (OLD." + quote(key) + " IS DISTINCT FROM NEW." + quote(key)
                    + ") EXECUTE FUNCTION public.sibench_reject_key_change()");
        }
        execute(database, "ALTER TABLE " + MARKER + " REPLICA IDENTITY FULL");
    }

    /** Debezium streams from the pre-created slot once it is active; the workload starts only then. */
    static void awaitSlot(PostgresDatabase database, Process sink, NativeProcess.Check check) throws Exception {
        long deadline = System.nanoTime() + CONNECT.toNanos();
        try (var control = database.connect("slot")) {
            while (true) {
                check.run();
                if (!sink.isAlive()) throw new IOException("pixels-sink exited with " + sink.exitValue());
                try (var query = control.createStatement();
                     var rows = query.executeQuery("SELECT active FROM pg_replication_slots WHERE slot_name = '" + PixelsSink.SLOT + "'")) {
                    if (rows.next() && rows.getBoolean(1)) return;
                }
                if (System.nanoTime() > deadline) throw new IOException("pixels-sink did not start streaming");
                Thread.sleep(200);
            }
        }
    }

    private static void execute(Connection database, String sql) throws Exception {
        try (var statement = database.createStatement()) { statement.execute(sql); }
    }

    private static String quote(String name) { return "\"" + name.replace("\"", "\"\"") + "\""; }

    private static double seconds(long started) { return (System.nanoTime() - started) / 1e9; }
    private static void json(Path path, Object value) throws IOException {
        Files.writeString(path, new GsonBuilder().setPrettyPrinting().create().toJson(value), StandardOpenOption.CREATE_NEW);
    }
}
