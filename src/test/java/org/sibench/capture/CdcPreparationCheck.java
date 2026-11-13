package org.sibench.capture;

import static org.junit.Assert.*;
import static org.sibench.config.ConfigValues.*;

import io.pixelsdb.pixels.sink.SinkProto;
import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.sibench.config.BenchmarkSelection;
import org.sibench.config.Engine;
import org.sibench.config.SibenchConfig;
import org.sibench.generation.PostgresDatabase;
import org.sibench.schema.SourceSchema;

/** Explicit real-PG capture checks, with eight baseline rows and independently observed commits. */
public class CdcPreparationCheck {
    @Test(timeout = 240_000) public void hybenchPreparationCommandPublishesBoundInput() throws Exception {
        prepareHybench(false);
    }

    static Path prepareHybench(boolean parallel) throws Exception {
        Path root = Path.of("").toRealPath();
        String build = System.getProperty("sibench.pixels.build"), client = System.getProperty("sibench.hybench.build");
        assertNotNull("Set sibench.pixels.build", build);assertNotNull("Set sibench.hybench.build", client);
        Path work = Files.createTempDirectory(root.resolve("results/capture"), "cdc-hybench-check-");
        var profile = DuckDbGenerationTest.profile("hybench");
        var nativeConfig = object(profile, "native");
        nativeConfig.put("build_directory", client);nativeConfig.put("sf", "1x");nativeConfig.put("timeout_minutes", 3);
        nativeConfig.put("clients", 2);nativeConfig.put("xp_transaction_clients", 2);
        nativeConfig.put("duration_minutes", 1);nativeConfig.put("phase_duration_minutes", Map.of("tp", 1, "xp", 1));
        nativeConfig.put("parameters", Map.of("customer_number", 300, "company_number", 2, "transfer_number", 6000,
                "checking_number", 600, "loanapps_number", 600, "loantrans_number", 600));
        profile.put("native", nativeConfig);
        profile.put("artifacts", Map.of("directory", root.relativize(work.resolve("input")).toString()));
        var yaml = new org.yaml.snakeyaml.Yaml();Path config = work.resolve("runtime.yaml"), engine = work.resolve("pixels.yaml"), benchmark = work.resolve("hybench.yaml");
        Files.writeString(benchmark, yaml.dump(profile));
        var pixels = new LinkedHashMap<>(parse(Files.readString(root.resolve("config/engine/pixels/pixels.yaml"))));
        pixels.put("build_directory", build);pixels.put("memory", Map.of("coordinator_heap", "1g", "retina_heap", "1g", "sink_heap", "1g"));
        pixels.put("properties", root.resolve("config/engine/pixels/pixels.properties").toString());
        pixels.put("sink_properties", root.resolve("config/engine/pixels/pixels-sink.properties").toString());
        if (parallel) {
            String storage = System.getProperty("sibench.storage.config");
            assertNotNull("Set sibench.storage.config", storage);
            pixels.put("load", Map.of("rows_per_file", 2000, "threads", 1));
            pixels.put("data_storage", Map.of("config", root.resolve(storage).toString(), "profile", "cos",
                    "prefix", "sibench/pixels/prepare-" + java.util.UUID.randomUUID()));
        }
        Files.writeString(engine, yaml.dump(pixels));
        Files.writeString(config, yaml.dump(Map.of("schema_version", 1, "purpose", "sibench",
                "benchmarks", List.of(Map.of("config", "config/benchmark/ycsb.yaml", "mode", "api", "engine", "rocksdb"),
                        Map.of("config", benchmark.toString(), "mode", "capture", "engine", "postgres")),
                "engines", Map.of("pixels", engine.toString(), "rocksdb", "/unavailable/engine-config.yaml"))));
        var output = new java.io.ByteArrayOutputStream();var errors = new java.io.ByteArrayOutputStream();
        int prepared = org.sibench.cli.Sibench.run(new String[] {"prepare-csv", "--root", root.toString(), config.toString(), work.resolve("csv").toString()},
                new java.io.PrintStream(output), new java.io.PrintStream(errors));
        assertEquals(errors.toString(java.nio.charset.StandardCharsets.UTF_8), 0, prepared);
        String csvIdentity = CsvInput.open(work.resolve("csv")).sha256();
        assertFalse(Files.exists(work.resolve("csv/database")));
        String[] command = parallel
                ? new String[] {"prepare-pixels-inputs", "--root", root.toString(), config.toString(), work.resolve("csv").toString(), work.resolve("baseline").toString()}
                : new String[] {"prepare-cdc", "--root", root.toString(), config.toString(), work.resolve("csv").toString()};
        int status = org.sibench.cli.Sibench.run(command,
                new java.io.PrintStream(output), new java.io.PrintStream(errors));
        assertEquals(errors.toString(java.nio.charset.StandardCharsets.UTF_8), 0, status);
        assertEquals("passed", CdcInput.check(work.resolve("input")).get("status"));
        assertFalse(Files.exists(work.resolve("input/database/data")));assertFalse(Files.exists(work.resolve("input/trace")));
        var document = com.google.gson.JsonParser.parseString(Files.readString(work.resolve("input/manifest.json"))).getAsJsonObject();
        assertEquals(csvIdentity, document.getAsJsonObject("source_evidence").get("csv_manifest_sha256").getAsString());
        assertEquals(csvIdentity, CsvInput.open(work.resolve("csv")).sha256());
        if (parallel) {
            var baseline = PixelsBaseline.open(work.resolve("baseline"));
            PixelsBaseline.match(baseline, document);
            assertFalse(Files.exists(work.resolve("baseline/pixels")));
            var storeConfig = root.resolve(System.getProperty("sibench.storage.config"));
            try (var store = new InputStore(org.sibench.config.ObjectStore.of(storeConfig, "cos", "sibench/inputs/"))) {
                var published = store.publish(work.resolve("baseline"));
                store.fetch(published.get("manifest_sha256").toString(), work.resolve("baseline-cache"));
                assertEquals(baseline.sha256(), PixelsBaseline.open(work.resolve("baseline-cache")).sha256());
                Files.writeString(work.resolve("baseline-publication.json"), new com.google.gson.GsonBuilder().setPrettyPrinting().create().toJson(published));
            }
        }
        assertTrue(document.getAsJsonObject("counts").get("businessChanges").getAsLong() > 0);
        assertEquals(2, document.getAsJsonObject("counts").get("markers").getAsInt());
        for (String phase : List.of("tp", "xp")) {
            var properties = new java.util.Properties();try (var file = Files.newInputStream(work.resolve("input/" + phase + "/resolved.props"))) { properties.load(file); }
            assertEquals("0", properties.getProperty("apclient"));assertEquals("0", properties.getProperty("xapclient"));
        }
        Files.writeString(work.resolve("acceptance.json"), new com.google.gson.GsonBuilder().setPrettyPrinting().create().toJson(
                Map.of("status", "passed", "input", work.resolve("input").toString(), "counts", document.getAsJsonObject("counts"))));
        return work;
    }

    @Test public void realPostgresPublishesPortableCompleteRowChangesWithoutPixels() throws Exception {
        run(false);
    }

    @Test public void terminatedCaptureCannotPublishACompleteInput() throws Exception {
        run(true);
    }

    static Path prepareInput() throws Exception { return run(false); }

    private static Path run(boolean terminate) throws Exception {
        Path root = Path.of("").toRealPath();
        String configured = System.getProperty("sibench.pixels.build");
        assertNotNull("Set sibench.pixels.build", configured);
        Path build = root.resolve(configured).toRealPath();
        var identity = PixelsBuild.verify(root, build);
        var source = SourceSchema.hybench(root.resolve("benchmark/HyBench-2024"));
        var settings = new LinkedHashMap<>(SibenchConfig.engine(root, SibenchConfig.read(root.resolve("config/sibench.yaml")), Engine.PIXELS));
        var overrides = new LinkedHashMap<>(object(settings, "sink_properties"));
        overrides.put("sink.proto.maxRecords", "2");settings.put("sink_properties", overrides);
        Path output = Files.createTempDirectory(root.resolve("results/capture"), "cdc-source-check-");
        var csvFixture = CsvInputTest.fixture(Files.createDirectory(output.resolve("csv-source")));
        var csv = CsvInput.open(csvFixture.output());
        CsvInput.materialize(csv, output);
        Path data = output.resolve("native/Data_1x");
        Path spec = output.resolve("pixels-schema.tsv");
        var sourceSettings = new LinkedHashMap<>(SibenchConfig.postgresSettings(object(object(settings, "source"), "settings")));
        sourceSettings.putAll(Map.of("wal_level", "logical", "max_wal_senders", "4", "max_replication_slots", "4"));
        var baseline = new LinkedHashMap<String, Long>(); source.tables().forEach(t -> baseline.put(t.name(), 1L));
        var ids = new ArrayList<String>();
        CdcInput.Counts counts;
        try (var database = PostgresDatabase.create(build.resolve("postgres/runtime"), output.resolve("database"), sourceSettings)) {
            try (var connection = database.connect("load")) {
                assertEquals(baseline, Hybench.load(connection, source.source(), data));
                PixelsGeneration.prepareSource(connection, source);
            }
            PixelsGeneration.createSlot(database, source);
            try (var capture = CdcCapture.start(build, settings, output.resolve("sink"), spec, database.port())) {
                PixelsGeneration.awaitSlot(database, capture.process(), capture::check);
                if (terminate) {
                    capture.process().destroyForcibly();
                    assertTrue(capture.process().waitFor(10, java.util.concurrent.TimeUnit.SECONDS));
                    assertThrows(java.io.IOException.class, capture::check);
                    assertFalse(Files.exists(output.resolve("manifest.json")));
                    assertFalse(Files.exists(output.resolve("cdc/files.json")));
                    return null;
                }
                try (var first = database.connect("first"); var second = database.connect("second")) {
                    first.setAutoCommit(false); second.setAutoCommit(false);
                    String firstId = id(first);
                    execute(first, "UPDATE savingaccount SET balance=2100 WHERE accountid=1");
                    String secondId = id(second);
                    execute(second, "UPDATE checkingaccount SET balance=2500 WHERE accountid=1");
                    second.commit(); ids.add(secondId);
                    execute(first, "UPDATE savingaccount SET balance=2200 WHERE accountid=1");
                    first.commit(); ids.add(firstId);
                }
                try (var connection = database.connect("changes")) {
                    connection.setAutoCommit(false);
                    ids.add(id(connection));execute(connection, "DELETE FROM savingaccount WHERE accountid=1");connection.commit();
                    ids.add(id(connection));execute(connection, "INSERT INTO savingaccount VALUES(1,1,2300,0,'2023-01-01',NULL)");connection.commit();
                    execute(connection, "UPDATE savingaccount SET balance=9999 WHERE accountid=1");connection.rollback();
                    ids.add(id(connection));
                    execute(connection, "UPDATE savingaccount SET balance=2400 WHERE accountid=1");
                    execute(connection, "UPDATE checkingaccount SET balance=2600 WHERE accountid=1");connection.commit();
                    assertEquals(2400, balance(connection, "savingaccount"));
                    assertEquals(2600, balance(connection, "checkingaccount"));
                    connection.rollback();
                }
                ids.add(CdcPreparation.marker(database, capture, 1));
                capture.finish();counts = capture.counts();
                assertEquals(new CdcInput.Counts(6, 8, 7, 1), counts);
            }
        }
        var selection = new BenchmarkSelection(root.resolve("config/benchmark/hybench.yaml"), "hybench", BenchmarkSelection.Mode.CDC, Engine.PIXELS);
        var input = new Generation.Input(selection, Map.of("native", Map.of("sf", "1x")), source, root, output, Map.of(), baseline, Map.of());
        CdcInput.publish(output, input, counts, Map.of("build", identity, "csv_manifest_sha256", csv.sha256(),
                "benchmark_source", Map.of("commit", source.sourceCommit(), "fingerprints", source.fingerprints())));
        try (var files = Files.list(output.resolve("cdc"))) { assertEquals(3, files.filter(p -> p.toString().endsWith(".proto")).count()); }
        Generation.deleteTree(output.resolve("database/data"));
        var observed = new ArrayList<String>();
        var changes = new ArrayList<String>();
        try (var files = Files.list(output.resolve("cdc"))) {
            for (Path file : files.filter(p -> p.toString().endsWith(".proto")).sorted().toList()) {
                try (var stream = new DataInputStream(Files.newInputStream(file))) {
                    while (stream.available() > 0) {
                        int key = stream.readInt(), size = stream.readInt();byte[] bytes = stream.readNBytes(size);
                        if (key == -1) {
                            var transaction = SinkProto.TransactionMetadata.parseFrom(bytes);
                            if (transaction.getStatus() == SinkProto.TransactionStatus.END) observed.add(transaction.getId());
                        } else {
                            var row = SinkProto.RowRecord.parseFrom(bytes);
                            if (!row.getSource().getTable().endsWith("account")) continue;
                            String before = row.hasBefore() ? Float.toString(ByteBuffer.wrap(row.getBefore().getValues(2).getValue().toByteArray()).getFloat()) : "absent";
                            String after = row.hasAfter() ? Float.toString(ByteBuffer.wrap(row.getAfter().getValues(2).getValue().toByteArray()).getFloat()) : "absent";
                            changes.add(row.getSource().getTable() + ":" + before + ":" + after);
                        }
                    }
                }
            }
        }
        assertEquals(ids, observed);
        assertEquals(List.of("checkingaccount:2000.0:2500.0", "savingaccount:2000.0:2100.0", "savingaccount:2100.0:2200.0",
                "savingaccount:2200.0:absent", "savingaccount:absent:2300.0", "savingaccount:2300.0:2400.0", "checkingaccount:2500.0:2600.0"), changes);
        Path moved = output.resolveSibling(output.getFileName() + "-portable");Files.move(output, moved);
        assertEquals("passed", CdcInput.check(moved).get("status"));
        assertFalse(Files.exists(moved.resolve("trace")));
        Files.writeString(moved.resolve("acceptance.json"), new com.google.gson.GsonBuilder().setPrettyPrinting().create()
                .toJson(Map.of("status", "passed", "source_commit_order", ids, "source_observed_changes", changes, "counts", counts)));
        System.out.println("Published CDC input: " + moved);
        return moved;
    }

    private static String id(Connection connection) throws Exception {
        try (var statement = connection.createStatement(); var result = statement.executeQuery("SELECT txid_current()::text")) {
            assertTrue(result.next()); return result.getString(1);
        }
    }
    private static void execute(Connection connection, String sql) throws Exception { try (var statement = connection.createStatement()) { assertEquals(1, statement.executeUpdate(sql)); } }
    private static int balance(Connection connection, String table) throws Exception {
        try (var statement = connection.createStatement();var rows = statement.executeQuery("SELECT balance FROM " + table + " WHERE accountid=1")) {
            assertTrue(rows.next());return rows.getInt(1);
        }
    }
}
