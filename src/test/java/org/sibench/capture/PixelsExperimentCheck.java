package org.sibench.capture;

import static org.junit.Assert.*;
import static org.sibench.config.ConfigValues.*;
import static org.sibench.capture.IndexOperationTrace.*;
import com.google.gson.*;
import java.nio.file.*;
import java.util.*;
import org.junit.Test;
import org.sibench.config.*;
import org.sibench.schema.SourceSchema;
import org.sibench.trace.IndexKey;

/** Explicit small real-service checks; not part of the ordinary unit test command. */
public class PixelsExperimentCheck {
    private static Path capturedInput;
    private static final Map<Path, Path> baselines = new HashMap<>();
    @Test public void finiteFilesApplyWithoutTraceAndExportOptionalTrace() throws Exception {
        Path input = input();
        for (boolean trace : new boolean[] {false, true}) {
            Path output = run(input, trace, true, 0, false, false);
            var result = read(output.resolve("result.json"));
            assertEquals(6, result.get("committed_transactions").getAsLong());
            assertEquals(8, result.get("committed_changes").getAsLong());
            assertEquals(trace, Files.exists(output.resolve("trace")));
            assertFalse(Files.exists(output.resolve("pixels")));
            assertFalse(Files.exists(output.resolve("database")));
            assertFalse(Files.exists(output.resolve("queries")));
            assertTargetIdsDifferFromCapture(output);
            if (trace) verifyTrace(output.resolve("trace"), 5);
        }
    }

    @Test public void completePrefixAndStaticModesDoNotConsumeExtraTransactions() throws Exception {
        Path input = input();
        Path prefix = run(input, true, true, 2, false, false);
        assertEquals(2, read(prefix.resolve("result.json")).get("committed_transactions").getAsLong());
        assertEquals(3, read(prefix.resolve("result.json")).get("committed_changes").getAsLong());
        verifyTrace(prefix.resolve("trace"), 2);
        Path baseline = run(input, false, false, 0, false, false);
        assertEquals(0, read(baseline.resolve("result.json")).get("committed_transactions").getAsLong());
        assertFalse(Files.exists(baseline.resolve("sink")));
    }

    @Test public void trinoReadsStaticAndUpdatedRowsAndSinkFreshnessIsIndependent() throws Exception {
        assertNotNull("Set sibench.trino.deployment on the Pixels host", System.getProperty("sibench.trino.deployment"));
        Path input = input();
        run(input, false, false, 0, true, false);
        run(input, false, true, 0, true, false);
        Path freshness = run(input, false, true, 0, false, true);
        assertFalse(Files.exists(freshness.resolve("queries")));
        assertFreshness(freshness);
    }

    @Test public void concurrentQueriesAndSinkFreshnessPreserveCommittedCounts() throws Exception {
        assertNotNull("Set sibench.trino.deployment on the Pixels host", System.getProperty("sibench.trino.deployment"));
        Path output = run(input(), false, true, 0, true, true, "during");
        assertEquals(6, read(output.resolve("result.json")).get("committed_transactions").getAsLong());
        try (var files = Files.list(output.resolve("queries"))) {
            assertEquals(3, files.filter(p -> p.toString().endsWith("-result.json")).count());
        }
        assertTargetIdsDifferFromCapture(output);
        assertFreshness(output);
    }

    private static void assertFreshness(Path freshness) throws Exception {
        assertTrue(Files.size(freshness.resolve("sink/freshness.csv")) > 0);
        for (String line : Files.readAllLines(freshness.resolve("sink/freshness.csv"))) {
            double age = Double.parseDouble(line.split(",")[1]);
            assertTrue("Freshness must measure this run rather than a timezone offset: " + age, age >= 0 && age < 120000);
        }
    }

    private static void assertTargetIdsDifferFromCapture(Path output) throws Exception {
        var tables = Files.readAllLines(output.resolve("pixels-tables.tsv"));
        String customer = tables.stream().filter(line -> line.startsWith("customer\t")).findFirst().orElseThrow();
        assertNotEquals("customer's capture-local frame ID is 1", 1L, Long.parseLong(customer.split("\t")[1]));
    }

    private static Path input() throws Exception {
        String configured = System.getProperty("sibench.cdc.input");
        if (configured != null) return Path.of(configured).toRealPath();
        if (capturedInput == null) capturedInput = CdcPreparationCheck.prepareInput();
        return capturedInput;
    }

    private static Path run(Path input, boolean trace, boolean updates, int prefix, boolean queries, boolean freshness) throws Exception {
        return run(input, trace, updates, prefix, queries, freshness, "after");
    }

    private static Path run(Path input, boolean trace, boolean updates, int prefix, boolean queries, boolean freshness, String timing) throws Exception {
        Path root = Path.of("").toRealPath();
        Path directory = Files.createTempDirectory(root.resolve("results/capture"), "pixels-file-check-");
        var engine = parse(Files.readString(root.resolve("config/engine/pixels/pixels.yaml")));
        engine.put("build_directory", System.getProperty("sibench.pixels.build"));
        engine.put("pixels_host", System.getProperty("sibench.pixels.host", "local"));
        engine.put("memory", Map.of("coordinator_heap", "1g", "retina_heap", "4g", "sink_heap", "1g"));
        engine.put("properties", root.resolve("config/engine/pixels/pixels.properties").toString());
        engine.put("sink_properties", root.resolve("config/engine/pixels/pixels-sink.properties").toString());
        String storage = System.getProperty("sibench.storage.config");
        assertNotNull("Set sibench.storage.config for COS baselines", storage);
        engine.put("data_storage", Map.of("config", root.resolve(storage).toString(), "profile", System.getProperty("sibench.storage.profile", "cos"),
                "prefix", "sibench/pixels/check-" + UUID.randomUUID()));
        var gson = new GsonBuilder().setPrettyPrinting().create();
        Path engineConfig = Files.writeString(directory.resolve("engine.json"), gson.toJson(engine));
        var experiment = org.sibench.config.ConfigValues.object(org.sibench.config.SibenchConfig.read(java.nio.file.Path.of("config/sibench.yaml")), "pixels_experiment");
        experiment.putAll(Map.of("trace", trace, "updates", updates, "max_transactions", prefix));
        Path sql = Files.createDirectory(directory.resolve("sql"));
        if (queries) {
            int saving = updates ? 2400 : 2000, checking = updates ? 2600 : 2000;
            if (timing.equals("during")) {
                Files.writeString(sql.resolve("01-saving.sql"), "SELECT 1 / IF(count(*)<=1 AND (min(balance) IS NULL OR min(balance) IN (2000,2200,2300,2400)),1,0) FROM savingaccount");
                Files.writeString(sql.resolve("02-checking.sql"), "SELECT 1 / IF(count(*)=1 AND min(balance) IN (2000,2500,2600),1,0) FROM checkingaccount");
                Files.writeString(sql.resolve("03-lookup.sql"), "SELECT 1 / IF(count(*)<=1,1,0) FROM savingaccount WHERE accountid=1");
            } else {
                Files.writeString(sql.resolve("01-saving.sql"), "SELECT 1 / IF(count(*)=1 AND min(balance)=" + saving + ",1,0) FROM savingaccount");
                Files.writeString(sql.resolve("02-checking.sql"), "SELECT 1 / IF(count(*)=1 AND min(balance)=" + checking + ",1,0) FROM checkingaccount");
                Files.writeString(sql.resolve("03-lookup.sql"), "SELECT 1 / IF(count(*)=1,1,0) FROM savingaccount WHERE accountid=1");
            }
        }
        experiment.put("queries", Map.of("enabled", queries, "directory", sql.toString(), "timing", timing, "rounds", 1));
        experiment.put("freshness", Map.of("enabled", freshness, "table", "savingaccount", "column", "fresh_ts", "sample_seconds", 5));
        if (queries || freshness) {
            var trino = object(experiment, "trino");
            trino.put("deployment_directory", System.getProperty("sibench.trino.deployment"));
            trino.putAll(Map.of("heap", "8G", "query_memory", "2GB", "query_total_memory", "4GB", "heap_headroom", "2GB", "threads", 4));
            experiment.put("trino", trino);
        }
        // Baseline preparation takes its bound from the selected HyBench profile, as CDC preparation does.
        var runtime = Map.of("schema_version", 1, "purpose", "sibench", "engines", Map.of("pixels", engineConfig.toString()), "pixels_experiment", experiment,
                "benchmarks", List.of(Map.of("config", "config/benchmark/hybench.yaml", "mode", "capture", "engine", "postgres")));
        Path config = Files.writeString(directory.resolve("runtime.json"), gson.toJson(runtime));
        Path output = directory.resolve("run");
        Path baseline = baselines.get(input);
        if (baseline == null) {
            baseline = directory.resolve("baseline");
            String configuredBaseline = System.getProperty("sibench.pixels.baseline");
            if (configuredBaseline == null) PixelsBaseline.prepare(root, config,
                    Path.of(System.getProperty("sibench.csv.input", input.resolve("csv-source").toString())), baseline);
            else baseline = Path.of(configuredBaseline);
            baselines.put(input, baseline);
        }
        String original = PixelsBaseline.open(baseline).sha256();
        PixelsExperiment.run(root, config, updates ? input : null, baseline, output);
        assertEquals(updates, Files.exists(output.resolve("input.json")));
        assertEquals(updates, read(output.resolve("result.json")).has("input_manifest_sha256"));
        assertEquals(original, PixelsBaseline.open(baseline).sha256());
        assertEquals(original, read(output.resolve("result.json")).get("baseline_manifest_sha256").getAsString());
        assertFalse("Restores must not re-encode CSV", Files.exists(output.resolve("pixels-inputs")));
        return output;
    }

    private static void verifyTrace(Path directory, int versions) throws Exception {
        var manifest = read(directory.resolve("manifest.json"));
        int table = 1 + SourceSchema.hybench(Path.of("benchmark/HyBench-2024")).tables().stream().map(SourceSchema.Table::name).toList().indexOf("savingaccount");
        try (var index = new IndexReferenceReplay(true, false)) {
            for (String stream : List.of("baseline", "workload")) ProtoTrace.source(directory, manifest, stream).read(op -> {
                if (!isControl(op)) index.acceptCommitted(op);
            });
            assertNotNull(index.lookupPrimary(table, IndexKey.ofInt(1), 0));
            assertNotNull(index.lookupPrimary(table, IndexKey.ofInt(1), 2));
            if (versions == 5) {
                assertNull(index.lookupPrimary(table, IndexKey.ofInt(1), 3));
                assertNotNull(index.lookupPrimary(table, IndexKey.ofInt(1), 4));
                assertEquals(5, index.lookupPrimary(table, IndexKey.ofInt(1), 5).commitVersion());
            }
        }
    }

    private static JsonObject read(Path path) throws Exception { return JsonParser.parseString(Files.readString(path)).getAsJsonObject(); }
}
