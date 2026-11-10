package org.sibench.capture;

import static org.junit.Assert.*;
import static org.sibench.config.ConfigValues.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.Test;

/**
 * Explicit small-data end-to-end acceptance of Pixels CDC generation on one host: real HyBench,
 * source PostgreSQL, pixels-sink, Retina and the tracer. Its reference replay, in both declared
 * value kinds, is outside production generation. Needs sibench.pixels.build (a two-phase
 * scripts/build-pixels.sh directory) and sibench.hybench.build.
 */
public class PixelsGenerationCheck {
    @Test public void hybenchCdcPublishesReplayableRowIdAndLocatorTrace() throws Exception {
        String build = System.getProperty("sibench.pixels.build"), client = System.getProperty("sibench.hybench.build");
        assertNotNull("Set sibench.pixels.build", build);
        assertNotNull("Set sibench.hybench.build", client);
        Path root = Path.of("").toRealPath();
        Path output = Files.createTempDirectory(root.resolve("results/capture"), "pixels-generation-");
        var document = DuckDbGenerationTest.profile("hybench");
        var config = object(document, "native");
        config.put("build_directory", client);
        config.put("sf", "1x");
        config.put("timeout_minutes", 30);
        config.put("clients", 4);
        config.put("xp_transaction_clients", 4);
        config.put("xp_query_clients", 2);
        config.put("parameters", Map.of("customer_number", 300, "company_number", 2, "transfer_number", 6000,
                "checking_number", 600, "loanapps_number", 600, "loantrans_number", 600));
        document.put("native", config);
        document.put("trace", Map.of("secondary_indexes", false, "workload_requests", 2000));
        document.put("artifacts", Map.of("directory", root.relativize(output.resolve("generated")).toString()));
        var yaml = new org.yaml.snakeyaml.Yaml();
        Path profile = output.resolve("profile.yaml"), settings = output.resolve("runtime.yaml"), engine = output.resolve("pixels.yaml");
        Files.writeString(profile, yaml.dump(document));
        var pixels = new java.util.LinkedHashMap<>(parse(Files.readString(root.resolve("config/engine/pixels/pixels.yaml"))));
        pixels.put("build_directory", build);
        pixels.put("memory", Map.of("coordinator_heap", "1g", "retina_heap", "4g", "sink_heap", "1g"));
        pixels.put("properties", root.resolve("config/engine/pixels/pixels.properties").toString());
        pixels.put("sink_properties", root.resolve("config/engine/pixels/pixels-sink.properties").toString());
        Files.writeString(engine, yaml.dump(pixels));
        Files.writeString(settings, yaml.dump(Map.of("schema_version", 1, "purpose", "sibench",
                "benchmarks", List.of(Map.of("config", profile.toString(), "mode", "cdc", "engine", "pixels")),
                "engines", Map.of("pixels", engine.toString()))));

        var manifest = Generation.run(root, settings).get(0);
        assertEquals(List.of("row_id", "locator"), manifest.get("value_kinds"));
        assertEquals(0L, manifest.get("aborted_transactions"));
        assertTrue(((Number) manifest.get("window_markers")).longValue() >= 2);
        var evidence = object(manifest, "source_evidence");
        assertEquals("HyBench TP and AT transactions; AP and IQ clients disabled", evidence.get("workload_scope"));
        assertEquals(java.util.Set.of("tp", "xp"), object(evidence, "stages").keySet());
        assertEquals(java.util.Set.of("tp", "xp"), object(object(evidence, "request_budget"), "stages").keySet());
        assertTrue(((Number) object(evidence, "request_budget").get("effective_requests")).longValue() >= 2000);
        assertFalse(Files.exists(output.resolve("generated/ap")));
        long transactions = 0;
        try (var batches = Files.list(output.resolve("generated/xp"))) {
            for (Path batch : batches.filter(Files::isDirectory).toList()) {
                var properties = new java.util.Properties();
                try (var file = Files.newInputStream(batch.resolve("resolved.props"))) { properties.load(file); }
                assertEquals("0", properties.getProperty("apclient"));
                assertEquals("0", properties.getProperty("xapclient"));
                assertEquals("4", properties.getProperty("xtpclient"));
                var counts = object(parse(Files.readString(batch.resolve("summary.json"))), "xp");
                for (int i = 1; i <= 6; i++) {
                    assertEquals("IQ must not run in CDC mode", 0L, ((Number) counts.get("iq" + i)).longValue());
                    transactions += ((Number) counts.get("at" + i)).longValue();
                }
            }
        }
        assertTrue("AT transactions must still run", transactions > 0);
        // Working state is gone after publication: source cluster data and the Pixels run directory.
        assertFalse(Files.exists(output.resolve("generated/database/data")));
        assertFalse(Files.exists(output.resolve("generated/pixels")));
        var report = org.sibench.replay.ReferenceReplay.run(output.resolve("generated/trace"),
                new org.sibench.replay.ReplayConfig(false, 0, 0, 0, 0, "operation"), output.resolve("reference.json"));
        assertEquals("passed", report.get("status"));
    }
}
