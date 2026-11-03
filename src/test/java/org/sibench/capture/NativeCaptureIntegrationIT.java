package org.sibench.capture;

import static org.junit.Assert.*;

import org.junit.Test;
import org.sibench.config.ConfigValues;
import org.sibench.replay.ReferenceReplay;
import org.sibench.replay.ReplayConfig;

import java.nio.file.*;
import java.util.*;

/** Explicit small native-client acceptance; requires the pinned clients and instrumented PG. */
public class NativeCaptureIntegrationIT {
    @Test(timeout = 120000)
    public void nativeClientsPublishReplayableProtoWithoutRawFiles() throws Exception {
        Path root = Path.of("").toAbsolutePath();
        Path run = Path.of(Objects.requireNonNull(System.getenv("SIBENCH_DIRECT_RUN")));
        String admin = Objects.requireNonNull(System.getenv("SIBENCH_DIRECT_PG_JDBC"));
        int port = Integer.parseInt(admin.substring(admin.lastIndexOf(':') + 1, admin.lastIndexOf('/')));
        for (String benchmark : List.of("sysbench", "ycsb")) {
            Path profile = profile(root, run.resolve(benchmark), benchmark, port, 64);
            var capture = CaptureProfile.read(root, profile);
            String jdbc = NativeCaptureConfig.read(root, profile).jdbc();
            stage(root, profile, "load");
            if (benchmark.equals("ycsb")) {
                // A successful client process does not establish that its final batch reached PG.
                Path shortProfile = profile(root, run.resolve("ycsb-incomplete-load"), benchmark, port, 65);
                var shortCapture = CaptureProfile.read(root, shortProfile);
                assertTrue(assertThrows(java.io.IOException.class, () ->
                        PgBaselineExport.export(jdbc, shortCapture.baseline(), shortCapture.layout()))
                        .getMessage().contains("different row count"));
                assertFalse(Files.exists(shortCapture.baseline().resolve(BaselineManifest.FILE)));
            }
            stage(root, profile, "export");
            CaptureCompleteness.snapshot(jdbc, "postgres",
                    capture.artifacts().resolve("index-counters-before.json"), profile.toString());
            stage(root, profile, "run");
            Path trace = capture.artifacts().resolve("trace");
            var manifest = org.sibench.trace.TraceManifest.read(trace);
            assertEquals("proto", manifest.get("output").getAsString());
            assertEquals("direct_source_events", manifest.get("generation").getAsString());
            assertEquals("consistent", manifest.get("capture_completeness").getAsString());
            assertEquals(64, manifest.get("baseline_rows").getAsLong());
            assertTrue(manifest.get("source_work_units").getAsLong() > 0);
            assertFalse(Files.exists(capture.artifacts().resolve("transactions")));
            try (var files = Files.list(capture.raw())) {
                assertFalse(files.anyMatch(path -> path.toString().endsWith(".jsonl.zst")));
            }
            Path reference = capture.artifacts().resolve("reference.json");
            assertEquals("passed", ReferenceReplay.run(trace, ReplayConfig.defaults(), List.of(true), reference).get("status"));
            var service = new org.sibench.adapters.ServiceConfig("rocksdb", null, capture.artifacts().resolve("index"), 0,
                    org.sibench.adapters.proto.ValueKind.ROW_ID, null, false);
            assertEquals("passed", org.sibench.adapters.ServiceReplay.run(trace, ReplayConfig.defaults(), service,
                    reference, capture.artifacts().resolve("service.json")).get("status"));
        }
    }

    static void stage(Path root, Path profile, String stage) throws Exception {
        Path executable = Path.of(System.getProperty("java.home"), "bin/java");
        var settings = org.sibench.config.SibenchConfig.read(profile.getParent().resolve("sibench.yaml"));
        var budget = new GenerationBudget(ConfigValues.object(settings, "generation"),
                DirectCapture.budgetPath(profile.getParent().getParent()));
        var result = NativeProcess.run("stage-" + stage,
                List.of(executable.toString(), "-Xmx1g", "-cp", root.resolve("build/sibench/sibench.jar").toString(),
                        NativeCapture.class.getName(), stage, root.toString(), profile.toString()),
                Map.of("SIBENCH_BUDGET_STOP", budget.enabled()
                        ? DirectCapture.budgetPath(profile.getParent().getParent()).toString() : ""),
                root, profile.getParent(), java.time.Duration.ofSeconds(90));
        assertEquals(Files.readString(result.err()), 0, result.exit());
    }

    static Path profile(Path root, Path artifacts, String benchmark, int port, int rows) throws Exception {
        Files.createDirectories(artifacts.resolve("native"));
        Files.createDirectory(artifacts.resolve("raw"));
        var runtime = org.sibench.config.SibenchConfig.read(root.resolve("config/sibench.yaml"));
        var generation = ConfigValues.object(runtime, "generation");
        generation.put("operation_target", 0);
        generation.put("output_byte_target", 0);
        runtime.put("generation", generation);
        Files.writeString(artifacts.resolve("native/sibench.yaml"), new org.yaml.snakeyaml.Yaml().dump(runtime));
        var document = ConfigValues.parse(Files.readString(root.resolve("config/benchmark/" + benchmark + ".yaml")));
        var settings = ConfigValues.object(document, "native");
        settings.put("threads", 1);
        settings.put("timeout_minutes", 1);
        if (benchmark.equals("sysbench")) {
            settings.put("tables", 1);
            settings.put("table-size", rows);
            settings.put("prepare_threads", 1);
        } else {
            settings.put("recordcount", rows);
            settings.put("insertcount", rows);
            settings.put("load_threads", 1);
            settings.put("load_batch_size", 16);
        }
        var workloads = ConfigValues.object(settings, "workloads");
        for (String name : workloads.keySet()) {
            var workload = ConfigValues.object(workloads, name);
            workload.put(benchmark.equals("sysbench") ? "events" : "operationcount", 4);
            workloads.put(name, workload);
        }
        settings.put("workloads", workloads);
        document.put("native", settings);
        var postgres = ConfigValues.object(document, "postgres");
        postgres.put("port", port);
        postgres.put("database", "native_probe_" + benchmark);
        document.put("postgres", postgres);
        document.put("artifacts", Map.of("directory", root.relativize(artifacts).toString()));
        var capture = ConfigValues.object(document, "capture");
        capture.put("raw_directory", root.relativize(artifacts.resolve("raw")).toString());
        document.put("capture", capture);
        Path profile = artifacts.resolve("native/benchmark.yaml");
        Files.writeString(profile, new org.yaml.snakeyaml.Yaml().dump(document));
        return profile;
    }
}
