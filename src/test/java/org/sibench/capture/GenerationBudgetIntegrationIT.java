package org.sibench.capture;

import static org.junit.Assert.*;

import org.junit.Test;
import org.sibench.config.ConfigValues;
import org.sibench.replay.ReferenceReplay;
import org.sibench.replay.ReplayConfig;
import org.sibench.trace.TraceManifest;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Explicit 64-row native acceptance of soft targets; requires patched clients and real PG. */
public class GenerationBudgetIntegrationIT {
    @Test(timeout = 180000)
    public void stopsNativeAdmissionAndPublishesOnlyCompleteAuditedTransactions() throws Exception {
        Path root = Path.of("").toAbsolutePath();
        Path run = Path.of(Objects.requireNonNull(System.getenv("SIBENCH_DIRECT_RUN")));
        String admin = Objects.requireNonNull(System.getenv("SIBENCH_DIRECT_PG_JDBC"));
        int port = Integer.parseInt(admin.substring(admin.lastIndexOf(':') + 1, admin.lastIndexOf('/')));
        for (String benchmark : List.of("sysbench", "ycsb"))
            for (String target : List.of("operation", "output", "startup")) {
                String name = "budget_" + target + "_" + benchmark;
                Path profile = NativeCaptureIntegrationIT.profile(root, run.resolve(name), benchmark, port, 64);
                var document = ConfigValues.parse(Files.readString(profile));
                var postgres = ConfigValues.object(document, "postgres");
                postgres.put("database", name);
                document.put("postgres", postgres);
                var nativeSettings = ConfigValues.object(document, "native");
                nativeSettings.put("threads", target.equals("startup") ? 32 : 2);
                var workloads = new LinkedHashMap<String, Object>();
                // RMW must finish its read and update even if the stop arrives between them.
                for (String workload : benchmark.equals("ycsb") ? List.of("workloadf", "workloada")
                        : List.of("oltp_read_write", "oltp_read_only"))
                    workloads.put(workload, Map.of(benchmark.equals("ycsb") ? "operationcount" : "events", 50000));
                nativeSettings.put("workloads", workloads);
                document.put("native", nativeSettings);
                if (target.equals("startup")) {
                    var output = ConfigValues.object(document, "capture");
                    output.put("output", "full");
                    document.put("capture", output);
                }
                Files.writeString(profile, new org.yaml.snakeyaml.Yaml().dump(document));
                var capture = CaptureProfile.read(root, profile);
                String jdbc = NativeCaptureConfig.read(root, profile).jdbc();
                NativeCaptureIntegrationIT.stage(root, profile, "load");
                NativeCaptureIntegrationIT.stage(root, profile, "export");
                long baselineBytes;
                try (var files = Files.walk(capture.baseline())) {
                    baselineBytes = files.filter(f -> f.toString().endsWith(".pb.zst"))
                            .mapToLong(f -> { try { return Files.size(f); } catch (java.io.IOException e) {
                                throw new java.io.UncheckedIOException(e); } }).sum();
                }
                Path runtime = capture.artifacts().resolve("native/sibench.yaml");
                var settings = ConfigValues.parse(Files.readString(runtime));
                var generation = ConfigValues.object(settings, "generation");
                generation.put("operation_target", 0);
                generation.put("output_byte_target", 0);
                generation.put(
                        target.equals("output") ? "output_byte_target" : "operation_target",
                        target.equals("output") ? baselineBytes + 64 : target.equals("startup") ? 1L : 40L);
                settings.put("generation", generation);
                Files.writeString(runtime, new org.yaml.snakeyaml.Yaml().dump(settings));
                CaptureCompleteness.snapshot(jdbc, "postgres",
                        capture.artifacts().resolve("index-counters-before.json"), profile.toString());
                NativeCaptureIntegrationIT.stage(root, profile, "run");
                Path trace = capture.artifacts().resolve("trace");
                var manifest = TraceManifest.read(trace);
                var budget = manifest.getAsJsonObject("generation_budget");
                assertTrue(budget.get("stop_requested").getAsBoolean());
                assertEquals(target.equals("output") ? "output_byte_target" : "operation_target",
                        budget.get("stop_reason").getAsString());
                assertEquals(manifest.get("workload_operations").getAsLong(), budget.get("actual_operations").getAsLong());
                long protoBytes = 0;
                for (String phase : List.of("baseline", "workload"))
                    for (var file : manifest.getAsJsonObject("proto_files").getAsJsonArray(phase))
                        protoBytes += Files.size(trace.resolve(file.getAsJsonObject().get("path").getAsString()));
                assertEquals(protoBytes, budget.get("actual_proto_bytes").getAsLong());
                assertEquals("consistent", manifest.get("capture_completeness").getAsString());
                assertFalse(Files.exists(trace.resolve("INCOMPLETE")));
                assertFalse(Files.exists(capture.artifacts().resolve("transactions")));
                var source = com.google.gson.JsonParser.parseString(Files.readString(
                        capture.raw().resolve("capture-run.json"))).getAsJsonObject();
                var completed = source.getAsJsonObject("client").getAsJsonArray("workloads");
                assertEquals(1, completed.size());
                var workload = completed.get(0).getAsJsonObject();
                long actual = workload.get(benchmark.equals("ycsb") ? "operationcount" : "events").getAsLong();
                assertTrue("Native work must stop before its requested 50000 operations", actual > 0 && actual < 50000);
                Path reference = capture.artifacts().resolve("reference.json");
                assertEquals("passed", ReferenceReplay.run(trace, ReplayConfig.defaults(), List.of(true), reference).get("status"));
                if (target.equals("startup"))
                    assertEquals("passed", org.sibench.replay.SnapshotLookupValidation.run(trace,
                            ReplayConfig.defaults(), capture.artifacts().resolve("oracle.json")).get("status"));
                var service = new org.sibench.adapters.ServiceConfig("rocksdb", null,
                        capture.artifacts().resolve("index"), 0, org.sibench.adapters.proto.ValueKind.ROW_ID, null, false);
                assertEquals("passed", org.sibench.adapters.ServiceReplay.run(trace, ReplayConfig.defaults(), service,
                        reference, capture.artifacts().resolve("service.json")).get("status"));
            }
    }
}
