package org.sibench.capture;

import static org.junit.Assert.*;
import static org.sibench.config.ConfigValues.*;
import com.google.gson.GsonBuilder;
import java.nio.file.*;
import java.util.*;
import org.junit.Test;
import org.sibench.trace.TraceManifest;

/** Explicit real upstream A/B/C/F/D/E generation, independent replay and relocated native reads. */
public class RocksGenerationCheck {
    @Test public void upstreamApiExportsFinalTraceAndRetainedRows() throws Exception {
        Path root = Path.of("").toRealPath();
        String runtime = System.getProperty("sibench.rocksdb.build"), client = System.getProperty("sibench.ycsb.build");
        assertNotNull("Set sibench.rocksdb.build", runtime); assertNotNull("Set sibench.ycsb.build", client);
        Path run = Files.createTempDirectory(root.resolve("results/capture"), "rocks-generation-check-");
        var document = DuckDbGenerationTest.profile("ycsb"); var n = object(document, "native");
        n.put("recordcount", 1000); n.put("insertcount", 1000); n.put("build_directory", client);
        var workloads = object(n, "workloads");
        for (String name : workloads.keySet()) { var workload = object(workloads, name); workload.put("operationcount", 320); workloads.put(name, workload); }
        n.put("workloads", workloads); document.put("native", n);
        document.put("artifacts", Map.of("directory", root.relativize(run.resolve("generated")).toString()));
        var json = new GsonBuilder().setPrettyPrinting().create();
        Path profile = run.resolve("profile.json"); Files.writeString(profile, json.toJson(document));
        Path config = run.resolve("runtime.json"); Files.writeString(config, json.toJson(Map.of(
                "schema_version", 1, "purpose", "sibench", "benchmarks", List.of(Map.of("config", profile.toString(), "mode", "api", "engine", "rocksdb")),
                "rocksdb", Map.of("build_directory", runtime, "cache_bytes", 32L << 20, "write_buffer_bytes", 4L << 20, "background_jobs", 2))));
        var result = Generation.run(root, config).get(0);
        assertEquals(1000L, result.get("baseline_rows")); assertEquals(1000L, result.get("baseline_operations"));
        assertEquals(0L, result.get("aborted_transactions"));
        Path trace = run.resolve("generated/trace");
        assertFalse(Files.exists(trace.resolve("INCOMPLETE")));
        org.sibench.replay.ReferenceReplay.run(trace, new org.sibench.replay.ReplayConfig(true, 0, 0, 0, 0, "operation"), run.resolve("reference.json"));
        var replay = new org.sibench.replay.ReplayConfig(true, 0, 0, 0, 0, "operation", 2, 64, 10);
        var service = new org.sibench.adapters.ServiceConfig("rocksdb", null, run.resolve("index"), 0,
                org.sibench.adapters.proto.ValueKind.ROW_ID, null, false);
        assertEquals("passed", org.sibench.adapters.ServiceReplay.run(trace, replay, service, run.resolve("replay.json")).get("status"));
        var manifest = TraceManifest.read(trace);
        var originals = new ArrayList<IndexOperationTrace.Operation>();
        ProtoTrace.source(trace, manifest, "baseline").read(originals::add);
        Path moved = run.resolve("relocated"); Files.move(trace, moved);
        try (var rows = TraceManifest.openData(moved, root.resolve(runtime))) {
            for (var original : originals) {
                var row = rows.snapshotRow(1, original.rowId(), 0).orElseThrow();
                assertEquals(original.key(), org.sibench.trace.IndexKey.ofText((String) row.get("ycsb_key")));
                for (int i = 0; i < 10; i++) assertEquals(100, ((byte[]) row.get("field" + i)).length);
            }
        }
        var rejected = assertThrows(IllegalArgumentException.class, () -> Generation.run(root, config));
        assertTrue(rejected.getMessage().contains("output already exists"));
    }
}
