package org.sibench.capture;

import static org.junit.Assert.*;
import static org.sibench.config.ConfigValues.*;
import com.google.gson.GsonBuilder;
import java.nio.file.*;
import java.util.*;
import org.junit.Test;
import org.sibench.trace.TraceManifest;

/** Explicit real upstream A/B/C/F/D/E generation and independent replay; the native database is discarded. */
public class RocksGenerationCheck {
    @Test public void upstreamApiExportsFinalTraceWithoutNativeData() throws Exception {
        for(boolean queries:List.of(false,true))generate(queries);
    }
    private void generate(boolean queries) throws Exception {
        Path root = Path.of("").toRealPath();
        String runtime = System.getProperty("sibench.rocksdb.build"), client = System.getProperty("sibench.ycsb.build");
        assertNotNull("Set sibench.rocksdb.build", runtime); assertNotNull("Set sibench.ycsb.build", client);
        Path run = Files.createTempDirectory(root.resolve("results/capture"), "rocks-generation-check-");
        var document = DuckDbGenerationTest.profile("ycsb"); var n = object(document, "native");
        n.put("recordcount", 1000); n.put("insertcount", 1000); n.put("build_directory", client);
        var workloads = object(n, "workloads");
        for (String name : workloads.keySet()) { var workload = object(workloads, name); workload.put("operationcount", 320); workloads.put(name, workload); }
        n.put("workloads", workloads); document.put("native", n);
        document.put("trace", Map.of("secondary_indexes", false, "export_queries", queries, "workload_requests", 2400));
        document.put("artifacts", Map.of("directory", root.relativize(run.resolve("generated")).toString()));
        var json = new GsonBuilder().setPrettyPrinting().create();
        Path profile = run.resolve("profile.json"); Files.writeString(profile, json.toJson(document));
        Path engine = run.resolve("rocksdb.json"); Files.writeString(engine, json.toJson(Map.of("schema_version", 1, "purpose", "engine",
                "engine", "rocksdb", "build_directory", runtime, "cache_bytes", 32L << 20, "write_buffer_bytes", 4L << 20, "background_jobs", 2)));
        Path config = run.resolve("runtime.json"); Files.writeString(config, json.toJson(Map.of(
                "schema_version", 1, "purpose", "sibench", "benchmarks", List.of(Map.of("config", profile.toString(), "mode", "api", "engine", "rocksdb")),
                "engines", Map.of("rocksdb", engine.toString()))));
        var result = Generation.run(root, config).get(0);
        assertEquals(1000L, result.get("baseline_rows")); assertEquals(1000L, result.get("baseline_operations"));
        assertEquals(0L, result.get("aborted_transactions"));
        assertEquals("primary_only", result.get("secondary_index_coverage"));
        var budget=object(object(result,"source_evidence"),"request_budget");
        assertTrue(((Number)budget.get("effective_requests")).longValue()>=2400);
        assertEquals(6,object(budget,"stages").size());
        var query=object(object(budget,"stages"),"workloadc");
        assertEquals(queries,((Number)query.get("effective_requests")).longValue()>0);
        assertEquals(queries,((Number)query.get("target_requests")).longValue()>0);
        assertTrue(Files.isRegularFile(run.resolve("generated/workloadc/batch-0/summary.json")));
        if(!queries)assertFalse(Files.exists(run.resolve("generated/workloadc/batch-1")));
        long independentReads=0;
        for(String name:workloads.keySet())if(!name.equals("workloadf")) {
            try(var batches=Files.list(run.resolve("generated/"+name))) {
                for(Path batch:batches.toList()) {
                    var summary=parse(Files.readString(batch.resolve("summary.json")));
                    for(String kind:List.of("READ","SCAN"))if(summary.containsKey(kind))
                        independentReads+=((Number)object(summary,kind).get("Operations")).longValue();
                }
            }
        }
        var files=(List<?>)object(result,"proto_files").get("workload");
        @SuppressWarnings("unchecked") var file=(Map<String,Object>)files.get(0);
        var projection=object(object(file,"statistics"),"source_projection");
        assertEquals(queries?0:independentReads,((Number)projection.get("excluded_query_search_members")).longValue());
        assertTrue(Files.exists(run.resolve("generated/workloada/batch-1")));
        Path trace = run.resolve("generated/trace");
        assertFalse(Files.exists(trace.resolve("INCOMPLETE")));
        org.sibench.replay.ReferenceReplay.run(trace, new org.sibench.replay.ReplayConfig(false, 0, 0, 0, 0, "operation"), run.resolve("reference.json"));
        var replay = new org.sibench.replay.ReplayConfig(false, 0, 0, 0, 0, "operation", 2, 64, 10);
        var service = new org.sibench.adapters.ServiceConfig("rocksdb", null, run.resolve("index"), 0,
                org.sibench.adapters.ValueKind.ROW_ID, null, false);
        assertEquals("passed", org.sibench.adapters.ServiceReplay.run(trace, replay, service, run.resolve("replay.json")).get("status"));
        assertFalse(TraceManifest.read(trace).has("native_data"));
        assertFalse(Files.exists(trace.resolve("data")));
        assertFalse(Files.exists(run.resolve("generated/database")));
        var rejected = assertThrows(IllegalArgumentException.class, () -> Generation.run(root, config));
        assertTrue(rejected.getMessage().contains("output already exists"));
    }
}
