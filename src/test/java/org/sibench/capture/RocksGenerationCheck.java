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
    @Test public void upstreamRandomGeneratorUsesTheConfiguredSeed()throws Exception {
        String client=System.getProperty("sibench.ycsb.build");assertNotNull(client);
        var urls=new ArrayList<java.net.URL>();
        try(var files=Files.list(NativeBuildCheck.ycsbLibraries(Path.of(client)))) {
            for(Path jar:files.filter(p->p.toString().endsWith(".jar")).toList())urls.add(jar.toUri().toURL());
        }
        try(var loader=new java.net.URLClassLoader(urls.toArray(java.net.URL[]::new),getClass().getClassLoader())) {
            var utils=Class.forName("site.ycsb.Utils",true,loader);var seed=utils.getMethod("seed",long.class);
            seed.invoke(null,20261019L);var random=(java.util.Random)utils.getMethod("random").invoke(null);long expected=random.nextLong();
            seed.invoke(null,20261019L);assertEquals(expected,((java.util.Random)utils.getMethod("random").invoke(null)).nextLong());
            seed.invoke(null,20261020L);assertNotEquals(expected,((java.util.Random)utils.getMethod("random").invoke(null)).nextLong());
        }
    }
    @Test public void upstreamApiExportsFinalTraceWithoutNativeData() throws Exception {
        generate();
    }
    private void generate() throws Exception {
        Path root = Path.of("").toRealPath();
        String runtime = System.getProperty("sibench.rocksdb.build"), client = System.getProperty("sibench.ycsb.build");
        assertNotNull("Set sibench.rocksdb.build", runtime); assertNotNull("Set sibench.ycsb.build", client);
        Path run = Files.createTempDirectory(root.resolve("results/capture"), "rocks-generation-check-");
        var document = DuckDbGenerationTest.profile("ycsb"); var n = object(document, "native");
        n.put("recordcount", 1000); n.put("insertcount", 1000); n.put("build_directory", client);
        var workloads = object(n, "workloads");
        for (String name : workloads.keySet()) { var workload = object(workloads, name); workload.put("operationcount", 320); workloads.put(name, workload); }
        n.put("workloads", workloads); document.put("native", n);
        document.put("trace", Map.of("secondary_indexes", false, "workload_requests", 2400));
        document.put("artifacts", Map.of("directory", root.relativize(run.resolve("generated")).toString()));
        var json = new GsonBuilder().setPrettyPrinting().create();
        Path profile = run.resolve("profile.json"); Files.writeString(profile, json.toJson(document));
        Path engine = run.resolve("rocksdb.json"); Files.writeString(engine, json.toJson(Map.of("schema_version", 1, "purpose", "engine",
                "engine", "rocksdb", "build_directory", runtime, "cache_bytes", 32L << 20, "write_buffer_bytes", 4L << 20, "background_jobs", 2)));
        Path config = run.resolve("runtime.json"); Files.writeString(config, json.toJson(Map.of(
                "schema_version", 1, "purpose", "sibench", "benchmarks", List.of(Map.of("config", profile.toString(), "mode", "api", "engine", "rocksdb")),
                "engines", Map.of("rocksdb", engine.toString()))));
        var results=Generation.run(root,config);
        var names=List.of("workloada","workloadb","workloadc","workloadf","workloadd","workloade");
        assertEquals(names.size(),results.size());
        String initialIdentity=null,baselineIdentity=null;
        for(int i=0;i<names.size();i++) {
            String name=names.get(i);var result=results.get(i);Path output=run.resolve("generated/"+name);
            assertEquals(1000L,result.get("baseline_rows"));assertEquals(1000L,result.get("baseline_operations"));
            assertEquals("primary_only",result.get("secondary_index_coverage"));
            var evidence=object(result,"source_evidence");var initial=object(evidence,"initial_state");
            if(initialIdentity==null){initialIdentity=(String)initial.get("physical_sha256");baselineIdentity=(String)result.get("baseline_trace_sha256");}
            assertEquals(initialIdentity,initial.get("physical_sha256"));assertEquals(baselineIdentity,result.get("baseline_trace_sha256"));
            assertEquals(baselineIdentity,initial.get("baseline_trace_sha256"));
            var budget=object(evidence,"request_budget");
            assertTrue(((Number)budget.get("effective_requests")).longValue()>=2400);
            assertEquals(Set.of(name),object(budget,"stages").keySet());
            long sourceReads=0;
            try(var batches=Files.list(output.resolve("workload"))) {
                for(Path batch:batches.toList()) {
                    var summary=parse(Files.readString(batch.resolve("summary.json")));
                    for(String kind:List.of("READ","SCAN"))if(summary.containsKey(kind))
                        sourceReads+=((Number)object(summary,kind).get("Operations")).longValue();
                    var arguments=com.google.gson.JsonParser.parseString(Files.readString(batch.resolve("arguments.json"))).getAsJsonArray();
                    assertTrue(arguments.asList().stream().anyMatch(a->a.getAsString().endsWith("workloads/"+name)));
                    assertTrue(arguments.asList().stream().anyMatch(a->a.getAsString().equals("sibench.seed=20261019")));
                }
            }
            var files=(List<?>)object(result,"proto_files").get("workload");
            @SuppressWarnings("unchecked") var file=(Map<String,Object>)files.get(0);
            var statistics=object(file,"statistics");var types=object(statistics,"request_types");
            assertEquals(sourceReads,((Number)types.getOrDefault("get",0L)).longValue()
                    +((Number)types.getOrDefault("scan",0L)).longValue());
            if(name.equals("workloadc")) {
                assertEquals(Map.of("get",budget.get("effective_requests")),statistics.get("request_types"));
            }
            Path trace=output.resolve("trace");
            assertFalse(Files.exists(trace.resolve("INCOMPLETE")));
            assertEquals("passed",org.sibench.replay.ReferenceReplay.run(trace,new org.sibench.replay.ReplayConfig(false,0,0,0,0,"operation", 1, 4096, 60, 1),output.resolve("reference.json")).get("status"));
            var replay=new org.sibench.replay.ReplayConfig(false,0,0,0,0,"operation",2,64,10, 1);
            var service=new org.sibench.adapters.ServiceConfig("rocksdb",null,output.resolve("index"),0,org.sibench.adapters.ValueKind.ROW_ID,null,false,true);
            assertEquals("passed",org.sibench.adapters.ServiceReplay.run(trace,replay,service,output.resolve("replay.json")).get("status"));
            assertFalse(TraceManifest.read(trace).has("native_data"));assertFalse(Files.exists(output.resolve("database")));
        }
        assertTrue(Files.exists(run.resolve("generated/workloadc")));
        assertFalse(Files.exists(run.resolve("generated/initial")));
        assertTrue(Files.isRegularFile(run.resolve("generated/traces.json")));
        var rejected = assertThrows(IllegalArgumentException.class, () -> Generation.run(root, config));
        assertTrue(rejected.getMessage().contains("output already exists"));
    }
}
