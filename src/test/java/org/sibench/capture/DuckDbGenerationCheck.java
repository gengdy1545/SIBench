package org.sibench.capture;

import org.sibench.config.Digests;
import static org.junit.Assert.*;
import static org.sibench.config.ConfigValues.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.sibench.adapters.ServiceConfig;
import org.sibench.adapters.ServiceReplay;
import org.sibench.adapters.ValueKind;
import org.sibench.replay.ReplayConfig;
import org.sibench.trace.TraceManifest;
import org.yaml.snakeyaml.Yaml;

/** Real capture-source generation using the configured dispatcher and portable consumers. */
public class DuckDbGenerationCheck {
    @Rule public Timeout timeout=Timeout.seconds(600);
    @Test public void queryFilteringPreservesRealCaptureSupplyUnderBothSettings() throws Exception {
        Path root=Path.of("").toRealPath();
        String client=System.getProperty("sibench.sysbench.build"),host=System.getProperty("sibench.duckdb.host");
        assertNotNull(client);assertNotNull(host);
        for(boolean queries:List.of(false,true)) {
            Path output=Files.createTempDirectory(root.resolve("results/capture"),"duckdb-query-budget-");
            var document=DuckDbGenerationTest.profile("sysbench");var n=object(document,"native");
            n.put("tables",2);n.put("table-size",512);n.put("prepare_threads",2);n.put("threads",2);
            n.put("timeout_minutes",2);n.put("build_directory",client);
            var workloads=new LinkedHashMap<String,Object>();
            for(String name:object(n,"workloads").keySet())workloads.put(name,Map.of("events",10));
            n.put("workloads",workloads);document.put("native",n);
            document.put("trace",Map.of("secondary_indexes",false,"export_queries",queries,"workload_requests",1000));
            document.put("artifacts",Map.of("directory",root.relativize(output.resolve("generated")).toString()));
            var yaml=new Yaml();Path profile=output.resolve("profile.yaml"),engine=output.resolve("duckdb.yaml"),runtime=output.resolve("runtime.yaml");
            Files.writeString(profile,yaml.dump(document));
            Files.writeString(engine,yaml.dump(Map.of("schema_version",1,"purpose","engine","engine","duckdb",
                    "build_directory",Path.of(host).getParent().toString(),"memory_limit","1GiB","threads",2)));
            Files.writeString(runtime,yaml.dump(Map.of("schema_version",1,"purpose","sibench",
                    "benchmarks",List.of(Map.of("config",profile.toString(),"mode","capture","engine","duckdb")),
                    "engines",Map.of("duckdb",engine.toString()))));
            var result=Generation.run(root,runtime).get(0);
            var budget=object(object(result,"source_evidence"),"request_budget");
            assertTrue(((Number)budget.get("effective_requests")).longValue()>=1000);
            var query=object(object(budget,"stages"),"oltp_point_select");
            assertEquals(queries,((Number)query.get("effective_requests")).longValue()>0);
            assertEquals(queries,((Number)query.get("target_requests")).longValue()>0);
            assertTrue(Files.isDirectory(output.resolve("generated/workload/oltp_point_select/batch-0")));
            if(!queries)assertFalse(Files.exists(output.resolve("generated/workload/oltp_point_select/batch-1")));
            assertEquals("passed",org.sibench.replay.ReferenceReplay.run(output.resolve("generated/trace"),
                    new ReplayConfig(false,0,0,0,0,"operation"),output.resolve("reference.json")).get("status"));
            assertFalse(Files.exists(output.resolve("generated/data")));
        }
    }

    @Test public void generatesTheCaptureSuitesAndReplaysRelocatedTraces()throws Exception {
        Path root=Path.of("").toRealPath();String retained=System.getProperty("sibench.generation.output");
        Files.createDirectories(root.resolve("results/capture"));
        Path output=retained==null?Files.createTempDirectory(root.resolve("results/capture"),"generation-"):Files.createDirectory(Path.of(retained));
        assertTrue(output.startsWith(root.resolve("results/capture")));
        var selections=new java.util.ArrayList<Map<String,String>>();
        for(String benchmark:List.of("sysbench","hybench")) {
            var document=DuckDbGenerationTest.profile(benchmark);var n=object(document,"native");
            String build=System.getProperty("sibench."+benchmark+".build");assertNotNull("Set the "+benchmark+" build",build);
            n.put("build_directory",build);
            if(benchmark.equals("sysbench")) {n.put("tables",2);n.put("table-size",512);n.put("prepare_threads",2);}
            else {
                n.put("sf","1x");n.put("timeout_minutes",5);n.put("duration_minutes",1);n.remove("phase_duration_minutes");
                for(String key:List.of("clients","ap_clients","xp_transaction_clients","xp_query_clients"))n.put(key,1);
                n.put("parameters",Map.of("customer_number",300,"company_number",2,"transfer_number",6000,
                        "checking_number",600,"loanapps_number",600,"loantrans_number",600));
            }
            if(!benchmark.equals("hybench")) {
                n.put("threads",2);var workloads=object(n,"workloads");
                for(String name:workloads.keySet()) {
                    var w=object(workloads,name);w.put("events",10);workloads.put(name,w);
                }
                n.put("workloads",workloads);
            }
            document.put("native",n);
            document.put("trace",Map.of("secondary_indexes",false,"workload_requests",1000));
            document.put("artifacts",Map.of("directory",root.relativize(output.resolve(benchmark)).toString()));
            Path profile=output.resolve(benchmark+".yaml");Files.writeString(profile,new Yaml().dump(document));
            selections.add(Map.of("config",profile.toString(),"engine","duckdb","mode","capture"));
        }
        Files.writeString(output.resolve("unselected.yaml"),"invalid unselected profile: [");
        String host=System.getProperty("sibench.duckdb.host");assertNotNull("Set sibench.duckdb.host",host);
        Path engine=output.resolve("duckdb.yaml");
        Files.writeString(engine,new Yaml().dump(Map.of("schema_version",1,"purpose","engine","engine","duckdb",
                "build_directory",Path.of(host).getParent().toString(),"memory_limit","1GiB","threads",2)));
        var runtime=new LinkedHashMap<String,Object>(Map.of("schema_version",1,"purpose","sibench","benchmarks",selections,
                "engines",Map.of("duckdb",engine.toString())));
        Path config=output.resolve("runtime.yaml");
        var invalid=new LinkedHashMap<>(runtime);invalid.put("benchmarks",List.of(selections.get(0),Map.of("config",selections.get(1).get("config"),"engine","duckdb","mode","api")));
        Files.writeString(config,new Yaml().dump(invalid));
        assertThrows(IllegalArgumentException.class,()->Generation.run(root,config));
        assertFalse(Files.exists(output.resolve("sysbench")));assertFalse(Files.exists(output.resolve("hybench")));
        Files.writeString(config,new Yaml().dump(runtime));
        var stdout=new java.io.ByteArrayOutputStream();var stderr=new java.io.ByteArrayOutputStream();
        int status=org.sibench.cli.Sibench.run(new String[]{"generate","--root",root.toString(),config.toString()},
                new java.io.PrintStream(stdout),new java.io.PrintStream(stderr));
        Files.writeString(output.resolve("cli.out"),stdout.toString(java.nio.charset.StandardCharsets.UTF_8));
        Files.writeString(output.resolve("cli.err"),stderr.toString(java.nio.charset.StandardCharsets.UTF_8));
        assertEquals(stderr.toString(),0,status);
        assertEquals(2,com.google.gson.JsonParser.parseString(stdout.toString()).getAsJsonArray().size());
        Path relocated=Files.createDirectory(output.resolve("relocated"));
        Files.writeString(output.resolve("relocation.json"),"{\"rule\":\"<benchmark>/trace moved to relocated/<benchmark> after publication\"}\n");
        var replay=new ReplayConfig(false,0,0,0,0,"operation",2,64,10);
        for(String benchmark:List.of("sysbench","hybench")) {
            Path directory=output.resolve(benchmark);Path trace=Files.move(directory.resolve("trace"),relocated.resolve(benchmark));
            var manifest=TraceManifest.read(trace);assertEquals(benchmark.equals("sysbench")?1024:8706,manifest.get("baseline_rows").getAsInt());
            assertEquals("primary_only",manifest.get("secondary_index_coverage").getAsString());
            assertEquals(manifest.get("baseline_rows"),manifest.get("baseline_operations"));
            assertTrue(manifest.getAsJsonObject("source_evidence").getAsJsonObject("request_budget")
                    .get("effective_requests").getAsLong()>=1000);
            assertFalse(manifest.has("native_data"));
            assertFalse(Files.exists(directory.resolve("data")));
            try(var windows=Files.walk(directory.resolve("workload"))){assertTrue(windows.noneMatch(p->p.endsWith("native.csv")));}
            var service=new ServiceConfig("rocksdb",null,directory.resolve("index"),0,ValueKind.ROW_ID,null,false,true);
            assertEquals("passed",ServiceReplay.run(trace,replay,service,directory.resolve("replay.json")).get("status"));
            assertEquals("passed",org.sibench.replay.ReferenceReplay.run(trace,replay,
                    directory.resolve("reference.json")).get("status"));
        }
        assertThrows(IllegalArgumentException.class,()->Generation.run(root,config));
    }
}
