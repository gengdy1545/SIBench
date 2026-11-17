package org.sibench.capture;

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
import org.sibench.replay.ReplayConfig;
import org.yaml.snakeyaml.Yaml;

/** Real capture-source generation using the configured dispatcher and portable consumers. */
public class DuckDbGenerationCheck {
    @Rule public Timeout timeout=Timeout.seconds(600);
    @Test public void aQueryOnlyWorkloadContributesToCaptureSupply() throws Exception {
        Path root=Path.of("").toRealPath();
        String client=System.getProperty("sibench.sysbench.build"),host=System.getProperty("sibench.duckdb.host");
        assertNotNull(client);assertNotNull(host);
        Path output=Files.createTempDirectory(root.resolve("results/capture"),"duckdb-query-budget-");
        var document=DuckDbGenerationTest.profile("sysbench");var n=object(document,"native");
        n.put("tables",2);n.put("table-size",512);n.put("prepare_threads",2);n.put("threads",2);
        n.put("timeout_minutes",2);n.put("build_directory",client);
        n.put("rand-type","uniform");
        n.put("workloads",Map.of("oltp_point_select",Map.of("script","oltp_point_select","events",10)));document.put("native",n);
        document.put("trace",Map.of("secondary_indexes",false,"workload_requests",1000));
        document.put("artifacts",Map.of("directory",root.relativize(output.resolve("generated")).toString()));
        var yaml=new Yaml();Path profile=output.resolve("profile.yaml"),engine=output.resolve("duckdb.yaml"),runtime=output.resolve("runtime.yaml");
        Files.writeString(profile,yaml.dump(document));
        Files.writeString(engine,yaml.dump(Map.of("schema_version",1,"purpose","engine","engine","duckdb",
                "build_directory",Path.of(host).getParent().toString(),"memory_limit","1GiB","threads",2)));
        Files.writeString(runtime,yaml.dump(Map.of("schema_version",1,"purpose","sibench",
                "benchmarks",Map.of("sysbench",org.sibench.config.BenchmarkFixtures.entry(profile,"duckdb",output.resolve("trace"))),
                "engines",Map.of("duckdb",engine.toString()))));
        var result=Generation.run(root,runtime).get(0);
        var budget=object(object(result,"source_evidence"),"request_budget");
        assertTrue(((Number)budget.get("effective_requests")).longValue()>=1000);
        assertFalse(budget.containsKey("stages"));
        assertTrue(((Number)budget.get("target_requests")).longValue()>0);
        assertTrue(Files.isDirectory(output.resolve("generated/oltp_point_select/workload/batch-0")));
        assertEquals("passed",org.sibench.replay.ReferenceReplay.run(output.resolve("trace"),
                new ReplayConfig(false,0,0,0,0,"operation", 1, 4096, 60, 1),output.resolve("reference.json")).get("status"));
        assertFalse(Files.exists(output.resolve("generated/oltp_point_select/data")));
    }

    @Test public void mixedCapturePublishesBothConfiguredSelectionsAndReplays()throws Exception {
        Path root=Path.of("").toRealPath();String retained=System.getProperty("sibench.generation.output");
        Files.createDirectories(root.resolve("results/capture"));
        Path output=retained==null?Files.createTempDirectory(root.resolve("results/capture"),"generation-"):Files.createDirectory(Path.of(retained));
        assertTrue(output.startsWith(root.resolve("results/capture")));
        var selections=new java.util.LinkedHashMap<String,Object>();
        for(String benchmark:List.of("sysbench","hybench")) {
            var document=DuckDbGenerationTest.profile(benchmark);var n=object(document,"native");
            String build=System.getProperty("sibench."+benchmark+".build");assertNotNull("Set the "+benchmark+" build",build);
            n.put("build_directory",build);
            if(benchmark.equals("sysbench")) {n.put("tables",2);n.put("table-size",512);n.put("prepare_threads",2);}
            else {
                n.put("sf","1x");n.put("timeout_minutes",5);n.put("duration_minutes",1);n.remove("phase_duration_minutes");
                for(String key:List.of("clients","xp_transaction_clients","xp_query_clients"))n.put(key,1);
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
            var entry=org.sibench.config.BenchmarkFixtures.entry(profile,"duckdb",output.resolve("traces").resolve(benchmark));
            entry.put("workload",benchmark.equals("hybench")?"xp":"oltp_read_write_zipf099");
            selections.put(benchmark,entry);
        }
        Files.writeString(output.resolve("unselected.yaml"),"invalid unselected profile: [");
        String host=System.getProperty("sibench.duckdb.host");assertNotNull("Set sibench.duckdb.host",host);
        Path engine=output.resolve("duckdb.yaml");
        Files.writeString(engine,new Yaml().dump(Map.of("schema_version",1,"purpose","engine","engine","duckdb",
                "build_directory",Path.of(host).getParent().toString(),"memory_limit","1GiB","threads",2)));
        var runtime=new LinkedHashMap<String,Object>(Map.of("schema_version",1,"purpose","sibench","benchmarks",selections,
                "engines",Map.of("duckdb",engine.toString())));
        Path config=output.resolve("runtime.yaml");
        var invalid=new LinkedHashMap<>(runtime);
        var invalidEntries=new LinkedHashMap<>(selections);var bad=object(selections,"hybench");bad.put("engine","rocksdb");invalidEntries.put("hybench",bad);invalid.put("benchmarks",invalidEntries);
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
        var moved=Files.createDirectory(output.resolve("relocated"));
        for(String benchmark:List.of("sysbench","hybench")) {
            var directory=output.resolve(benchmark);
            directory=directory.resolve(benchmark.equals("hybench")?"xp":"oltp_read_write_zipf099");
            var trace=Files.move(output.resolve("traces").resolve(benchmark),moved.resolve(benchmark));
            assertEquals("passed",org.sibench.replay.ReferenceReplay.run(trace,
                    new ReplayConfig(false,0,0,0,0, "operation", 1, 4096, 60, 1),directory.resolve("reference.json")).get("status"));
            assertFalse(Files.exists(directory.resolve("data")));
            assertEquals("passed",org.sibench.adapters.IndexTestSupport.replay(trace,
                    new ReplayConfig(false,0,0,0,0, "operation", 1, 4096, 60, 1),directory.resolve("index"),
                    org.sibench.adapters.ValueKind.ROW_ID,directory.resolve("service.json")).get("status"));
        }
        assertThrows(IllegalArgumentException.class,()->Generation.run(root,config));
    }
}
