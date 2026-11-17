package org.sibench.capture;

import static org.junit.Assert.*;
import static org.sibench.config.ConfigValues.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.config.Engine;
import org.sibench.generation.NativeHistory;
import org.sibench.generation.PostgresDatabase;
import org.sibench.schema.SourceSchema;

/** Explicit small-data end-to-end acceptance; its reference replay is outside production generation. */
public class PostgresGenerationCheck {
    @Rule public TemporaryFolder temporary=new TemporaryFolder();
    @Test public void sysbenchWritesFinalTraceDuringLoadAndWorkload()throws Exception {
        run(true);
    }
    @Test public void primaryOnlyExportRetainsNativeSecondaryIndexesAndReplays() throws Exception {
        run(false);
    }
    @Test public void sharedOutputBudgetControlsRealSourceAdmission() throws Exception {
        sharedOutputBudget();
    }
    @Test public void sysbenchReadWriteCapturesBothIndexScopesAndLocalReads()throws Exception {
        Path root=Path.of("").toRealPath(),output=Files.createTempDirectory(root.resolve("results/capture"),"sysbench-read-write-");
        Path configuration=settings(root,output),profile=output.resolve("profile.yaml");
        var document=parse(Files.readString(profile));var nativeConfig=object(document,"native");
        nativeConfig.put("rand-type","zipfian");nativeConfig.put("workloads",Map.of("oltp_read_write",Map.of("script","oltp_read_write","events",256,"rand-zipfian-exp",0.99)));document.put("native",nativeConfig);
        document.put("trace",Map.of("secondary_indexes",true,"workload_requests",2400));
        Files.writeString(profile,new org.yaml.snakeyaml.Yaml().dump(document));
        var result=Generation.run(root,configuration).get(0);
        var file=object(object(result,"proto_files"),"workload");
        var statistics=object(file,"statistics");
        assertTrue(((Number)object(statistics,"source_projection").get("transaction_local_search_members")).longValue()>0);
        assertTrue(((Number)object(object(statistics,"index_operations"),"primary").get("get")).longValue()>0);
        assertTrue(((Number)object(object(statistics,"index_operations"),"secondary").get("put")).longValue()>0);
        assertEquals("passed",org.sibench.replay.ReferenceReplay.run(output.resolve("trace"),
                new org.sibench.replay.ReplayConfig(true,0,0,0,0, "operation", 1, 4096, 60, 1),output.resolve("reference.json")).get("status"));
        assertFalse(Files.exists(output.resolve("generated/oltp_read_write/database/data")));
    }
    @Test(timeout=360000) public void hybenchTpAndXpPublishIndependentTracesAndReplay() throws Exception {
        Path root=Path.of("").toRealPath();
        Path output=Files.createTempDirectory(root.resolve("results/capture"),"hybench-query-stages-");
        Path configuration=settings(root,output);
        var document=DuckDbGenerationTest.profile("hybench");var n=object(document,"native");
        String client=System.getProperty("sibench.hybench.build");assertNotNull("Set sibench.hybench.build",client);
        n.put("build_directory",client);n.put("sf","1x");n.put("timeout_minutes",2);
        n.put("duration_minutes",1);n.remove("phase_duration_minutes");
        for(String key:java.util.List.of("clients","xp_transaction_clients","xp_query_clients"))n.put(key,1);
        n.put("parameters",Map.of("customer_number",300,"company_number",2,"transfer_number",6000,
                "checking_number",600,"loanapps_number",600,"loantrans_number",600));
        document.put("native",n);
        document.put("trace",Map.of("secondary_indexes",true));
        document.put("artifacts",Map.of("directory",root.relativize(output.resolve("generated")).toString()));
        Files.writeString(output.resolve("profile.yaml"),new org.yaml.snakeyaml.Yaml().dump(document));
        var runtime=parse(Files.readString(configuration));var selections=new LinkedHashMap<String,Object>();
        for(String phase:java.util.List.of("tp","xp"))selections.put("hybench-"+phase,
                org.sibench.config.BenchmarkFixtures.entry(output.resolve("profile.yaml"),"postgres",
                        output.resolve("traces/hybench-"+phase),phase));
        runtime.put("benchmarks",selections);
        Files.writeString(configuration,new org.yaml.snakeyaml.Yaml().dump(runtime));
        var results=Generation.run(root,configuration);assertEquals(2,results.size());
        for(int index=0;index<2;index++) {
            String phase=index==0?"tp":"xp";var result=results.get(index);
            assertEquals(8706L,result.get("baseline_rows"));
            var batch=(Map<String,Object>)((java.util.List<?>)object(result,"source_evidence").get("batches")).get(0);
            var counts=object(object(batch,"client"),phase);
            assertEquals(phase.equals("tp")?18:12,counts.size());
            for(var count:counts.values())assertTrue(((Number)count).longValue()>0);
            var coverage=object(result,"query_coverage");
            for(int i=1;i<=6;i++)assertEquals(phase.equals("xp"),
                    ((Number)object(object(coverage,"iq"),"IQ-"+i).get("successful_queries")).longValue()>0);
            Path generated=output.resolve("generated").resolve(phase);
            assertEquals("passed",org.sibench.replay.ReferenceReplay.run(output.resolve("traces/hybench-"+phase),
                    new org.sibench.replay.ReplayConfig(true,0,0,0,0, "operation", 1, 4096, 60, 1),generated.resolve("reference.json")).get("status"));
            assertFalse(Files.exists(generated.resolve("database/data")));
            var properties=new java.util.Properties();
            try(var file=Files.newInputStream(generated.resolve("workload/batch-0/resolved.props"))){properties.load(file);}
            assertEquals(phase,properties.getProperty("sibench.phase"));
        }
    }
    private void sharedOutputBudget() throws Exception {
        Path root=Path.of("").toRealPath();
        Path output=Files.createTempDirectory(root.resolve("results/capture"),"postgres-budget-");
        var configuration=settings(root,output);
        var profile=output.resolve("profile.yaml");
        var document=parse(Files.readString(profile));
        document.put("trace",Map.of("secondary_indexes",false,"workload_requests",600));
        Files.writeString(profile,new org.yaml.snakeyaml.Yaml().dump(document));
        var result=Generation.run(root,configuration).get(0);
        assertEquals(1024L,result.get("baseline_operations"));
        assertFalse(Files.exists(output.resolve("generated/oltp_point_select/database/data")));
        var budget=object(object(result,"source_evidence"),"request_budget");
        assertTrue(((Number)budget.get("effective_requests")).longValue()>=600);
        assertFalse(budget.containsKey("stages"));
        assertTrue(((Number)budget.get("effective_requests")).longValue()>0);
        assertTrue(((Number)budget.get("target_requests")).longValue()>0);
        assertTrue(Files.isRegularFile(output.resolve("generated/oltp_point_select/workload/batch-0/summary.json")));
        assertEquals("passed",org.sibench.replay.ReferenceReplay.run(output.resolve("trace"),
                new org.sibench.replay.ReplayConfig(false,0,0,0,0,"operation", 1, 4096, 60, 1),output.resolve("reference.json")).get("status"));
    }
    @Test public void failedPublicationRetainsTheClosedDatabase() throws Exception {
        Path root=Path.of("").toRealPath();
        Path output=Files.createTempDirectory(root.resolve("results/capture"),"postgres-publication-");
        Path trace=output.resolve("generated/oltp_point_select/trace"),settings=settings(root,output);
        var executor=Executors.newSingleThreadExecutor();
        try {
            // An occupied partial-manifest path makes publication fail after normal shutdown.
            var occupied=executor.submit(()->{
                while(!Files.isDirectory(trace))Thread.sleep(5);
                return Files.createDirectory(trace.resolve("manifest.json.partial"));
            });
            var failure=assertThrows(java.nio.file.FileAlreadyExistsException.class,()->Generation.run(root,settings));
            assertTrue(occupied.isDone());assertTrue(failure.getMessage().contains("manifest.json.partial"));
        }finally{executor.shutdownNow();}
        assertTrue(Files.exists(trace.resolve("INCOMPLETE")));assertFalse(Files.exists(trace.resolve("manifest.json")));
        assertTrue(Files.isRegularFile(output.resolve("generated/oltp_point_select/database/data/PG_VERSION")));
        assertTrue(Files.isRegularFile(output.resolve("generated/oltp_point_select/failure.txt")));
    }
    private static Path settings(Path root,Path output) throws Exception {
        String runtime=System.getProperty("sibench.postgres.runtime"),client=System.getProperty("sibench.sysbench.build");
        assertNotNull(runtime);assertNotNull(client);
        var document=DuckDbGenerationTest.profile("sysbench");var config=object(document,"native");
        config.put("tables",2);config.put("table-size",512);config.put("threads",2);config.put("prepare_threads",2);
        config.put("build_directory",client);
        config.put("rand-type","uniform");
        config.put("workloads",Map.of("oltp_point_select",Map.of("script","oltp_point_select","events",32)));document.put("native",config);
        document.put("trace",Map.of("secondary_indexes",false,"workload_requests",600));
        document.put("artifacts",Map.of("directory",root.relativize(output.resolve("generated")).toString()));
        var yaml=new org.yaml.snakeyaml.Yaml();Path profile=output.resolve("profile.yaml"),settings=output.resolve("runtime.yaml");
        Files.writeString(profile,yaml.dump(document));
        Path engine=output.resolve("postgres.yaml");
        Files.writeString(engine,yaml.dump(Map.of("schema_version",1,"purpose","engine","engine","postgres",
                "build_directory",Path.of(runtime).getParent().toString(),"settings",org.sibench.generation.PostgresTestSettings.SETTINGS)));
        Files.writeString(settings,yaml.dump(Map.of("schema_version",1,"purpose","sibench",
                "benchmarks",Map.of("sysbench", org.sibench.config.BenchmarkFixtures.entry(profile,"postgres",output.resolve("trace"))),
                "engines",Map.of("postgres",engine.toString()))));
        return settings;
    }
    private void run(boolean secondaryIndexes) throws Exception {
        String runtime=System.getProperty("sibench.postgres.runtime"),client=System.getProperty("sibench.sysbench.build");
        assertNotNull(runtime);assertNotNull(client);
        Path root=Path.of("").toRealPath(),output=temporary.getRoot().toPath(),sockets=Files.createTempDirectory("pg-generation-");
        var document=DuckDbGenerationTest.profile("sysbench");var config=object(document,"native");
        config.put("tables",2L);config.put("table-size",512L);config.put("threads",2L);config.put("prepare_threads",2L);
        config.put("timeout_minutes",1L);
        var workloads=new LinkedHashMap<String,Object>();
        workloads.put("oltp_update_index",Map.of("script","oltp_update_index","events",256L,"rand-zipfian-exp",0.8));
        config.put("workloads",workloads);document.put("native",config);
        var source=SourceSchema.read(root,document);var executor=Executors.newSingleThreadExecutor();
        try(var capture=new PostgresCapture(sockets.resolve("capture.sock"),output.resolve("pending"),64L<<20,4096);
                var database=PostgresDatabase.create(Path.of(runtime),output.resolve("source"),sockets.resolve("capture.sock"),org.sibench.generation.PostgresTestSettings.SETTINGS)) {
            try(var setup=database.connect("load-schema")){PostgresDatabase.unloggedTables(setup);}
            PostgresSysbench.run(root,root.resolve(client),config,database,output.resolve("load"),"oltp_read_write",null);
            var trace=new PostgresTrace(output.resolve("trace"),source,secondaryIndexes,null);
            try(var setup=database.connect("load")) {
                PostgresDatabase.finishLoad(setup);
                setup.setAutoCommit(false);NativeHistory.initialize(setup,Engine.POSTGRES);int id=0;
                for(var table:source.tables())NativeHistory.install(setup,Engine.POSTGRES,++id,"public",table.name(),"id");
                PostgresCapture.install(setup);setup.commit();
                trace.baseline(setup,Map.of("sbtest1",512L,"sbtest2",512L));setup.commit();
                setup.setAutoCommit(true);try(var statement=setup.createStatement()){statement.execute("VACUUM ANALYZE");}
            }
            var projected=executor.submit(()->{
                try(var control=database.connect("projection")) {
                    control.setAutoCommit(false);trace.workload(control,capture);
                }catch(Exception failure){capture.fail(failure);throw failure;}
                return null;
            });
            var stages=new LinkedHashMap<String,Object>();
            for(String workload:object(config,"workloads").keySet())stages.put(workload,
                    PostgresSysbench.run(root,root.resolve(client),config,database,output.resolve(workload),workload,capture));
            try(var control=database.connect("finish")){capture.finishSources(control);}
            projected.get(30,TimeUnit.SECONDS);
            var manifest=trace.publish(Map.of("stages",stages));
            assertEquals(1024L,manifest.get("baseline_rows"));assertEquals(secondaryIndexes?2048L:1024L,manifest.get("baseline_operations"));assertEquals(256L,manifest.get("committed_transactions"));
            assertFalse(Files.exists(output.resolve("trace/INCOMPLETE")));
            org.sibench.replay.ReferenceReplay.run(output.resolve("trace"),new org.sibench.replay.ReplayConfig(secondaryIndexes,0,0,0,0,"operation", 1, 4096, 60, 1),output.resolve("reference.json"));
            assertFalse(Files.exists(output.resolve("trace/data")));
        }finally{executor.shutdownNow();Files.deleteIfExists(sockets.resolve("capture.sock"));Files.delete(sockets);}
    }
}
