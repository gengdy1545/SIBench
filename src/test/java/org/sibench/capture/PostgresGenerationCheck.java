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
        for(boolean queries:java.util.List.of(false,true))sharedOutputBudget(queries);
    }
    private void sharedOutputBudget(boolean queries) throws Exception {
        Path root=Path.of("").toRealPath();
        Path output=Files.createTempDirectory(root.resolve("results/capture"),"postgres-budget-");
        var configuration=settings(root,output);
        var profile=output.resolve("profile.yaml");
        var document=parse(Files.readString(profile));
        document.put("trace",Map.of("secondary_indexes",false,"export_queries",queries,"workload_requests",600));
        Files.writeString(profile,new org.yaml.snakeyaml.Yaml().dump(document));
        var result=Generation.run(root,configuration).get(0);
        assertEquals(1024L,result.get("baseline_operations"));
        assertFalse(Files.exists(output.resolve("generated/database/data")));
        var budget=object(object(result,"source_evidence"),"request_budget");
        assertTrue(((Number)budget.get("effective_requests")).longValue()>=600);
        assertEquals(6,object(budget,"stages").size());
        var query=object(object(budget,"stages"),"oltp_point_select");
        assertEquals(queries,((Number)query.get("effective_requests")).longValue()>0);
        assertEquals(queries,((Number)query.get("target_requests")).longValue()>0);
        assertTrue(Files.isRegularFile(output.resolve("generated/oltp_point_select/batch-0/summary.json")));
        assertFalse(Files.exists(output.resolve("generated/oltp_point_select/batch-1"))&&!queries);
        assertTrue(Files.exists(output.resolve("generated/oltp_read_write/budget.stop")));
        assertEquals("passed",org.sibench.replay.ReferenceReplay.run(output.resolve("generated/trace"),
                new org.sibench.replay.ReplayConfig(false,0,0,0,0,"operation"),output.resolve("reference.json")).get("status"));
    }
    @Test public void failedPublicationRetainsTheClosedDatabase() throws Exception {
        Path root=Path.of("").toRealPath();
        Path output=Files.createTempDirectory(root.resolve("results/capture"),"postgres-publication-");
        Path trace=output.resolve("generated/trace"),settings=settings(root,output);
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
        assertTrue(Files.isRegularFile(output.resolve("generated/database/data/PG_VERSION")));
        assertTrue(Files.isRegularFile(output.resolve("generated/failure.txt")));
    }
    private static Path settings(Path root,Path output) throws Exception {
        String runtime=System.getProperty("sibench.postgres.runtime"),client=System.getProperty("sibench.sysbench.build");
        assertNotNull(runtime);assertNotNull(client);
        var document=DuckDbGenerationTest.profile("sysbench");var config=object(document,"native");
        config.put("tables",2);config.put("table-size",512);config.put("threads",2);config.put("prepare_threads",2);
        config.put("build_directory",client);
        var workloads=new LinkedHashMap<String,Object>();
        for(String workload:object(config,"workloads").keySet()) workloads.put(workload,Map.of("events",32));
        config.put("workloads",workloads);document.put("native",config);
        document.put("trace",Map.of("secondary_indexes",false,"workload_requests",600));
        document.put("artifacts",Map.of("directory",root.relativize(output.resolve("generated")).toString()));
        var yaml=new org.yaml.snakeyaml.Yaml();Path profile=output.resolve("profile.yaml"),settings=output.resolve("runtime.yaml");
        Files.writeString(profile,yaml.dump(document));
        Path engine=output.resolve("postgres.yaml");
        Files.writeString(engine,yaml.dump(Map.of("schema_version",1,"purpose","engine","engine","postgres",
                "build_directory",Path.of(runtime).getParent().toString(),"settings",org.sibench.generation.PostgresTestSettings.SETTINGS)));
        Files.writeString(settings,yaml.dump(Map.of("schema_version",1,"purpose","sibench",
                "benchmarks",java.util.List.of(Map.of("config",profile.toString(),"mode","capture","engine","postgres")),
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
        for(String workload:object(config,"workloads").keySet())workloads.put(workload,Map.of("events",256L));
        config.put("workloads",workloads);document.put("native",config);
        var source=SourceSchema.read(root,document);var executor=Executors.newSingleThreadExecutor();
        try(var capture=new PostgresCapture(sockets.resolve("capture.sock"),output.resolve("pending"),64L<<20,4096);
                var database=PostgresDatabase.create(Path.of(runtime),output.resolve("source"),sockets.resolve("capture.sock"),org.sibench.generation.PostgresTestSettings.SETTINGS)) {
            try(var setup=database.connect("load-schema")){PostgresDatabase.unloggedTables(setup);}
            PostgresSysbench.run(root,root.resolve(client),config,database,output.resolve("load"),"oltp_read_write",null);
            var trace=new PostgresTrace(output.resolve("trace"),source,secondaryIndexes);
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
            assertEquals(1024L,manifest.get("baseline_rows"));assertEquals(secondaryIndexes?2048L:1024L,manifest.get("baseline_operations"));assertEquals(1536L,manifest.get("committed_transactions"));
            assertFalse(Files.exists(output.resolve("trace/INCOMPLETE")));
            org.sibench.replay.ReferenceReplay.run(output.resolve("trace"),new org.sibench.replay.ReplayConfig(secondaryIndexes,0,0,0,0,"operation"),output.resolve("reference.json"));
            assertFalse(Files.exists(output.resolve("trace/data")));
        }finally{executor.shutdownNow();Files.deleteIfExists(sockets.resolve("capture.sock"));Files.delete(sockets);}
    }
}
