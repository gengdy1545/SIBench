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
        long rowId;int initialK;
        try(var capture=new PostgresCapture(sockets.resolve("capture.sock"),output.resolve("pending"),64L<<20,4096);
                var database=PostgresDatabase.create(Path.of(runtime),output.resolve("source"),sockets.resolve("capture.sock"))) {
            try(var setup=database.connect("load-schema")){PostgresDatabase.unloggedTables(setup);}
            PostgresSysbench.run(root,root.resolve(client),config,database,output.resolve("load"),"oltp_read_write",null);
            var trace=new PostgresTrace(output.resolve("trace"),source,secondaryIndexes);
            try(var setup=database.connect("load")) {
                PostgresDatabase.finishLoad(setup);
                setup.setAutoCommit(false);NativeHistory.initialize(setup,Engine.POSTGRES);int id=0;
                for(var table:source.tables())NativeHistory.install(setup,Engine.POSTGRES,++id,"public",table.name(),"id");
                PostgresCapture.install(setup);setup.commit();
                trace.baseline(setup,Map.of("sbtest1",512L,"sbtest2",512L));setup.commit();
                try(var query=setup.createStatement();var row=query.executeQuery("SELECT i._sib_row_id,s.k FROM sbtest1 s JOIN sibench_history.identities_1 i USING(id) WHERE id=1")) {
                    assertTrue(row.next());rowId=row.getLong(1);initialK=row.getInt(2);
                }
                setup.commit();
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
            try(var control=database.connect("independent-acceptance")){NativeHistory.verifyCommits(control);}
            database.seal();Files.move(database.data(),output.resolve("trace/data"));
            var manifest=trace.publish(Path.of(runtime),Map.of("stages",stages));
            assertEquals(1024L,manifest.get("baseline_rows"));assertEquals(secondaryIndexes?2048L:1024L,manifest.get("baseline_operations"));assertEquals(1536L,manifest.get("committed_transactions"));
            assertFalse(Files.exists(output.resolve("trace/INCOMPLETE")));
            org.sibench.replay.ReferenceReplay.run(output.resolve("trace"),new org.sibench.replay.ReplayConfig(secondaryIndexes,0,0,0,0,"operation"),output.resolve("reference.json"));
            Path moved=output.resolve("moved");Files.move(output.resolve("trace"),moved);
            try(var rows=org.sibench.trace.TraceManifest.openData(moved,Path.of(runtime),output.resolve("reader"))) {
                assertEquals(initialK,((Number)rows.snapshotRow(1,rowId,0).orElseThrow().get("k")).intValue());
            }
        }finally{executor.shutdownNow();Files.deleteIfExists(sockets.resolve("capture.sock"));Files.delete(sockets);}
    }
}
