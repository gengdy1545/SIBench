package org.sibench.capture;

import org.sibench.config.Digests;
import com.google.gson.GsonBuilder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.sibench.config.Engine;
import org.sibench.generation.NativeHistory;
import org.sibench.generation.PostgresDatabase;
import static org.sibench.config.ConfigValues.*;

/** One fresh PG cluster: native load, online index export, normal shutdown and publication. */
final class PostgresGeneration {
    private PostgresGeneration() {}
    static Map<String,Object> generate(Generation.Plan plan,Generation.Input input)throws Exception {
        long started=System.nanoTime();Path output=input.output();
        Files.createDirectories(output.getParent());Files.createDirectory(output);
        Path sockets=Files.createTempDirectory("sibench-capture-"),endpoint=sockets.resolve("pg.sock");
        var build=plan.engines().get(Engine.POSTGRES);Path runtime=build.directory().resolve("runtime");
        var executor=Executors.newSingleThreadExecutor();Throwable primary=null;
        var timings=new LinkedHashMap<String,Double>();
        try(var capture=new PostgresCapture(endpoint,output.resolve("pending"),4L<<30,262144);
                var database=PostgresDatabase.create(runtime,output.resolve("database"),endpoint)) {
            try {
                json(output.resolve("effective-config.json"),Map.of("runtime",plan.runtime(),"benchmark",input.document(),
                        "engine",input.selection().engine(),"mode",input.selection().mode()));
                json(output.resolve("engine-build.json"),build.identity());json(output.resolve("client-build.json"),input.buildIdentity());
                long loadStart=System.nanoTime();
                try(var setup=database.connect("load-schema")) { PostgresDatabase.unloggedTables(setup); }
                PostgresSysbench.run(plan.root(),input.build(),input.nativeConfig(),database,output.resolve("load"),"oltp_read_write",null);
                timings.put("native_load_seconds",seconds(loadStart));long baselineStart=System.nanoTime();
                var trace=new PostgresTrace(output.resolve("trace"),input.source(),Generation.secondaryIndexes(input.document()));
                try(var setup=database.connect("baseline")) {
                    PostgresDatabase.finishLoad(setup);
                    // Mark loaded pages visible before copying keys, avoiding another wide-table scan.
                    try(var query=setup.createStatement()) {
                        for(var table:input.source().tables())query.execute("VACUUM ANALYZE "+"\""+table.name().replace("\"","\"\"")+"\"");
                    }
                    setup.setAutoCommit(false);NativeHistory.initialize(setup,Engine.POSTGRES);int id=0;
                    for(var table:input.source().tables())NativeHistory.install(setup,Engine.POSTGRES,++id,"public",table.name(),
                            table.indexes().stream().filter(org.sibench.schema.SourceSchema.Index::primary).findFirst().orElseThrow().column());
                    PostgresCapture.install(setup);setup.commit();trace.baseline(setup,input.baselineRows());setup.commit();
                    setup.setAutoCommit(true);try(var query=setup.createStatement()){query.execute("ANALYZE");}
                }
                timings.put("baseline_indexes_and_maintenance_seconds",seconds(baselineStart));
                var projected=executor.submit(()->{
                    try(var control=database.connect("projection")) {
                        control.setAutoCommit(false);trace.workload(control,capture);
                    }catch(Exception failure){capture.fail(failure);throw failure;}
                    return null;
                });
                long workloadStart=System.nanoTime();var stages=new LinkedHashMap<String,Object>();
                for(String workload:object(input.nativeConfig(),"workloads").keySet()) {
                    stages.put(workload,PostgresSysbench.run(plan.root(),input.build(),input.nativeConfig(),database,
                            output.resolve(workload),workload,capture));
                }

                try(var control=database.connect("source-drain")){capture.finishSources(control);}
                projected.get(input.timeout().toMillis(),TimeUnit.MILLISECONDS);
                timings.put("workload_and_export_seconds",seconds(workloadStart));
                long closeStart=System.nanoTime();database.seal();
                Files.move(database.data(),output.resolve("trace/data"));
                timings.put("native_close_seconds",seconds(closeStart));long publicationStart=System.nanoTime();
                var evidence=Map.<String,Object>of("engine",build.identity(),"client",input.buildIdentity(),"stages",stages,
                        "effective_config_sha256",Digests.sha256(output.resolve("effective-config.json")),"baseline_rows",input.baselineRows());
                var manifest=trace.publish(runtime,evidence);
                timings.put("publication_seconds",seconds(publicationStart));timings.put("total_seconds",seconds(started));
                json(output.resolve("result.json"),Map.of("status","complete","trace",output.resolve("trace").toString(),"timings",timings));
                return manifest;
            }catch(Exception|Error failure){capture.fail(failure);throw failure;}
        }catch(Exception|Error failure) {
            primary=failure;var stack=new java.io.StringWriter();failure.printStackTrace(new java.io.PrintWriter(stack));
            try{Files.writeString(output.resolve("failure.txt"),stack.toString(),StandardOpenOption.CREATE_NEW);}
            catch(Exception diagnostic){failure.addSuppressed(diagnostic);}throw failure;
        }finally {
            executor.shutdownNow();
            try {
                if(!executor.awaitTermination(10,TimeUnit.SECONDS))throw new java.io.IOException("PG projection worker did not stop");
                Files.deleteIfExists(endpoint);Files.deleteIfExists(sockets);
            }catch(Exception cleanup){if(primary!=null)primary.addSuppressed(cleanup);else throw cleanup;}
        }
    }
    private static double seconds(long start){return (System.nanoTime()-start)/1e9;}
    private static void json(Path path,Object value)throws java.io.IOException {
        Files.writeString(path,new GsonBuilder().setPrettyPrinting().create().toJson(value),StandardOpenOption.CREATE_NEW);
    }
}
