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

/** One fresh PG cluster: native load, online index export, normal shutdown and Trace publication. */
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
                var database=PostgresDatabase.create(runtime,output.resolve("database"),endpoint,
                        org.sibench.config.SibenchConfig.postgresSettings(object(build.settings(),"settings")))) {
            try {
                json(output.resolve("effective-config.json"),Generation.effectiveConfig(plan,input));
                json(output.resolve("engine-build.json"),build.identity());json(output.resolve("client-build.json"),input.buildIdentity());
                long loadStart=System.nanoTime();
                boolean hybench=input.selection().benchmark().equals("hybench");
                String url="jdbc:postgresql://127.0.0.1:"+database.port()+"/postgres?user=sibench&ApplicationName=sibench:hybench";
                try(var setup=database.connect("load-schema")) { PostgresDatabase.unloggedTables(setup); }
                if(hybench)loadHybench(plan,input,database,output,url);
                else PostgresSysbench.run(plan.root(),input.build(),input.nativeConfig(),database,output.resolve("load"),"oltp_read_write",null);
                timings.put("native_load_seconds",seconds(loadStart));long baselineStart=System.nanoTime();
                var budget=new RequestBudget(Generation.requestTarget(input.document()),input.timeout());
                var trace=new PostgresTrace(output.resolve("trace"),input.source(),Generation.secondaryIndexes(input.document()),budget);
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
                // HyBench runs AP once, then shares the remaining requests between TP and XP.
                var workloads=hybench?java.util.List.of("ap","tp","xp"):java.util.List.copyOf(object(input.nativeConfig(),"workloads").keySet());
                int remaining=hybench?2:workloads.size();
                for(String workload:workloads) {
                    Path stage=Files.createDirectory(output.resolve(workload));
                    boolean setup=hybench&&workload.equals("ap");
                    Path stop=setup?null:stage.resolve("budget.stop");
                    budget.begin(workload,setup?0:remaining--,stop);
                    var batches=new java.util.ArrayList<Map<String,Object>>();
                    do {
                        budget.checkTime();long before=budget.count();
                        Path batch=stage.resolve("batch-"+batches.size());
                        batches.add(hybench?runHybench(plan,input,database,capture,batch,workload,url,stop)
                                :PostgresSysbench.run(plan.root(),input.build(),input.nativeConfig(),database,batch,workload,capture));
                        trace.awaitProjection(capture,input.timeout());
                        if(budget.enabled()&&!setup)budget.requireProgress(before);
                    } while(budget.enabled()&&!setup&&!budget.reached());
                    budget.end();stages.put(workload,batches);
                }
                json(output.resolve("request-budget.json"),budget.finish());

                try(var control=database.connect("source-drain")){capture.finishSources(control);}
                projected.get(input.timeout().toMillis(),TimeUnit.MILLISECONDS);
                timings.put("workload_and_export_seconds",seconds(workloadStart));
                // The Trace is complete without native rows: stop the cluster normally, publish, then discard its data.
                long closeStart=System.nanoTime();database.close();
                timings.put("native_close_seconds",seconds(closeStart));
                var evidence=Map.<String,Object>of("engine",build.identity(),"client",input.buildIdentity(),"stages",stages,
                        "request_budget",budget.finish(),"effective_config_sha256",Digests.sha256(output.resolve("effective-config.json")),"baseline_rows",input.baselineRows());
                var manifest=Generation.publishThenDiscard(()->trace.publish(evidence),database.data(),timings);
                timings.put("total_seconds",seconds(started));
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
    /** Upstream CSVs are generated once and loaded with the same native DDL/COPY as DuckDB. */
    private static void loadHybench(Generation.Plan plan,Generation.Input input,PostgresDatabase database,Path output,String url)throws Exception {
        Path load=Files.createDirectory(output.resolve("load"));
        Generation.hybench(plan,input,load,true,null,Engine.POSTGRES,url,null,()->{});
        Path data=output.resolve("native/Data_"+string(input.nativeConfig(),"sf"));
        Path script=input.source().source().resolve("conf/load_data_pg.sql"),manifest=load.resolve("inputs.json");
        HybenchInputs.record(data,script,manifest);
        try(var setup=database.connect("load")){Generation.requireRows(input.baselineRows(),Hybench.load(setup,input.source().source(),data));}
        HybenchInputs.requireRuntimeInputs(data,manifest);
        HybenchInputs.dropLoadInputs(data,script);
    }

    /** One HyBench phase window; its source sessions must all close before the next window starts. */
    private static Map<String,Object> runHybench(Generation.Plan plan,Generation.Input input,PostgresDatabase database,
            PostgresCapture capture,Path batch,String phase,String url,Path stop)throws Exception {
        Files.createDirectory(batch);var previous=capture.completedSessions();
        Generation.hybench(plan,input,batch,false,phase,Engine.POSTGRES,url,stop,capture::check);
        try(var control=database.connect("source-counts")){capture.awaitSources(control);}
        var sessions=new java.util.ArrayList<>(capture.completedSessions());sessions.removeAll(previous);
        if(sessions.isEmpty())throw new java.io.IOException("HyBench window opened no captured PG session: "+batch);
        var source=new LinkedHashMap<String,Long>();
        for(var session:sessions)for(var count:object(session,"counts").entrySet())
            source.merge(count.getKey(),((Number)count.getValue()).longValue(),Math::addExact);
        var client=com.google.gson.JsonParser.parseString(Files.readString(batch.resolve("summary.json"))).getAsJsonObject();
        if(phase.equals("tp"))requireTpWindow(client.getAsJsonObject("tp"),source,sessions.size(),positive(input.nativeConfig(),"clients"));
        return Map.of("phase",phase,"client",new com.google.gson.Gson().fromJson(client,Map.class),"source",source,"sessions",sessions.size());
    }

    /**
     * Independent client evidence for a TP window: one session per client, and HyBench samples every
     * committed or business-rolled-back transaction but not a conflict abort. AP/IQ samples count
     * statements inside long transactions and XP adds freshness sessions, so only TP has these exact bounds.
     */
    static void requireTpWindow(com.google.gson.JsonObject samples,Map<String,Long> source,int sessions,long clients)throws java.io.IOException {
        long sampled=0;
        for(var entry:samples.entrySet())sampled=Math.addExact(sampled,entry.getValue().getAsLong());
        long commits=source.get("commits"),aborts=source.get("aborts");
        if(sessions!=clients||sampled<commits||sampled>commits+aborts)
            throw new java.io.IOException("HyBench TP client evidence differs from captured sessions: sessions "+sessions+"/"+clients
                    +", samples "+sampled+", commits "+commits+", aborts "+aborts);
    }

    private static double seconds(long start){return (System.nanoTime()-start)/1e9;}
    private static void json(Path path,Object value)throws java.io.IOException {
        Files.writeString(path,new GsonBuilder().setPrettyPrinting().create().toJson(value),StandardOpenOption.CREATE_NEW);
    }
}
