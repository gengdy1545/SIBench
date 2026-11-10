package org.sibench.capture;

import static org.sibench.config.ConfigValues.*;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.sibench.generation.PostgresDatabase;

/** Pinned Lua scripts and libpq client; direct source counters certify each completed workload. */
final class PostgresSysbench {
    private PostgresSysbench() {}
    static Map<String,Object> run(Path root,Path build,Map<String,Object> config,PostgresDatabase database,
            Path logs,String workload,PostgresCapture capture)throws Exception {
        boolean load=capture==null;
        Path stop=logs.getParent().resolve("budget.stop");
        var previous=load?List.<Map<String,Object>>of():capture.completedSessions();
        long events=load?0:positive(object(object(config,"workloads"),workload),"events");
        Path script=build.resolve("src/lua/"+workload+".lua");
        var command=new ArrayList<>(List.of(build.resolve("src/sysbench").toString(),script.toString(),
                "--db-driver=pgsql","--pgsql-host=127.0.0.1","--pgsql-port="+database.port(),
                "--pgsql-user=sibench","--pgsql-db=postgres","--report-interval=0","--time=0","--events="+events,
                "--threads="+positive(config,load?"prepare_threads":"threads")));
        for(String key:List.of("tables","table-size","secondary","create-secondary","auto-inc","skip-trx","rand-seed","rand-type")) {
            Object value=config.get(key);command.add("--"+key+"="+(value instanceof Boolean flag?(flag?"on":"off"):value));
        }
        command.add(load?"prepare":"run");
        var result=NativeProcess.run(load?"prepare":"run",command,
                Map.of("LUA_PATH",build.resolve("src/lua")+"/?.lua;;","PGAPPNAME",load?"sibench-load":"sibench:sysbench",
                        "SIBENCH_BUDGET_STOP",load?"":stop.toString()),
                root,logs,Duration.ofMinutes(positive(config,"timeout_minutes")),()->{if(capture!=null)capture.check();});
        if(result.exit()!=0)throw new IOException("sysbench exited with "+result.exit()+"; see "+logs);
        if(load)return Map.of();
        try(var control=database.connect("source-counts")){capture.awaitSources(control);}
        var sessions=new ArrayList<>(capture.completedSessions());sessions.removeAll(previous);
        var measured=SysbenchSummary.parse(result.stdout());
        long actual=measured.getOrDefault("transactions",-1L);
        if(actual<0 || actual>events || actual!=events&&!Files.isRegularFile(stop) || measured.getOrDefault("reconnects",0L)!=0)
            throw new IOException("sysbench did not complete its configured events without reconnection");
        events=actual;
        for(String field:List.of("read","write","other","total","ignored errors"))
            if(!measured.containsKey(field))throw new IOException("Missing sysbench count: "+field);
        var source=new LinkedHashMap<String,Long>();
        for(var session:sessions)for(var count:object(session,"counts").entrySet())
            source.merge(count.getKey(),((Number)count.getValue()).longValue(),Math::addExact);
        var control=SysbenchControlStatements.derive(script,bool(config,"skip-trx"),events,measured.get("ignored errors"));
        if(sessions.size()!=positive(config,"threads"))throw new IOException("sysbench source session count differs from configured workers");
        long executions=certifiedExecutions(workload,measured,source.getOrDefault("requests",0L),
                source.getOrDefault("aborts",0L),source.getOrDefault("failed_executions",0L));
        SysbenchControlStatements.requireAgreement(workload,measured.get("total"),executions,control);
        if(source.getOrDefault("successful_executions",0L)!=executions
                ||source.getOrDefault("commits",0L)!=(control.explicitTransactions()?events:executions)
                ||source.getOrDefault("zero_row_modifications",0L)!=measured.get("other")-control.controlStatements())
            throw new IOException("sysbench client/source completion counts disagree: "+measured+" / "+source);
        var report=Map.<String,Object>of("workload",workload,"client",measured,"source",source,"sessions",sessions,
                "control_statements",control.describe());
        Files.writeString(logs.resolve("summary.json"),new com.google.gson.GsonBuilder().setPrettyPrinting().create().toJson(report),
                java.nio.file.StandardOpenOption.CREATE_NEW);
        return report;
    }

    static long certifiedExecutions(String script,Map<String,Long> counts,long requests,long aborts,long failed)throws IOException {
        long ignored=counts.getOrDefault("ignored errors",0L);
        if(aborts!=ignored||failed!=ignored||failed>requests)
            throw new IOException(script+": driver ignored errors and server aborted executions disagree");
        return requests-failed;
    }
}
