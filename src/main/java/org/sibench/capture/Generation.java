package org.sibench.capture;

import org.sibench.config.Digests;
import static org.sibench.config.ConfigValues.*;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.sibench.config.BenchmarkSelection;
import org.sibench.config.BenchmarkSelection.Mode;
import org.sibench.config.SibenchConfig;
import org.sibench.config.Engine;
import org.sibench.generation.DuckDbDatabase;
import org.sibench.generation.NativeHistory;
import org.sibench.generation.DuckDbHostDriver;
import org.sibench.schema.SourceSchema;
import org.sibench.workloads.ScaleInspector;
import org.sibench.workloads.HybenchParameters;

/** Selected native clients share one fresh database each, from load through publication. */
public final class Generation {
    private Generation() {}
    record Input(BenchmarkSelection selection,Map<String,Object> document,SourceSchema source,
            Path build,Path output,Map<String,Object> buildIdentity,
            Map<String,Long> baselineRows,Map<String,Object> parameters) {
        Map<String,Object> nativeConfig(){return object(document,"native");}
        Duration timeout(){return Duration.ofMinutes(positive(nativeConfig(),"timeout_minutes"));}
    }
    record NativeBuild(Path directory,Map<String,Object> identity) {}
    record Plan(Path root,Map<String,Object> runtime,Map<Engine,NativeBuild> engines,List<Input> inputs) {}

    /** Validate every selected input before creating any run output or launching a database. */
    static Plan plan(Path root,Path configuration)throws Exception {
        root=root.toRealPath();var runtime=SibenchConfig.read(root.resolve(configuration));
        var selections=BenchmarkSelection.read(root,runtime);
        var engines=new java.util.EnumMap<Engine,NativeBuild>(Engine.class);
        for(var selection:selections)if(!engines.containsKey(selection.engine())) {
            if(selection.engine()==Engine.DUCKDB) {
                var engine=object(runtime,"duckdb");keys(engine,Set.of("build_directory","memory_limit","threads"));
                require(string(engine,"memory_limit").matches("[1-9][0-9]*(KiB|MiB|GiB)"),"Invalid DuckDB memory_limit");
                require(positive(engine,"threads")<=256,"DuckDB threads exceeds 256");
                Path build=NativeBuildCheck.engineDirectory(root,runtime).toRealPath();
                require(Files.isExecutable(build.resolve("duckdb_host"))&&Files.isRegularFile(build.resolve("src/libduckdb.so")),"Missing DuckDB host/library build");
                engines.put(Engine.DUCKDB,new NativeBuild(build,NativeBuildCheck.verifyEngine(root,build)));
            }else if(selection.engine()==Engine.POSTGRES) {
                Path build=NativeBuildCheck.engineDirectory(root,runtime,"postgres").toRealPath();
                engines.put(Engine.POSTGRES,new NativeBuild(build,NativeBuildCheck.verifyPostgres(root,build)));
            }else {
                Path build=NativeBuildCheck.engineDirectory(root,runtime,"rocksdb").toRealPath();
                engines.put(Engine.ROCKSDB,new NativeBuild(build,NativeBuildCheck.verifyRocks(root,build)));
            }
        }
        var inputs=new ArrayList<Input>();
        for(var selection:selections) {
            var document=parse(Files.readString(selection.config()));
            keys(document,Set.of("schema_version","purpose","benchmark","source","native","trace","artifacts"));
            secondaryIndexes(document);
            requestTarget(document);
            require(integer(document,"schema_version")==1&&string(document,"purpose").equals("native_capture"),"Expected native_capture profile");
            keys(object(document,"source"),Set.of("path","commit"));keys(object(document,"artifacts"),Set.of("directory"));
            var source=SourceSchema.read(root,document);var nativeConfig=object(document,"native");
            validateNative(selection.benchmark(),nativeConfig);
            var parameters=selection.benchmark().equals("hybench")?HybenchParameters.read(source.source(),nativeConfig):Map.<String,Object>of();
            var baselineRows=parameters.isEmpty()?ScaleInspector.nativeRows(root,document):HybenchParameters.rows(parameters);
            Path client=root.resolve(string(nativeConfig,"build_directory")).toRealPath();
            Path output=outputDirectory(root,string(object(document,"artifacts"),"directory"));
            for(var previous:inputs)require(!output.startsWith(previous.output())&&!previous.output().startsWith(output),"Overlapping generation outputs");
            var identity=NativeBuildCheck.verify(root,selection.benchmark(),source.source(),client,source.sourceCommit());
            inputs.add(new Input(selection,document,source,client,output,identity,Map.copyOf(baselineRows),parameters));
        }
        return new Plan(root,runtime,Map.copyOf(engines),List.copyOf(inputs));
    }

    static Path outputDirectory(Path root,String configured) throws IOException {
        Path output=root.resolve(configured).toAbsolutePath().normalize();
        require(output.startsWith(root.resolve("results/capture"))&&!output.equals(root.resolve("results/capture")),
                "Generation output must be under results/capture");
        require(!Files.exists(output,java.nio.file.LinkOption.NOFOLLOW_LINKS),"Generation output already exists: "+output);
        Path ancestor=output.getParent();
        while(!Files.exists(ancestor,java.nio.file.LinkOption.NOFOLLOW_LINKS))ancestor=ancestor.getParent();
        output=ancestor.toRealPath().resolve(ancestor.relativize(output)).normalize();
        require(output.startsWith(root.resolve("results/capture")),"Generation output escapes results/capture");
        return output;
    }

    static boolean secondaryIndexes(Map<String,Object> document) {
        if (!document.containsKey("trace")) return false;
        var trace = object(document,"trace");
        keys(trace,Set.of("secondary_indexes", "workload_requests"));
        return bool(trace,"secondary_indexes");
    }

    static long requestTarget(Map<String,Object> document) {
        if (!document.containsKey("trace")) return 0;
        var trace = object(document,"trace");
        return trace.containsKey("workload_requests") ? positive(trace,"workload_requests") : 0;
    }

    static void validateNative(String benchmark,Map<String,Object> n) {
        var common=Set.of("build_directory","timeout_minutes");
        var allowed=new java.util.HashSet<>(common);
        allowed.addAll(switch(benchmark) {
            case "ycsb"->Set.of("table","recordcount","insertstart","insertcount","fieldcount","fieldlength",
                    "fieldlengthdistribution","insertorder","zeropadding","threads","load_threads","workloads");
            case "sysbench"->Set.of("tables","table-size","secondary","create-secondary","auto-inc","skip-trx",
                    "rand-seed","rand-type","threads","prepare_threads","binary","workloads");
            case "hybench"->Set.of("sf","clients","ap_clients","xp_transaction_clients","xp_query_clients",
                    "duration_minutes","phase_duration_minutes","parameters");
            default->throw new IllegalArgumentException("Unknown benchmark");
        });keys(n,allowed);
        require(positive(n,"timeout_minutes")<=1440,"timeout_minutes exceeds 1440");string(n,"build_directory");
        if(benchmark.equals("hybench")) {
            require(string(n,"sf").matches("[1-9][0-9]*x"),"Invalid HyBench scale");
            HybenchParameters.validate(n);
            for(String key:List.of("clients","ap_clients","xp_transaction_clients","xp_query_clients"))
                require(positive(n,key)<=256,"Invalid HyBench client count");
            require(positive(n,"duration_minutes")<=1440,"Invalid HyBench duration");
            if(n.containsKey("phase_duration_minutes")) {
                var phases=object(n,"phase_duration_minutes");keys(phases,Set.of("ap","tp","xp"));
                for(String phase:phases.keySet())require(positive(phases,phase)<=1440,"Invalid phase duration");
            }
            return;
        }
        require(positive(n,"threads")<=256,"Client threads exceeds 256");
        if(benchmark.equals("ycsb")) {
            require(string(n,"table").equals("usertable"),"YCSB table must be usertable");
            require(positive(n,"recordcount")==positive(n,"insertcount"),"YCSB insertcount must equal recordcount");
            require(integer(n,"insertstart")>=0&&integer(n,"zeropadding")>=1,"Invalid YCSB key range");
            Math.addExact(integer(n,"insertstart"),integer(n,"recordcount"));
            require(positive(n,"fieldcount")<=1024,"Too many YCSB fields");positive(n,"fieldlength");
            require(positive(n,"load_threads")<=64,"Too many load threads");
            require(Set.of("hashed","ordered").contains(string(n,"insertorder")),"Invalid YCSB insert order");
            require(Set.of("constant","uniform","zipfian").contains(string(n,"fieldlengthdistribution")),"Invalid field length distribution");
        }else {
            require(positive(n,"tables")<=10000,"Too many sysbench tables");positive(n,"table-size");
            require(positive(n,"prepare_threads")<=64,"Too many prepare threads");
            require(!bool(n,"secondary")&&bool(n,"create-secondary"),"sysbench requires primary and k indexes");
            bool(n,"auto-inc");bool(n,"skip-trx");require(integer(n,"rand-seed")>=0,"Negative random seed");
            require(Set.of("uniform","gaussian","special","pareto","zipfian").contains(string(n,"rand-type")),"Invalid random distribution");
            require(string(n,"binary").equals("src/sysbench"),"Use the verified sysbench binary");
        }
        var workloads=object(n,"workloads");require(!workloads.isEmpty(),"At least one workload is required");
        var names=benchmark.equals("ycsb")?Set.of("workloada","workloadb","workloadc","workloadf","workloadd","workloade"):
                Set.of("oltp_read_write","oltp_update_index","oltp_update_non_index","oltp_delete","oltp_insert","oltp_point_select");
        for(String name:workloads.keySet()) {
            require(names.contains(name),"Unsupported workload: "+name);var workload=object(workloads,name);
            keys(workload,benchmark.equals("ycsb")?Set.of("operationcount","properties"):Set.of("events"));
            positive(workload,benchmark.equals("ycsb")?"operationcount":"events");
            if(workload.containsKey("properties")) {
                var properties=object(workload,"properties");keys(properties,Set.of("readallfields","writeallfields"));
                properties.keySet().forEach(key->bool(properties,key));
            }
        }
    }

    public static List<Map<String,Object>> run(Path root,Path configuration)throws Exception {
        Plan plan=plan(root,configuration);var reports=new ArrayList<Map<String,Object>>();
        for(Input input:plan.inputs())reports.add(switch(input.selection().engine()) {
            case POSTGRES -> PostgresGeneration.generate(plan,input);
            case ROCKSDB -> RocksGeneration.generate(plan,input);
            case DUCKDB -> generate(plan,input);
        });
        return List.copyOf(reports);
    }

    private static Map<String,Object> generate(Plan plan,Input input)throws Exception {
        long started=System.nanoTime();var timings=new LinkedHashMap<String,Double>();
        var nativeBuild=plan.engines().get(Engine.DUCKDB);var settings=object(plan.runtime(),"duckdb");
        String memory=string(settings,"memory_limit");int threads=Math.toIntExact(positive(settings,"threads"));
        Path out=input.output();Files.createDirectories(out.getParent());Files.createDirectory(out);
        Path scratch=Files.createTempDirectory("sibench-duckdb-");Path socket=scratch.resolve("host.sock");
        Process host=null;DuckDbTrace.Draft draft;Throwable primaryFailure=null;
        try {
            json(out.resolve("effective-config.json"),Map.of("runtime",plan.runtime(),"benchmark",input.document(),"mode",input.selection().mode(),"engine",input.selection().engine()));
            var engine=new LinkedHashMap<String,Object>(nativeBuild.identity());
            engine.put("memory_limit",memory);engine.put("threads",threads);
            json(out.resolve("engine-build.json"),engine);json(out.resolve("client-build.json"),input.buildIdentity());
            var process=new ProcessBuilder(nativeBuild.directory().resolve("duckdb_host").toString(),out.resolve("data").toString(),socket.toString(),memory,Integer.toString(threads))
                    .redirectError(out.resolve("host.err").toFile()).redirectOutput(out.resolve("host.out").toFile());
            process.environment().remove("LD_LIBRARY_PATH");process.environment().remove("LD_PRELOAD");
            host=process.start();long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(30);
            while(!Files.exists(socket)) {
                if(!host.isAlive())throw new IOException("DuckDB host failed during setup; see "+out.resolve("host.err"));
                if(System.nanoTime()>deadline)throw new IOException("DuckDB host setup timed out");Thread.sleep(20);
            }
            try(var database=DuckDbHostDriver.open(socket)) {
                var loaded=DuckDbDatabase.identity(database);
                require(loaded.sourceId()!=null&&loaded.sourceId().matches("[0-9a-f]{7,40}")
                                &&string(object(engine,"inputs"),"source_commit").startsWith(loaded.sourceId()),
                        "Loaded DuckDB source differs from the verified engine build");
                engine.put("engine",loaded);
                var expected=input.baselineRows();
                long loadStart=System.nanoTime();
                Path load=Files.createDirectory(out.resolve("load"));
                if(input.selection().benchmark().equals("sysbench"))sysbench(plan,input,socket,load,"oltp_read_write",true);
                else hybench(plan,input,load,true,null,Engine.DUCKDB,DuckDbHostDriver.PREFIX+socket,null,()->{});
                if(input.selection().benchmark().equals("hybench")) {
                    Path data=out.resolve("native/Data_"+string(input.nativeConfig(),"sf"));
                    Path script=input.source().source().resolve("conf/load_data_pg.sql"),manifest=load.resolve("inputs.json");
                    HybenchInputs.record(data,script,manifest);
                    // COPY returns actual inserted counts; subsequent index/sequence setup does not change rows.
                    requireRows(expected,Hybench.load(database,input.source().source(),data));
                    HybenchInputs.requireRuntimeInputs(data,manifest);
                    HybenchInputs.dropLoadInputs(data,script);
                }
                timings.put("native_load_seconds",seconds(loadStart));long baselineStart=System.nanoTime();
                database.setAutoCommit(false);NativeHistory.initialize(database, org.sibench.config.Engine.DUCKDB);int tableId=0;
                for(var table:input.source().tables())NativeHistory.install(database, org.sibench.config.Engine.DUCKDB,++tableId,"main",table.name(),
                        table.indexes().stream().filter(SourceSchema.Index::primary).findFirst().orElseThrow().column());
                database.commit();
                var budget=new RequestBudget(requestTarget(input.document()),input.timeout());
                try(var trace=new DuckDbTrace(input.selection().mode(),input.source(),out.resolve("trace"),secondaryIndexes(input.document()),budget)) {
                    trace.baseline(database,expected);database.commit();database.setAutoCommit(true);
                    timings.put("baseline_indexes_seconds",seconds(baselineStart));long workloadStart=System.nanoTime();
                    boolean hybench=input.selection().benchmark().equals("hybench");
                    var names=hybench?List.of("ap","tp","xp"):List.copyOf(object(input.nativeConfig(),"workloads").keySet());
                    int remaining=hybench?2:names.size();long rawPrefix=0;double projectionSeconds=0;
                    Path workloads=Files.createDirectory(out.resolve("workload"));
                    for(String name:names) {
                        Path stageRoot=Files.createDirectory(workloads.resolve(name));
                        boolean setup=hybench&&name.equals("ap");
                        budget.begin(name,setup?0:remaining--,null);
                        int batch=0;
                        do {
                            budget.checkTime();long before=budget.count();
                            Path window=Files.createDirectory(stageRoot.resolve("batch-"+batch++)),raw=window.resolve("native.csv");
                            sql(database,"CALL enable_logging(['SibenchQuery','SibenchTransaction','SibenchMutation','SibenchIndex','SibenchIndexRows','SibenchConsumption'],storage_path="+literal(raw.toString())+")");
                            Stage stage=hybench?hybench(plan,input,window,false,name,Engine.DUCKDB,DuckDbHostDriver.PREFIX+socket,null,()->{}):sysbench(plan,input,socket,window,name,false);
                            sql(database,"CALL disable_logging()");
                            sql(database,"SET logging_storage='memory'");
                            long projectionStart=System.nanoTime();
                            try(var capture=DuckDbHostDriver.open(socket)) {
                                var counts=DuckDbFacts.load(capture,raw,rawPrefix);
                                rawPrefix=Math.addExact(rawPrefix,counts.values().stream().mapToLong(Long::longValue).sum());
                                var receipts=DuckDbReceipts.load(capture,stage.receipts(),window.resolve("receipts"));
                                DuckDbStatements.bind(capture,receipts);DuckDbReceipts.bind(capture);
                                if(stage.calls()>=0)require(number(capture,"SELECT count(*) FROM sibench_sql_calls WHERE kind='query' AND error_code=0")==stage.calls(),"sysbench source summary/receipt count mismatch");
                                DuckDbTransactions.bind(capture,"data",trace.lastVersion());DuckDbIdentities.bind(capture);
                                DuckDbAccesses.bind(capture,IndexCatalog.duckDbDefinitions(input.source()));DuckDbMutations.bind(capture);
                                trace.append(capture);
                            }
                            projectionSeconds+=seconds(projectionStart);
                            // The window's facts and receipts are projected and checked; they are not retained.
                            Files.delete(raw);deleteTree(window.resolve("clients"));deleteTree(window.resolve("receipts"));
                            if(budget.enabled()&&!setup)budget.requireProgress(before);
                        } while(budget.enabled()&&!setup&&!budget.reached());
                        budget.end();
                    }
                    timings.put("workload_and_projection_seconds",seconds(workloadStart));
                    timings.put("facts_and_projection_seconds",projectionSeconds);
                    json(out.resolve("request-budget.json"),budget.finish());
                    var evidence=new LinkedHashMap<String,Object>();evidence.put("engine",engine);evidence.put("client",input.buildIdentity());
                    evidence.put("effective_config_sha256",Digests.sha256(out.resolve("effective-config.json")));
                    evidence.put("baseline_rows",expected);evidence.put("request_budget",budget.finish());
                    if(hybench) {
                        evidence.put("load_inputs_sha256",Digests.sha256(load.resolve("inputs.json")));
                        evidence.put("parameters_sha256",Digests.sha256(out.resolve("native/parameters/parameters.toml")));
                    }
                    draft=trace.finish(database,evidence);
                }
            }
            long closeStart=System.nanoTime();
            stop(host);host=null;deleteTree(out.resolve("data"));
            timings.put("native_close_seconds",seconds(closeStart));long publicationStart=System.nanoTime();
            var manifest=draft.publish();
            timings.put("publication_seconds",seconds(publicationStart));timings.put("total_seconds",seconds(started));
            json(out.resolve("result.json"),Map.of("status","complete","trace",out.resolve("trace").toString(),"timings",timings));
            return manifest;
        }catch(Exception|Error failure) {
            primaryFailure=failure;
            try{Files.writeString(out.resolve("failure.txt"),failure.toString()+"\n",StandardOpenOption.CREATE_NEW);}catch(Exception diagnostic){failure.addSuppressed(diagnostic);}
            if(host!=null)try{stop(host);}catch(Exception cleanup){failure.addSuppressed(cleanup);}host=null;throw failure;
        }finally {
            try {
                if(host!=null)stop(host);
                Files.deleteIfExists(socket);Files.deleteIfExists(scratch);
            }catch(Exception cleanup){if(primaryFailure!=null)primaryFailure.addSuppressed(cleanup);else throw cleanup;}
        }
    }

    private static double seconds(long start){return (System.nanoTime()-start)/1e9;}
    /** Removes a disposable working tree, such as a closed engine's data directory. */
    static void deleteTree(Path root)throws IOException {
        try(var paths=Files.walk(root)){for(Path path:paths.sorted(java.util.Comparator.reverseOrder()).toList())Files.delete(path);}
    }
    record Stage(List<Path> receipts,long calls,long inserted) {}
    private static Stage sysbench(Plan plan,Input input,Path socket,Path stage,String workload,boolean load)throws Exception {
        var n=input.nativeConfig();Path receipts=Files.createDirectory(stage.resolve("clients"));
        long events=load?0:positive(object(object(n,"workloads"),workload),"events");
        var args=new ArrayList<>(List.of(input.build().resolve("src/sysbench").toString(),input.build().resolve("src/lua/"+workload+".lua").toString(),
                "--db-driver=duckdb","--duckdb-socket="+socket,"--duckdb-ledger-dir="+receipts,"--time=0","--events="+events,
                "--threads="+positive(n,load?"prepare_threads":"threads")));
        for(String key:List.of("tables","table-size","secondary","create-secondary","auto-inc","skip-trx","rand-seed","rand-type")) {
            Object value=n.get(key);args.add("--"+key+"="+(value instanceof Boolean flag?(flag?"on":"off"):value));
        }
        args.add(load?"prepare":"run");var result=NativeProcess.run(load?"prepare":"run",args,
                Map.of("LUA_PATH",input.build().resolve("src/lua")+"/?.lua;;"),plan.root(),stage,input.timeout());
        require(result.exit()==0,"sysbench process failed: "+stage);long calls=-1;
        if(!load) {
            var measured=SysbenchSummary.parse(result.stdout());require(measured.getOrDefault("transactions",-1L)==events,"sysbench did not complete configured events");
            require(measured.getOrDefault("reconnects",0L)==0,"Unexpected sysbench reconnection");calls=measured.getOrDefault("total",-1L);
            require(calls>=0,"Missing sysbench statement count");json(stage.resolve("summary.json"),measured);
        }
        var files=files(receipts);require(!files.isEmpty(),"sysbench missing session receipts");return new Stage(files,calls,0);
    }

    /** One upstream client process; DuckDB connections leave receipts, PG sessions are captured natively. */
    static Stage hybench(Plan plan,Input input,Path stage,boolean load,String selectedPhase,Engine engine,String url,
            Path stop,NativeProcess.Check check)throws Exception {
        Path nativeDirectory=input.output().resolve("native");Files.createDirectories(nativeDirectory);
        if(!load)HybenchInputs.requireRuntimeInputs(nativeDirectory.resolve("Data_"+string(input.nativeConfig(),"sf")),
                input.output().resolve("load/inputs.json"));
        var properties=new java.util.Properties();try(var source=Files.newInputStream(input.source().source().resolve("conf/pg.props"))){properties.load(source);}
        for(String key:new ArrayList<>(properties.stringPropertyNames()))if(key.startsWith("url")||key.startsWith("username")||key.startsWith("password")||key.startsWith("classname"))properties.remove(key);
        var n=input.nativeConfig();properties.setProperty("sf",string(n,"sf"));properties.setProperty("db",engine==Engine.DUCKDB?"duckdb":"postgreSQL");
        Path parameters=nativeDirectory.resolve("parameters"),parameterFile=parameters.resolve("parameters.toml");
        String parameterText=new com.moandjiezana.toml.TomlWriter().write(Map.of(string(n,"sf"),input.parameters()));
        if(load) {
            Files.createDirectory(parameters);Files.writeString(parameterFile,parameterText,StandardOpenOption.CREATE_NEW);
        }else require(Files.readString(parameterFile).equals(parameterText),"HyBench effective parameters changed after load");
        properties.setProperty("url",url);properties.setProperty("url_ap",url);
        properties.setProperty("tpclient",Long.toString(positive(n,"clients")));properties.setProperty("apclient",Long.toString(positive(n,"ap_clients")));
        properties.setProperty("xtpclient",Long.toString(positive(n,"xp_transaction_clients")));properties.setProperty("xapclient",Long.toString(positive(n,"xp_query_clients")));
        for(String phase:List.of("ap","tp","xp"))properties.setProperty(phase+"RunMins",Long.toString(n.containsKey("phase_duration_minutes")&&object(n,"phase_duration_minutes").containsKey(phase)?positive(object(n,"phase_duration_minutes"),phase):positive(n,"duration_minutes")));
        if(selectedPhase!=null)properties.setProperty("sibench.phase",selectedPhase);
        Path receipts=engine==Engine.DUCKDB?Files.createDirectory(stage.resolve("clients")):null;
        if(receipts!=null)properties.setProperty("sibench.receipts",receipts.toString());
        Path resolved=stage.resolve("resolved.props");try(var file=Files.newOutputStream(resolved,StandardOpenOption.CREATE_NEW)){properties.store(file,"Effective native HyBench configuration");}
        String cp=parameters+":"+input.build().resolve("target/classes")+":"+input.build().resolve("lib/*")+":"+NativeProcess.runtimeClasspath();
        var command=List.of(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-Xmx512m","-cp",cp,Hybench.class.getName(),load?"generate":"run",resolved.toString(),input.build().resolve("conf/stmt_postgres.toml").toString(),stage.resolve("summary.json").toString());
        var result=NativeProcess.run(load?"generate":"run",command,stop==null?Map.of():Map.of("SIBENCH_BUDGET_STOP",stop.toString()),
                nativeDirectory,stage,input.timeout(),check);
        require(result.exit()==0,"HyBench process failed: "+stage);return new Stage(receipts==null?List.of():files(receipts),-1,0);
    }

    static void requireRows(Map<String,Long> expected,Map<String,Long> actual) throws IOException {
        if(!expected.equals(actual))throw new IOException("Native baseline row counts differ: expected "+expected+", actual "+actual);
    }

    private static void stop(Process process)throws Exception {
        try {
            process.destroy();if(!process.waitFor(30,TimeUnit.SECONDS)) {process.destroyForcibly();process.waitFor(5,TimeUnit.SECONDS);throw new IOException("DuckDB host shutdown timed out");}
            if(process.exitValue()!=0)throw new IOException("DuckDB host exited with "+process.exitValue());
        }finally{if(process.isAlive())process.destroyForcibly();}
    }
    private static List<Path> files(Path directory)throws IOException {try(var files=Files.list(directory)){return files.sorted().toList();}}
    private static void keys(Map<String,Object> map,Set<String> allowed){for(String key:map.keySet())require(allowed.contains(key),"Unsupported generation option: "+key);}
    private static void json(Path file,Object value)throws IOException {Files.writeString(file,new GsonBuilder().setPrettyPrinting().create().toJson(value),StandardOpenOption.CREATE_NEW);}
    private static void sql(Connection db,String text)throws Exception {try(var q=db.createStatement()){q.execute(text);}}
    private static long number(Connection db,String text)throws Exception {try(var q=db.createStatement();var r=q.executeQuery(text)){if(!r.next())throw new IOException("Missing native count");return r.getLong(1);}}
    private static String quote(String value){return "\""+value.replace("\"","\"\"")+"\"";}
    private static String literal(String value){return "'"+value.replace("'","''")+"'";}
}
