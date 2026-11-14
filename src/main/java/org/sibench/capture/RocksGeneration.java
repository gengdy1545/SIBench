package org.sibench.capture;

import org.sibench.config.Digests;
import static org.sibench.config.ConfigValues.*;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.net.URLClassLoader;
import java.nio.file.*;
import java.util.*;
import org.sibench.generation.RocksDbRows;
import org.sibench.generation.RocksDbTimestamps;

/** Independent YCSB Traces restored from one closed initial database; all working databases are discarded. */
final class RocksGeneration {
    private RocksGeneration() {}
    static List<Map<String,Object>> generate(Generation.Plan plan,Generation.Input input) throws Exception {
        Path out=input.output();
        Files.createDirectories(out.getParent());Files.createDirectory(out);
        var build=plan.engines().get(org.sibench.config.Engine.ROCKSDB);
        RocksDbTimestamps.load(build.directory().resolve("libsibench_rocksdb.so"));
        var config=build.settings();
        var settings=new RocksDbRows.Settings(positive(config,"cache_bytes"),positive(config,"write_buffer_bytes"),
                Math.toIntExact(positive(config,"background_jobs")));
        var n=input.nativeConfig();int fields=Math.toIntExact(positive(n,"fieldcount"));
        long expected=positive(n,"recordcount");
        var timings=new LinkedHashMap<String,Double>();
        json(out.resolve("effective-config.json"),Generation.effectiveConfig(plan,input));
        Path initial=Files.createDirectory(out.resolve("initial"));
        try {
            long loadStarted=System.nanoTime();RocksTrace.Baseline baseline;Map<String,Object> load;
            try(var trace=new RocksTrace(initial.resolve("trace"),input.source(),false,null)) {
                try(var rows=new RocksDbRows(initial.resolve("database"),fields,settings,trace::event)) {
                    var binding=new RocksYcsb(rows,trace::baseline,fields);RocksYcsb.install(binding);
                    try {
                        load=stage(plan,input,binding,"load",expected,out.resolve("load"),new RequestBudget(0,input.timeout()));
                        Generation.requireRows(input.baselineRows(),Map.of(string(n,"table"),rows.baselineRows()));
                        rows.finishLoad();trace.finishBaseline(expected);
                    }finally{RocksYcsb.install(null);}
                }
                baseline=trace.baseline();
            }
            timings.put("initial_load_and_export_seconds",seconds(loadStarted));
            var reports=new ArrayList<Map<String,Object>>();String initialIdentity=null;
            var catalog=new LinkedHashMap<String,Object>();
            for(String name:Generation.workloads(input.document())) {
                var document=new LinkedHashMap<>(input.document());var nativeConfig=new LinkedHashMap<>(n);
                nativeConfig.put("workloads",Map.of(name,object(object(n,"workloads"),name)));document.put("native",nativeConfig);
                Path destination=out.resolve(name);
                document.put("artifacts",Map.of("directory",plan.root().relativize(destination).toString()));
                var source=org.sibench.schema.SourceSchema.of("ycsb",input.source().source(),nativeConfig);
                var entry=plan.selections().stream().filter(s->s.benchmark().equals("ycsb")&&s.workload().equals(name)).findFirst().orElseThrow();
                var selected=new Generation.Input(entry,document,source,input.build(),destination,
                        input.buildIdentity(),input.baselineRows(),input.parameters());
                var report=workload(plan,selected,name,settings,initial.resolve("database"),baseline,load,initialIdentity);
                String identity=(String)object(object(report,"source_evidence"),"initial_state").get("physical_sha256");
                if(initialIdentity==null)initialIdentity=identity;
                reports.add(report);catalog.put(entry.name(),Map.of("trace",entry.directory().toString(),"manifest_sha256",Digests.sha256(destination.resolve("trace/manifest.json"))));
            }
            String identity=initialIdentity;
            Generation.publishThenDiscard(()->{
                json(out.resolve("traces.json"),Map.of("traces",catalog,"baseline_rows",expected,"seed",integer(n,"seed"),
                        "initial_state_sha256",identity==null?"no_enabled_workloads":identity,"timings",timings));
                return null;
            },initial,timings);
            return List.copyOf(reports);
        }catch(Exception|Error failure){diagnostic(out,failure);throw failure;}
    }

    private static Map<String,Object> workload(Generation.Plan plan,Generation.Input input,String name,
            RocksDbRows.Settings settings,Path initial,RocksTrace.Baseline baseline,Map<String,Object> load,String expectedIdentity) throws Exception {
        long started=System.nanoTime();Path out=input.output();Files.createDirectory(out);
        var build=plan.engines().get(org.sibench.config.Engine.ROCKSDB);var n=input.nativeConfig();
        var timings=new LinkedHashMap<String,Double>();var stages=new LinkedHashMap<String,Object>();
        json(out.resolve("effective-config.json"),Generation.effectiveConfig(plan,input));
        json(out.resolve("engine-build.json"),build.identity());json(out.resolve("client-build.json"),input.buildIdentity());
        var budget=new RequestBudget(Generation.requestTarget(input.document()),input.timeout());
        try(var trace=new RocksTrace(out.resolve("trace"),input.source(),Generation.secondaryIndexes(input.document()),budget,baseline)) {
            long restore=System.nanoTime();String identity;long closeStarted;
            try(var rows=RocksDbRows.restore(initial,out.resolve("database"),Math.toIntExact(positive(n,"fieldcount")),settings,
                    baseline.rows(),trace::event)) {
                identity=rows.initialStateIdentity();
                if(expectedIdentity!=null&&!expectedIdentity.equals(identity))throw new IOException("YCSB initial physical state changed between workloads");
                timings.put("initial_state_restore_seconds",seconds(restore));
                var binding=new RocksYcsb(rows,trace::baseline,Math.toIntExact(positive(n,"fieldcount")));binding.budget(budget);
                RocksYcsb.install(binding);
                try {
                    budget.begin(name,1,null);
                    long workloadStarted=System.nanoTime(),records=baseline.rows();
                    var batches=new ArrayList<Map<String,Object>>();Path work=Files.createDirectory(out.resolve("workload"));
                    do {
                        budget.checkTime();long before=budget.count();RocksDbTimestamps.releaseMemory();
                        var result=stage(plan,input,binding,name,records,work.resolve("batch-"+batches.size()),budget);
                        batches.add(result);records+=((Number)result.get("inserted")).longValue();
                        if(budget.enabled())budget.requireProgress(before);
                    }while(budget.enabled()&&!budget.reached());
                    budget.end();stages.put(name,batches);json(out.resolve("request-budget.json"),budget.finish());
                    timings.put("workload_and_export_seconds",seconds(workloadStarted));
                }finally{RocksYcsb.install(null);}
                closeStarted=System.nanoTime();
            }
            trace.close();timings.put("native_close_seconds",seconds(closeStarted));
            var evidence=Map.<String,Object>of("engine",build.identity(),"settings",build.settings(),"client",input.buildIdentity(),
                    "stages",stages,"request_budget",budget.finish(),"baseline_rows",input.baselineRows(),
                    "initial_state",Map.of("physical_sha256",identity,"baseline_trace_sha256",baseline.identity(),
                            "seed",integer(n,"seed"),"load",load,"cache_condition","Fresh RocksDB block cache; OS page cache is not flushed"),
                    "effective_config_sha256",Digests.sha256(out.resolve("effective-config.json")));
            var manifest=Generation.publishThenDiscard(()->trace.publish(evidence),out.resolve("database"),timings);
            timings.put("total_seconds",seconds(started));
            json(out.resolve("result.json"),Map.of("status","complete","trace",input.selection().directory().toString(),"timings",timings));
            return manifest;
        }catch(Exception|Error failure){diagnostic(out,failure);throw failure;}
    }
    private static void diagnostic(Path output,Throwable failure) {
        var stack=new java.io.StringWriter();failure.printStackTrace(new java.io.PrintWriter(stack));
        try{Files.writeString(output.resolve("failure.txt"),stack.toString(),StandardOpenOption.CREATE_NEW);}
        catch(IOException secondary){failure.addSuppressed(secondary);}
    }

    private static Map<String, Object> stage(Generation.Plan plan, Generation.Input input, RocksYcsb binding,
            String name, long records, Path stage, RequestBudget budget) throws Exception {
        boolean load = name.equals("load"); var n = input.nativeConfig();
        Files.createDirectory(stage);
        var properties = new LinkedHashMap<String, Object>();
        for (String key : List.of("table", "fieldcount", "fieldlength", "fieldlengthdistribution", "insertorder", "zeropadding", "insertstart"))
            properties.put(key, n.get(key));
        properties.put("sibench.seed",integer(n,"seed"));
        properties.put("recordcount", records); properties.put("sibench.baseline", load);
        long wanted = load ? positive(n, "insertcount") : positive(object(object(n, "workloads"), name), "operationcount");
        properties.put(load ? "insertcount" : "operationcount", wanted);
        properties.put("exportfile", stage.resolve("measurements.txt"));
        properties.put("maxexecutiontime", input.timeout().toSeconds());
        if (!load && object(object(n, "workloads"), name).containsKey("properties"))
            properties.putAll(object(object(object(n, "workloads"), name), "properties"));
        long threads = positive(n, load ? "load_threads" : "threads");
        var args = new ArrayList<>(List.of(load ? "-load" : "-t", "-db", "org.sibench.capture.YcsbBinding", "-threads",
                Long.toString(threads), "-P", input.source().source().resolve("workloads/" + (load ? "workloada" : name)).toString()));
        properties.forEach((key, value) -> args.addAll(List.of("-p", key + "=" + value)));
        json(stage.resolve("arguments.json"), args);
        binding.stage(load);
        var urls = new ArrayList<java.net.URL>();
        try (var jars = Files.list(NativeBuildCheck.ycsbLibraries(input.build()))) {
            for (Path jar : jars.filter(p -> p.toString().endsWith(".jar")).sorted().toList()) urls.add(jar.toUri().toURL());
        }
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        java.io.PrintStream stdout = System.out, stderr = System.err;
        try (var loader = new URLClassLoader(urls.toArray(java.net.URL[]::new), RocksGeneration.class.getClassLoader());
             var output = new java.io.PrintStream(Files.newOutputStream(stage.resolve("client.out"), StandardOpenOption.CREATE_NEW));
             var error = new java.io.PrintStream(Files.newOutputStream(stage.resolve("client.err"), StandardOpenOption.CREATE_NEW))) {
            Thread.currentThread().setContextClassLoader(loader); System.setOut(output); System.setErr(error);
            try { Class.forName("site.ycsb.Client", true, loader).getMethod("run", String[].class).invoke(null, (Object) args.toArray(String[]::new)); }
            catch (java.lang.reflect.InvocationTargetException failure) { throw new IOException("YCSB client failed: " + stage, failure.getCause()); }
        } finally {
            System.setOut(stdout); System.setErr(stderr); Thread.currentThread().setContextClassLoader(previous);
        }
        var measured = YcsbSummary.parse(Files.readString(stage.resolve("measurements.txt")));
        long observed=YcsbSummary.observed(measured).get("total");
        if (observed>wanted || observed!=wanted && (load || !budget.reached()))
            throw new IOException("YCSB stopped before its configured operations or request budget: " + stage);
        long calls = 0;
        for (String kind : List.of("READ", "SCAN", "INSERT", "UPDATE", "DELETE")) calls += YcsbSummary.operations(measured, kind);
        if (binding.completed(threads) != calls) throw new IOException("YCSB binding/upstream operation counts disagree");
        long inserted = measured.getOrDefault("INSERT", Map.of()).getOrDefault("Return=OK", 0L);
        if (load && inserted != wanted) throw new IOException("YCSB baseline contains unsuccessful inserts");
        json(stage.resolve("summary.json"), measured);
        return Map.of("calls", calls, "inserted", inserted);
    }
    private static double seconds(long started) { return (System.nanoTime() - started) / 1e9; }
    private static void json(Path path, Object value) throws IOException {
        Files.writeString(path, new GsonBuilder().setPrettyPrinting().create().toJson(value), StandardOpenOption.CREATE_NEW);
    }
}
