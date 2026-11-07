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

/** One embedded native database from streaming upstream load through final publication. */
final class RocksGeneration {
    private RocksGeneration() {}
    static Map<String, Object> generate(Generation.Plan plan, Generation.Input input) throws Exception {
        long start = System.nanoTime(); var timings = new LinkedHashMap<String, Double>();
        var build = plan.engines().get(org.sibench.config.Engine.ROCKSDB);
        Path library = build.directory().resolve("libsibench_rocksdb.so");
        RocksDbTimestamps.load(library);
        var config = object(plan.runtime(), "rocksdb");
        var settings = new RocksDbRows.Settings(positive(config, "cache_bytes"), positive(config, "write_buffer_bytes"),
                Math.toIntExact(positive(config, "background_jobs")));
        var n = input.nativeConfig(); int fields = Math.toIntExact(positive(n, "fieldcount"));
        Path out = input.output(); Files.createDirectories(out.getParent()); Files.createDirectory(out);
        json(out.resolve("effective-config.json"), Map.of("runtime", plan.runtime(), "benchmark", input.document(), "engine", "rocksdb", "mode", "api"));
        json(out.resolve("engine-build.json"), build.identity()); json(out.resolve("client-build.json"), input.buildIdentity());
        var stages = new LinkedHashMap<String, Object>();
        try (var trace = new RocksTrace(out.resolve("trace"), input.source(), Generation.secondaryIndexes(input.document()))) {
            long closeStarted;
            var rows = new RocksDbRows(out.resolve("trace/data"), fields, settings, trace::event);
            try (rows) {
                var binding = new RocksYcsb(rows, trace::baseline, fields);
                RocksYcsb.install(binding);
                try {
                    long load = System.nanoTime();
                    stages.put("load", stage(plan, input, binding, "load", positive(n, "recordcount")));
                    long expected = positive(n, "recordcount");
                    Generation.requireRows(input.baselineRows(), Map.of(string(n, "table"), rows.baselineRows()));
                    rows.finishLoad(); trace.finishBaseline(expected);
                    timings.put("baseline_generation_load_export_seconds", seconds(load));
                    long workload = System.nanoTime(), records = expected;
                    for (String name : object(n, "workloads").keySet()) {
                        // Return freed load/previous-stage allocations before the next client starts.
                        RocksDbTimestamps.releaseMemory();
                        var result = stage(plan, input, binding, name, records); stages.put(name, result);
                        records += ((Number) result.get("inserted")).longValue();
                    }
                    timings.put("workload_and_export_seconds", seconds(workload));
                    long seal = System.nanoTime(); rows.seal();
                    timings.put("native_flush_seconds", seconds(seal));
                    json(out.resolve("engine-statistics.json"), rows.statistics());
                } finally { RocksYcsb.install(null); }
                closeStarted = System.nanoTime();
            }
            trace.close(); timings.put("native_close_seconds", seconds(closeStarted));
            long publication = System.nanoTime();
            var manifest = trace.publish(library, rows.writtenFiles(), Map.of("engine", build.identity(), "settings", config,
                    "client", input.buildIdentity(), "stages", stages, "baseline_rows", input.baselineRows(),
                    "effective_config_sha256", Digests.sha256(out.resolve("effective-config.json"))));
            timings.put("publication_seconds", seconds(publication)); timings.put("total_seconds", seconds(start));
            json(out.resolve("result.json"), Map.of("status", "complete", "trace", out.resolve("trace").toString(), "timings", timings));
            return manifest;
        } catch (Exception | Error failure) {
            var stack = new java.io.StringWriter(); failure.printStackTrace(new java.io.PrintWriter(stack));
            try { Files.writeString(out.resolve("failure.txt"), stack.toString(), StandardOpenOption.CREATE_NEW); }
            catch (IOException diagnostic) { failure.addSuppressed(diagnostic); }
            throw failure;
        }
    }

    private static Map<String, Object> stage(Generation.Plan plan, Generation.Input input, RocksYcsb binding,
            String name, long records) throws Exception {
        boolean load = name.equals("load"); var n = input.nativeConfig();
        Path stage = Files.createDirectory(input.output().resolve(name));
        var properties = new LinkedHashMap<String, Object>();
        for (String key : List.of("table", "fieldcount", "fieldlength", "fieldlengthdistribution", "insertorder", "zeropadding", "insertstart"))
            properties.put(key, n.get(key));
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
        if (YcsbSummary.observed(measured).get("total") != wanted) throw new IOException("YCSB did not complete configured operations: " + stage);
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
