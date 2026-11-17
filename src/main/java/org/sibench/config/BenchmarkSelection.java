package org.sibench.config;

import static org.sibench.config.ConfigValues.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Named workload selections and their persistent Trace locations. */
public record BenchmarkSelection(Path config, String benchmark, Mode mode, Engine engine,
        String name, String workload, Path directory, boolean generate, String upload) {
    public enum Mode { API, CAPTURE, CDC }

    public static void validate(Object value) {
        var selections = object(Map.of("benchmarks", value == null ? "" : value), "benchmarks");
        require(!selections.isEmpty(), "benchmarks must be a nonempty mapping");
        for (String name : selections.keySet()) {
            require(name.matches("[a-z0-9][a-z0-9_-]*"), "Invalid benchmark entry name: " + name);
            var item = object(selections, name);
            for (String key : item.keySet()) require(Set.of("config", "engine", "workload", "trace").contains(key), "Unknown benchmark option: " + key);
            require(!string(item, "config").isBlank(), "Benchmark config must not be blank");
            engine(item);
            if (item.containsKey("workload")) require(!string(item, "workload").isBlank(), "Empty workload selection");
            var trace = object(item, "trace");
            SibenchConfig.keys(trace, Set.of("generate", "directory", "upload"), "benchmark trace");
            bool(trace, "generate");
            String directory = string(trace, "directory");
            require(!directory.isBlank() && !directory.contains("://"), "Trace directory must be a local path");
            var upload = object(trace, "upload");
            for (String key : upload.keySet()) require(Set.of("enabled", "path").contains(key), "Unknown Trace upload option: " + key);
            boolean enabled = bool(upload, "enabled");
            if (enabled || upload.containsKey("path")) ObjectStore.uri(string(upload, "path"));
        }
    }

    /** Consumers resolve paths without loading source configurations, builds or cloud credentials. */
    public static Map<String, Path> directories(Path root, Map<String, Object> runtime) {
        validate(runtime.get("benchmarks"));
        var selections = object(runtime, "benchmarks");
        var result = new LinkedHashMap<String, Path>();
        for (String name : selections.keySet()) {
            Path directory = root.resolve(string(object(object(selections, name), "trace"), "directory")).toAbsolutePath().normalize();
            for (Path previous : result.values()) require(!directory.startsWith(previous) && !previous.startsWith(directory), "Overlapping Trace directories");
            result.put(name, directory);
        }
        return java.util.Collections.unmodifiableMap(result);
    }

    public static Path directory(Path root, Map<String, Object> runtime, String name) {
        Path result = directories(root, runtime).get(name);
        require(result != null, "Unknown benchmark entry: " + name);
        return result;
    }

    public static List<BenchmarkSelection> read(Path root, Map<String, Object> runtime) throws IOException {
        var directories = directories(root, runtime);
        var selections = object(runtime, "benchmarks");
        var identities = new HashSet<String>();
        var sources = new LinkedHashMap<String, String>();
        var result = new ArrayList<BenchmarkSelection>();
        for (String name : selections.keySet()) {
            var item = object(selections, name);
            Path path = root.resolve(string(item, "config")).toRealPath();
            var profile = parse(Files.readString(path));
            String benchmark = string(profile, "benchmark");
            require(Set.of("ycsb", "sysbench", "hybench").contains(benchmark), "Unsupported benchmark: " + benchmark);
            Engine engine = engine(item);
            // The engine fixes the generation mode: RocksDB records YCSB API calls, SQL engines capture.
            boolean supported = benchmark.equals("ycsb") ? engine == Engine.ROCKSDB
                    : engine == Engine.DUCKDB || engine == Engine.POSTGRES;
            require(supported, "Unsupported benchmark/engine combination: " + benchmark + "/" + engine);
            Mode mode = engine == Engine.ROCKSDB ? Mode.API : Mode.CAPTURE;
            String source = path + ":" + mode + ":" + engine;
            String previous = sources.putIfAbsent(benchmark, source);
            require(previous == null || previous.equals(source), "Conflicting source configurations for " + benchmark);
            var workloads = benchmark.equals("hybench") ? Set.of("tp", "xp") : object(object(profile, "native"), "workloads").keySet();
            require(item.containsKey("workload") || workloads.size() == 1, "Select one workload for " + name);
            String workload = item.containsKey("workload") ? string(item, "workload") : workloads.iterator().next();
            require(workloads.contains(workload), "Unknown workload for " + name + ": " + workload);
            require(identities.add(benchmark + ":" + workload), "Duplicate benchmark workload: " + benchmark + "/" + workload);
            var trace = object(item, "trace"); var upload = object(trace, "upload");
            result.add(new BenchmarkSelection(path, benchmark, mode, engine, name, workload, directories.get(name),
                    bool(trace, "generate"), bool(upload, "enabled") ? string(upload, "path") : null));
        }
        return List.copyOf(result);
    }

    private static Engine engine(Map<String, Object> item) { return Engine.of(string(item, "engine")); }
}
