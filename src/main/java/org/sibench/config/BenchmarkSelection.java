package org.sibench.config;

import static org.sibench.config.ConfigValues.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Resolves exactly the selected benchmark profiles, without generating data or opening a database. */
public record BenchmarkSelection(Path config, String benchmark, Mode mode,Engine engine) {
    public enum Mode { API, CAPTURE }

    public static void validate(Object value) {
        require(value instanceof List<?> && !((List<?>) value).isEmpty(),
                "benchmarks must be a nonempty list");
        var paths = new HashSet<String>();
        for (Object entry : (List<?>) value) {
            var item = item(entry);
            String config = string(item, "config");
            require(!config.isBlank(), "Benchmark config must not be blank");
            require(paths.add(config), "Duplicate benchmark config: " + config);
            mode(item);
            engine(item);
        }
    }

    public static List<BenchmarkSelection> read(Path root, Map<String, Object> runtime) throws IOException {
        Object selections = runtime.get("benchmarks");
        validate(selections);
        Path project = root.toRealPath();
        var names = new HashSet<String>();
        var paths = new HashSet<Path>();
        var result = new ArrayList<BenchmarkSelection>();
        for (Object entry : (List<?>) selections) {
            var item = item(entry);
            Path path = project.resolve(string(item, "config")).toRealPath();
            require(paths.add(path), "Duplicate benchmark config: " + path);
            var profile = parse(Files.readString(path));
            String benchmark = string(profile, "benchmark");
            require(Set.of("ycsb", "sysbench", "hybench").contains(benchmark),
                    "Unsupported benchmark: " + benchmark);
            require(names.add(benchmark), "Duplicate benchmark: " + benchmark);
            Mode mode = mode(item);
            require(mode == (benchmark.equals("ycsb") ? Mode.API : Mode.CAPTURE),
                    "Unsupported benchmark/mode combination: " + benchmark + "/" + string(item, "mode"));
            require((benchmark.equals("ycsb")) == (engine(item) == Engine.ROCKSDB),
                    "YCSB requires RocksDB; RocksDB supports only YCSB API generation");
            result.add(new BenchmarkSelection(path, benchmark, mode,engine(item)));
        }
        return List.copyOf(result);
    }

    private static Map<String, Object> item(Object entry) {
        var item = object(java.util.Collections.singletonMap("benchmark selection", entry), "benchmark selection");
        require(item.keySet().equals(Set.of("config", "mode","engine")),
                "Each benchmark selection requires exactly config, mode and engine");
        return item;
    }

    private static Mode mode(Map<String, Object> item) {
        return switch (string(item, "mode")) {
            case "api" -> Mode.API;
            case "capture" -> Mode.CAPTURE;
            default -> throw new IllegalArgumentException("Unknown benchmark mode: " + string(item, "mode"));
        };
    }

    private static Engine engine(Map<String,Object> item) {
        return switch(string(item,"engine")) {
            case "duckdb"->Engine.DUCKDB;
            case "postgres"->Engine.POSTGRES;
            case "rocksdb"->Engine.ROCKSDB;
            default->throw new IllegalArgumentException("Unknown benchmark engine: "+string(item,"engine"));
        };
    }
}
