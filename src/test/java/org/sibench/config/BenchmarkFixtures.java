package org.sibench.config;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/** Complete runtime entries for source integration tests. */
public final class BenchmarkFixtures {
    private BenchmarkFixtures() {}

    public static Map<String, Object> entry(Path config, String engine, Path trace) {
        return new LinkedHashMap<>(Map.of("config", config.toString(), "engine", engine,
                "trace", Map.of("generate", true, "directory", trace.toString(), "upload", Map.of("enabled", false))));
    }

    public static Map<String, Object> entry(Path config, String engine, Path trace, String workload) {
        var entry = entry(config, engine, trace);
        entry.put("workload", workload);
        return entry;
    }
}
