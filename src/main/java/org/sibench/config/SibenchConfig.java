package org.sibench.config;

import static org.sibench.config.ConfigValues.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

/** Runtime settings shared by replay, analysis and native generation. */
public final class SibenchConfig {
    private SibenchConfig() {}

    public static Map<String, Object> read(Path path) throws IOException {
        var document = parse(Files.readString(path));
        validate(document);
        return document;
    }

    public static void validate(Map<String, Object> document) {
        require(integer(document, "schema_version") == 1, "Expected SIBench config v1");
        require(string(document, "purpose").equals("sibench"), "Expected sibench purpose");
        for (String key : document.keySet())
            require(Set.of("schema_version", "purpose", "target", "replay",
                    "analysis", "benchmarks", "engines", "pixels_experiment").contains(key), "Unknown SIBench option: " + key);
        if (document.containsKey("pixels_experiment")) PixelsExperimentConfig.settings(document);
        if (document.containsKey("benchmarks")) BenchmarkSelection.validate(document.get("benchmarks"));
        if (document.containsKey("engines")) {
            var engines = object(document, "engines");
            for (String name : engines.keySet()) {
                Engine.of(name);
                require(!string(engines, name).isBlank(), "Engine config path must not be blank: " + name);
            }
        }
    }

    /** Reads and validates the engine configuration the runtime selects; paths resolve against the project root. */
    public static Map<String, Object> engine(Path root, Map<String, Object> runtime, Engine engine) throws IOException {
        var engines = object(runtime, "engines");
        String name = engine.configName();
        require(engines.containsKey(name), "No engine configuration selected for " + name);
        Path file = root.resolve(string(engines, name));
        var document = parse(Files.readString(file));
        require(integer(document, "schema_version") == 1 && string(document, "purpose").equals("engine")
                && string(document, "engine").equals(name), "Expected " + name + " engine config v1");
        var settings = new java.util.LinkedHashMap<>(document);
        settings.keySet().removeAll(Set.of("schema_version", "purpose", "engine"));
        require(!string(settings, "build_directory").isBlank(), name + " build_directory must not be blank");
        switch (engine) {
            case DUCKDB -> {
                keys(settings, Set.of("build_directory", "memory_limit", "threads"), name);
                require(string(settings, "memory_limit").matches("[1-9][0-9]*(KiB|MiB|GiB)"), "Invalid DuckDB memory_limit");
                require(positive(settings, "threads") <= 256, "DuckDB threads exceeds 256");
            }
            case POSTGRES -> {
                keys(settings, Set.of("build_directory", "settings"), name);
                postgresSettings(object(settings, "settings"));
            }
            case ROCKSDB -> {
                keys(settings, Set.of("build_directory", "cache_bytes", "write_buffer_bytes", "background_jobs"), name);
                new org.sibench.generation.RocksDbRows.Settings(positive(settings, "cache_bytes"),
                        positive(settings, "write_buffer_bytes"), Math.toIntExact(positive(settings, "background_jobs")));
            }
            case PIXELS -> settings = new java.util.LinkedHashMap<>(PixelsConfig.read(file.getParent(), settings));
        }
        return java.util.Collections.unmodifiableMap(settings);
    }

    /** PostgreSQL server settings; the generator owns network endpoints and the capture's query identifiers. */
    public static Map<String, String> postgresSettings(Map<String, Object> settings) {
        var result = new java.util.LinkedHashMap<String, String>();
        for (var entry : settings.entrySet()) {
            require(entry.getKey().matches("[a-z_][a-z0-9_.]*"), "Invalid PostgreSQL setting name: " + entry.getKey());
            require(!Set.of("listen_addresses", "port", "unix_socket_directories", "compute_query_id", "data_directory",
                    "hba_file", "ident_file", "external_pid_file", "session_replication_role").contains(entry.getKey()),
                    "PostgreSQL setting is assigned by the generator: " + entry.getKey());
            Object value = entry.getValue();
            require(value instanceof String || value instanceof Number || value instanceof Boolean,
                    "PostgreSQL setting must be a scalar: " + entry.getKey());
            result.put(entry.getKey(), value instanceof Boolean flag ? (flag ? "on" : "off") : value.toString());
        }
        return java.util.Collections.unmodifiableMap(result);
    }

    static void keys(Map<String, Object> map, Set<String> required, String name) {
        for (String key : map.keySet()) require(required.contains(key), "Unknown " + name + " engine option: " + key);
        for (String key : required) require(map.containsKey(key), "Missing " + name + " engine option: " + key);
    }
}
