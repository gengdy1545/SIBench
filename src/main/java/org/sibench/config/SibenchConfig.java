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
                    "analysis", "benchmarks", "duckdb", "postgres", "rocksdb").contains(key), "Unknown SIBench option: " + key);
        if (document.containsKey("benchmarks")) BenchmarkSelection.validate(document.get("benchmarks"));
        if(document.containsKey("postgres")) {
            var postgres=object(document,"postgres");
            require(postgres.keySet().equals(Set.of("build_directory")),"PostgreSQL currently requires only build_directory");
            require(!string(postgres,"build_directory").isBlank(),"PostgreSQL build_directory must not be blank");
        }
        if (document.containsKey("rocksdb")) {
            var rocks = object(document, "rocksdb");
            require(rocks.keySet().equals(Set.of("build_directory", "cache_bytes", "write_buffer_bytes", "background_jobs")),
                    "RocksDB requires build_directory, cache_bytes, write_buffer_bytes and background_jobs");
            require(!string(rocks, "build_directory").isBlank(), "RocksDB build_directory must not be blank");
            new org.sibench.generation.RocksDbRows.Settings(positive(rocks, "cache_bytes"),
                    positive(rocks, "write_buffer_bytes"), Math.toIntExact(positive(rocks, "background_jobs")));
        }
    }

}
