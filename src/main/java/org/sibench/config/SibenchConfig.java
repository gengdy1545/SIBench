package org.sibench.config;

import static org.sibench.config.ConfigValues.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.Set;

/** Runtime settings shared by replay, analysis and the capture PostgreSQL server. */
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
            require(Set.of("schema_version", "purpose", "postgres", "target", "replay",
                    "versions", "analysis").contains(key), "Unknown SIBench option: " + key);
    }

    /** Materialize PG's native format only after validating every setting; never overwrite it. */
    public static void writePostgres(Path config, Path output) throws IOException {
        var settings = object(read(config), "postgres");
        var text = new StringBuilder("# Generated from the run's SIBench configuration.\n");
        for (var setting : settings.entrySet()) {
            require(Set.of("listen_addresses", "timezone", "log_statement", "compute_query_id",
                    "max_parallel_workers_per_gather", "max_parallel_maintenance_workers",
                    "autovacuum", "shared_buffers", "max_wal_size", "min_wal_size",
                    "checkpoint_timeout", "checkpoint_completion_target", "wal_buffers",
                    "synchronous_commit", "maintenance_work_mem").contains(setting.getKey()),
                    "Unsupported PostgreSQL setting: " + setting.getKey());
            Object value = setting.getValue();
            require(value instanceof String || value instanceof Boolean || value instanceof Number,
                    "PostgreSQL settings must be scalar values");
            if (value instanceof Number number)
                require(Double.isFinite(number.doubleValue()), "PostgreSQL settings must be finite");
            String literal = value instanceof Boolean flag ? (flag ? "on" : "off") : value.toString();
            require(literal.indexOf('\n') < 0 && literal.indexOf('\r') < 0 && literal.indexOf('\0') < 0,
                    "PostgreSQL settings must fit on one line");
            text.append(setting.getKey()).append(" = '")
                    .append(literal.replace("\\", "\\\\").replace("'", "''")).append("'\n");
        }
        Files.writeString(output, text, StandardOpenOption.CREATE_NEW);
    }
}
