package org.sibench.config;

import static org.sibench.config.ConfigValues.*;
import java.util.Map;
import java.util.Set;

/** Independent switches for finite Pixels experiments over one published CDC input. */
public final class PixelsExperimentConfig {
    private PixelsExperimentConfig() {}

    public static Map<String, Object> settings(Map<String, Object> runtime) {
        var settings = object(runtime, "pixels_experiment");
        SibenchConfig.keys(settings, Set.of("trace", "updates", "max_transactions", "timeout_seconds", "queries", "freshness", "trino"), "Pixels experiment");
        bool(settings, "trace");
        bool(settings, "updates");
        require(integer(settings, "max_transactions") >= 0, "max_transactions must be nonnegative (zero means all)");
        require(positive(settings, "timeout_seconds") <= 86400, "Pixels experiment timeout exceeds 24 hours");
        var queries = object(settings, "queries");
        SibenchConfig.keys(queries, Set.of("enabled", "timing", "directory", "rounds"), "Pixels queries");
        bool(queries, "enabled");
        require(Set.of("after", "during").contains(string(queries, "timing")), "Query timing must be after or during");
        positive(queries, "rounds");
        string(queries, "directory");
        var freshness = object(settings, "freshness");
        SibenchConfig.keys(freshness, Set.of("enabled", "table", "column", "sample_seconds"), "Pixels freshness");
        bool(freshness, "enabled");
        string(freshness, "table");
        string(freshness, "column");
        require(positive(freshness, "sample_seconds") <= 3600, "Freshness sampling exceeds one hour");
        require(!bool(settings, "trace") || (!bool(queries, "enabled") && !bool(freshness, "enabled")),
                "Index Trace preparation disables business queries and freshness");
        require(bool(settings, "updates") || integer(settings, "max_transactions") == 0,
                "A static experiment cannot select an update prefix");
        require(!bool(freshness, "enabled") || bool(settings, "updates"), "Freshness requires this run's update timestamps");
        if (bool(freshness, "enabled")) {
            require(string(freshness, "table").matches("[a-z][a-z0-9_]*")
                    && Set.of("fresh_ts", "freshness_ts").contains(string(freshness, "column")),
                    "Freshness requires a dedicated fresh_ts or freshness_ts column");
        }
        var trino = object(settings, "trino");
        SibenchConfig.keys(trino, Set.of("server", "user", "catalog", "schema", "timeout_seconds", "deployment_directory",
                "heap", "query_memory", "query_total_memory", "heap_headroom", "threads"), "Trino");
        for (String key : Set.of("server", "user", "catalog", "schema", "deployment_directory")) string(trino, key);
        for (String key : Set.of("catalog", "schema")) require(string(trino, key).matches("[A-Za-z][A-Za-z0-9_]*"), "Invalid Trino " + key);
        require(string(trino, "user").matches("[A-Za-z0-9_.@-]+"), "Invalid Trino user");
        require(string(trino, "deployment_directory").isEmpty() || java.nio.file.Path.of(string(trino, "deployment_directory")).isAbsolute(),
                "Managed Trino requires an absolute deployment directory on the Pixels host");
        for (String key : Set.of("heap", "query_memory", "query_total_memory", "heap_headroom"))
            require(string(trino, key).matches("[1-9][0-9]*[GM]B?"), "Invalid Trino memory setting: " + key);
        require(bytes(string(trino, "query_memory")) <= bytes(string(trino, "query_total_memory"))
                && Math.addExact(bytes(string(trino, "query_total_memory")), bytes(string(trino, "heap_headroom"))) <= bytes(string(trino, "heap")),
                "Trino query memory plus heap headroom exceeds the JVM heap");
        require(positive(trino, "threads") <= 256, "Trino threads exceeds 256");
        require(positive(trino, "timeout_seconds") <= 86400, "Trino timeout exceeds 24 hours");
        boolean external = !string(trino, "server").isEmpty(), managed = !string(trino, "deployment_directory").isEmpty();
        require(!external || !managed, "Choose either trino.server or trino.deployment_directory");
        if (external) {
            new org.sibench.query.TrinoQueries(java.net.URI.create(string(trino, "server")), string(trino, "user"),
                    string(trino, "catalog"), string(trino, "schema"), java.time.Duration.ofSeconds(positive(trino, "timeout_seconds")));
            require(!bool(freshness, "enabled") || java.net.URI.create(string(trino, "server")).getScheme().equals("http"),
                    "Sink freshness currently requires an HTTP Trino JDBC endpoint");
        }
        require(!bool(queries, "enabled") && !bool(freshness, "enabled") || external || managed,
                "Queries and freshness require trino.server or trino.deployment_directory");
        require(!bool(queries, "enabled") || !string(queries, "directory").isBlank(), "Enabled queries require a SQL directory");
        return settings;
    }

    private static long bytes(String value) {
        long amount = Long.parseLong(value.replaceAll("[GMB]", ""));
        return Math.multiplyExact(amount, value.contains("G") ? 1L << 30 : 1L << 20);
    }
}
