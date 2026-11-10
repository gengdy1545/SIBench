package org.sibench.config;

import static org.sibench.config.ConfigValues.*;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

/**
 * Pixels CDC generation settings. The property files hold overrides of the pinned upstream
 * defaults; endpoints, run directories and switches that define the generated Trace's semantics
 * belong to the generator and are rejected here.
 */
public final class PixelsConfig {
    private PixelsConfig() {}

    private static final Set<String> PIXELS_OWNED = Set.of("pixels.var.dir", "etcd.hosts", "retina.enable",
            "index.server.enabled", "enabled.single.point.index.schemes", "enabled.main.index.scheme",
            "retina.storage.gc.enabled", "retina.upsert-mode.enabled", "retina.tile.visibility.capacity");
    private static final Set<String> SINK_OWNED = Set.of("sink.datasource", "sink.datasource.engine.format",
            "sink.debezium.dialect", "sink.mode", "sink.retina.mode", "sink.trans.mode", "sink.storage.loop",
            "sink.monitor.freshness.timestamp", "sink.retina.trans.request.batch", "sink.retina.embedded");
    private static final Set<String> DEBEZIUM_OWNED = Set.of("name", "connector.class", "provide.transaction.metadata",
            "plugin.name", "slot.name", "schema.include.list", "table.include.list", "snapshot.mode", "topic.prefix",
            "decimal.handling.mode", "time.precision.mode", "key.converter", "value.converter");

    static Map<String, Object> read(Path directory, Map<String, Object> settings) throws IOException {
        SibenchConfig.keys(settings, Set.of("build_directory", "pixels_host", "memory", "load", "source",
                "properties", "sink_properties"), "pixels");
        String host = string(settings, "pixels_host");
        require(host.equals("local") || host.matches("[A-Za-z0-9.-]+:[1-9][0-9]{0,4}"),
                "pixels_host must be local or HOST:PORT");
        var memory = object(settings, "memory");
        SibenchConfig.keys(memory, Set.of("coordinator_heap", "retina_heap", "sink_heap"), "pixels memory");
        for (String key : memory.keySet()) require(string(memory, key).matches("[1-9][0-9]*[mg]"), "Invalid JVM heap: " + key);
        var load = object(settings, "load");
        SibenchConfig.keys(load, Set.of("rows_per_file", "threads"), "pixels load");
        positive(load, "rows_per_file");
        require(positive(load, "threads") <= 256, "Pixels load threads exceeds 256");
        var source = object(settings, "source");
        SibenchConfig.keys(source, Set.of("settings"), "pixels source");
        var postgres = SibenchConfig.postgresSettings(object(source, "settings"));
        for (String key : Set.of("wal_level", "max_wal_senders", "max_replication_slots"))
            require(!postgres.containsKey(key), "Source PostgreSQL logical replication setting is generator-owned: " + key);
        var result = new LinkedHashMap<>(settings);
        result.put("properties", pixelsProperties(properties(directory.resolve(string(settings, "properties")))));
        result.put("sink_properties", sinkProperties(properties(directory.resolve(string(settings, "sink_properties")))));
        return result;
    }

    /** Pixels overrides; location-like keys always come from the generator's run layout. */
    public static Map<String, String> pixelsProperties(Map<String, String> overrides) {
        for (String key : overrides.keySet())
            require(!PIXELS_OWNED.contains(key) && !key.matches(".*\\.(host|hosts|port|dir|path|location|folder|url)"),
                    "Pixels property is assigned by the generator: " + key);
        return overrides;
    }

    public static Map<String, String> sinkProperties(Map<String, String> overrides) {
        for (String key : overrides.keySet()) {
            require(key.startsWith("sink.") || key.startsWith("debezium."), "Unsupported pixels-sink property: " + key);
            String debezium = key.startsWith("debezium.") ? key.substring("debezium.".length()) : null;
            require(!SINK_OWNED.contains(key) && !key.startsWith("sink.query.") && !key.startsWith("sink.proto.")
                    && !key.startsWith("sink.csv.") && !key.startsWith("sink.flink.") && !key.startsWith("sink.remote.")
                    && (debezium == null || !DEBEZIUM_OWNED.contains(debezium) && !debezium.startsWith("database.")
                        && !debezium.startsWith("offset.") && !debezium.startsWith("schema.history.")
                        && !debezium.startsWith("publication.") && !debezium.startsWith("transforms")),
                    "pixels-sink property is assigned by the generator: " + key);
        }
        return overrides;
    }

    /** Reads a properties file in key order; duplicate keys are rejected rather than silently replaced. */
    public static Map<String, String> properties(Path file) throws IOException {
        var result = new TreeMap<String, String>();
        var duplicates = new Properties() {
            @Override public synchronized Object put(Object key, Object value) {
                require(result.put((String) key, (String) value) == null, "Duplicate property " + key + " in " + file);
                return null;
            }
        };
        try (Reader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            duplicates.load(reader);
        }
        return java.util.Collections.unmodifiableMap(result);
    }
}
