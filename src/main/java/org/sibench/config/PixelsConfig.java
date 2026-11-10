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
    private static final Set<String> SINK_OVERRIDES = Set.of("sink.monitor.enable", "sink.monitor.report.enable",
            "sink.monitor.freshness.level", "sink.retina.log.queue", "sink.datasource.rate.limit", "sink.retina.client",
            "sink.retina.trans.limit", "sink.datasource.decode.threads", "sink.timeout.ms", "sink.flush.batch.size",
            "sink.flush.interval.ms", "sink.commit.method", "sink.commit.batch.size", "sink.commit.batch.worker",
            "sink.commit.batch.delay", "sink.trans.batch.size", "debezium.tombstones.on.delete", "debezium.max.batch.size",
            "debezium.max.queue.size", "debezium.max.queue.size.in.bytes", "debezium.poll.interval.ms");

    static Map<String, Object> read(Path directory, Map<String, Object> settings) throws IOException {
        SibenchConfig.keys(settings, Set.of("build_directory", "pixels_host", "memory", "load", "source",
                "properties", "sink_properties"), "pixels");
        String host = string(settings, "pixels_host");
        require(host.equals("local") || host.matches("[A-Za-z0-9.-]+:[1-9][0-9]{0,4}"),
                "pixels_host must be local or HOST:PORT");
        require(host.equals("local") || Integer.parseInt(host.substring(host.lastIndexOf(':') + 1)) <= 65535,
                "Invalid Pixels host port");
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
        for (String key : overrides.keySet()) {
            require(!PIXELS_OWNED.contains(key) && !key.matches(".*\\.(host|hosts|port|dir|path|location|folder|url)"),
                    "Pixels property is assigned by the generator: " + key);
            require(overrides.get(key).indexOf('\n') < 0 && overrides.get(key).indexOf('\r') < 0,
                    "Multiline Pixels property: " + key);
        }
        return overrides;
    }

    public static Map<String, String> sinkProperties(Map<String, String> overrides) {
        for (String key : overrides.keySet()) {
            require(SINK_OVERRIDES.contains(key), "Unsupported or generator-owned pixels-sink property: " + key);
            require(!key.equals("sink.monitor.freshness.level") || overrides.get(key).equals("none"),
                    "Pixels CDC generation disables freshness query workloads");
            require(overrides.get(key).indexOf('\n') < 0 && overrides.get(key).indexOf('\r') < 0,
                    "Multiline pixels-sink property: " + key);
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
