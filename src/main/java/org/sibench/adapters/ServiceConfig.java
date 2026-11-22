package org.sibench.adapters;

import org.sibench.config.ObjectStore;

import static org.sibench.config.ConfigValues.*;


import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * What an index server serves ({@code service} of an index_service config). Its
 * {@code backend} is {@code relive} with an HTTP endpoint, {@code rocksdb} for fixtures,
 * or {@code rocksdb_cloud}/{@code slatedb} with a {@code storage} URI
 * resolved through the runtime configuration's AWS or COS connection. {@code value} is
 * {@code locator} or {@code row_id}: what an index entry points to.
 * {@code reopen: true} serves an index this server created before instead of a new one: after a
 * restart, or for rocksdb-cloud without its local directory, from the bucket alone. The object-storage
 * backends require {@code wal}: whether every write is also recorded in the engine's WAL. The local
 * RocksDB fixture always keeps its WAL.
 */
public record ServiceConfig(
        String adapter,
        String endpoint,
        Path path,
        int port,
        ValueKind value,
        ObjectStore objects,
        boolean reopen,
        boolean wal,
        Execution execution) {
    /** SIBench service/bridge resources, independent of the backend's internal worker pools. */
    public record Execution(int readThreads, int writeThreads, Path statistics) {
        public Execution {
            require(readThreads > 0 && writeThreads > 0, "Execution thread counts must be positive");
            require(statistics != null, "Execution needs a statistics output path");
        }
    }

    public ServiceConfig {
        require(execution != null, "An index service needs execution settings");
        require(port >= 0 && port <= 65535, "Invalid index service port");
        require(Set.of("relive", "rocksdb", "rocksdb_cloud", "slatedb").contains(adapter), "Unknown service backend: " + adapter);
        boolean relive = adapter.equals("relive");
        require(!relive || endpoint != null && !endpoint.isBlank(), "The Relive bridge needs an endpoint");
        require(relive || path != null, "An index needs a path");
        require(!adapter.equals("relive") || path == null && objects == null && !reopen && wal,
                "Relive owns its storage, reopening and durability policy");
        require(!(adapter.equals("rocksdb_cloud") || adapter.equals("slatedb")) || objects != null,
                "An object-storage backend needs storage settings");
        require(wal || adapter.equals("rocksdb_cloud") || adapter.equals("slatedb"),
                "Only object-storage backends can disable the WAL");
    }

    /** A server with visible mutation acknowledgements and versioned reads. */
    public static ServiceConfig ofServer(Path root, Path path, Map<String, Object> runtime) throws java.io.IOException {
        Map<String, Object> document = parse(Files.readString(root.resolve(path)));
        keys(document, Set.of("schema_version", "purpose", "service", "execution"));
        require(integer(document, "schema_version") == 1, "Expected index service config v1");
        require(string(document, "purpose").equals("index_service"), "Expected index_service purpose");
        var settings = object(document, "execution");
        keys(settings, Set.of("read_threads", "write_threads", "statistics"));
        var execution = new Execution(Math.toIntExact(positive(settings, "read_threads")),
                Math.toIntExact(positive(settings, "write_threads")), root.resolve(string(settings, "statistics")));
        var service = object(document, "service");
        keys(
                service,
                Set.of(
                        "backend", "endpoint", "path", "port", "value", "storage",
                        "reopen", "wal"));
        String backend = string(service, "backend");
        require(
                Set.of("relive", "rocksdb", "rocksdb_cloud", "slatedb").contains(backend),
                "backend must be relive, rocksdb, rocksdb_cloud or slatedb");
        if (backend.equals("relive")) {
            keys(service, Set.of("backend", "endpoint", "port", "value"));
            return new ServiceConfig(backend, string(service, "endpoint"), null,
                    Math.toIntExact(positive(service, "port")), value(service), null, false, true, execution);
        }
        require(!service.containsKey("endpoint"), "endpoint is only for the Relive bridge");
        boolean cloud = !backend.equals("rocksdb");
        for (String key : List.of("storage", "wal"))
            require(cloud == service.containsKey(key), key + " is required only for object-storage backends");
        return new ServiceConfig(
                backend,
                null,
                root.resolve(string(service, "path")),
                Math.toIntExact(positive(service, "port")),
                value(service),
                cloud
                        ? ObjectStore.of(
                                runtime, string(service, "storage"))
                        : null,
                service.containsKey("reopen") && bool(service, "reopen"),
                !cloud || bool(service, "wal"), execution);
    }

    /** This server's index, new or reopened. */
    public IndexBackend backend() throws java.io.IOException {
        if (adapter.equals("relive")) return new ReliveBackend(endpoint, value);
        if (adapter.equals("slatedb")) return VersionedIndexBackend.slate(path, value, objects, reopen, wal);
        if (objects == null)
            return reopen ? VersionedIndexBackend.open(path, value) : VersionedIndexBackend.create(path, value);
        return reopen
                ? VersionedIndexBackend.openCloud(path, value, objects, wal)
                : VersionedIndexBackend.cloud(path, value, objects, wal);
    }

    static ValueKind value(Map<String, Object> service) {
        return switch (string(service, "value")) {
            case "locator" -> ValueKind.LOCATOR;
            case "row_id" -> ValueKind.ROW_ID;
            default -> throw new IllegalArgumentException("value must be locator or row_id");
        };
    }

    private static void keys(Map<String, Object> map, Set<String> allowed) {
        for (String key : map.keySet()) require(allowed.contains(key), "Unknown key: " + key);
    }
}
