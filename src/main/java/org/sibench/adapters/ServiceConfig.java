package org.sibench.adapters;

import org.sibench.config.ObjectStore;

import static org.sibench.config.ConfigValues.*;


import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Where a replay sends its operations ({@code target.service} of a replay config), or what an
 * index server serves ({@code service} of an index_service config).
 *
 * <ul>
 *   <li>{@code adapter: grpc} with {@code endpoint: host:port} — any service speaking the protocol.
 *   <li>{@code adapter: rocksdb} with {@code path} — a fresh RocksDB index in the driver process.
 * </ul>
 *
 * {@code value} is {@code locator} or {@code row_id}: what an index entry points to. An index
 * server's {@code backend} is {@code relive} with an HTTP endpoint, {@code rocksdb} for fixtures,
 * or {@code rocksdb_cloud}/{@code slatedb} with a {@code storage} URI
 * resolved through the runtime configuration's AWS or COS connection.
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
        boolean wal) {
    public ServiceConfig {
        require(port >= 0 && port <= 65535, "Invalid index service port");
        require(Set.of("grpc", "relive", "rocksdb", "rocksdb_cloud", "slatedb").contains(adapter), "Unknown service adapter: " + adapter);
        boolean remote = adapter.equals("grpc") || adapter.equals("relive");
        require(!remote || endpoint != null && !endpoint.isBlank(), "A remote service needs an endpoint");
        require(remote || path != null, "An index needs a path");
        require(!adapter.equals("relive") || path == null && objects == null && !reopen && wal,
                "Relive owns its storage, reopening and forced journal policy");
        require(!(adapter.equals("rocksdb_cloud") || adapter.equals("slatedb")) || objects != null,
                "An object-storage backend needs storage settings");
        require(wal || adapter.equals("rocksdb_cloud") || adapter.equals("slatedb"),
                "Only object-storage backends can disable the WAL");
    }

    /** The required service target of a replay config. */
    public static ServiceConfig ofReplay(Path root, Map<String, Object> document) {
        var target = object(document, "target");

        var service = object(target, "service");
        keys(service, Set.of("adapter", "endpoint", "path", "value"));
        String adapter = string(service, "adapter");
        require(Set.of("grpc", "rocksdb").contains(adapter), "Replay adapter must be grpc or rocksdb");
        return new ServiceConfig(
                adapter,
                service.containsKey("endpoint") ? string(service, "endpoint") : null,
                service.containsKey("path") ? root.resolve(string(service, "path")) : null,
                0,
                value(service),
                null,
                false,
                true);
    }

    /** A server with visible mutation acknowledgements and versioned reads. */
    public static ServiceConfig ofServer(Path root, Path path, Map<String, Object> runtime) throws java.io.IOException {
        Map<String, Object> document = parse(Files.readString(root.resolve(path)));
        keys(document, Set.of("schema_version", "purpose", "service"));
        require(integer(document, "schema_version") == 1, "Expected index service config v1");
        require(string(document, "purpose").equals("index_service"), "Expected index_service purpose");
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
                    Math.toIntExact(positive(service, "port")), value(service), null, false, true);
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
                !cloud || bool(service, "wal"));
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

    private static ValueKind value(Map<String, Object> service) {
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
