package org.sibench.adapters;

import static org.sibench.config.ConfigValues.*;

import org.sibench.adapters.proto.ValueKind;

import java.nio.file.Files;
import java.nio.file.Path;
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
 * {@code value} is {@code locator} or {@code row_id}: what an index entry points to.
 */
public record ServiceConfig(
        String adapter, String endpoint, Path path, int port, ValueKind value, boolean ordered) {
    public ServiceConfig {
        require(Set.of("grpc", "rocksdb").contains(adapter), "Unknown service adapter: " + adapter);
        require(!adapter.equals("grpc") || endpoint != null, "A gRPC target needs an endpoint");
        require(!adapter.equals("rocksdb") || path != null, "A RocksDB index needs a path");
    }

    /** The service target of a replay config, or null when it replays into the reference. */
    public static ServiceConfig ofReplay(Path root, Map<String, Object> document) {
        var target = object(document, "target");
        if (!target.containsKey("service")) return null;
        var service = object(target, "service");
        keys(service, Set.of("adapter", "endpoint", "path", "value"));
        String adapter = string(service, "adapter");
        // In process, requests run on the sending thread: in send order whatever the client.
        return new ServiceConfig(
                adapter,
                service.containsKey("endpoint") ? string(service, "endpoint") : null,
                service.containsKey("path") ? root.resolve(string(service, "path")) : null,
                0,
                value(service),
                adapter.equals("rocksdb"));
    }

    /**
     * An index server: a backend, its storage and the port it listens on. {@code ordered} makes it
     * execute requests in the driver's send order across client streams (Request.sequence).
     */
    public static ServiceConfig ofServer(Path root, Path path) throws java.io.IOException {
        Map<String, Object> document = parse(Files.readString(root.resolve(path)));
        keys(document, Set.of("schema_version", "purpose", "service"));
        require(integer(document, "schema_version") == 1, "Expected index service config v1");
        require(string(document, "purpose").equals("index_service"), "Expected index_service purpose");
        var service = object(document, "service");
        keys(service, Set.of("backend", "path", "port", "value", "ordered"));
        require(string(service, "backend").equals("rocksdb"), "Only the RocksDB backend is implemented");
        return new ServiceConfig(
                "rocksdb",
                null,
                root.resolve(string(service, "path")),
                Math.toIntExact(positive(service, "port")),
                value(service),
                service.containsKey("ordered") && bool(service, "ordered"));
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
