package org.sibench.adapters;

import static org.junit.Assert.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ServiceConfigTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test public void serverPortMustFitTheNetworkPortRange() {
        for (int port : new int[] {-1, 65536})
            assertThrows(IllegalArgumentException.class, () -> new ServiceConfig("rocksdb", null,
                    temporary.getRoot().toPath(), port, ValueKind.ROW_ID, null, false, true, new ServiceConfig.Execution(4, 2, temporary.getRoot().toPath().resolve("stats.jsonl"))));
        assertThrows(IllegalArgumentException.class, () -> new ServiceConfig("rocksdb", null,
                temporary.getRoot().toPath(), 0, ValueKind.ROW_ID, null, false, false, new ServiceConfig.Execution(4, 2, temporary.getRoot().toPath().resolve("stats.jsonl"))));
    }

    @Test
    public void serverRejectsUnknownServiceFields() throws Exception {
        Path config = temporary.getRoot().toPath().resolve("index.yaml");
        String current = """
                schema_version: 1
                purpose: index_service
                execution:
                  read_threads: 4
                  write_threads: 2
                  statistics: statistics.jsonl
                service:
                  backend: rocksdb
                  path: index
                  port: 50551
                  value: row_id
                """;
        Files.writeString(config, current);
        var parsed = ServiceConfig.ofServer(temporary.getRoot().toPath(), config, Map.of());
        assertEquals("rocksdb", parsed.adapter());
        assertEquals(new ServiceConfig.Execution(4, 2, temporary.getRoot().toPath().resolve("statistics.jsonl")), parsed.execution());
        for (String invalid : new String[]{current.replace("read_threads: 4", "read_threads: 0"),
                current.replace("write_threads: 2", "write_threads: -1"),
                current.replace("write_threads: 2", "write_threads: 1.5"),
                current.replace("statistics: statistics.jsonl", "unknown: statistics.jsonl")}) {
            Files.writeString(config, invalid);
            assertThrows(IllegalArgumentException.class, () -> ServiceConfig.ofServer(temporary.getRoot().toPath(), config, Map.of()));
        }
        Files.writeString(config, current + "  unknown_option: true\n");
        var error = assertThrows(IllegalArgumentException.class,
                () -> ServiceConfig.ofServer(temporary.getRoot().toPath(), config, Map.of()));
        assertTrue(error.getMessage().contains("Unknown key: unknown_option"));
        // The generation fixture keeps its WAL; only comparison backends select it.
        Files.writeString(config, current + "  wal: false\n");
        assertThrows(IllegalArgumentException.class, () -> ServiceConfig.ofServer(temporary.getRoot().toPath(), config, Map.of()));
    }

    @Test
    public void replayTargetIsOnlyAnEndpointAndValueKind() {
        var target = ServiceTarget.of(Map.of("target", Map.of("service", Map.of(
                "endpoint", "127.0.0.1:50551", "value", "locator"))));
        assertEquals(new ServiceTarget("127.0.0.1:50551", ValueKind.LOCATOR), target);
        // Backends, local paths and the removed adapter selector belong to the index server.
        for (String key : new String[] {"adapter", "backend", "path", "unknown_option"}) {
            var document = Map.<String, Object>of("target", Map.of("service", Map.of(
                    "endpoint", "127.0.0.1:50551", "value", "row_id", key, "grpc")));
            var error = assertThrows(IllegalArgumentException.class, () -> ServiceTarget.of(document));
            assertTrue(error.getMessage().contains("Unknown key: " + key));
        }
        assertThrows(IllegalArgumentException.class, () -> ServiceTarget.of(Map.of("target", Map.of("service",
                Map.of("value", "row_id")))));
    }

    @Test
    public void objectBackendsKeepTheirSelectedEngineAndRequireStorage() throws Exception {
        Path root = temporary.getRoot().toPath();
        Path config = root.resolve("index.yaml");
        // Parsing requires credentials, but this test never connects or exposes their values.
        String variable = System.getenv().entrySet().stream().filter(e -> !e.getValue().isEmpty())
                .map(Map.Entry::getKey).findFirst().orElseThrow();
        var runtime = Map.<String,Object>of("object_storage", Map.of("aws", Map.of(
                "endpoint", "http://127.0.0.1:1", "region", "us-east-1", "addressing", "path",
                "access_key_env", variable, "secret_key_env", variable)));
        for (String backend : new String[] {"rocksdb_cloud", "slatedb"}) {
            String base = """
                    schema_version: 1
                    purpose: index_service
                    execution:
                      read_threads: 4
                      write_threads: 2
                      statistics: statistics.jsonl
                    service:
                      backend: %s
                      path: index
                      port: 50551
                      value: row_id
                    """.formatted(backend);
            Files.writeString(config, base);
            assertThrows(IllegalArgumentException.class, () -> ServiceConfig.ofServer(root, config, runtime));
            String storage = base + "  storage: s3://test/test/run\n";
            Files.writeString(config, storage);
            assertTrue(assertThrows(IllegalArgumentException.class, () -> ServiceConfig.ofServer(root, config, runtime))
                    .getMessage().contains("wal"));
            for (boolean wal : new boolean[] {false, true}) {
                Files.writeString(config, storage + "  wal: " + wal + "\n");
                var selected = ServiceConfig.ofServer(root, config, runtime);
                assertEquals(backend, selected.adapter());
                assertEquals("test/run", selected.objects().objectPath());
                assertEquals(wal, selected.wal());
            }
            Files.writeString(config, storage + "  wal: 'false'\n");
            assertThrows(IllegalArgumentException.class, () -> ServiceConfig.ofServer(root, config, runtime));
        }
    }

    @Test
    public void reliveBridgeOwnsNoLocalStorageOrWalPolicy() throws Exception {
        Path root = temporary.getRoot().toPath(), config = root.resolve("relive.yaml");
        String base = """
                schema_version: 1
                purpose: index_service
                execution:
                  read_threads: 4
                  write_threads: 2
                  statistics: statistics.jsonl
                service:
                  backend: relive
                  endpoint: http://127.0.0.1:7001
                  port: 50551
                  value: row_id
                """;
        Files.writeString(config, base);
        var selected = ServiceConfig.ofServer(root, config, Map.of());
        assertEquals("relive", selected.adapter());
        assertEquals("http://127.0.0.1:7001", selected.endpoint());
        assertNull(selected.path());
        assertNull(selected.objects());
        assertTrue(selected.wal());
        for (String forbidden : new String[] {"path: local", "storage: cos://bucket/prefix", "wal: false", "reopen: true"}) {
            Files.writeString(config, base + "  " + forbidden + "\n");
            assertThrows(IllegalArgumentException.class, () -> ServiceConfig.ofServer(root, config, Map.of()));
        }
    }
}
