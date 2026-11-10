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
                    temporary.getRoot().toPath(), port, org.sibench.adapters.proto.ValueKind.ROW_ID, null, false));
    }

    @Test
    public void serverRejectsUnknownServiceFields() throws Exception {
        Path config = temporary.getRoot().toPath().resolve("index.yaml");
        String current = """
                schema_version: 1
                purpose: index_service
                service:
                  backend: rocksdb
                  path: index
                  port: 50551
                  value: row_id
                """;
        Files.writeString(config, current);
        assertEquals("rocksdb", ServiceConfig.ofServer(temporary.getRoot().toPath(), config).adapter());
        Files.writeString(config, current + "  unknown_option: true\n");
        var error = assertThrows(IllegalArgumentException.class,
                () -> ServiceConfig.ofServer(temporary.getRoot().toPath(), config));
        assertTrue(error.getMessage().contains("Unknown key: unknown_option"));
    }

    @Test
    public void replayRejectsUnknownServiceFields() {
        var document = Map.<String, Object>of("target", Map.of("service", Map.of(
                "adapter", "rocksdb", "path", "index", "value", "row_id", "unknown_option", false)));
        var error = assertThrows(IllegalArgumentException.class,
                () -> ServiceConfig.ofReplay(temporary.getRoot().toPath(), document));
        assertTrue(error.getMessage().contains("Unknown key: unknown_option"));
    }

    @Test
    public void objectBackendsKeepTheirSelectedEngineAndRequireStorage() throws Exception {
        Path root = temporary.getRoot().toPath();
        Path config = root.resolve("index.yaml");
        // Parsing requires credentials, but this test never connects or exposes their values.
        String variable = System.getenv().entrySet().stream().filter(e -> !e.getValue().isEmpty())
                .map(Map.Entry::getKey).findFirst().orElseThrow();
        Files.writeString(root.resolve("storage.yaml"), """
                profiles:
                  test:
                    kind: s3
                    endpoint: http://127.0.0.1:1
                    region: us-east-1
                    bucket: test
                    prefix: test/
                    addressing: path
                    access_key_env: %s
                    secret_key_env: %s
                """.formatted(variable, variable));
        for (String backend : new String[] {"rocksdb_cloud", "slatedb"}) {
            String base = """
                    schema_version: 1
                    purpose: index_service
                    service:
                      backend: %s
                      path: index
                      port: 50551
                      value: row_id
                    """.formatted(backend);
            Files.writeString(config, base);
            assertThrows(IllegalArgumentException.class, () -> ServiceConfig.ofServer(root, config));
            Files.writeString(config, base + "  storage: storage.yaml\n  profile: test\n  object_path: test/run\n");
            var selected = ServiceConfig.ofServer(root, config);
            assertEquals(backend, selected.adapter());
            assertEquals("test/run", selected.objects().objectPath());
        }
    }

    @Test
    public void objectBackendsAreRemoteReplayTargets() {
        var document = Map.<String, Object>of("target", Map.of("service", Map.of(
                "adapter", "slatedb", "path", "index", "value", "row_id")));
        assertThrows(IllegalArgumentException.class,
                () -> ServiceConfig.ofReplay(temporary.getRoot().toPath(), document));
    }
}
