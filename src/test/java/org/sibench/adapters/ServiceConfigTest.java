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

    @Test
    public void serverRejectsRemovedSchedulingSwitches() throws Exception {
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
        for (String key : new String[] {"ordered", "publication"}) {
            Files.writeString(config, current + "  " + key + ": true\n");
            var error = assertThrows(IllegalArgumentException.class,
                    () -> ServiceConfig.ofServer(temporary.getRoot().toPath(), config));
            assertTrue(error.getMessage().contains("Unknown key: " + key));
        }
    }

    @Test
    public void replayRejectsTheRemovedPublicationSwitch() {
        var document = Map.<String, Object>of("target", Map.of("service", Map.of(
                "adapter", "rocksdb", "path", "index", "value", "row_id", "publication", false)));
        assertThrows(IllegalArgumentException.class,
                () -> ServiceConfig.ofReplay(temporary.getRoot().toPath(), document));
    }
}
