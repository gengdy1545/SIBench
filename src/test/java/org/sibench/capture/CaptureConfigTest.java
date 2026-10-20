package org.sibench.capture;

import static org.junit.Assert.*;

import org.junit.Test;

import java.nio.file.*;

public class CaptureConfigTest {
    @Test
    public void validatesNativeProfileAndRejectsUnsupportedClients() throws Exception {
        Path root = Path.of("").toAbsolutePath();
        Path config = root.resolve("config/hybench_sf1.yaml");
        var c = CaptureConfig.read(root, config);
        assertEquals(60, c.seconds());
        assertEquals(1, c.clients());
        assertEquals("35", c.nativeProperties().getProperty("tp3_percent"));
        assertEquals(268435456, c.layout().targetBytes());
        Path invalid = Files.createTempFile(root.resolve("build/sibench"), "capture-", ".yaml");
        try {
            Files.writeString(
                    invalid, Files.readString(config).replace("clients: 1", "clients: 2"));
            assertThrows(IllegalArgumentException.class, () -> CaptureConfig.read(root, invalid));
        } finally {
            Files.delete(invalid);
        }
    }
}
