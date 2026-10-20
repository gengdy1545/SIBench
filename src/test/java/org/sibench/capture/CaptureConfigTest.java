package org.sibench.capture;

import static org.junit.Assert.*;

import org.junit.Test;

import java.nio.file.*;

public class CaptureConfigTest {
    @Test
    public void validatesNativeProfileAndRejectsUnsupportedClients() throws Exception {
        Path root = Path.of("").toAbsolutePath();
        Path config = root.resolve("config/hybench_tp_subset_sf1.yaml");
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

    @Test
    public void defaultProfileUsesNativeHtapWithoutOverridingTransactionWeights() throws Exception {
        Path root = Path.of("").toAbsolutePath();
        var c = CaptureConfig.read(root, Path.of("config/hybench_sf1.yaml"));
        assertEquals("runhtap", c.mode());
        var p = c.nativeProperties();
        assertFalse(p.containsKey("tp3_percent"));
        assertEquals("1", p.getProperty("apclient"));
        assertEquals("1", p.getProperty("xtpclient"));
        assertEquals("1", p.getProperty("xapclient"));
        assertEquals(p.getProperty("url"), p.getProperty("url_ap"));
        assertEquals(CaptureDriver.class.getName(), p.getProperty("classname_ap"));
        assertEquals("1", p.getProperty("xpRunMins"));
        assertEquals("2", p.getProperty("tpRunMins"));
        Path implicit = Files.createTempFile(root.resolve("build/sibench"), "capture-", ".yaml");
        try {
            Files.writeString(implicit,
                    Files.readString(root.resolve("config/hybench_sf1.yaml"))
                            .replace("  mode: runhtap\n", ""));
            assertEquals("runhtap", CaptureConfig.read(root, implicit).mode());
        } finally {
            Files.delete(implicit);
        }
    }
}
