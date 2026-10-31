package org.sibench.capture;

import static org.junit.Assert.*;

import org.junit.Test;

import java.nio.file.*;

public class CaptureConfigTest {
    @Test
    public void profileUsesNativeHtapWithUpstreamTransactionWeights() throws Exception {
        Path root = Path.of("").toAbsolutePath();
        var c = CaptureConfig.read(root, Path.of("src/test/resources/config/hybench_sf1.yaml"));
        assertEquals(536870912, c.layout().targetBytes());
        assertEquals("fixture_hybench_base", c.cloneFrom());
        var p = c.nativeProperties();
        assertFalse(p.containsKey("tp3_percent"));
        assertEquals("1", p.getProperty("apclient"));
        assertEquals("1", p.getProperty("xtpclient"));
        assertEquals("1", p.getProperty("xapclient"));
        assertEquals(p.getProperty("url"), p.getProperty("url_ap"));
        assertEquals(CaptureDriver.class.getName(), p.getProperty("classname_ap"));
        assertEquals("1", p.getProperty("xpRunMins"));
        assertEquals("2", p.getProperty("tpRunMins"));
    }

    @Test
    public void rejectsUnsupportedClientCounts() throws Exception {
        Path root = Path.of("").toAbsolutePath();
        Path config = root.resolve("src/test/resources/config/hybench_sf1.yaml");
        Path invalid = Files.createTempFile(root.resolve("build/sibench"), "capture-", ".yaml");
        try {
            Files.writeString(
                    invalid, Files.readString(config).replace("ap_clients: 1", "ap_clients: 257"));
            assertThrows(IllegalArgumentException.class, () -> CaptureConfig.read(root, invalid));
        } finally {
            Files.delete(invalid);
        }
    }
}
