package org.sibench.capture;

import static org.junit.Assert.*;

import static org.sibench.config.ConfigValues.*;

import org.junit.Test;

import java.nio.file.*;

public class NativeCaptureConfigTest {
    /** HyBench SF1 baseline rows: the size every captured source is aligned to. */
    private static final long SF1_ROWS = 8_706_000;

    private final Path root = Path.of("").toAbsolutePath();

    @Test
    public void captureBaselinesMatchTheHybenchRowCount() throws Exception {
        var ycsb = NativeCaptureConfig.read(root, Path.of("config/ycsb_8m.yaml"));
        assertEquals(SF1_ROWS, positive(ycsb.nativeConfig(), "recordcount"));
        assertEquals(SF1_ROWS, positive(ycsb.nativeConfig(), "insertcount"));
        var sysbench = NativeCaptureConfig.read(root, Path.of("config/sysbench_8m.yaml"));
        assertEquals(
                SF1_ROWS,
                positive(sysbench.nativeConfig(), "tables")
                        * positive(sysbench.nativeConfig(), "table-size"));
        // The smoke profiles stay small on purpose; they are not validation evidence.
        assertTrue(
                positive(
                                NativeCaptureConfig.read(root, Path.of("config/ycsb_small.yaml"))
                                        .nativeConfig(),
                                "recordcount")
                        < 100_000);
    }

    @Test
    public void rejectsMultiSessionCaptureAndOutOfRangeLimits() throws Exception {
        Path source = root.resolve("config/ycsb_8m.yaml");
        String text = Files.readString(source);
        for (String[] change :
                new String[][] {
                    {"  threads: 1\n", "  threads: 2\n"},
                    {"load_threads: 16", "load_threads: 65"},
                    {"timeout_minutes: 360", "timeout_minutes: 1441"},
                }) {
            assertTrue(text.contains(change[0]));
            Path invalid = Files.createTempFile(root.resolve("build/sibench"), "native-", ".yaml");
            try {
                Files.writeString(invalid, text.replace(change[0], change[1]));
                assertThrows(
                        IllegalArgumentException.class, () -> NativeCaptureConfig.read(root, invalid));
            } finally {
                Files.delete(invalid);
            }
        }
    }
}
