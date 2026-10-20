package org.sibench.replay;

import static org.junit.Assert.*;
import static org.sibench.config.ConfigValues.parse;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Test;

public class ReplayConfigTest {
    @Test
    public void validatesProfilesAndComputesOverflowCheckedBudget() throws Exception {
        Path root = Path.of("").toAbsolutePath();
        var simple = ReplayConfig.read(root, Path.of("config/index_replay.yaml"));
        assertTrue(simple.secondaryIndexes());
        assertTrue(simple.coalesceTransactionWrites());
        assertFalse(ReplayConfig.from(parse(Files.readString(Path.of("config/index_replay.yaml"))
                .replace("coalesce_same_key: true", "coalesce_same_key: false"))).coalesceTransactionWrites());
        assertEquals(0, simple.requiredOperations());
        var longRun = ReplayConfig.read(root, Path.of("config/long_replay.yaml"));
        assertEquals(666000000, longRun.requiredOperations());
        assertEquals(2, new ReplayConfig(false, "none", 1, 1, 0, 1).requiredOperations());
        assertEquals(120, new ReplayConfig(false, "none", 1, 100, 0, 20).requiredOperations());
        assertThrows(ArithmeticException.class,
                () -> new ReplayConfig(true, "none", Long.MAX_VALUE, 2, 0, 0));
        assertThrows(ArithmeticException.class,
                () -> new ReplayConfig(true, "none", 1, Long.MAX_VALUE, 0, 100));
    }

    @Test
    public void rejectsRepetitionUnknownOptionsAndUnsupportedReferenceGc() throws Exception {
        String text = Files.readString(Path.of("config/index_replay.yaml"));
        for (String invalid : new String[] {
                text.replace("single_pass", "cycle"),
                text.replace("single_pass", "single_pass\n  repeat: 2"),
                text.replace("enabled: true", "enabled: 'false'"),
                text.replace("coalesce_same_key: true", "coalesce_same_key: 'false'"),
                text.replace("owner: none", "owner: immediate"),
                text.replace("gc:\n", "gc:\n    send_delete: false\n")
        }) {
            assertThrows(IllegalArgumentException.class, () -> ReplayConfig.from(parse(invalid)));
        }
        for (String owner : new String[] {"backend", "benchmark"})
            assertThrows(IllegalArgumentException.class,
                    () -> new ReplayConfig(true, owner, 0, 0, 0, 0).requireReferenceCapabilities());
        assertThrows(IllegalArgumentException.class,
                () -> new ReplayConfig(true, "none", 0, 300000, 0, 0));
        assertThrows(IllegalArgumentException.class,
                () -> new ReplayConfig(true, "none", 1, 1, -1, 0));
    }
}
