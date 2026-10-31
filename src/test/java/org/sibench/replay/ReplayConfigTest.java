package org.sibench.replay;

import static org.junit.Assert.*;
import static org.sibench.config.ConfigValues.object;
import static org.sibench.config.ConfigValues.parse;

import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public class ReplayConfigTest {
    @Test
    public void validatesProfilesAndComputesOverflowCheckedBudget() throws Exception {
        Path root = Path.of("").toAbsolutePath();
        String template = Files.readString(root.resolve("config/sibench.yaml"));
        var simple = ReplayConfig.read(root, Path.of("config/sibench.yaml"));
        assertTrue(simple.secondaryIndexes());
        assertEquals(java.util.List.of(true), ReplayConfig.rowIdModes(parse(template)));
        assertEquals(java.util.List.of(false), ReplayConfig.rowIdModes(parse(template.replace("value: row_id", "value: locator"))));
        assertEquals(64, simple.clientCount());
        assertEquals(4096, simple.maxInFlight());
        assertEquals(60, simple.requestTimeoutSeconds());
        for (int clients : new int[] {64, 128})
            assertEquals(clients, ReplayConfig.from(parse(template.replace(
                    "client_count: 64", "client_count: " + clients))).clientCount());
        assertEquals("operation", simple.dispatchMode());
        assertEquals(
                "operation",
                ReplayConfig.from(
                                parse(
                                        Files.readString(Path.of("config/sibench.yaml"))
                                                .replace("  dispatch_mode: operation\n", "")))
                        .dispatchMode());
        assertEquals(
                "transaction",
                ReplayConfig.from(
                                parse(
                                        Files.readString(Path.of("config/sibench.yaml"))
                                                .replace(
                                                        "dispatch_mode: operation",
                                                        "dispatch_mode: transaction")))
                        .dispatchMode());
        assertEquals(0, simple.requiredOperations());
        var longDocument = parse(template);
        var replay = object(longDocument, "replay");
        replay.put("client_count", 128);
        replay.put("trace_budget", Map.of("duration_seconds", 1800,
                "planned_operations_per_second", 300000,
                "warmup_operations", 18000000, "reserve_percent", 20));
        longDocument.put("replay", replay);
        var longRun = ReplayConfig.from(longDocument);
        assertEquals(666000000, longRun.requiredOperations());
        assertEquals(128, longRun.clientCount());
        assertEquals(2, new ReplayConfig(false, "none", 1, 1, 0, 1).requiredOperations());
        assertEquals(120, new ReplayConfig(false, "none", 1, 100, 0, 20).requiredOperations());
        assertThrows(
                ArithmeticException.class,
                () -> new ReplayConfig(true, "none", Long.MAX_VALUE, 2, 0, 0));
        assertThrows(
                ArithmeticException.class,
                () -> new ReplayConfig(true, "none", 1, Long.MAX_VALUE, 0, 100));
    }

    @Test
    public void rejectsRepetitionUnknownOptionsAndUnsupportedReferenceGc() throws Exception {
        String text = Files.readString(Path.of("config/sibench.yaml"));
        for (String invalid :
                new String[] {
                    text.replace("single_pass", "cycle"),
                    text.replace("single_pass", "single_pass\n  repeat: 2"),
                    text.replace("enabled: true", "enabled: 'false'"),
                    text.replace("dispatch_mode: operation", "dispatch_mode: parallel"),
                    text.replace("client_count: 64", "client_count: 0"),
                    text.replace("max_in_flight: 4096", "max_in_flight: 0"),
                    text.replace("request_timeout_seconds: 60", "request_timeout_seconds: 86401"),
                    text.replace("client_count: 64", "client_count: 64\n  routing: index_key"),
                    text.replace(
                            "secondary_indexes:",
                            "transaction_writes:\n"
                                    + "    coalesce_same_key: true\n"
                                    + "  secondary_indexes:"),
                    text.replace("owner: none", "owner: immediate"),
                    text.replace("gc:\n", "gc:\n    send_delete: false\n")
                }) {
            assertThrows(IllegalArgumentException.class, () -> ReplayConfig.from(parse(invalid)));
        }
        for (String owner : new String[] {"backend", "benchmark"})
            assertThrows(
                    IllegalArgumentException.class,
                    () -> new ReplayConfig(true, owner, 0, 0, 0, 0).requireReferenceCapabilities());
        assertThrows(
                IllegalArgumentException.class,
                () -> new ReplayConfig(true, "none", 0, 300000, 0, 0));
        assertThrows(
                IllegalArgumentException.class, () -> new ReplayConfig(true, "none", 1, 1, -1, 0));
    }
}
