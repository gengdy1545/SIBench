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
        assertFalse(simple.secondaryIndexes());
        var withSecondary = parse(template);
        var target = object(withSecondary, "target");
        target.put("secondary_indexes", Map.of("enabled", true));
        withSecondary.put("target", target);
        assertTrue(ReplayConfig.from(withSecondary).secondaryIndexes());
        assertEquals(org.sibench.adapters.ValueKind.ROW_ID,
                org.sibench.adapters.ServiceConfig.ofReplay(root, parse(template)).value());
        assertEquals(org.sibench.adapters.ValueKind.LOCATOR,
                org.sibench.adapters.ServiceConfig.ofReplay(root, parse(template.replace("value: row_id", "value: locator"))).value());
        assertEquals(64, simple.clientCount());
        assertEquals(4096, simple.maxInFlight());
        assertEquals(600, simple.requestTimeoutSeconds());
        // An omitted dispatch setting would otherwise replay with a different client count or timeout.
        for (String key : java.util.List.of("dispatch_mode", "client_count", "max_in_flight",
                "request_timeout_seconds", "baseline_batch_operations")) {
            var omitted = parse(template);
            var replay = object(omitted, "replay");
            replay.remove(key);
            omitted.put("replay", replay);
            assertTrue(key, assertThrows(IllegalArgumentException.class, () -> ReplayConfig.from(omitted))
                    .getMessage().contains(key));
        }
        for (int clients : new int[] {64, 128})
            assertEquals(clients, ReplayConfig.from(parse(template.replace("client_count: 64", "client_count: " + clients)
                    .replace("baseline_batch_operations: 64", "baseline_batch_operations: " + 4096 / clients))).clientCount());
        assertEquals(64, simple.baselineBatchOperations());
        // Every client holding a partial batch must still leave an in-flight slot.
        for (String invalid : new String[] {"0", "66", "\"64\""})
            assertThrows(invalid, IllegalArgumentException.class, () -> ReplayConfig.from(parse(
                    template.replace("baseline_batch_operations: 64", "baseline_batch_operations: " + invalid))));
        assertThrows(IllegalArgumentException.class, () -> ReplayConfig.from(parse(template.replace("client_count: 64", "client_count: 128"))));
        assertEquals("operation", simple.dispatchMode());
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
        replay.put("baseline_batch_operations", 32);
        replay.put("trace_budget", Map.of("duration_seconds", 1800,
                "planned_operations_per_second", 300000,
                "warmup_operations", 18000000, "reserve_percent", 20));
        longDocument.put("replay", replay);
        var longRun = ReplayConfig.from(longDocument);
        assertEquals(666000000, longRun.requiredOperations());
        assertEquals(128, longRun.clientCount());
        assertEquals(2, new ReplayConfig(false, 1, 1, 0, 1).requiredOperations());
        assertEquals(120, new ReplayConfig(false, 1, 100, 0, 20).requiredOperations());
        assertThrows(
                ArithmeticException.class,
                () -> new ReplayConfig(true, Long.MAX_VALUE, 2, 0, 0));
        assertThrows(
                ArithmeticException.class,
                () -> new ReplayConfig(true, 1, Long.MAX_VALUE, 0, 100));
    }

    @Test
    public void rejectsUnknownOptionsAndInvalidDispatchValues() throws Exception {
        String text = Files.readString(Path.of("config/sibench.yaml"));
        for (String invalid :
                new String[] {
                    text.replace("target:", "target:\n  value: locator"),
                    text.replace("replay:", "replay:\n  repeat: 2"),
                    text.replace("enabled: false", "enabled: 'false'"),
                    text.replace("dispatch_mode: operation", "dispatch_mode: parallel"),
                    text.replace("client_count: 64", "client_count: 0"),
                    text.replace("max_in_flight: 4096", "max_in_flight: 0"),
                    text.replace("request_timeout_seconds: 600", "request_timeout_seconds: 86401"),
                    text + "\nversions:\n  gc:\n    owner: immediate\n"
                }) {
            assertNotEquals("The invalid fixture must change the public template", text, invalid);
            assertThrows(IllegalArgumentException.class, () -> ReplayConfig.from(parse(invalid)));
        }
        var serviceless = parse(text);
        var target = object(serviceless, "target");
        target.remove("service");
        target.put("value", "locator");
        serviceless.put("target", target);
        assertThrows(IllegalArgumentException.class, () -> ReplayConfig.from(serviceless));
        assertThrows(
                IllegalArgumentException.class,
                () -> new ReplayConfig(true, 0, 300000, 0, 0));
        assertThrows(
                IllegalArgumentException.class, () -> new ReplayConfig(true, 1, 1, -1, 0));
    }
}
