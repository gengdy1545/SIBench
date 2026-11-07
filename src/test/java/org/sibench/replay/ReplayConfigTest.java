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
        assertTrue(ReplayConfig.from(parse(template.replace("enabled: false", "enabled: true"))).secondaryIndexes());
        assertEquals(org.sibench.adapters.proto.ValueKind.ROW_ID,
                org.sibench.adapters.ServiceConfig.ofReplay(root, parse(template)).value());
        assertEquals(org.sibench.adapters.proto.ValueKind.LOCATOR,
                org.sibench.adapters.ServiceConfig.ofReplay(root, parse(template.replace("value: row_id", "value: locator"))).value());
        assertEquals(64, simple.clientCount());
        assertEquals(4096, simple.maxInFlight());
        assertEquals(600, simple.requestTimeoutSeconds());
        var omittedTimeout = parse(template);
        var omittedReplay = object(omittedTimeout, "replay");
        omittedReplay.remove("request_timeout_seconds");
        omittedTimeout.put("replay", omittedReplay);
        assertEquals(60, ReplayConfig.from(omittedTimeout).requestTimeoutSeconds());
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
    public void rejectsRepetitionUnknownOptionsAndUnsupportedGc() throws Exception {
        String text = Files.readString(Path.of("config/sibench.yaml"));
        for (String invalid :
                new String[] {
                    text.replace("target:", "target:\n  value: locator"),
                    text.replace("replay:", "replay:\n  strategy: cycle"),
                    text.replace("replay:", "replay:\n  repeat: 2"),
                    text.replace("enabled: false", "enabled: 'false'"),
                    text.replace("dispatch_mode: operation", "dispatch_mode: parallel"),
                    text.replace("client_count: 64", "client_count: 0"),
                    text.replace("max_in_flight: 4096", "max_in_flight: 0"),
                    text.replace("request_timeout_seconds: 600", "request_timeout_seconds: 86401"),
                    text.replace("client_count: 64", "client_count: 64\n  routing: index_key"),
                    text.replace(
                            "secondary_indexes:",
                            "transaction_writes:\n"
                                    + "    coalesce_same_key: true\n"
                                    + "  secondary_indexes:"),
                    text + "\nversions:\n  gc:\n    owner: immediate\n",
                    text + "\nversions:\n  gc:\n    send_delete: false\n"
                }) {
            assertNotEquals("The invalid fixture must change the public template", text, invalid);
            assertThrows(IllegalArgumentException.class, () -> ReplayConfig.from(parse(invalid)));
        }
        var retired = parse(text);
        var target = object(retired, "target");
        target.remove("service");
        target.put("value", "locator");
        retired.put("target", target);
        assertThrows(IllegalArgumentException.class, () -> ReplayConfig.from(retired));
        assertThrows(
                IllegalArgumentException.class,
                () -> new ReplayConfig(true, 0, 300000, 0, 0));
        assertThrows(
                IllegalArgumentException.class, () -> new ReplayConfig(true, 1, 1, -1, 0));
    }

    @Test
    public void rejectsRetiredFixedOptions() throws Exception {
        String source = Files.readString(Path.of("config/sibench.yaml"));
        for (String strategy : new String[] {"single_pass", "cycle"}) {
            var document = parse(source);
            var replay = object(document, "replay");
            replay.put("strategy", strategy);
            document.put("replay", replay);
            assertThrows(IllegalArgumentException.class, () -> ReplayConfig.from(document));
        }
        for (String owner : new String[] {"none", "backend", "benchmark"}) {
            var document = parse(source);
            document.put("versions", Map.of("gc", Map.of("owner", owner)));
            assertThrows(IllegalArgumentException.class, () -> ReplayConfig.from(document));
        }
        var previous = parse(source);
        var replay = object(previous, "replay");
        replay.put("strategy", "single_pass");
        previous.put("replay", replay);
        previous.put("versions", Map.of("gc", Map.of("owner", "none")));
        assertThrows(IllegalArgumentException.class, () -> ReplayConfig.from(previous));
    }
}
