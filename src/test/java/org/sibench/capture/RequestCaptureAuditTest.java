package org.sibench.capture;

import static org.junit.Assert.*;

import com.google.gson.JsonPrimitive;
import com.google.gson.JsonParser;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.sql.Types;

public class RequestCaptureAuditTest {
    private static final String HEADER = "{\"event\":\"capture_start\",\"pid\":42,\"detail\":\"summary\"}\n";

    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void normalizesJdbcTypesWithoutApproximateNumericTolerance() {
        assertTrue(equal("40000", "40000.0", Types.REAL));
        assertTrue(equal("0.1", "0.10000000149011612", Types.REAL));
        assertFalse(equal("1", "1.0001", Types.REAL));
        assertFalse(equal("0.1", "0.10000000149011612", Types.DOUBLE));
        assertTrue(equal("1.0", "1.00", Types.NUMERIC));
        assertTrue(equal("2024-01-01 00:00:00", "2024-01-01 00:00:00.0", Types.TIMESTAMP));
        assertTrue(equal("accept ", "accept", Types.CHAR));
        assertFalse(equal("accept ", "accept", Types.VARCHAR));
    }

    private static boolean equal(String a, String b, int type) {
        return RequestCaptureAudit.equalResult(new JsonPrimitive(a), new JsonPrimitive(b), type);
    }

    @Test
    public void directEventsMatchResultsThatArriveBeforeClientConsumption() throws Exception {
        for (boolean wrong : java.util.List.of(false, true)) {
            var audit = new RequestCaptureAudit.State(false);
            audit.server(com.google.gson.JsonParser.parseString(HEADER).getAsJsonObject());
            audit.server(com.google.gson.JsonParser.parseString(
                    "{\"event\":\"executor_start\",\"pid\":42,\"application\":\"a\",\"sql\":\"SELECT 1\"}").getAsJsonObject());
            for (String line : java.util.List.of(
                    "{\"event\":\"connection\",\"pid\":42}",
                    "{\"event\":\"request_start\",\"application\":\"a\",\"sql\":\"SELECT 1\",\"parameters\":{}}"))
                audit.client(com.google.gson.JsonParser.parseString(line).getAsJsonObject());
            for (String line : java.util.List.of(
                    "{\"event\":\"executor_run_end\",\"pid\":42,\"application\":\"a\",\"depth\":1,\"returned\":1,\"row_hashes\":[\"0000000000000001\"]}",
                    "{\"event\":\"executor_end\",\"pid\":42,\"application\":\"a\"}",
                    "{\"event\":\"transaction_commit\",\"pid\":42,\"application\":\"a\"}"))
                audit.server(com.google.gson.JsonParser.parseString(line).getAsJsonObject());
            for (String line : java.util.List.of(
                    "{\"event\":\"request_return\",\"application\":\"a\"}",
                    "{\"event\":\"consumed_row\",\"application\":\"a\",\"row_hash\":\"000000000000000" + (wrong ? "2" : "1") + "\"}",
                    "{\"event\":\"commit\",\"application\":\"a\"}",
                    "{\"event\":\"close\",\"application\":\"a\"}"))
                audit.client(com.google.gson.JsonParser.parseString(line).getAsJsonObject());
            if (wrong) assertThrows(IOException.class, audit::finish);
            else assertEquals(1L, audit.finish().get("client_consumed_rows_checked"));
        }
    }

    @Test
    public void acceptsMatchedRollbackAndCloseAbortInSingleSessionCapture() throws Exception {
        var directory = temporary.newFolder().toPath();
        var client = directory.resolve("client.jsonl.zst");
        var server = directory.resolve("server.jsonl.zst");
        write(
                client,
                """
                {"event":"connection","pid":42}
                {"event":"request_start","application":"a","sql":"SELECT 1","parameters":{}}
                {"event":"request_return","application":"a"}
                {"event":"rollback","application":"a"}
                {"event":"request_start","application":"b","sql":"SELECT 2","parameters":{}}
                {"event":"request_return","application":"b"}
                {"event":"close","application":"b"}
                """);
        String pg = HEADER +
                """
                {"event":"executor_start","pid":42,"application":"a","sql":"SELECT 1"}
                {"event":"executor_run_end","pid":42,"application":"a","depth":1,"returned":0,"row_hashes":[]}
                {"event":"executor_end","pid":42,"application":"a"}
                {"event":"transaction_abort","pid":42,"application":"a"}
                {"event":"executor_start","pid":42,"application":"b","sql":"SELECT 2"}
                {"event":"executor_run_end","pid":42,"application":"b","depth":1,"returned":0,"row_hashes":[]}
                {"event":"executor_end","pid":42,"application":"b"}
                """;
        write(
                server,
                pg + "{\"event\":\"transaction_abort\",\"pid\":42,\"application\":\"b\"}\n");
        var audit = RequestCaptureAudit.audit(client, server, true);
        assertEquals("passed", audit.get("status"));
        assertEquals(1L, audit.get("explicit_rollbacks"));
        assertEquals(1L, audit.get("close_aborts"));
        write(server, pg);
        assertThrows(java.io.IOException.class, () -> RequestCaptureAudit.audit(client, server, true));
        write(
                server,
                pg.replace(
                                "\"event\":\"transaction_abort\",\"pid\":42,\"application\":\"a\"",
                                "\"event\":\"transaction_abort\",\"pid\":42,\"application\":\"b\"")
                        + "{\"event\":\"transaction_abort\",\"pid\":42,\"application\":\"b\"}\n");
        assertThrows(java.io.IOException.class, () -> RequestCaptureAudit.audit(client, server, true));
    }

    /**
     * A summary capture compares the client's row hashes with the server's; a row that kept its
     * text is also hashed from it, so a stale hash cannot pass.
     */
    @Test
    public void comparesConsumedRowsWithServerRowHashes() throws Exception {
        var directory = temporary.newFolder().toPath();
        var client = directory.resolve("client.jsonl.zst");
        var server = directory.resolve("server.jsonl.zst");
        String hash = ResultRowHash.of(java.util.List.of("2000.0"), java.util.List.of(Types.REAL));
        String requests =
                """
                {"event":"connection","pid":42}
                {"event":"request_start","application":"a","sql":"SELECT balance FROM t","parameters":{}}
                {"event":"request_return","application":"a"}
                %s
                {"event":"commit","application":"a"}
                {"event":"close","application":"a"}
                """;
        String pg = HEADER +
                """
                {"event":"executor_start","pid":42,"application":"a","sql":"SELECT balance FROM t","depth":1}
                {"event":"executor_run_end","pid":42,"application":"a","depth":1,"returned":1,"row_hashes":["%s"]}
                {"event":"executor_end","pid":42,"application":"a","depth":1}
                {"event":"transaction_commit","pid":42,"application":"a"}
                """;
        write(server, pg.formatted(ResultRowHash.of(java.util.List.of("2000"), java.util.List.of(Types.REAL))));
        write(client, requests.formatted("{\"event\":\"consumed_row\",\"application\":\"a\",\"row_hash\":\"" + hash + "\"}"));
        assertEquals(1L, RequestCaptureAudit.audit(client, server, false).get("client_consumed_rows_checked"));
        // A row with its text: the hash must be the text's, and both must match the server.
        String text = "{\"event\":\"consumed_row\",\"application\":\"a\",\"row_hash\":\"%s\",\"1\":\"2000.0\",\"column_types\":{\"1\":7}}";
        write(client, requests.formatted(text.formatted(hash)));
        assertEquals(1L, RequestCaptureAudit.audit(client, server, false).get("client_consumed_rows_checked"));
        write(client, requests.formatted("{\"event\":\"consumed_row\",\"application\":\"a\",\"1\":\"2000.0\",\"column_types\":{\"1\":7}}"));
        assertThrows(IOException.class, () -> RequestCaptureAudit.audit(client, server, false));
        write(client, requests.formatted(text.formatted("0000000000000000")));
        assertTrue(assertThrows(java.io.IOException.class, () -> RequestCaptureAudit.audit(client, server, false))
                .getMessage().contains("hash differs from its values"));
        write(server, pg.formatted("0000000000000000"));
        write(client, requests.formatted("{\"event\":\"consumed_row\",\"application\":\"a\",\"row_hash\":\"" + hash + "\"}"));
        assertTrue(assertThrows(java.io.IOException.class, () -> RequestCaptureAudit.audit(client, server, false))
                .getMessage().contains("differs from same-execution server result"));
    }

    @Test
    public void rejectsRollbackWithoutCompletedRequest() throws Exception {
        var directory = temporary.newFolder().toPath();
        var client = directory.resolve("client.jsonl.zst");
        var server = directory.resolve("server.jsonl.zst");
        write(
                client,
                """
                {"event":"connection","pid":42}
                {"event":"request_start","application":"a","sql":"SELECT 1","parameters":{}}
                {"event":"rollback","application":"a"}
                {"event":"request_return","application":"a"}
                {"event":"close","application":"a"}
                """);
        write(server, "");
        assertThrows(java.io.IOException.class, () -> RequestCaptureAudit.audit(client, server, true));
    }

    private static final String AUTOCOMMIT_CLIENT =
            """
            {"event":"connection","pid":42}
            {"event":"request_start","application":"a","sql":"SELECT c FROM t WHERE id = ?","parameters":{"1":"5"},"autocommit":true}
            {"event":"request_return","application":"a"}
            {"event":"autocommit","application":"a","implicit_commits":2}
            {"event":"close","application":"a"}
            """;

    @Test
    public void consumedPrefixesKeepExactOrderAcrossExecutorReturns() throws Exception {
        var directory = temporary.newFolder().toPath();
        var client = directory.resolve("client.jsonl.zst");
        var server = directory.resolve("server.jsonl.zst");
        String rows = "";
        for (int i = 0; i < 9; i++)
            rows += "{\"event\":\"consumed_row\",\"application\":\"a\",\"row_hash\":\""
                    + "%016x".formatted(i) + "\"}\n";
        write(client, AUTOCOMMIT_CLIENT.replace(
                "{\"event\":\"close\",\"application\":\"a\"}", rows + "{\"event\":\"close\",\"application\":\"a\"}"));
        String pg = server(2, "").replace(
                "\"returned\":0,\"row_hashes\":[]",
                "\"returned\":4,\"row_hashes\":[\"0000000000000000\",\"0000000000000001\",\"0000000000000002\",\"0000000000000003\"]");
        pg += "{\"event\":\"executor_run_end\",\"pid\":42,\"application\":\"a\",\"depth\":1,\"returned\":6,\"row_hashes\":[\"0000000000000004\",\"0000000000000005\",\"0000000000000006\",\"0000000000000007\",\"0000000000000008\",\"0000000000000009\"]}\n";
        write(server, pg);
        assertEquals(9L, RequestCaptureAudit.audit(client, server, false).get("client_consumed_rows_checked"));
        write(server, pg.replace("0000000000000007", "0000000000000008"));
        assertThrows(IOException.class, () -> RequestCaptureAudit.audit(client, server, false));
        write(server, pg.replace("\"returned\":6", "\"returned\":4")
                .replace(",\"0000000000000008\",\"0000000000000009\"", ""));
        assertThrows(IOException.class, () -> RequestCaptureAudit.audit(client, server, false));
    }

    private static String server(int commits, String scan) {
        return HEADER + "{\"event\":\"executor_start\",\"pid\":42,\"application\":\"a\",\"sql\":\"SELECT c FROM t WHERE id = $1\"}\n"
                + scan
                + "{\"event\":\"executor_run_end\",\"pid\":42,\"application\":\"a\",\"depth\":1,\"returned\":0,\"row_hashes\":[]}\n"
                + "{\"event\":\"transaction_commit\",\"pid\":42,\"application\":\"a\"}\n".repeat(commits);
    }

    private static String scan(String strategy, String flags, String value) {
        return "{\"event\":\"scan_rescan\",\"pid\":42,\"application\":\"a\",\"relation\":\"t_pkey\","
                + "\"scan\":\"s\",\"keys\":[{\"attribute\":1,\"strategy\":"
                + strategy
                + ",\"flags\":"
                + flags
                + ",\"value\":"
                + value
                + "}]}\n";
    }

    @Test
    public void autocommitConnectionsCommitTheLabelAndTheRequest() throws Exception {
        var directory = temporary.newFolder().toPath();
        var client = directory.resolve("client.jsonl.zst");
        var server = directory.resolve("server.jsonl.zst");
        write(client, AUTOCOMMIT_CLIENT);
        write(server, server(2, ""));
        assertEquals("passed", RequestCaptureAudit.audit(client, server, true).get("status"));
        // One commit fewer, or one more, is an event-loss signature.
        write(server, server(1, ""));
        assertThrows(java.io.IOException.class, () -> RequestCaptureAudit.audit(client, server, true));
        write(server, server(3, ""));
        assertThrows(java.io.IOException.class, () -> RequestCaptureAudit.audit(client, server, true));
    }

    @Test
    public void pointAccessBindingsAreCheckedOnlyWhenRequested() throws Exception {
        var directory = temporary.newFolder().toPath();
        var client = directory.resolve("client.jsonl.zst");
        var server = directory.resolve("server.jsonl.zst");
        write(client, AUTOCOMMIT_CLIENT);
        // Key 7 was never bound by the client: a join-derived key, or a capture error.
        write(server, server(2, scan("3", "0", "\"7\"")));
        assertThrows(
                java.io.IOException.class,
                () -> RequestCaptureAudit.audit(client, server, true));
        assertEquals(
                "passed", RequestCaptureAudit.audit(client, server, false).get("status"));
        write(server, server(2, scan("3", "0", "\"5\"")));
        assertEquals("passed", RequestCaptureAudit.audit(client, server, true).get("status"));
        // Planner endpoint probes (IS NOT NULL, no bound value) are not search predicates.
        write(server, server(2, scan("0", "129", "null")));
        assertEquals("passed", RequestCaptureAudit.audit(client, server, true).get("status"));
    }

    /**
     * A commit the client issues after its transaction already ended writes no server commit: the
     * transaction closed in an abort and the request changed no row, so nothing is lost. The audit
     * records it; the same request with row changes is still a lost commit and fails.
     */
    @Test
    public void aCommitAfterTheTransactionAlreadyEndedIsRecordedNotToleratedAway() throws Exception {
        var directory = temporary.newFolder().toPath();
        var client = directory.resolve("client.jsonl.zst");
        var server = directory.resolve("server.jsonl.zst");
        write(client, COMMIT_AFTER_ROLLBACK_CLIENT);
        write(server, server(0, "") + abortEvent());
        var report = RequestCaptureAudit.audit(client, server, true);
        assertEquals("passed", report.get("status"));
        assertEquals(1L, ((Map<?, ?>) report.get("client_commits_ended_by_server_abort")).get("a"));
    }

    private static final String COMMIT_AFTER_ROLLBACK_CLIENT =
            """
            {"event":"connection","pid":42}
            {"event":"request_start","application":"a","sql":"SELECT c FROM t WHERE id = ?","parameters":{"1":"5"}}
            {"event":"request_return","application":"a"}
            {"event":"rollback","application":"a"}
            {"event":"commit","application":"a"}
            {"event":"close","application":"a"}
            """;

    @Test
    public void aCommitThatChangedRowsButNeverCommittedFails() throws Exception {
        var directory = temporary.newFolder().toPath();
        var client = directory.resolve("client.jsonl.zst");
        var server = directory.resolve("server.jsonl.zst");
        write(client, COMMIT_AFTER_ROLLBACK_CLIENT);
        write(
                server,
                server(0, "")
                        + abortEvent()
                        + "{\"event\":\"row_change\",\"pid\":42,\"application\":\"a\","
                        + "\"relation\":\"t\",\"operation\":\"update\"}\n");
        assertThrows(
                java.io.IOException.class, () -> RequestCaptureAudit.audit(client, server, true));
    }

    private static String abortEvent() {
        return "{\"event\":\"transaction_abort\",\"pid\":42,\"application\":\"a\"}\n";
    }

    @Test
    public void delayedConsumptionOfEarlierRequestRemainsChecked() throws Exception {
        var directory = temporary.newFolder().toPath();
        var client = directory.resolve("client.jsonl.zst");
        var server = directory.resolve("server.jsonl.zst");
        String rows = """
                {"event":"connection","pid":42}
                {"event":"request_start","application":"sibench:j1:1:1","request":1,"sql":"SELECT 1","parameters":{}}
                {"event":"request_return","application":"sibench:j1:1:1"}
                {"event":"request_start","application":"sibench:j1:1:2","request":2,"sql":"SELECT 2","parameters":{}}
                {"event":"request_return","application":"sibench:j1:1:2"}
                {"event":"consumed_row","application":"sibench:j1:1:1","row_hash":"0000000000000001"}
                {"event":"consumed_row","application":"sibench:j1:1:2","row_hash":"0000000000000002"}
                {"event":"request_end","application":"sibench:j1:1:1"}
                {"event":"request_end","application":"sibench:j1:1:2"}
                {"event":"commit","application":"sibench:j1:1:2"}
                {"event":"close","application":"sibench:j1:1:2"}
                """;
        write(client, rows);
        write(server, HEADER + """
                {"event":"executor_start","pid":42,"application":"sibench:j1:1:1","sql":"SELECT 1"}
                {"event":"executor_run_end","pid":42,"application":"sibench:j1:1:1","depth":1,"returned":1,"row_hashes":["0000000000000001"]}
                {"event":"executor_end","pid":42,"application":"sibench:j1:1:1"}
                {"event":"scan_end","pid":42,"application":"sibench:j1:1:1"}
                {"event":"executor_start","pid":42,"application":"sibench:j1:1:2","sql":"SELECT 2"}
                {"event":"executor_run_end","pid":42,"application":"sibench:j1:1:2","depth":1,"returned":1,"row_hashes":["0000000000000002"]}
                {"event":"executor_end","pid":42,"application":"sibench:j1:1:2"}
                {"event":"transaction_commit","pid":42,"application":"sibench:j1:1:2"}
                """);
        assertEquals(2L, RequestCaptureAudit.audit(client, server, false).get("client_consumed_rows_checked"));
        write(client, rows.replace("0000000000000001", "0000000000000003"));
        assertThrows(IOException.class, () -> RequestCaptureAudit.audit(client, server, false));
    }

    @Test
    public void rejectsCloseWhoseOpenRequestWasNeverRecorded() throws Exception {
        var directory = temporary.newFolder().toPath();
        var client = directory.resolve("client.jsonl.zst");
        var server = directory.resolve("server.jsonl.zst");
        write(client, AUTOCOMMIT_CLIENT.replace(
                "{\"event\":\"autocommit\",\"application\":\"a\",\"implicit_commits\":2}\n", "")
                .replace("{\"event\":\"close\",\"application\":\"a\"}",
                        "{\"event\":\"close\",\"application\":\"unknown\"}"));
        write(server, server(0, ""));
        assertThrows(IOException.class, () -> RequestCaptureAudit.audit(client, server, true));
    }

    @Test
    public void closedRequestsAreCheckedAndReleasedDuringLongSessions() throws Exception {
        for (boolean clientFirst : new boolean[] {true, false}) {
            var audit = new RequestCaptureAudit.State(false);
            audit.client(JsonParser.parseString("{\"event\":\"connection\",\"pid\":42}").getAsJsonObject());
            audit.server(JsonParser.parseString(HEADER).getAsJsonObject());
            for (int i = 1; i <= 2000; i++) {
                String app = "sibench:j1:1:" + i;
                String client = """
                        {"event":"request_start","application":"%s","request":%d,"sql":"SELECT 1","parameters":{}}
                        {"event":"request_return","application":"%s"}
                        {"event":"consumed_row","application":"%s","row_hash":"0000000000000001"}
                        {"event":"request_end","application":"%s"}
                        {"event":"commit","application":"%s"}
                        """.formatted(app, i, app, app, app, app);
                String server = """
                        {"event":"executor_start","pid":42,"application":"%s","sql":"SELECT 1"}
                        {"event":"executor_run_end","pid":42,"application":"%s","depth":1,"returned":1,"row_hashes":["0000000000000001"]}
                        {"event":"executor_end","pid":42,"application":"%s"}
                        {"event":"transaction_commit","pid":42,"application":"%s"}
                        """.formatted(app, app, app, app);
                if (clientFirst) {
                    for (String line : client.lines().toList()) audit.client(JsonParser.parseString(line).getAsJsonObject());
                    for (String line : server.lines().toList()) audit.server(JsonParser.parseString(line).getAsJsonObject());
                } else {
                    for (String line : server.lines().toList()) audit.server(JsonParser.parseString(line).getAsJsonObject());
                    for (String line : client.lines().toList()) audit.client(JsonParser.parseString(line).getAsJsonObject());
                }
            }
            audit.client(JsonParser.parseString("{\"event\":\"close\",\"application\":\"sibench:j1:1:2000\"}").getAsJsonObject());
            var report = audit.finish();
            assertEquals(2000L, report.get("requests"));
            assertEquals(2000L, report.get("client_consumed_rows_checked"));
            assertTrue(((Number) report.get("peak_pending_requests")).intValue() <= 2);
            assertEquals(report, audit.finish());
        }
    }

    private static Path write(Path file, String text) throws IOException {
        try (var out = new com.github.luben.zstd.ZstdOutputStream(Files.newOutputStream(file))) {
            out.write(text.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
        return file;
    }
}
