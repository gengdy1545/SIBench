package org.sibench.capture;

import static org.junit.Assert.*;

import com.google.gson.JsonPrimitive;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.util.Map;
import java.sql.Types;

public class RequestCaptureAuditTest {
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
    public void acceptsMatchedRollbackAndCloseAbortInSingleSessionCapture() throws Exception {
        var directory = temporary.newFolder().toPath();
        var client = directory.resolve("client.jsonl");
        var server = directory.resolve("server.jsonl");
        Files.writeString(
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
        String pg =
                """
                {"event":"executor_start","pid":42,"application":"a","sql":"SELECT 1"}
                {"event":"executor_run_end","pid":42,"application":"a"}
                {"event":"executor_end","pid":42,"application":"a"}
                {"event":"transaction_abort","pid":42,"application":"a"}
                {"event":"executor_start","pid":42,"application":"b","sql":"SELECT 2"}
                {"event":"executor_run_end","pid":42,"application":"b"}
                {"event":"executor_end","pid":42,"application":"b"}
                """;
        Files.writeString(
                server,
                pg + "{\"event\":\"transaction_abort\",\"pid\":42,\"application\":\"b\"}\n");
        var audit = RequestCaptureAudit.audit(client, server, true);
        assertEquals("passed", audit.get("status"));
        assertEquals(1L, audit.get("explicit_rollbacks"));
        assertEquals(1L, audit.get("close_aborts"));
        Files.writeString(server, pg);
        assertThrows(java.io.IOException.class, () -> RequestCaptureAudit.audit(client, server, true));
        Files.writeString(
                server,
                pg.replace(
                                "\"event\":\"transaction_abort\",\"pid\":42,\"application\":\"a\"",
                                "\"event\":\"transaction_abort\",\"pid\":42,\"application\":\"b\"")
                        + "{\"event\":\"transaction_abort\",\"pid\":42,\"application\":\"b\"}\n");
        assertThrows(java.io.IOException.class, () -> RequestCaptureAudit.audit(client, server, true));
    }

    @Test
    public void rejectsRollbackWithoutCompletedRequest() throws Exception {
        var directory = temporary.newFolder().toPath();
        var client = directory.resolve("client.jsonl");
        var server = directory.resolve("server.jsonl");
        Files.writeString(
                client,
                """
                {"event":"connection","pid":42}
                {"event":"request_start","application":"a","sql":"SELECT 1","parameters":{}}
                {"event":"rollback","application":"a"}
                {"event":"request_return","application":"a"}
                {"event":"close","application":"a"}
                """);
        Files.writeString(server, "");
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

    private static String server(int commits, String scan) {
        return "{\"event\":\"executor_start\",\"pid\":42,\"application\":\"a\",\"sql\":\"SELECT c FROM t WHERE id = $1\"}\n"
                + scan
                + "{\"event\":\"executor_run_end\",\"pid\":42,\"application\":\"a\"}\n"
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
        var client = directory.resolve("client.jsonl");
        var server = directory.resolve("server.jsonl");
        Files.writeString(client, AUTOCOMMIT_CLIENT);
        Files.writeString(server, server(2, ""));
        assertEquals("passed", RequestCaptureAudit.audit(client, server, true).get("status"));
        // One commit fewer, or one more, is an event-loss signature.
        Files.writeString(server, server(1, ""));
        assertThrows(java.io.IOException.class, () -> RequestCaptureAudit.audit(client, server, true));
        Files.writeString(server, server(3, ""));
        assertThrows(java.io.IOException.class, () -> RequestCaptureAudit.audit(client, server, true));
    }

    @Test
    public void pointAccessBindingsAreCheckedOnlyWhenRequested() throws Exception {
        var directory = temporary.newFolder().toPath();
        var client = directory.resolve("client.jsonl");
        var server = directory.resolve("server.jsonl");
        Files.writeString(client, AUTOCOMMIT_CLIENT);
        // Key 7 was never bound by the client: a join-derived key, or a capture error.
        Files.writeString(server, server(2, scan("3", "0", "\"7\"")));
        assertThrows(
                java.io.IOException.class,
                () -> RequestCaptureAudit.audit(client, server, true));
        assertEquals(
                "passed", RequestCaptureAudit.audit(client, server, false).get("status"));
        Files.writeString(server, server(2, scan("3", "0", "\"5\"")));
        assertEquals("passed", RequestCaptureAudit.audit(client, server, true).get("status"));
        // Planner endpoint probes (IS NOT NULL, no bound value) are not search predicates.
        Files.writeString(server, server(2, scan("0", "129", "null")));
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
        var client = directory.resolve("client.jsonl");
        var server = directory.resolve("server.jsonl");
        Files.writeString(client, COMMIT_AFTER_ROLLBACK_CLIENT);
        Files.writeString(server, server(0, "") + abortEvent());
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
        var client = directory.resolve("client.jsonl");
        var server = directory.resolve("server.jsonl");
        Files.writeString(client, COMMIT_AFTER_ROLLBACK_CLIENT);
        Files.writeString(
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
}
