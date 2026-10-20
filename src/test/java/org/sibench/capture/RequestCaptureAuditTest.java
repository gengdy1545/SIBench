package org.sibench.capture;

import static org.junit.Assert.*;

import com.google.gson.JsonPrimitive;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
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

    @Test
    public void rejectsAbortWithWrongRollbackIdentityEvenWhenCountsMatch() throws Exception {
        var client = Files.createTempFile("sibench-request-client-", ".jsonl");
        var server = Files.createTempFile("sibench-request-server-", ".jsonl");
        try {
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
            Files.writeString(
                    server,
                    """
                    {"event":"executor_start","pid":42,"application":"a","sql":"SELECT 1"}
                    {"event":"executor_run_end","pid":42,"application":"a"}
                    {"event":"executor_start","pid":42,"application":"b","sql":"SELECT 2"}
                    {"event":"executor_run_end","pid":42,"application":"b"}
                    {"event":"transaction_abort","pid":42,"application":"b"}
                    {"event":"transaction_abort","pid":42,"application":"b"}
                    """);
            assertThrows(
                    java.io.IOException.class,
                    () -> RequestCaptureAudit.audit(client, server, true));
            Files.writeString(
                    server,
                    Files.readString(server)
                            .replaceFirst(
                                    "\"event\":\"transaction_abort\",\"pid\":42,\"application\":\"b\"",
                                    "\"event\":\"transaction_abort\",\"pid\":42,\"application\":\"a\""));
            assertEquals("passed", RequestCaptureAudit.audit(client, server, true).get("status"));
        } finally {
            Files.delete(client);
            Files.delete(server);
        }
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
                {"event":"executor_end","pid":42,"application":"a"}
                {"event":"transaction_abort","pid":42,"application":"a"}
                {"event":"executor_start","pid":42,"application":"b","sql":"SELECT 2"}
                {"event":"executor_end","pid":42,"application":"b"}
                """;
        Files.writeString(
                server,
                pg + "{\"event\":\"transaction_abort\",\"pid\":42,\"application\":\"b\"}\n");
        var audit = RequestCaptureAudit.audit(client, server);
        assertEquals("passed", audit.get("status"));
        assertEquals(1L, audit.get("explicit_rollbacks"));
        assertEquals(1L, audit.get("close_aborts"));
        Files.writeString(server, pg);
        assertThrows(java.io.IOException.class, () -> RequestCaptureAudit.audit(client, server));
        Files.writeString(
                server,
                pg.replace(
                                "\"event\":\"transaction_abort\",\"pid\":42,\"application\":\"a\"",
                                "\"event\":\"transaction_abort\",\"pid\":42,\"application\":\"b\"")
                        + "{\"event\":\"transaction_abort\",\"pid\":42,\"application\":\"b\"}\n");
        assertThrows(java.io.IOException.class, () -> RequestCaptureAudit.audit(client, server));
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
        assertThrows(java.io.IOException.class, () -> RequestCaptureAudit.audit(client, server));
    }
}
