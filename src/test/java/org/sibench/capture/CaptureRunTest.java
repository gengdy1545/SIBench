package org.sibench.capture;

import static org.junit.Assert.*;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

public class CaptureRunTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void preflightMismatchKeepsEvidenceAndStartsNoNativeSession() throws Exception {
        Path directory = temporary.newFolder().toPath();
        try (CaptureRun run = CaptureRun.begin(directory)) {
            run.schemaPreflight(Map.of("status", "schema_mismatch",
                    "differences", java.util.List.of("missing index idx_transfer_3")));
            run.fail(new IOException("schema mismatch"), Map.of("operation", "native_run"));
        }
        JsonObject manifest = manifest(directory);
        assertEquals("failed", manifest.get("status").getAsString());
        assertEquals("schema_mismatch", manifest.getAsJsonObject("schema_preflight").get("status").getAsString());
        try (var paths = Files.list(directory)) {
            assertEquals(0, paths.filter(p -> p.getFileName().toString().startsWith("jdbc-")).count());
        }
    }

    @Test
    public void swallowedJdbcFailuresStillInvalidateRunAndKeepOriginalCause() throws Exception {
        for (String method :
                new String[] {
                    "prepareStatement", "executeQuery", "commit", "setAutoCommit", "next", "close"
                }) {
            Path directory = temporary.newFolder().toPath();
            SQLException original = new SQLException("original " + method, "23505", 7);
            boolean[] closed = {false};
            try (CaptureRun run = CaptureRun.begin(directory)) {
                Connection connection =
                        CaptureDriver.wrap(actual(method, original, closed), directory);
                try {
                    switch (method) {
                        case "prepareStatement" -> connection.prepareStatement("SELECT ?");
                        case "executeQuery" -> {
                            var statement = connection.prepareStatement("SELECT ?");
                            statement.setInt(1, 42);
                            statement.executeQuery();
                        }
                        case "next" ->
                                connection.prepareStatement("SELECT ?").executeQuery().next();
                        case "commit" -> connection.commit();
                        case "setAutoCommit" -> connection.setAutoCommit(false);
                        case "close" -> connection.close();
                        default -> fail();
                    }
                    fail("Expected JDBC error");
                } catch (SQLException error) {
                    assertSame(original, error);
                }
                try {
                    CaptureRun.checkFailure();
                    fail("Native code must not hide JDBC failure");
                } catch (SQLException error) {
                    assertSame(original, error);
                }
                JsonObject failure = manifest(directory).getAsJsonObject("failure");
                assertEquals("23505", failure.get("sqlstate").getAsString());
                assertEquals(7, failure.get("error_code").getAsInt());
                assertEquals(method, failure.get("operation").getAsString());
                if (method.equals("executeQuery"))
                    assertEquals(42, failure.getAsJsonObject("parameters").get("1").getAsInt());
                if (!method.equals("close")) connection.close();
            }
            assertTrue(closed[0]);
            assertEquals("failed", manifest(directory).get("status").getAsString());
            assertThrows(
                    IOException.class,
                    () -> CaptureRun.verifyInput(directory.resolve("pg-123.jsonl")));
            assertThrows(IOException.class, () -> CaptureRun.begin(directory));
        }
    }

    @Test
    public void completionRequiresAuditAndDetectsSubsequentFileChanges() throws Exception {
        Path directory = temporary.newFolder().toPath(), pg = directory.resolve("pg-123.jsonl");
        try (CaptureRun run = CaptureRun.begin(directory)) {
            try (var connection = CaptureDriver.wrap(actual("", null, new boolean[1]), directory)) {
                connection.prepareStatement("SELECT 1").executeQuery();
                connection.commit();
            }
            Path jdbc = jdbc(directory);
            String application =
                    Files.readAllLines(jdbc).stream()
                            .map(line -> JsonParser.parseString(line).getAsJsonObject())
                            .filter(e -> e.get("event").getAsString().equals("request_start"))
                            .findFirst()
                            .orElseThrow()
                            .get("application")
                            .getAsString();
            Files.writeString(
                    pg,
                    event("executor_start", application, ",\"sql\":\"SELECT 1\"")
                            + event("executor_run_end", application, "")
                            + event("executor_end", application, "")
                            + event("transaction_commit", application, ""));
            run.complete();
        }
        assertEquals("complete", manifest(directory).get("status").getAsString());
        CaptureRun.verifyInput(pg);
        Path jdbc = jdbc(directory);
        String original = Files.readString(jdbc);
        Files.writeString(jdbc, original + "{}\n");
        assertThrows(IOException.class, () -> CaptureRun.verifyInput(pg));
        Files.writeString(jdbc, original);
        Files.writeString(pg, Files.readString(pg) + "{}\n");
        assertThrows(IOException.class, () -> CaptureRun.verifyInput(pg));
    }

    @Test
    public void incompleteOrFailedPrefixCannotBeCompiledEvenBeforeBaselineCheck() throws Exception {
        Path directory = temporary.newFolder().toPath();
        try (CaptureRun run = CaptureRun.begin(directory)) {
            Path raw = directory.resolve("pg-123.jsonl");
            Files.writeString(raw, "{}\n");
            assertThrows(
                    IOException.class,
                    () -> IndexTraceCompiler.compile(null, null, raw, directory.resolve("output"), null));
            assertFalse(Files.exists(directory.resolve("output")));
            run.fail(new SQLException("broken", "08006"), Map.of("operation", "commit"));
            assertThrows(
                    IOException.class,
                    () -> IndexTraceCompiler.compile(null, null, raw, directory.resolve("output"), null));
        }
    }

    @Test
    public void missingServerOutputAndAbandonedRunNeverPublishCompletion() throws Exception {
        Path directory = temporary.newFolder().toPath();
        try (CaptureRun run = CaptureRun.begin(directory)) {
            try (var connection = CaptureDriver.wrap(actual("", null, new boolean[1]), directory)) {
                connection.prepareStatement("SELECT 1").executeQuery();
                connection.commit();
            }
            assertThrows(IOException.class, () -> run.complete());
        }
        assertEquals("failed", manifest(directory).get("status").getAsString());
    }

    @Test
    public void initializationFailureClosesConnectionAndPreservesOriginalException()
            throws Exception {
        Path directory = temporary.newFolder().toPath();
        SQLException original = new SQLException("cannot get pid", "08006");
        boolean[] closed = {false};
        try (CaptureRun run = CaptureRun.begin(directory)) {
            Connection actual =
                    proxy(
                            Connection.class,
                            (p, m, a) -> {
                                if (m.getName().equals("createStatement")) throw original;
                                if (m.getName().equals("close")) {
                                    closed[0] = true;
                                    throw new SQLException("secondary close error");
                                }
                                return null;
                            });
            SQLException caught =
                    assertThrows(SQLException.class, () -> CaptureDriver.wrap(actual, directory));
            assertSame(original, caught);
            assertEquals(1, caught.getSuppressed().length);
            assertTrue(closed[0]);
        }
    }

    @Test
    public void captureWithoutARunManifestIsRejected() throws Exception {
        Path directory = temporary.newFolder().toPath();
        Path pg = directory.resolve("pg-123.jsonl");
        Files.writeString(pg, event("transaction_commit", "fixture", ""));
        assertThrows(IOException.class, () -> CaptureRun.verifyInput(pg));
    }

    @Test
    public void htapAuditsAllSessionsAndAcceptsMatchedBusinessRollback() throws Exception {
        Path directory = temporary.newFolder().toPath();
        try (CaptureRun run = CaptureRun.begin(directory)) {
            run.phase("xp", null);
            for (int pid : new int[] {123, 124}) {
                Path jdbc = directory.resolve("jdbc-" + pid + ".jsonl");
                CaptureRun.session(jdbc, pid);
                Files.writeString(jdbc, """
                        {"event":"connection","pid":%d,"phase":"xp"}
                        {"event":"request_start","application":"p%d","sql":"SELECT 1","parameters":{}}
                        {"event":"request_return","application":"p%d"}
                        {"event":"rollback","application":"p%d"}
                        {"event":"close","application":"p%d"}
                        """.formatted(pid, pid, pid, pid, pid));
                Files.writeString(directory.resolve("pg-" + pid + ".jsonl"), """
                        {"event":"executor_start","pid":%d,"application":"p%d","sql":"SELECT 1"}
                        {"event":"executor_end","pid":%d,"application":"p%d"}
                        {"event":"executor_run_end","pid":%d,"application":"p%d"}
                        {"event":"transaction_abort","pid":%d,"application":"p%d"}
                        """.formatted(pid, pid, pid, pid, pid, pid, pid, pid));
                CaptureRun.sessionClosed(jdbc);
            }
            run.complete();
        }
        assertEquals(2, manifest(directory).getAsJsonArray("sessions").size());
        CaptureRun.verifyInput(directory);
        // Concurrent sessions are no longer rejected outright; they are ordered by PostgreSQL's
        // transaction completion counter, which these fixture traces do not carry.
        assertEquals("concurrent", IndexTraceCompiler.sessions(directory).order());
        assertEquals(2, IndexTraceCompiler.sessions(directory).files().size());
        Path extra = directory.resolve("pg-unregistered.jsonl");
        Files.writeString(extra, "{}\n");
        assertThrows(IOException.class, () -> CaptureRun.verifyInput(directory));
        Files.delete(extra);
        Path server = directory.resolve("pg-124.jsonl");
        Files.writeString(server, Files.readString(server) + "{}\n");
        assertThrows(IOException.class, () -> CaptureRun.verifyInput(directory));
    }

    @Test
    public void delayedResultConsumptionKeepsItsOriginalRequestIdentity() throws Exception {
        Path directory = temporary.newFolder().toPath();
        ResultSet result = proxy(ResultSet.class, (p, m, a) -> switch (m.getName()) {
            case "next" -> true;
            case "getMetaData" -> proxy(java.sql.ResultSetMetaData.class,
                    (q, n, b) -> n.getName().equals("getColumnCount") ? 1 : java.sql.Types.INTEGER);
            case "getString" -> "42";
            default -> null;
        });
        Connection actual = actual("", null, new boolean[1]);
        Connection delayed = proxy(Connection.class, (p, m, a) -> {
            if (m.getName().equals("prepareStatement"))
                return proxy(PreparedStatement.class, (q, n, b) -> switch (n.getName()) {
                    case "executeQuery", "getResultSet" -> result;
                    case "executeUpdate", "getUpdateCount" -> 0;
                    default -> null;
                });
            return m.invoke(actual, a);
        });
        try (CaptureRun run = CaptureRun.begin(directory);
                Connection connection = CaptureDriver.wrap(delayed, directory)) {
            PreparedStatement first = connection.prepareStatement("SELECT 42");
            ResultSet rows = first.executeQuery();
            connection.prepareStatement("UPDATE savingaccount SET balance = 1").executeUpdate();
            rows.next();
            first.getResultSet().next();
        }
        var events = Files.readAllLines(jdbc(directory)).stream()
                .map(line -> JsonParser.parseString(line).getAsJsonObject()).toList();
        String application = events.stream()
                .filter(e -> e.get("event").getAsString().equals("request_start"))
                .findFirst().orElseThrow().get("application").getAsString();
        var consumed = events.stream()
                .filter(e -> e.get("event").getAsString().equals("consumed_row")).toList();
        assertEquals(2, consumed.size());
        for (var row : consumed) {
            assertEquals(application, row.get("application").getAsString());
            assertEquals(1, row.get("request").getAsInt());
            assertEquals("42", row.get("1").getAsString());
        }
    }

    @Test
    public void ordinaryStatementsUseSameRequestCaptureAsPreparedStatements() throws Exception {
        Path directory = temporary.newFolder().toPath();
        try (CaptureRun run = CaptureRun.begin(directory);
                Connection connection =
                        CaptureDriver.wrap(actual("", null, new boolean[1]), directory)) {
            connection.createStatement().executeQuery("SELECT 1");
        }
        var request = Files.readAllLines(jdbc(directory)).stream()
                .map(line -> JsonParser.parseString(line).getAsJsonObject())
                .filter(e -> e.get("event").getAsString().equals("request_start"))
                .findFirst().orElseThrow();
        assertEquals("SELECT 1", request.get("sql").getAsString());
        assertTrue(request.getAsJsonObject("parameters").isEmpty());
    }

    private static Connection actual(String failed, SQLException error, boolean[] closed) {
        ResultSet pid =
                proxy(
                        ResultSet.class,
                        (p, m, a) ->
                                switch (m.getName()) {
                                    case "next" -> true;
                                    case "getInt" -> 123;
                                    default -> null;
                                });
        ResultSet result =
                proxy(
                        ResultSet.class,
                        (p, m, a) -> {
                            if (failed.equals(m.getName())) throw error;
                            return m.getName().equals("next") ? false : null;
                        });
        Statement label =
                proxy(
                        Statement.class,
                        (p, m, a) -> {
                            if (failed.equals("execute") && m.getName().equals("execute"))
                                throw error;
                            if (m.getName().equals("getUpdateCount")) return -1;
                            return m.getName().equals("executeQuery")
                                    ? pid
                                    : m.getName().equals("execute") ? true : null;
                        });
        PreparedStatement statement =
                proxy(
                        PreparedStatement.class,
                        (p, m, a) -> {
                            if (failed.equals(m.getName())
                                    || failed.equals("statement_close")
                                            && m.getName().equals("close")) throw error;
                            return switch (m.getName()) {
                                case "executeQuery" -> result;
                                case "getUpdateCount" -> -1;
                                default -> null;
                            };
                        });
        return proxy(
                Connection.class,
                (p, m, a) -> {
                    if (m.getName().equals("close")) closed[0] = true;
                    if (failed.equals(m.getName())) throw error;
                    return switch (m.getName()) {
                        case "createStatement" -> label;
                        case "prepareStatement" -> statement;
                        // HyBench-style explicit transactions unless a test says otherwise.
                        case "getAutoCommit" -> false;
                        default -> null;
                    };
                });
    }

    @Test
    public void autocommitConnectionsRecordTheirImplicitCommits() throws Exception {
        for (boolean autocommit : new boolean[] {false, true}) {
            Path directory = temporary.newFolder().toPath();
            Connection base = actual("", null, new boolean[1]);
            Connection connection =
                    proxy(
                            Connection.class,
                            (p, m, a) ->
                                    m.getName().equals("getAutoCommit")
                                            ? (Object) autocommit
                                            : m.invoke(base, a));
            try (CaptureRun run = CaptureRun.begin(directory);
                    Connection wrapped = CaptureDriver.wrap(connection, directory)) {
                wrapped.prepareStatement("SELECT 1").executeQuery();
            }
            var events =
                    Files.readAllLines(jdbc(directory)).stream()
                            .map(line -> JsonParser.parseString(line).getAsJsonObject())
                            .toList();
            var start =
                    events.stream()
                            .filter(e -> e.get("event").getAsString().equals("request_start"))
                            .findFirst()
                            .orElseThrow();
            var implicit =
                    events.stream()
                            .filter(e -> e.get("event").getAsString().equals("autocommit"))
                            .toList();
            assertEquals(autocommit, start.has("autocommit"));
            assertEquals(autocommit ? 1 : 0, implicit.size());
            if (autocommit) {
                // The label statement and the request itself each commit on the server.
                assertEquals(2, implicit.get(0).get("implicit_commits").getAsInt());
                assertEquals(
                        start.get("application").getAsString(),
                        implicit.get(0).get("application").getAsString());
            }
        }
    }

    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return type.cast(
                Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, handler));
    }

    private static Path jdbc(Path directory) throws IOException {
        try (var paths = Files.list(directory)) {
            return paths.filter(p -> p.getFileName().toString().startsWith("jdbc-"))
                    .findFirst()
                    .orElseThrow();
        }
    }

    private static JsonObject manifest(Path directory) throws IOException {
        return JsonParser.parseString(Files.readString(directory.resolve("capture-run.json")))
                .getAsJsonObject();
    }

    private static String event(String type, String application, String details) {
        return "{\"event\":\""
                + type
                + "\",\"pid\":123,\"application\":\""
                + application
                + "\""
                + details
                + "}\n";
    }
}
