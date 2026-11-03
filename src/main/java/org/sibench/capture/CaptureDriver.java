package org.sibench.capture;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.BufferedWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.*;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/** JDBC decoration of the actual native execution; no SQL is replayed for observation. */
public final class CaptureDriver implements Driver {
    private static final AtomicLong CONNECTIONS = new AtomicLong();
    private final org.postgresql.Driver postgres = new org.postgresql.Driver();

    static {
        try {
            DriverManager.registerDriver(new CaptureDriver());
        } catch (SQLException error) {
            throw new ExceptionInInitializerError(error);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) return null;
        Connection actual;
        try {
            actual = postgres.connect(url.replace("jdbc:sibench:", "jdbc:postgresql:"), info);
        } catch (SQLException error) {
            CaptureRun.recordFailure(error, Map.of("operation", "connect"));
            throw error;
        }
        return wrap(actual, Path.of(System.getProperty("sibench.capture.directory")));
    }

    static Connection wrap(Connection actual, Path directory) throws SQLException {
        try {
            return new Session(actual, directory).wrap();
        } catch (IOException | SQLException error) {
            CaptureRun.recordFailure(error, Map.of("operation", "open_capture"));
            try {
                actual.close();
            } catch (SQLException close) {
                error.addSuppressed(close);
            }
            if (error instanceof SQLException sql) throw sql;
            throw new SQLException("Cannot open capture", error);
        }
    }

    @Override
    public boolean acceptsURL(String url) {
        return url != null && url.startsWith("jdbc:sibench:");
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String u, Properties p) {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() {
        return Logger.getLogger("org.sibench.capture");
    }

    private static Object invoke(Method method, Object target, Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException error) {
            throw error.getCause();
        }
    }

    /** The zstd level of the client traces. */
    static final int ZSTD_LEVEL = 3;

    /** Native phase cancellation must not close the channel halfway through a source fact. */
    static java.io.OutputStream directOutput(java.nio.channels.SocketChannel channel) throws IOException {
        channel.configureBlocking(false);
        return new java.io.OutputStream() {
            @Override public void write(int value) throws IOException { write(new byte[] {(byte) value}); }
            @Override public void write(byte[] bytes, int offset, int length) throws IOException {
                boolean interrupted = Thread.interrupted();
                try {
                    var buffer = java.nio.ByteBuffer.wrap(bytes, offset, length);
                    while (buffer.hasRemaining()) {
                        if (channel.write(buffer) == 0) {
                            interrupted |= Thread.interrupted();
                            java.util.concurrent.locks.LockSupport.parkNanos(1_000_000);
                        }
                    }
                } finally {
                    if (interrupted) Thread.currentThread().interrupt();
                }
            }
            @Override public void close() throws IOException { channel.close(); }
        };
    }

    private static final class Session {
        private final Connection connection;
        private final long id = CONNECTIONS.incrementAndGet();
        private final BufferedWriter output;
        private final boolean direct = System.getenv("SIBENCH_CAPTURE_SOCKET") != null;
        private final Gson json = new GsonBuilder().serializeNulls().create();
        private long request, transaction = 1;
        private String application;
        private final String phase = CaptureRun.phase();
        private final Path path;
        private final java.util.Set<ResultCapture> openResults = new java.util.LinkedHashSet<>();
        Session(Connection connection, Path directory) throws IOException, SQLException {
            this.connection = connection;
            path =
                    directory.resolve(
                            "jdbc-" + ProcessHandle.current().pid() + "-" + id + ".jsonl.zst");
            if (direct) {
                var channel = java.nio.channels.SocketChannel.open(java.net.StandardProtocolFamily.UNIX);
                channel.connect(java.net.UnixDomainSocketAddress.of(System.getenv("SIBENCH_CAPTURE_SOCKET")));
                output = new BufferedWriter(new java.io.OutputStreamWriter(
                        directOutput(channel), java.nio.charset.StandardCharsets.UTF_8), 1 << 16);
                output.write(path.getFileName().toString());
                output.newLine();
            } else {
            output =
                    new BufferedWriter(
                            new java.io.OutputStreamWriter(
                                    new com.github.luben.zstd.ZstdOutputStream(
                                            new java.io.BufferedOutputStream(
                                                    Files.newOutputStream(path, StandardOpenOption.CREATE_NEW), 1 << 16),
                                            ZSTD_LEVEL),
                                    java.nio.charset.StandardCharsets.UTF_8),
                            1 << 16);
            }
            try (Statement s = connection.createStatement();
                    ResultSet r = s.executeQuery("SELECT pg_backend_pid()")) {
                if (!r.next()) throw new SQLException("Missing backend PID");
                int pid = r.getInt(1);
                CaptureRun.session(path, pid);
                emit("connection", Map.of("pid", pid));
            } catch (IOException | SQLException error) {
                try {
                    output.close();
                } catch (IOException close) {
                    error.addSuppressed(close);
                }
                throw error;
            }
        }

        private Map<String, Object> context(String operation, String sql) {
            Map<String, Object> details = new LinkedHashMap<>();
            details.put("operation", operation);
            details.put("connection", id);
            details.put("transaction", transaction);
            details.put("request", request);
            details.put("application", application);
            details.put("sql", sql);
            return details;
        }

        private void error(SQLException error, String event, String operation, String sql) {
            Map<String, Object> details = context(operation, sql);
            CaptureRun.recordFailure(error, details);
            details.put("sqlstate", error.getSQLState());
            details.put("error_code", error.getErrorCode());
            details.put("message", error.getMessage());
            try {
                emit(event, details);
            } catch (SQLException logging) {
                error.addSuppressed(logging);
            }
        }

        private void emit(String event, Map<String, ?> details) throws SQLException {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("event", event);
            entry.put("connection", id);
            entry.put("transaction", transaction);
            entry.put("request", request);
            entry.put("application", application);
            if (phase != null) entry.put("phase", phase);
            entry.putAll(details);
            try {
                output.write(json.toJson(entry));
                output.newLine();
                if (direct) output.flush();
            } catch (IOException e) {
                SQLException error = new SQLException("Trace write failure", e);
                CaptureRun.recordFailure(error, context("write_trace", null));
                throw error;
            }
        }

        private void mark(String sql, Map<Integer, Object> parameters) throws SQLException {
            request++;
            application = "sibench:j" + ProcessHandle.current().pid() + ":" + id + ":" + request;
            // Session label only; never re-execute or rewrite the benchmark statement.
            try (Statement label = connection.createStatement()) {
                label.execute("SET application_name = '" + application + "'");
            }
            Map<String, Object> details = new LinkedHashMap<>();
            details.put("sql", sql);
            details.put("parameters", new LinkedHashMap<>(parameters));
            details.put("start_ns", System.nanoTime());
            // An autocommit connection commits the label and then the request itself.
            if (connection.getAutoCommit()) details.put("autocommit", true);
            emit("request_start", details);
        }

        Connection wrap() {
            return (Connection)
                    Proxy.newProxyInstance(
                            CaptureDriver.class.getClassLoader(),
                            new Class<?>[] {Connection.class},
                            (proxy, method, args) -> {
                                String name = method.getName();
                                if (name.equals("close")) {
                                    SQLException failure = null;
                                    try {
                                        invoke(method, connection, args);
                                    } catch (SQLException error) {
                                        failure = error;
                                        error(error, "connection_error", name, null);
                                    }
                                    try {
                                        if (failure == null) {
                                            for (var capture : java.util.List.copyOf(openResults)) capture.end();
                                            emit("close", Map.of());
                                            if (direct) emit("capture_end", Map.of());
                                        }
                                    } catch (SQLException logging) {
                                        failure = logging;
                                    } finally {
                                        try {
                                            output.close();
                                            CaptureRun.sessionClosed(path);
                                        } catch (IOException close) {
                                            if (failure == null)
                                                failure =
                                                        new SQLException(
                                                                "Cannot close capture", close);
                                            else failure.addSuppressed(close);
                                        }
                                    }
                                    if (failure != null) {
                                        CaptureRun.recordFailure(failure, context(name, null));
                                        throw failure;
                                    }
                                    return null;
                                }
                                try {
                                    if (name.equals("prepareStatement")) {
                                        PreparedStatement statement =
                                                (PreparedStatement)
                                                        invoke(method, connection, args);
                                        return statement(statement, (String) args[0]);
                                    }
                                    if (name.equals("createStatement"))
                                        return statement((Statement) invoke(method, connection, args), null);
                                    if (name.equals("commit") || name.equals("rollback")) {
                                        Object result = invoke(method, connection, args);
                                        emit(name, Map.of());
                                        transaction++;
                                        return result;
                                    }
                                    return invoke(method, connection, args);
                                } catch (SQLException error) {
                                    error(
                                            error,
                                            name.equals("commit") || name.equals("rollback")
                                                    ? "transaction_error"
                                                    : "connection_error",
                                            name,
                                            name.equals("prepareStatement")
                                                    ? (String) args[0]
                                                    : null);
                                    throw error;
                                }
                            });
        }

        private final class ResultCapture {
            final Map<String, Object> identity;
            boolean ended, exhausted;
            long consumed;

            ResultCapture() {
                identity = Map.of("application", application, "request", request, "transaction", transaction);
                openResults.add(this);
            }

            void end() throws SQLException {
                if (ended) return;
                var counts = new LinkedHashMap<String, Object>(identity);
                counts.put("consumed_rows", consumed);
                counts.put("exhausted", exhausted);
                emit("request_end", counts);
                ended = true;
                openResults.remove(this);
            }
        }

        Statement statement(Statement statement, String sql) {
            Map<Integer, Object> parameters = new LinkedHashMap<>();
            ResultCapture[] resultCapture = {null};
            return (Statement)
                    Proxy.newProxyInstance(
                            CaptureDriver.class.getClassLoader(),
                            new Class<?>[] {sql == null ? Statement.class : PreparedStatement.class},
                            (proxy, method, args) -> {
                                String name = method.getName();
                                String executedSql = sql == null && args != null && args.length > 0
                                        && args[0] instanceof String text ? text : sql;
                                try {
                                    if (name.startsWith("set")
                                            && args != null
                                            && args.length >= 2
                                            && args[0] instanceof Integer index) {
                                        parameters.put(
                                                index, name.equals("setNull") ? null : args[1]);
                                    }
                                    if (name.equals("clearParameters")) parameters.clear();
                                    if (name.equals("executeQuery")
                                            || name.equals("executeUpdate")
                                            || name.equals("execute")) {
                                        if (resultCapture[0] != null) resultCapture[0].end();
                                        mark(executedSql, parameters);
                                        resultCapture[0] = new ResultCapture();
                                        Object result = invoke(method, statement, args);
                                        emit(
                                                "request_return",
                                                Map.of(
                                                        "end_ns",
                                                        System.nanoTime(),
                                                        "update_count",
                                                        statement.getUpdateCount()));
                                        // The server committed the label statement and the request.
                                        if (connection.getAutoCommit())
                                            emit("autocommit", Map.of("implicit_commits", 2));
                                        if (result instanceof ResultSet rows)
                                            return rows(rows, executedSql, resultCapture[0]);
                                        if (!Boolean.TRUE.equals(result)) resultCapture[0].end();
                                        return result;
                                    }
                                    if (name.equals("getResultSet")) {
                                        Object result = invoke(method, statement, args);
                                        return result instanceof ResultSet rows
                                                ? rows(rows, sql, resultCapture[0]) : result;
                                    }
                                    Object result = invoke(method, statement, args);
                                    if (name.equals("close") && resultCapture[0] != null) resultCapture[0].end();
                                    return result;
                                } catch (SQLException error) {
                                    Map<String, Object> details = context(name, executedSql);
                                    details.put("parameters", new LinkedHashMap<>(parameters));
                                    CaptureRun.recordFailure(error, details);
                                    error(error, "request_error", name, executedSql);
                                    throw error;
                                }
                            });
        }

        ResultSet rows(ResultSet rows, String sql, ResultCapture capture) {
            return (ResultSet)
                    Proxy.newProxyInstance(
                            CaptureDriver.class.getClassLoader(),
                            new Class<?>[] {ResultSet.class},
                            (proxy, method, args) -> {
                                try {
                                    Object result = invoke(method, rows, args);
                                    if (method.getName().equals("next")) {
                                        if (Boolean.TRUE.equals(result)) capture.consumed++;
                                        else {
                                            capture.exhausted = true;
                                            capture.end();
                                        }
                                    }
                                    if (method.getName().equals("close")) capture.end();
                                    return result;
                                } catch (SQLException error) {
                                    error(error, "result_error", method.getName(), sql);
                                    throw error;
                                }
                            });
        }
    }
}
