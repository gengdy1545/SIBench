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

    private static final class Session {
        private final Connection connection;
        private final long id = CONNECTIONS.incrementAndGet();
        private final BufferedWriter output;
        private final Gson json = new GsonBuilder().serializeNulls().create();
        private long request, transaction = 1;
        private String application;
        private final String phase = CaptureRun.phase();
        private final Path path;

        Session(Connection connection, Path directory) throws IOException, SQLException {
            this.connection = connection;
            path =
                    directory.resolve(
                            "jdbc-" + ProcessHandle.current().pid() + "-" + id + ".jsonl");
            output = Files.newBufferedWriter(path, StandardOpenOption.CREATE_NEW);
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
                output.flush();
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
                                        if (failure == null) emit("close", Map.of());
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

        Statement statement(Statement statement, String sql) {
            Map<Integer, Object> parameters = new LinkedHashMap<>();
            Map<String, Object> resultIdentity = new LinkedHashMap<>();
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
                                        mark(executedSql, parameters);
                                        resultIdentity.clear();
                                        resultIdentity.put("application", application);
                                        resultIdentity.put("request", request);
                                        resultIdentity.put("transaction", transaction);
                                        Object result = invoke(method, statement, args);
                                        emit(
                                                "request_return",
                                                Map.of(
                                                        "end_ns",
                                                        System.nanoTime(),
                                                        "update_count",
                                                        statement.getUpdateCount()));
                                        if (result instanceof ResultSet rows)
                                            return rows(rows, executedSql, resultIdentity);
                                        return result;
                                    }
                                    if (name.equals("getResultSet")) {
                                        Object result = invoke(method, statement, args);
                                        return result instanceof ResultSet rows
                                                ? rows(rows, sql, resultIdentity) : result;
                                    }
                                    return invoke(method, statement, args);
                                } catch (SQLException error) {
                                    Map<String, Object> details = context(name, executedSql);
                                    details.put("parameters", new LinkedHashMap<>(parameters));
                                    CaptureRun.recordFailure(error, details);
                                    error(error, "request_error", name, executedSql);
                                    throw error;
                                }
                            });
        }

        ResultSet rows(ResultSet rows, String sql, Map<String, Object> resultIdentity) {
            Map<String, Object> identity = new LinkedHashMap<>(resultIdentity);
            return (ResultSet)
                    Proxy.newProxyInstance(
                            CaptureDriver.class.getClassLoader(),
                            new Class<?>[] {ResultSet.class},
                            (proxy, method, args) -> {
                                try {
                                    Object result = invoke(method, rows, args);
                                    if (method.getName().equals("next")
                                            && Boolean.TRUE.equals(result)) {
                                        Map<String, Object> values = new LinkedHashMap<>(identity);
                                        Map<String, Integer> types = new LinkedHashMap<>();
                                        ResultSetMetaData metadata = rows.getMetaData();
                                        for (int i = 1;
                                                i <= metadata.getColumnCount();
                                                i++) {
                                            values.put(Integer.toString(i), rows.getString(i));
                                            types.put(Integer.toString(i), metadata.getColumnType(i));
                                        }
                                        values.put("column_types", types);
                                        emit("consumed_row", values);
                                    }
                                    return result;
                                } catch (SQLException error) {
                                    error(error, "result_error", method.getName(), sql);
                                    throw error;
                                }
                            });
        }
    }
}
