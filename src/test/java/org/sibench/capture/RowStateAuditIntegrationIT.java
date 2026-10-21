package org.sibench.capture;

import static org.junit.Assert.*;

import com.google.gson.*;

import org.junit.Test;
import org.sibench.config.ConfigValues;
import org.sibench.generation.BaselineGenerator;
import org.sibench.schema.SourceSchema;

import java.io.IOException;
import java.nio.file.*;
import java.sql.*;
import java.util.*;

/** Opt-in tests against the instrumented PG service; only disposable databases are mutated. */
public class RowStateAuditIntegrationIT {
    private final Path root = Path.of("").toAbsolutePath();
    private final String user = System.getenv().getOrDefault("SIBENCH_ROW_TEST_USER", "postgres");
    private final String password = System.getenv().getOrDefault("PGPASSWORD", "");
    private final Gson json = new GsonBuilder().setPrettyPrinting().create();

    @Test
    public void actualCapturedChangesAndPrimaryKeyRejection() throws Exception {
        String admin = System.getenv("SIBENCH_ROW_TEST_JDBC");
        String capture = System.getenv("SIBENCH_ROW_CAPTURE_DIR");
        assertNotNull("Set SIBENCH_ROW_TEST_JDBC explicitly", admin);
        assertNotNull("Set SIBENCH_ROW_CAPTURE_DIR to the server capture directory", capture);
        Path evidence =
                Path.of(
                        System.getProperty(
                                "sibench.row.evidence", "build/sibench/row-audit-integration"));
        Files.createDirectory(evidence);
        var checks = new ArrayList<Map<String, Object>>();
        for (String benchmark : List.of("hybench", "ycsb", "sysbench")) {
            var document =
                    ConfigValues.parse(
                            Files.readString(
                                    root.resolve(
                                            "config/"
                                                    + (benchmark.equals("hybench")
                                                            ? "hybench_sf1"
                                                            : benchmark + "_large")
                                                    + ".yaml")));
            var nativeConfig = ConfigValues.object(document, "native");
            if (benchmark.equals("sysbench")) nativeConfig.put("tables", 2);
            document.put("native", nativeConfig);
            var schema = SourceSchema.read(root, document);
            Path directory = Files.createDirectory(evidence.resolve(benchmark));
            try (var database = new Database(admin, benchmark)) {
                database.install(benchmark, schema);
                var initial = new LinkedHashMap<String, List<JsonObject>>();
                for (var table : schema.tables()) {
                    var row = initialRow(table, benchmark);
                    initial.put(table.name(), List.of(row));
                    database.insert(table, row);
                }
                Path baseline = RowAuditTestSupport.baseline(directory, schema, initial);
                PgBaselineVerify.verify(database.jdbc, user, schema, baseline);
                Path raw =
                        record(
                                database,
                                directory.resolve("accepted"),
                                Path.of(capture),
                                c -> {
                                    for (var table : schema.tables()) {
                                        var pk = SourceRow.primary(table).name();
                                        var column =
                                                table.columns().stream()
                                                        .filter(v -> !v.name().equals(pk))
                                                        .findFirst()
                                                        .orElseThrow();
                                        String value =
                                                column.type().equals("integer")
                                                        ? "21"
                                                        : "'updated '";
                                        execute(
                                                c,
                                                "UPDATE "
                                                        + table.name()
                                                        + " SET "
                                                        + column.name()
                                                        + "="
                                                        + value);
                                    }
                                    c.commit();
                                    var table = schema.tables().get(0);
                                    String pk = SourceRow.primary(table).name();
                                    Object old = benchmark.equals("ycsb") ? "user020 " : 20;
                                    execute(
                                            c,
                                            "DELETE FROM " + table.name() + " WHERE " + pk + "=?",
                                            old);
                                    insert(c, table, initial.get(table.name()).get(0));
                                    for (String key :
                                            benchmark.equals("ycsb")
                                                    ? List.of("user010", "user015")
                                                    : List.of("-30", "0")) {
                                        var added = initial.get(table.name()).get(0).deepCopy();
                                        added.addProperty(pk, key);
                                        insert(c, table, added);
                                    }
                                    c.commit();
                                    var column =
                                            table.columns().stream()
                                                    .filter(v -> !v.name().equals(pk))
                                                    .findFirst()
                                                    .orElseThrow();
                                    execute(
                                            c,
                                            "UPDATE "
                                                    + table.name()
                                                    + " SET "
                                                    + column.name()
                                                    + "="
                                                    + (column.type().equals("integer")
                                                            ? "99"
                                                            : "'aborted'")
                                                    + " WHERE "
                                                    + pk
                                                    + "=?",
                                            old);
                                    c.rollback();
                                });
                Path report = directory.resolve("audit.json");
                var result =
                        RowStateAudit.audit(database.jdbc, user, schema, baseline, raw, report);
                assertEquals("passed_source_row_state", result.get("status"));
                assertEquals(
                        (long) schema.tables().size() + 4, result.get("committed_row_changes"));
                assertEquals(1L, result.get("discarded_aborted_row_changes"));
                RowStateAudit.audit(
                        "jdbc:postgresql://127.0.0.1:1/unreachable",
                        user,
                        schema,
                        baseline,
                        raw,
                        report);
                checks.add(
                        Map.of(
                                "benchmark",
                                benchmark,
                                "check",
                                "baseline_capture_final_state_and_offline_reuse",
                                "status",
                                "passed"));
                for (boolean commit : List.of(false, true)) {
                    Path rejected =
                            record(
                                    database,
                                    directory.resolve(commit ? "pk-commit" : "pk-rollback"),
                                    Path.of(capture),
                                    c -> {
                                        var table = schema.tables().get(0);
                                        String pk = SourceRow.primary(table).name();
                                        execute(
                                                c,
                                                "UPDATE "
                                                        + table.name()
                                                        + " SET "
                                                        + pk
                                                        + "=? WHERE "
                                                        + pk
                                                        + "=?",
                                                benchmark.equals("ycsb") ? "user030 " : 30,
                                                benchmark.equals("ycsb") ? "user020 " : 20);
                                        if (commit) c.commit();
                                        else c.rollback();
                                    });
                    Path rejectedReport = rejected.getParent().resolve("audit.json");
                    IOException error =
                            assertThrows(
                                    IOException.class,
                                    () ->
                                            RowStateAudit.audit(
                                                    database.jdbc,
                                                    user,
                                                    schema,
                                                    baseline,
                                                    rejected,
                                                    rejectedReport));
                    assertTrue(
                            error.getMessage(),
                            error.getMessage().contains("Primary key value UPDATE"));
                    assertFalse(Files.exists(rejectedReport));
                    Files.writeString(
                            rejected.getParent().resolve("rejection.json"),
                            json.toJson(Map.of("status", "rejected", "reason", error.getMessage())),
                            StandardOpenOption.CREATE_NEW);
                    checks.add(
                            Map.of(
                                    "benchmark",
                                    benchmark,
                                    "check",
                                    commit ? "committed_pk_update" : "aborted_pk_update",
                                    "status",
                                    "rejected_as_required"));
                }
            }
        }
        Files.writeString(
                evidence.resolve("integration-summary.json"),
                json.toJson(
                        Map.of(
                                "status",
                                "passed",
                                "checks",
                                checks,
                                "native_ycsb_sysbench_execution",
                                false,
                                "temporary_databases",
                                "dropped")),
                StandardOpenOption.CREATE_NEW);
    }

    private JsonObject initialRow(SourceSchema.Table table, String benchmark) {
        var row = new JsonObject();
        for (var column : table.columns()) {
            String value =
                    switch (column.type()) {
                        case "integer" -> "20";
                        case "real" -> "1.25";
                        case "date" -> "2026-10-21";
                        case "timestamp without time zone" -> "2026-10-21 01:02:03.123456";
                        default -> "value ";
                    };
            row.addProperty(column.name(), value);
        }
        row.addProperty(
                SourceRow.primary(table).name(), benchmark.equals("ycsb") ? "user020 " : "20");
        return row;
    }

    private interface Work {
        void run(Connection connection) throws Exception;
    }

    private Path record(Database database, Path directory, Path capture, Work work)
            throws Exception {
        Files.createDirectory(directory);
        Connection actual = database.connect();
        int pid;
        try (var statement = actual.createStatement();
                var rows = statement.executeQuery("SELECT pg_backend_pid()")) {
            rows.next();
            pid = rows.getInt(1);
        }
        try (Connection connection = CaptureDriver.wrap(actual, directory)) {
            connection.setAutoCommit(false);
            work.run(connection);
        }
        Path raw = directory.resolve("pg-" + pid + ".jsonl");
        Files.copy(capture.resolve(raw.getFileName()), raw);
        Path client;
        try (var files = Files.list(directory)) {
            client =
                    files.filter(p -> p.getFileName().toString().startsWith("jdbc-"))
                            .findFirst()
                            .orElseThrow();
        }
        RequestCaptureAudit.audit(client, raw);
        Files.writeString(
                directory.resolve("capture-run.json"),
                json.toJson(
                        Map.of(
                                "status",
                                "complete",
                                "pg_file",
                                raw.getFileName().toString(),
                                "pg_sha256",
                                BaselineGenerator.sha256(raw),
                                "jdbc_file",
                                client.getFileName().toString(),
                                "jdbc_sha256",
                                BaselineGenerator.sha256(client))),
                StandardOpenOption.CREATE_NEW);
        return raw;
    }

    private static void execute(Connection c, String sql) throws SQLException {
        try (var statement = c.createStatement()) {
            statement.execute(sql);
        }
    }

    private static void execute(Connection c, String sql, Object... parameters)
            throws SQLException {
        try (var statement = c.prepareStatement(sql)) {
            for (int i = 0; i < parameters.length; i++) statement.setObject(i + 1, parameters[i]);
            statement.executeUpdate();
        }
    }

    private static void insert(Connection c, SourceSchema.Table table, JsonObject row)
            throws SQLException {
        String placeholders = String.join(",", Collections.nCopies(table.columns().size(), "?"));
        try (var statement =
                c.prepareStatement(
                        "INSERT INTO " + table.name() + " VALUES (" + placeholders + ")")) {
            for (int i = 0; i < table.columns().size(); i++) {
                var column = table.columns().get(i);
                String value = row.get(column.name()).getAsString();
                switch (column.type()) {
                    case "integer" -> statement.setLong(i + 1, Long.parseLong(value));
                    case "real" -> statement.setFloat(i + 1, Float.parseFloat(value));
                    case "date" -> statement.setDate(i + 1, java.sql.Date.valueOf(value));
                    case "timestamp without time zone" ->
                            statement.setTimestamp(i + 1, Timestamp.valueOf(value));
                    default -> statement.setString(i + 1, value);
                }
            }
            assertEquals(1, statement.executeUpdate());
        }
    }

    private final class Database implements AutoCloseable {
        final String admin, name, jdbc;

        Database(String admin, String benchmark) throws Exception {
            this.admin = admin;
            name =
                    "sibench_row_test_"
                            + benchmark
                            + "_"
                            + UUID.randomUUID().toString().replace("-", "");
            int slash = admin.indexOf('/', "jdbc:postgresql://".length());
            assertTrue("Admin URL must contain database", slash > 0);
            jdbc =
                    admin.substring(0, slash + 1)
                            + name
                            + (admin.contains("?") ? admin.substring(admin.indexOf('?')) : "");
            try (var c = DriverManager.getConnection(admin, user, password)) {
                execute(c, "CREATE DATABASE " + name + " TEMPLATE template0");
            }
        }

        Connection connect() throws SQLException {
            return DriverManager.getConnection(jdbc, user, password);
        }

        void insert(SourceSchema.Table table, JsonObject row) throws Exception {
            try (var c = connect()) {
                RowStateAuditIntegrationIT.insert(c, table, row);
            }
        }

        void install(String benchmark, SourceSchema schema) throws Exception {
            try (var c = connect()) {
                if (benchmark.equals("hybench")) {
                    execute(
                            c,
                            Files.readString(
                                    root.resolve("benchmark/HyBench-2024/conf/ddl_pg.sql")));
                    execute(
                            c,
                            Files.readString(
                                    root.resolve(
                                            "benchmark/HyBench-2024/conf/create_index_pg.sql")));
                } else if (benchmark.equals("ycsb")) {
                    StringBuilder ddl =
                            new StringBuilder(
                                    "CREATE TABLE usertable(YCSB_KEY VARCHAR PRIMARY KEY");
                    for (int i = 0; i < 10; i++) ddl.append(", FIELD").append(i).append(" TEXT");
                    execute(c, ddl.append(")").toString());
                } else {
                    for (int i = 1; i <= 2; i++)
                        execute(
                                c,
                                "CREATE TABLE sbtest"
                                        + i
                                        + "(id SERIAL PRIMARY KEY, k INTEGER DEFAULT 0 NOT NULL, c"
                                        + " CHAR(120) DEFAULT '' NOT NULL, pad CHAR(60) DEFAULT ''"
                                        + " NOT NULL); CREATE INDEX k_"
                                        + i
                                        + " ON sbtest"
                                        + i
                                        + "(k)");
                }
                String library =
                        root.resolve("build/services/postgresql-capture/row_capture.so")
                                .toString()
                                .replace("'", "''");
                execute(
                        c,
                        "CREATE FUNCTION sibench_row_capture() RETURNS trigger AS '"
                                + library
                                + "', 'sibench_row_capture' LANGUAGE C");
                for (var table : schema.tables())
                    execute(
                            c,
                            "CREATE TRIGGER sibench_row_capture AFTER INSERT OR UPDATE OR DELETE ON"
                                + " "
                                    + table.name()
                                    + " FOR EACH ROW EXECUTE FUNCTION sibench_row_capture()");
            }
        }

        @Override
        public void close() throws Exception {
            try (var c = DriverManager.getConnection(admin, user, password)) {
                execute(c, "DROP DATABASE " + name + " WITH (FORCE)");
            }
        }
    }
}
