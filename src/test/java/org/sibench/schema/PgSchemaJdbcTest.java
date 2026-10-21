package org.sibench.schema;

import static org.junit.Assert.*;

import org.junit.Test;
import org.sibench.schema.PgSchemaCheck.*;

import java.io.IOException;
import java.lang.reflect.*;
import java.sql.*;
import java.util.*;

public class PgSchemaJdbcTest {
    @Test
    public void jdbcCatalogReadsAllThreeLayoutsAndEmitsSourceAndActualEvidence() throws Exception {
        for (String path :
                List.of(
                        "config/hybench_sf1.yaml",
                        "config/ycsb_large.yaml",
                        "config/sysbench_large.yaml")) {
            SourceSchema schema = SourceSchemaTest.read(path);
            Fixture fixture = new Fixture(PgSchemaCheckTest.matching(schema, "public"), "public");
            var report = PgSchemaCheck.check(fixture.connection(), schema, "public");
            assertEquals("schema_verified", report.get("status"));
            assertEquals(schema.fingerprints(), report.get("source_schema_sha256"));
            assertEquals(fixture.tables, report.get("actual_tables"));
            assertTrue(fixture.readOnly);
            assertEquals(1, fixture.commits);
            assertEquals(0, fixture.rollbacks);
            assertTrue(fixture.autoCommit);
        }
    }

    @Test
    public void shadowingTableFailsEvenWhenTargetSchemaItselfMatches() throws Exception {
        SourceSchema schema = SourceSchemaTest.read("config/ycsb_large.yaml");
        Fixture fixture = new Fixture(PgSchemaCheckTest.matching(schema, "public"), "shadow");
        Mismatch mismatch =
                assertThrows(
                        Mismatch.class,
                        () -> PgSchemaCheck.check(fixture.connection(), schema, "public"));
        assertTrue(mismatch.getMessage().contains("unqualified SQL resolves"));
        assertEquals("schema_mismatch", mismatch.report().get("status"));
        assertFalse(((List<?>) mismatch.report().get("differences")).isEmpty());
    }

    @Test
    public void driftAndReadFailuresKeepDiagnosticsAndRollbackWithoutChangingSource()
            throws Exception {
        SourceSchema schema = SourceSchemaTest.read("config/ycsb_large.yaml");
        Fixture empty = new Fixture(List.of(), "public");
        assertTrue(
                assertThrows(
                                Mismatch.class,
                                () -> PgSchemaCheck.check(empty.connection(), schema, "public"))
                        .getMessage()
                        .contains("Missing table"));
        Fixture failed = new Fixture(List.of(), "public");
        failed.failure = new SQLException("catalog unavailable", "08006");
        assertSame(
                failed.failure,
                assertThrows(
                        SQLException.class,
                        () -> PgSchemaCheck.check(failed.connection(), schema, "public")));
        assertEquals(1, failed.rollbacks);
        assertEquals(0, failed.commits);
        assertTrue(failed.autoCommit);
    }

    @Test
    public void everyDeclaredEnvironmentFieldMustMatchActualConnection() throws Exception {
        Map<String, Object> config =
                Map.of(
                        "database",
                        "database",
                        "user",
                        "postgres",
                        "version",
                        "14.24",
                        "port",
                        55433,
                        "isolation",
                        "read_committed",
                        "max_parallel_workers_per_gather",
                        0);
        Connection connection =
                proxy(
                        Connection.class,
                        (p, m, a) -> {
                            if (!m.getName().equals("createStatement")) throw new AssertionError(m);
                            return proxy(
                                    Statement.class,
                                    (sp, sm, sa) -> {
                                        if (sm.getName().equals("close")) return null;
                                        if (!sm.getName().equals("executeQuery"))
                                            throw new AssertionError(sm);
                                        return rows(
                                                Collections.singletonList(
                                                        new Object[] {
                                                            "database",
                                                            "postgres",
                                                            "14.24",
                                                            "55433",
                                                            "read committed",
                                                            "0"
                                                        }));
                                    });
                        });
        PgSchemaCheck.checkConfiguredEnvironment(connection, config);
        for (String field : config.keySet()) {
            var changed = new HashMap<>(config);
            changed.put(field, "wrong");
            assertTrue(
                    assertThrows(
                                    IOException.class,
                                    () ->
                                            PgSchemaCheck.checkConfiguredEnvironment(
                                                    connection, changed))
                            .getMessage()
                            .contains(field));
        }
    }

    @Test
    public void cleanupFailuresDoNotReplaceOriginalSchemaOrCatalogDiagnostics() throws Exception {
        SourceSchema schema = SourceSchemaTest.read("config/ycsb_large.yaml");
        Fixture mismatch = new Fixture(List.of(), "public");
        mismatch.cleanupFailure = new SQLException("cannot restore connection");
        Mismatch original =
                assertThrows(
                        Mismatch.class,
                        () -> PgSchemaCheck.check(mismatch.connection(), schema, "public"));
        assertTrue(original.getMessage().contains("Missing table"));
        assertSame(mismatch.cleanupFailure, original.getSuppressed()[0]);

        Fixture failed = new Fixture(List.of(), "public");
        failed.failure = new SQLException("catalog unavailable");
        failed.cleanupFailure = new SQLException("cannot restore connection");
        SQLException caught =
                assertThrows(
                        SQLException.class,
                        () -> PgSchemaCheck.check(failed.connection(), schema, "public"));
        assertSame(failed.failure, caught);
        assertSame(failed.cleanupFailure, caught.getSuppressed()[0]);
    }

    private static final class Fixture {
        final List<ActualTable> tables;
        final String resolution;
        boolean autoCommit = true, readOnly;
        int commits, rollbacks;
        SQLException failure;
        SQLException cleanupFailure;

        Fixture(List<ActualTable> tables, String resolution) {
            this.tables = tables;
            this.resolution = resolution;
        }

        Connection connection() {
            return proxy(
                    Connection.class,
                    (p, m, args) ->
                            switch (m.getName()) {
                                case "getAutoCommit" -> autoCommit;
                                case "setAutoCommit" -> {
                                    autoCommit = (boolean) args[0];
                                    yield null;
                                }
                                case "setReadOnly" -> {
                                    if (!(boolean) args[0] && cleanupFailure != null)
                                        throw cleanupFailure;
                                    readOnly |= (boolean) args[0];
                                    yield null;
                                }
                                case "setTransactionIsolation", "close" -> null;
                                case "commit" -> {
                                    commits++;
                                    yield null;
                                }
                                case "rollback" -> {
                                    rollbacks++;
                                    yield null;
                                }
                                case "createStatement" ->
                                        proxy(
                                                Statement.class,
                                                (sp, sm, sa) -> {
                                                    if (sm.getName().equals("close")) return null;
                                                    if (!sm.getName().equals("executeQuery"))
                                                        throw new AssertionError(sm);
                                                    if (failure != null) throw failure;
                                                    assertTrue(sa[0].toString().contains("SELECT"));
                                                    return rows(
                                                            Collections.singletonList(
                                                                    new Object[] {
                                                                        "database",
                                                                        "postgres",
                                                                        "14.24",
                                                                        "\"$user\", public",
                                                                        "C",
                                                                        "C"
                                                                    }));
                                                });
                                case "prepareStatement" -> query(args[0].toString());
                                default -> throw new AssertionError(m);
                            });
        }

        PreparedStatement query(String sql) {
            var parameters = new HashMap<Integer, Object>();
            return proxy(
                    PreparedStatement.class,
                    (p, m, args) -> {
                        if (m.getName().equals("close")) return null;
                        if (m.getName().equals("setString") || m.getName().equals("setLong")) {
                            parameters.put((int) args[0], args[1]);
                            return null;
                        }
                        if (!m.getName().equals("executeQuery")) throw new AssertionError(m);
                        var values = new ArrayList<Object[]>();
                        if (sql.contains("c.relkind IN")) {
                            assertEquals("public", parameters.get(1));
                            for (int i = 0; i < tables.size(); i++) {
                                var t = tables.get(i);
                                values.add(
                                        new Object[] {
                                            (long) i + 1,
                                            t.name(),
                                            t.kind(),
                                            t.persistence(),
                                            t.inherited(),
                                            t.rowSecurity()
                                        });
                            }
                        } else if (sql.contains("pg_catalog.to_regclass")) {
                            values.add(new Object[] {resolution, parameters.get(1)});
                        } else {
                            int parameter = sql.contains("FROM pg_catalog.pg_attribute a") ? 2 : 1;
                            var table =
                                    tables.get(
                                            Math.toIntExact((long) parameters.get(parameter)) - 1);
                            if (parameter == 2) {
                                assertEquals(
                                        "\"public\".\"" + table.name() + "\"", parameters.get(1));
                                for (var c : table.columns())
                                    values.add(
                                            new Object[] {
                                                c.name(),
                                                c.type(),
                                                c.nullable(),
                                                c.defaultExpression(),
                                                c.identity(),
                                                c.generated(),
                                                c.dropped(),
                                                c.collation(),
                                                c.sequence()
                                            });
                            } else if (sql.contains("pg_catalog.pg_index i")) {
                                for (var i : table.indexes())
                                    values.add(
                                            new Object[] {
                                                i.name(),
                                                i.column(),
                                                i.primary(),
                                                i.unique(),
                                                i.method(),
                                                i.attributes(),
                                                i.keys(),
                                                i.valid(),
                                                i.ready(),
                                                i.live(),
                                                i.immediate(),
                                                i.exclusion(),
                                                i.expression(),
                                                i.partial(),
                                                i.operatorClass(),
                                                i.defaultOperatorClass(),
                                                i.columnCollation(),
                                                i.options(),
                                                i.definition()
                                            });
                            } else if (sql.contains("pg_catalog.pg_constraint")) {
                                for (var constraint : table.otherConstraints())
                                    values.add(new Object[] {constraint});
                            } else throw new AssertionError(sql);
                        }
                        return rows(values);
                    });
        }
    }

    private static ResultSet rows(List<Object[]> values) {
        int[] cursor = {-1};
        return proxy(
                ResultSet.class,
                (p, m, args) ->
                        switch (m.getName()) {
                            case "next" -> ++cursor[0] < values.size();
                            case "close" -> null;
                            case "getString" -> {
                                Object value = values.get(cursor[0])[(int) args[0] - 1];
                                yield value == null ? null : value.toString();
                            }
                            case "getBoolean" -> (boolean) values.get(cursor[0])[(int) args[0] - 1];
                            case "getInt" ->
                                    ((Number) values.get(cursor[0])[(int) args[0] - 1]).intValue();
                            case "getLong" ->
                                    ((Number) values.get(cursor[0])[(int) args[0] - 1]).longValue();
                            default -> throw new AssertionError(m);
                        });
    }

    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return type.cast(
                Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, handler));
    }
}
