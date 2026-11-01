package org.sibench.capture;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

import org.junit.Test;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.UUID;

/** Real PG: the common load policy must precede rows and preserve transactions. */
public class NativeLoadIntegrationIT {
    @Test
    public void nativeDdlCreatesUnloggedTablesAndIndexesWithoutChangingTransactions() throws Exception {
        String admin = System.getenv("SIBENCH_SCHEMA_TEST_JDBC");
        assumeTrue(admin != null && !admin.isBlank());
        String user = System.getenv().getOrDefault("SIBENCH_SCHEMA_TEST_USER", "postgres");
        String password = System.getenv().getOrDefault("PGPASSWORD", "");
        String name = "sibench_unlogged_" + UUID.randomUUID().toString().replace("-", "");
        String jdbc = admin.substring(0, admin.lastIndexOf('/') + 1) + name;
        try (var control = DriverManager.getConnection(admin, user, password);
                var statement = control.createStatement()) {
            statement.execute("CREATE DATABASE " + name);
            try (var connection = DriverManager.getConnection(jdbc, user, password);
                    var sql = connection.createStatement()) {
                NativeLoad.unloggedTables(connection);
                for (String ddl : HybenchNativeLoad.statements(Path.of("benchmark/HyBench-2024/conf/ddl_pg.sql")))
                    sql.execute(ddl);
                sql.execute("CREATE TABLE sample(id SERIAL PRIMARY KEY, value text)");
                sql.execute("CREATE INDEX sample_value ON sample(value)");
                try (var rows = sql.executeQuery("SELECT count(*),bool_and(relpersistence='u')"
                        + " FROM pg_class WHERE relnamespace='public'::regnamespace AND relkind IN ('r','i')")) {
                    assertTrue(rows.next());
                    assertTrue(rows.getLong(1) >= 11);
                    assertTrue(rows.getBoolean(2));
                }
                connection.setAutoCommit(false);
                sql.execute("INSERT INTO sample(value) VALUES ('kept')");
                connection.commit();
                sql.execute("UPDATE sample SET value='rolled back'");
                connection.rollback();
                try (var rows = sql.executeQuery("SELECT value FROM sample")) {
                    assertTrue(rows.next());
                    assertEquals("kept", rows.getString(1));
                    assertFalse(rows.next());
                }
                connection.commit();
                connection.setAutoCommit(true);
                sql.execute("DROP EVENT TRIGGER sibench_unlogged_tables");
                sql.execute("DROP FUNCTION public.sibench_unlogged_tables()");
                sql.execute("CREATE TABLE after_load(id int)");
                try (var rows = sql.executeQuery("SELECT relpersistence FROM pg_class WHERE oid='after_load'::regclass")) {
                    assertTrue(rows.next());
                    assertEquals("p", rows.getString(1));
                }
            } finally {
                statement.execute("DROP DATABASE " + name);
            }
        }
    }
}
