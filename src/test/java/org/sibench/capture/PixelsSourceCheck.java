package org.sibench.capture;

import static org.junit.Assert.*;
import static org.sibench.config.ConfigValues.*;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.config.Engine;
import org.sibench.config.SibenchConfig;
import org.sibench.generation.PostgresDatabase;
import org.sibench.schema.SourceSchema;

/** Source-only native check; requires sibench.postgres.runtime. */
public class PixelsSourceCheck {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test public void keyGuardPreservesSourceBehaviorWithoutCallsForUnchangedKeys() throws Exception {
        String runtime = System.getProperty("sibench.postgres.runtime");
        assertNotNull("Set sibench.postgres.runtime", runtime);
        Path root = Path.of("").toRealPath(), data = temporary.newFolder("csv").toPath();
        HybenchTest.fixture(data);
        var source = SourceSchema.hybench(root.resolve("benchmark/HyBench-2024"));
        var engine = SibenchConfig.engine(root, SibenchConfig.read(root.resolve("config/sibench.yaml")), Engine.PIXELS);
        var settings = SibenchConfig.postgresSettings(object(object(engine, "source"), "settings"));
        try (var database = PostgresDatabase.create(Path.of(runtime), temporary.getRoot().toPath().resolve("database"), settings);
             var connection = database.connect("source-guard"); var statement = connection.createStatement()) {
            Hybench.load(connection, source.source(), data);
            String indexes = "SELECT array_agg(indexdef ORDER BY tablename,indexname)::text FROM pg_indexes "
                    + "WHERE schemaname='public' AND tablename<>'sibench_marker'";
            String originalIndexes = value(connection, indexes);
            PixelsGeneration.prepareSource(connection, source);
            assertEquals(originalIndexes, value(connection, indexes));
            for (var table : source.tables()) {
                String key = table.indexes().stream().filter(SourceSchema.Index::primary).findFirst().orElseThrow().column();
                String originalKey = value(connection, "SELECT " + key + " FROM " + table.name());
                var failure = assertThrows(SQLException.class,
                        () -> statement.executeUpdate("UPDATE " + table.name() + " SET " + key + "=" + key + "+1000"));
                assertTrue(failure.getMessage(), failure.getMessage().contains("primary key assignment is unsupported"));
                assertEquals(originalKey, value(connection, "SELECT " + key + " FROM " + table.name()));
            }
            assertEquals(1, statement.executeUpdate("UPDATE savingaccount SET balance=2100 WHERE accountid=1"));
            connection.setAutoCommit(false);
            statement.executeUpdate("UPDATE savingaccount SET balance=3100 WHERE accountid=1");
            assertThrows(SQLException.class,
                    () -> statement.executeUpdate("UPDATE savingaccount SET accountid=99 WHERE accountid=1"));
            connection.rollback();
            connection.setAutoCommit(true);
            assertEquals("2100", value(connection, "SELECT balance FROM savingaccount WHERE accountid=1"));

            // A BEFORE trigger, named before or after the guard, can change the key even when the UPDATE does not assign it.
            statement.execute("CREATE FUNCTION change_key() RETURNS trigger LANGUAGE plpgsql AS $$ "
                    + "BEGIN NEW.accountid=99; RETURN NEW; END $$");
            for (String trigger : new String[] {"a_change_key", "z_change_key"}) {
                statement.execute("CREATE TRIGGER " + trigger + " BEFORE UPDATE ON savingaccount "
                        + "FOR EACH ROW EXECUTE FUNCTION change_key()");
                var indirect = assertThrows(trigger, SQLException.class,
                        () -> statement.executeUpdate("UPDATE savingaccount SET balance=2200 WHERE accountid=1"));
                assertTrue(indirect.getMessage(), indirect.getMessage().contains("primary key assignment is unsupported"));
                assertEquals("2100", value(connection, "SELECT balance FROM savingaccount WHERE accountid=1"));
                statement.execute("DROP TRIGGER " + trigger + " ON savingaccount");
            }

            for (var table : source.tables()) {
                String key = table.indexes().stream().filter(SourceSchema.Index::primary).findFirst().orElseThrow().column();
                String sql = "EXPLAIN (ANALYZE,FORMAT JSON) UPDATE " + table.name() + " SET " + key + "=" + key;
                var plan = com.google.gson.JsonParser.parseString(value(connection, sql))
                        .getAsJsonArray().get(0).getAsJsonObject();
                assertEquals(table.name(), 0, plan.getAsJsonArray("Triggers").size());
            }
            assertEquals(originalIndexes, value(connection, indexes));
        }
    }

    private static String value(Connection connection, String sql) throws SQLException {
        try (var statement = connection.createStatement(); var rows = statement.executeQuery(sql)) {
            assertTrue(rows.next());
            String value = rows.getString(1);
            assertFalse(rows.next());
            return value;
        }
    }
}
