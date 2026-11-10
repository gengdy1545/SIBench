package org.sibench.generation;

import static org.junit.Assert.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import org.duckdb.DuckDBConnection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class DuckDbDatabaseTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test public void engineIdentityMatchesThePinnedNativeSource() throws Exception {
        Path root = temporary.getRoot().toPath().resolve("identity");
        try (var database = DuckDbDatabase.create(root)) {
            var identity = DuckDbDatabase.identity(database);
            var process = new ProcessBuilder("git", "rev-parse", ":third_party/duckdb").start();
            String pin = new String(process.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8).strip();
            assertEquals(0, process.waitFor());
            assertTrue("Unexpected source identity: " + identity, identity.sourceId().matches("[0-9a-f]{7,40}"));
            assertTrue("JDBC and native capture source must match: " + identity, pin.startsWith(identity.sourceId()));
            assertFalse(identity.version().isBlank());
        }
    }

    @Test public void charKeysPreserveTrailingSpacesLikeVarchar() throws Exception {
        try (var database = java.sql.DriverManager.getConnection("jdbc:duckdb:");
                var sql = database.createStatement()) {
            sql.execute("CREATE TABLE labels (value CHAR(10) PRIMARY KEY)");
            sql.execute("INSERT INTO labels VALUES ('value'), ('value   ')");
            try (var rows = sql.executeQuery("SELECT value FROM labels ORDER BY value")) {
                assertTrue(rows.next()); assertEquals("value", rows.getString(1));
                assertTrue(rows.next()); assertEquals("value   ", rows.getString(1));
                assertFalse(rows.next());
            }
            var source = org.sibench.schema.SourceSchema.hybench(Path.of("benchmark/HyBench-2024"));
            var catalog = org.sibench.capture.IndexCatalog.fromSchema(source);
            var text = catalog.definitions().stream().filter(org.sibench.capture.IndexCatalog.Definition::textual)
                    .findFirst().orElseThrow();
            assertNotEquals(text.encode("value"), text.encode("value   "));
        }
    }

    @Test public void rejectsExistingRunDirectories() throws Exception {
        Path root = temporary.getRoot().toPath().resolve("fresh");
        try (var ignored = DuckDbDatabase.create(root)) {
            assertThrows(java.nio.file.FileAlreadyExistsException.class, () -> DuckDbDatabase.create(root));
        }
    }

    @Test public void preservesSnapshotsAndRejectsConcurrentWritesWithoutImplicitRetry() throws Exception {
        Path root = temporary.getRoot().toPath().resolve("transactions");
        try (var writer = DuckDbDatabase.create(root);
                var reader = writer.unwrap(DuckDBConnection.class).duplicate();
                var competitor = writer.unwrap(DuckDBConnection.class).duplicate();
                var sql = writer.createStatement()) {
            sql.execute("CREATE TABLE rows(id BIGINT PRIMARY KEY, value VARCHAR)");
            sql.execute("INSERT INTO rows VALUES (1, 'A')");
            reader.setAutoCommit(false);
            assertEquals("A", value(reader));
            writer.setAutoCommit(false);
            sql.execute("UPDATE rows SET value='B' WHERE id=1");
            assertEquals("A", value(reader));
            assertEquals("B", value(writer));
            competitor.setAutoCommit(false);
            try (var other = competitor.createStatement()) {
                assertThrows(SQLException.class, () -> other.execute("UPDATE rows SET value='wrong' WHERE id=1"));
            }
            competitor.rollback();
            writer.commit();
            assertEquals("A", value(reader));
            reader.commit();
            assertEquals("B", value(reader));
            reader.commit();
            sql.execute("DELETE FROM rows WHERE id=1");
            writer.rollback();
            assertEquals("B", value(writer));
            writer.commit();
        }
    }

    private static String value(Connection connection) throws SQLException {
        try (var sql = connection.createStatement(); var rows = sql.executeQuery("SELECT value FROM rows WHERE id=1")) {
            assertTrue(rows.next());
            String value = rows.getString(1);
            assertFalse(rows.next());
            return value;
        }
    }
}
