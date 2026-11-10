package org.sibench.generation;

import static org.junit.Assert.*;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class NativeHistoryTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    private Connection database(Path path) throws Exception {
        var database = DuckDbDatabase.create(path);
        execute(database, "CREATE TABLE items(id BIGINT PRIMARY KEY, value VARCHAR)");
        execute(database, "INSERT INTO items VALUES(1,'A'), (2,'untouched')");
        database.setAutoCommit(false);
        NativeHistory.initialize(database, org.sibench.config.Engine.DUCKDB);
        NativeHistory.install(database, org.sibench.config.Engine.DUCKDB, 1, "main", "items", "id");
        database.commit();
        database.setAutoCommit(true);
        return database;
    }

    @Test public void updatesKeepTheirRowIdAndReinsertionsGetNewOnes() throws Exception {
        try (var database = database(temporary.getRoot().toPath().resolve("identities"))) {
            long original = number(database, "SELECT _sib_row_id FROM sibench_history.identities_1 WHERE id=1 AND _sib_tx=0");
            assertEquals(2, number(database, "SELECT count(DISTINCT _sib_row_id) FROM sibench_history.identities_1"));
            database.setAutoCommit(false);
            execute(database, "UPDATE items SET value='B' WHERE id=1");
            assertEquals(2, number(database, "SELECT count(*) FROM sibench_history.identities_1"));
            execute(database, "DELETE FROM items WHERE id=1");
            execute(database, "INSERT INTO items VALUES(1,'reinserted')");
            long transaction = number(database, "SELECT current_transaction_id()");
            database.commit();
            long replacement = number(database, "SELECT _sib_row_id FROM sibench_history.identities_1 WHERE id=1 AND _sib_tx<>0");
            assertNotEquals(original, replacement);
            assertEquals(transaction, number(database, "SELECT _sib_tx FROM sibench_history.identities_1 WHERE _sib_row_id=" + replacement));
            // Earlier versions are not retained: only identity tables exist in the history schema.
            assertEquals(0, number(database, "SELECT count(*) FROM duckdb_tables() WHERE schema_name='sibench_history' "
                    + "AND table_name NOT IN ('tables','identities_1')"));
        }
    }

    @Test public void rolledBackInsertsAndTriggerFailuresLeaveNoIdentity() throws Exception {
        try (var database = database(temporary.getRoot().toPath().resolve("rollback"))) {
            database.setAutoCommit(false);
            execute(database, "INSERT INTO items VALUES(3,'discarded')");
            assertEquals(3, number(database, "SELECT count(*) FROM sibench_history.identities_1"));
            database.rollback();
            assertEquals(2, number(database, "SELECT count(*) FROM sibench_history.identities_1"));
            assertThrows(SQLException.class, () -> execute(database, "UPDATE items SET id=4 WHERE id=1"));
            database.rollback();
            assertEquals(1, number(database, "SELECT count(*) FROM items WHERE id=1 AND value='A'"));
            assertEquals(0, number(database, "SELECT count(*) FROM items WHERE id IN(3,4)"));
            assertEquals(2, number(database, "SELECT count(*) FROM sibench_history.identities_1"));
        }
    }

    @Test public void rejectsPrimaryKeyPermutationsThatPreserveTheKeySet() throws Exception {
        try (var database = database(temporary.getRoot().toPath().resolve("key-permutation"))) {
            assertThrows(SQLException.class, () -> execute(database, "UPDATE items SET id=3-id"));
            assertEquals(1, number(database, "SELECT count(*) FROM items WHERE id=1 AND value='A'"));
        }
    }

    private static long number(Connection database, String query) throws SQLException {
        try (var statement = database.createStatement(); var rows = statement.executeQuery(query)) {
            assertTrue(rows.next());
            return rows.getLong(1);
        }
    }

    private static void execute(Connection database, String sql) throws SQLException {
        try (var statement = database.createStatement()) { statement.execute(sql); }
    }
}
