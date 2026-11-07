package org.sibench.generation;

import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import org.duckdb.DuckDBConnection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class NativeHistoryTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    private Connection database(Path path) throws Exception {
        var database = DuckDbDatabase.create(path);
        execute(database, "CREATE TABLE items(id BIGINT PRIMARY KEY, value VARCHAR, amount DECIMAL(18,4))");
        execute(database, "INSERT INTO items VALUES(1,'A',12.3400), (2,'untouched',NULL)");
        database.setAutoCommit(false);
        NativeHistory.initialize(database, org.sibench.config.Engine.DUCKDB);
        NativeHistory.install(database, org.sibench.config.Engine.DUCKDB, 1, "main", "items", "id");
        database.commit();
        database.setAutoCommit(true);
        assertEquals(0, number(database, "SELECT count(*) FROM sibench_history.rows_1"));
        return database;
    }

    @Test public void retainsBaselineAndTransactionPrefixesAfterCheckpointAndReopen() throws Exception {
        Path path = temporary.getRoot().toPath().resolve("history");
        long transaction, firstUpdate, originalRowId, replacementRowId;
        try (var database = database(path)) {
            originalRowId = number(database, "SELECT _sib_row_id FROM sibench_history.identities_1 WHERE id=1");
            database.setAutoCommit(false);
            execute(database, "UPDATE items SET value='B' WHERE id=1");
            transaction = number(database, "SELECT _sib_tx FROM sibench_history.rows_1");
            firstUpdate = number(database, "SELECT _sib_query FROM sibench_history.rows_1");
            execute(database, "UPDATE items SET value='C', amount=NULL WHERE id=1");
            database.commit();
            NativeHistory.recordCommit(database, transaction, 1);
            database.commit();
            execute(database, "DELETE FROM items WHERE id=1");
            long deleted = number(database, "SELECT _sib_tx FROM sibench_history.rows_1 WHERE _sib_kind='D'");
            database.commit();
            NativeHistory.recordCommit(database, deleted, 2);
            database.commit();
            execute(database, "INSERT INTO items VALUES(1,'reinserted',99.0001)");
            replacementRowId = number(database, "SELECT _sib_row_id FROM sibench_history.identities_1 WHERE id=1 AND _sib_tx<>0");
            assertNotEquals(originalRowId, replacementRowId);
            long inserted = number(database, "SELECT _sib_tx FROM sibench_history.rows_1 WHERE _sib_kind='I'");
            database.commit();
            NativeHistory.recordCommit(database, inserted, 3);
            NativeHistory.seal(database);
            database.commit();
            database.setAutoCommit(true);
            execute(database, "CHECKPOINT");
            assertEquals(4, number(database, "SELECT count(*) FROM sibench_history.rows_1"));
            assertEquals(0, number(database, "SELECT count(*) FROM sibench_history.rows_1 WHERE _sib_kind='I' AND value IS NOT NULL"));
        }
        try (var database = DuckDbDatabase.openReadOnly(path)) {
            assertEquals("A", value(database, 0, 0, 0));
            assertEquals("B", value(database, 0, transaction, firstUpdate));
            assertEquals("C", value(database, 1, 0, 0));
            assertTrue(NativeHistory.read(database, 1, 1L, 2, 0, 0).isEmpty());
            assertEquals("reinserted", value(database, 3, 0, 0));
            var baseline = NativeHistory.read(database, 1, 1L, 0, 0, 0).orElseThrow();
            assertEquals(new BigDecimal("12.3400"), baseline.get("amount"));
            assertNull(NativeHistory.read(database, 1, 1L, 1, 0, 0).orElseThrow().get("amount"));
            assertEquals("untouched", NativeHistory.read(database, 1, 2L, 0, 0, 0).orElseThrow().get("value"));
            assertTrue(NativeHistory.read(database, 1, 999L, 3, 0, 0).isEmpty());
            assertEquals("A", NativeHistory.readRow(database, 1, originalRowId, 0, 0, 0).orElseThrow().get("value"));
            assertEquals("B", NativeHistory.readRow(database, 1, originalRowId, 0, transaction, firstUpdate).orElseThrow().get("value"));
            assertTrue(NativeHistory.readRow(database, 1, originalRowId, 2, 0, 0).isEmpty());
            assertTrue(NativeHistory.readRow(database, 1, originalRowId, 3, 0, 0).isEmpty());
            assertTrue(NativeHistory.readRow(database, 1, replacementRowId, 0, 0, 0).isEmpty());
            assertEquals("reinserted", NativeHistory.readRow(database, 1, replacementRowId, 3, 0, 0).orElseThrow().get("value"));
        }
    }

    @Test public void rollsBackRowsAndHistoryTogetherIncludingTriggerFailures() throws Exception {
        try (var database = database(temporary.getRoot().toPath().resolve("rollback"))) {
            database.setAutoCommit(false);
            execute(database, "UPDATE items SET value='discarded' WHERE id=1");
            execute(database, "INSERT INTO items VALUES(3,'discarded',0)");
            assertEquals(2, number(database, "SELECT count(*) FROM sibench_history.rows_1"));
            database.rollback();
            assertEquals(0, number(database, "SELECT count(*) FROM sibench_history.rows_1"));
            assertEquals(2, number(database, "SELECT count(*) FROM sibench_history.identities_1"));
            assertThrows(SQLException.class, () -> execute(database, "UPDATE items SET id=4 WHERE id=1"));
            database.rollback();
            assertEquals(1, number(database, "SELECT count(*) FROM items WHERE id=1 AND value='A'"));
            assertEquals(0, number(database, "SELECT count(*) FROM items WHERE id IN(3,4)"));
            assertEquals(0, number(database, "SELECT count(*) FROM sibench_history.rows_1"));
            NativeHistory.seal(database);
            database.commit();
        }
    }

    @Test public void independentAcceptanceRejectsMissingCommitFactsAndReadingBeforeSeal() throws Exception {
        try (var database = database(temporary.getRoot().toPath().resolve("incomplete"))) {
            execute(database, "UPDATE items SET value='B' WHERE id=1");
            assertThrows(SQLException.class, () -> NativeHistory.read(database, 1, 1L, 0, 0, 0));
            database.setAutoCommit(false);
            assertThrows(SQLException.class, () -> NativeHistory.verifyCommits(database));
            database.rollback();
            long transaction = number(database, "SELECT _sib_tx FROM sibench_history.rows_1");
            NativeHistory.recordCommit(database, transaction, 1);
            NativeHistory.seal(database);
            database.commit();
            assertEquals("A", value(database, 0, 0, 0));
        }
    }

    @Test public void historyFollowsConcurrentSnapshotVisibilityAndWriteConflicts() throws Exception {
        try (var writer = database(temporary.getRoot().toPath().resolve("concurrent"));
                var reader = writer.unwrap(DuckDBConnection.class).duplicate();
                var competing = writer.unwrap(DuckDBConnection.class).duplicate()) {
            reader.setAutoCommit(false);
            assertEquals(0, number(reader, "SELECT count(*) FROM sibench_history.rows_1"));
            writer.setAutoCommit(false);
            execute(writer, "UPDATE items SET value='B' WHERE id=1");
            competing.setAutoCommit(false);
            assertThrows(SQLException.class, () -> execute(competing, "UPDATE items SET value='wrong' WHERE id=1"));
            competing.rollback();
            long transaction = number(writer, "SELECT _sib_tx FROM sibench_history.rows_1");
            writer.commit();
            assertEquals(0, number(reader, "SELECT count(*) FROM sibench_history.rows_1"));
            assertEquals(1, number(reader, "SELECT count(*) FROM items WHERE value='A'"));
            reader.commit();
            assertEquals(1, number(reader, "SELECT count(*) FROM sibench_history.rows_1"));
            reader.commit();
            NativeHistory.recordCommit(writer, transaction, 1);
            NativeHistory.seal(writer);
            writer.commit();
            assertEquals("A", value(writer, 0, 0, 0));
            assertEquals("B", value(writer, 1, 0, 0));
        }
    }

    @Test public void insertionDeletionAndLocalPrefixesDoNotInventRows() throws Exception {
        Path path = temporary.getRoot().toPath().resolve("births");
        long transaction, inserted, deleted;
        try (var database = database(path)) {
            database.setAutoCommit(false);
            execute(database, "INSERT INTO items VALUES(3,'born',NULL)");
            transaction = number(database, "SELECT _sib_tx FROM sibench_history.rows_1 WHERE id=3");
            inserted = number(database, "SELECT _sib_query FROM sibench_history.rows_1 WHERE id=3");
            execute(database, "DELETE FROM items WHERE id=3");
            deleted = number(database, "SELECT _sib_query FROM sibench_history.rows_1 WHERE id=3 AND _sib_kind='D'");
            database.commit();
            NativeHistory.recordCommit(database, transaction, 1);
            NativeHistory.seal(database);
            database.commit();
            database.setAutoCommit(true);
            assertThrows(SQLException.class, () -> execute(database, "UPDATE items SET value='after seal' WHERE id=1"));
            assertThrows(SQLException.class, () -> execute(database, "INSERT INTO items VALUES(4,'after seal',0)"));
            assertThrows(SQLException.class, () -> execute(database, "DELETE FROM items WHERE id=1"));
            assertThrows(SQLException.class, () -> NativeHistory.recordCommit(database, transaction + 1, 2));
        }
        try (var database = DuckDbDatabase.openReadOnly(path)) {
            assertTrue(NativeHistory.read(database, 1, 3L, 0, 0, 0).isEmpty());
            assertTrue(NativeHistory.read(database, 1, 3L, 1, 0, 0).isEmpty());
            assertEquals("born", NativeHistory.read(database, 1, 3L, 0, transaction, inserted).orElseThrow().get("value"));
            assertTrue(NativeHistory.read(database, 1, 3L, 0, transaction, deleted).isEmpty());
            assertEquals("A", value(database, 1, 0, 0));
        }
    }

    @Test public void rejectsPrimaryKeyPermutationsThatPreserveTheKeySet() throws Exception {
        try (var database = database(temporary.getRoot().toPath().resolve("key-permutation"))) {
            assertThrows(SQLException.class, () -> execute(database, "UPDATE items SET id=3-id"));
            assertEquals(1, number(database, "SELECT count(*) FROM items WHERE id=1 AND value='A'"));
            assertEquals(0, number(database, "SELECT count(*) FROM sibench_history.rows_1"));
        }
    }

    @Test public void binaryRowsRemainUsableAfterClosingTheReader() throws Exception {
        Path path=temporary.getRoot().toPath().resolve("binary");
        try(var database=DuckDbDatabase.create(path)) {
            execute(database,"CREATE TABLE items(id BIGINT PRIMARY KEY,payload BLOB)");
            execute(database,"INSERT INTO items VALUES(1,from_hex('00ff01')),(2,NULL),(3,from_hex(''))");
            database.setAutoCommit(false);NativeHistory.initialize(database, org.sibench.config.Engine.DUCKDB);
            NativeHistory.install(database, org.sibench.config.Engine.DUCKDB,1,"main","items","id");database.commit();
            execute(database,"UPDATE items SET payload=from_hex('ff0002') WHERE id=1");
            long transaction=number(database,"SELECT _sib_tx FROM sibench_history.rows_1");database.commit();
            NativeHistory.recordCommit(database,transaction,1);NativeHistory.seal(database);database.commit();
        }
        Object before,after,empty;
        try(var reader=DuckDbDatabase.openReadOnly(path)) {
            before=NativeHistory.read(reader,1,1L,0,0,0).orElseThrow().get("payload");
            after=NativeHistory.read(reader,1,1L,1,0,0).orElseThrow().get("payload");
            assertNull(NativeHistory.read(reader,1,2L,1,0,0).orElseThrow().get("payload"));
            empty=NativeHistory.read(reader,1,3L,1,0,0).orElseThrow().get("payload");
        }
        assertArrayEquals(new byte[]{0,(byte)255,1},(byte[])before);
        assertArrayEquals(new byte[]{(byte)255,0,2},(byte[])after);
        assertArrayEquals(new byte[0],(byte[])empty);
    }

    private static String value(Connection database, long version, long transaction, long query) throws SQLException {
        return (String) NativeHistory.read(database, 1, 1L, version, transaction, query).orElseThrow().get("value");
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
