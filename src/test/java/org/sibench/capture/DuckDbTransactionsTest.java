package org.sibench.capture;

import static org.junit.Assert.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

public class DuckDbTransactionsTest {
    private static void execute(Connection database, String sql) throws SQLException {
        try (var statement = database.createStatement()) { statement.execute(sql); }
    }

    private Connection fixture() throws Exception {
        var database = DriverManager.getConnection("jdbc:duckdb:");
        execute(database, "CREATE TEMP TABLE sibench_statements(connection_id BIGINT, transaction_id BIGINT, query_id BIGINT)");
        execute(database, "INSERT INTO sibench_statements VALUES(1,1,1),(2,2,2),(1,3,3),(2,4,4),(1,5,5)");
        execute(database, "CREATE TEMP TABLE sibench_transaction(database_name VARCHAR, connection_id BIGINT, "
                + "transaction_id BIGINT, event VARCHAR, engine_transaction UBIGINT, snapshot_start UBIGINT, "
                + "commit_stamp UBIGINT, resolution_order UBIGINT)");
        // Deliberately not log-file order. Resolution slots include Abort; snapshots see only prior commits.
        execute(database, "INSERT INTO sibench_transaction VALUES "
                + "('data',1,5,'commit',4611686018427387909,40,50,12),"
                + "('data',2,2,'rollback',4611686018427387906,15,0,4),"
                + "('data',1,1,'begin',4611686018427387905,10,0,0),"
                + "('data',1,3,'commit',4611686018427387907,25,30,8),"
                + "('data',2,4,'begin',4611686018427387908,35,0,0),"
                + "('data',1,5,'begin',4611686018427387909,40,0,0),"
                + "('data',1,1,'commit',4611686018427387905,10,20,2),"
                + "('data',1,3,'begin',4611686018427387907,25,0,0),"
                + "('data',2,2,'begin',4611686018427387906,15,0,0),"
                + "('data',2,4,'rollback',4611686018427387908,35,0,10)");
        execute(database, "CREATE TEMP TABLE sibench_mutation(connection_id BIGINT, transaction_id BIGINT, query_id BIGINT)");
        execute(database, "CREATE TEMP TABLE sibench_index(connection_id BIGINT, transaction_id BIGINT, query_id BIGINT, snapshot_start UBIGINT)");
        execute(database, "INSERT INTO sibench_index VALUES(2,2,2,15),(1,5,5,40)");
        return database;
    }

    @Test public void mapsNativeCommitOrderAndSnapshotsWhilePreservingAbortSlots() throws Exception {
        try (var database = fixture()) {
            DuckDbTransactions.bind(database, "data");
            var actual = new ArrayList<String>();
            try (var statement = database.createStatement(); var rows = statement.executeQuery(
                    "SELECT transaction_id,version,read_version,outcome FROM sibench_transactions ORDER BY version")) {
                while (rows.next()) actual.add(rows.getLong(1) + ":" + rows.getLong(2) + ":" + rows.getLong(3) + ":" + rows.getString(4));
            }
            assertEquals(List.of("1:1:0:commit", "2:2:0:rollback", "3:3:1:commit", "4:4:3:rollback", "5:5:3:commit"), actual);
        }
    }

    @Test public void refusesMissingAndContradictoryNativeFactsAtomically() throws Exception {
        for (String corrupt : List.of(
                "DELETE FROM sibench_transaction WHERE transaction_id=2 AND event='begin'",
                "INSERT INTO sibench_transaction SELECT * FROM sibench_transaction WHERE event='rollback' AND transaction_id=2",
                "UPDATE sibench_transaction SET snapshot_start=11 WHERE transaction_id=1 AND event='commit'",
                "UPDATE sibench_transaction SET resolution_order=2 WHERE transaction_id=2 AND event='rollback'",
                "UPDATE sibench_transaction SET commit_stamp=60 WHERE transaction_id=3 AND event='commit'",
                "UPDATE sibench_transaction SET commit_stamp=1 WHERE event='rollback'",
                "UPDATE sibench_transaction SET database_name='other' WHERE transaction_id=1",
                "UPDATE sibench_index SET snapshot_start=999 WHERE transaction_id=2",
                "DELETE FROM sibench_transaction WHERE transaction_id=2")) {
            try (var database = fixture()) {
                execute(database, corrupt);
                assertThrows(corrupt, SQLException.class, () -> DuckDbTransactions.bind(database, "data"));
                assertTrue(database.getAutoCommit());
                try (var statement = database.createStatement(); var rows = statement.executeQuery(
                        "SELECT count(*) FROM duckdb_tables() WHERE table_name='sibench_transactions'")) {
                    assertTrue(rows.next()); assertEquals(0, rows.getLong(1));
                }
            }
        }
    }

    @Test public void statementsWithoutStorageTransactionsDoNotInventResolutions() throws Exception {
        try (var database = fixture()) {
            execute(database, "DELETE FROM sibench_transaction"); execute(database, "DELETE FROM sibench_index");
            DuckDbTransactions.bind(database, "data");
            try (var statement = database.createStatement(); var rows = statement.executeQuery("SELECT count(*) FROM sibench_read_points")) {
                assertTrue(rows.next()); assertEquals(0, rows.getLong(1));
            }
        }
    }
}
