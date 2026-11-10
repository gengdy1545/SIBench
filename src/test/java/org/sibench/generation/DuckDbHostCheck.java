package org.sibench.generation;

import static org.junit.Assert.*;

import java.io.*;
import java.math.BigDecimal;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.channels.Channels;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.List;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

/** Explicit integration check: -Dtest=DuckDbHostCheck -Dsibench.duckdb.host=/path/to/duckdb_host. */
public class DuckDbHostCheck extends DuckDbHostFixture {
    private static void execute(Connection connection, String sql) throws SQLException {
        try (var statement = connection.createStatement()) { statement.execute(sql); }
    }
    private static long number(Connection connection, String sql) throws SQLException {
        try (var statement = connection.createStatement(); var rows = statement.executeQuery(sql)) {
            assertTrue(rows.next()); long value = rows.getLong(1); assertFalse(rows.next()); return value;
        }
    }

    @Test public void bindsExactTypesAndStreamsMultipleBatches() throws Exception {
        try (var database = connect()) {
            execute(database, "CREATE TABLE typed(id BIGINT PRIMARY KEY, text VARCHAR, amount DECIMAL(38,9), "
                    + "stamp TIMESTAMP, raw BLOB, optional BIGINT, enabled BOOLEAN)");
            long key = 9_007_199_254_740_993L;
            var amount = new BigDecimal("12345678901234567890123456789.123456789");
            var stamp = Timestamp.valueOf("2026-11-04 12:34:56.123456");
            byte[] raw = {0, 1, -1, 39, 92};
            try (var insert = database.prepareStatement("INSERT INTO typed VALUES(?,?,?,?,?,?,?)")) {
                insert.setLong(1, key); insert.setString(2, "中文'; DROP TABLE typed; --");
                insert.setBigDecimal(3, amount); insert.setTimestamp(4, stamp); insert.setBytes(5, raw);
                insert.setNull(6, Types.BIGINT); insert.setBoolean(7, true);
                assertEquals(1, insert.executeUpdate());
            }
            try (var query = database.prepareStatement("SELECT * FROM typed WHERE id=?")) {
                query.setLong(1, key);
                try (var row = query.executeQuery()) {
                    assertTrue(row.next()); assertEquals(key, row.getLong("id"));
                    assertEquals("中文'; DROP TABLE typed; --", row.getString(2));
                    assertEquals(amount, row.getObject("amount")); assertEquals(stamp, row.getTimestamp(4));
                    assertArrayEquals(raw, row.getBytes(5)); assertEquals(0, row.getLong(6)); assertTrue(row.wasNull());
                    assertTrue(row.getBoolean(7)); assertFalse(row.wasNull());
                    assertEquals(Types.DECIMAL, row.getMetaData().getColumnType(3));
                    assertFalse(row.next());
                }
            }
            try (var query = database.createStatement(); var rows = query.executeQuery("SELECT range FROM range(10000)")) {
                int count = 0;
                while (rows.next()) assertEquals(count++, rows.getLong(1));
                assertEquals(10000, count); assertFalse(rows.next());
            }
        }
    }

    @Test public void engineOwnsSnapshotsLocalWritesAbortAndConflict() throws Exception {
        try (var writer = connect(); var reader = connect(); var competitor = connect()) {
            execute(writer, "CREATE TABLE items(id INTEGER PRIMARY KEY, value INTEGER)");
            execute(writer, "INSERT INTO items VALUES(1,10)");
            writer.setAutoCommit(false); reader.setAutoCommit(false); competitor.setAutoCommit(false);
            assertEquals(10, number(reader, "SELECT value FROM items WHERE id=1"));
            execute(writer, "UPDATE items SET value=20 WHERE id=1");
            assertEquals(20, number(writer, "SELECT value FROM items WHERE id=1"));
            assertEquals(10, number(reader, "SELECT value FROM items WHERE id=1"));
            assertThrows(SQLException.class, () -> execute(competitor, "UPDATE items SET value=90 WHERE id=1"));
            competitor.rollback();
            writer.rollback();
            assertEquals(10, number(writer, "SELECT value FROM items WHERE id=1"));
            execute(writer, "UPDATE items SET value=30 WHERE id=1"); writer.commit();
            assertEquals(10, number(reader, "SELECT value FROM items WHERE id=1"));
            reader.commit(); assertEquals(30, number(reader, "SELECT value FROM items WHERE id=1"));
            execute(writer, "UPDATE items SET value=40 WHERE id=1");
            writer.setAutoCommit(true);
            reader.rollback(); assertEquals(40, number(reader, "SELECT value FROM items WHERE id=1"));
            assertFalse(reader.getAutoCommit());
        }
    }

    @Test public void closeAndBrokenTransportRollbackInTheEngine() throws Exception {
        try (var observer = connect()) {
            execute(observer, "CREATE TABLE items(id INTEGER PRIMARY KEY, value INTEGER)");
            execute(observer, "INSERT INTO items VALUES(1,10)");
            try (var writer = connect()) {
                writer.setAutoCommit(false); execute(writer, "UPDATE items SET value=20 WHERE id=1");
                assertEquals(20, number(writer, "SELECT value FROM items"));
            }
            assertEquals(10, number(observer, "SELECT value FROM items"));
            try (var raw = SocketChannel.open(StandardProtocolFamily.UNIX)) {
                raw.connect(UnixDomainSocketAddress.of(socket));
                var out = new DataOutputStream(Channels.newOutputStream(raw));
                var in = new DataInputStream(Channels.newInputStream(raw));
                for (String sql : List.of("BEGIN", "UPDATE items SET value=99 WHERE id=1")) {
                    byte[] bytes = sql.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    out.writeByte(1); out.writeInt(bytes.length); out.write(bytes); out.writeInt(0); out.flush();
                    assertEquals(0, in.readByte()); assertEquals(0, in.readByte()); in.readLong();
                }
                // A partial next request closes the session; no control message asks DuckDB to roll back.
                out.writeByte(1); out.writeInt(12); out.writeByte('S'); out.flush();
            }
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (true) {
                try { execute(observer, "UPDATE items SET value=value+1 WHERE id=1"); break; }
                catch (SQLException e) { if (System.nanoTime() >= deadline) throw e; Thread.sleep(10); }
            }
            assertEquals(11, number(observer, "SELECT value FROM items"));
        }
    }

    @Test public void sqlAndParameterFailuresDoNotDesynchronizeTheConnection() throws Exception {
        try (var database = connect(); var statement = database.createStatement()) {
            assertThrows(SQLException.class, () -> statement.execute("SELECT missing FROM nonexistent"));
            assertEquals(42, number(database, "SELECT 42"));
            try (var query = database.prepareStatement("SELECT ? + ?")) {
                query.setInt(2, 2);
                assertThrows(SQLException.class, query::executeQuery);
                query.setInt(1, 1);
                try (var rows = query.executeQuery()) { assertTrue(rows.next()); assertEquals(3, rows.getInt(1)); }
                assertThrows(SQLException.class, () -> query.setObject(1, new Object()));
                assertThrows(SQLException.class, () -> query.setBigDecimal(1, new BigDecimal("1E+1000000000")));
                assertThrows(SQLException.class, () -> query.setTimestamp(1, Timestamp.valueOf("2026-11-04 01:02:03.123456789")));
                assertThrows(SQLException.class, () -> query.setObject(1, java.time.LocalTime.parse("01:02:03.123456789")));
            }
            try (var query = database.prepareStatement("SELECT ?::INTEGER")) {
                query.setString(1, "invalid"); assertThrows(SQLException.class, query::executeQuery);
                query.setInt(1, 7);
                try (var rows = query.executeQuery()) { assertTrue(rows.next()); assertEquals(7, rows.getInt(1)); }
            }
            assertThrows(SQLFeatureNotSupportedException.class, () -> database.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY));
            assertThrows(SQLException.class, database::commit);
            assertEquals(17, number(database, "SELECT 17"));
        }
    }

    @Test public void closesEarlierResultsAndRejectsLossyConversions() throws Exception {
        try (var database = connect(); var first = database.createStatement(); var second = database.createStatement()) {
            var old = first.executeQuery("SELECT * FROM range(100000)"); assertTrue(old.next());
            try (var rows = second.executeQuery("SELECT 9223372036854775807::BIGINT, 1.5::DECIMAL(2,1)")) {
                assertTrue(old.isClosed()); assertThrows(SQLException.class, old::next);
                assertTrue(rows.next()); assertThrows(SQLDataException.class, () -> rows.getInt(1));
                assertThrows(SQLDataException.class, () -> rows.getLong(2));
            }
            assertEquals(9, number(database, "SELECT 9"));
        }
    }

    @Test public void hostSessionsRetainNativeIndexAndRollbackFacts() throws Exception {
        try (var observer = connect()) {
            execute(observer, "CREATE TABLE items(id BIGINT PRIMARY KEY, value BIGINT)");
            execute(observer, "INSERT INTO items SELECT range, range FROM range(10000)");
            execute(observer, "SET logging_storage='memory'");
            execute(observer, "CALL enable_logging(['SibenchIndex','SibenchIndexRows','SibenchTransaction'])");
            long transaction;
            try (var writer = connect()) {
                writer.setAutoCommit(false);
                execute(writer, "UPDATE items SET value=91 WHERE id=42");
                assertEquals(91, number(writer, "SELECT value FROM items WHERE id=42"));
                transaction = number(writer, "SELECT current_transaction_id()");
            }
            assertEquals(42, number(observer, "SELECT value FROM items WHERE id=42"));
            assertEquals(1, number(observer, "SELECT count(*) FROM duckdb_logs_parsed('SibenchTransaction') "
                    + "WHERE transaction_id=" + transaction + " AND event='rollback' AND commit_stamp=0 AND resolution_order>0"));
            assertTrue(number(observer, "SELECT count(*) FROM duckdb_logs_parsed('SibenchIndex') "
                    + "WHERE transaction_id=" + transaction + " AND adopted") > 0);
            assertTrue(number(observer, "SELECT count(*) FROM duckdb_logs_parsed('SibenchIndexRows') "
                    + "WHERE transaction_id=" + transaction + " AND batch_id>0") > 0);
        }
    }

    @Test public void queryFactsDistinguishCompletionFailureAndAbandonment() throws Exception {
        try (var observer = connect(); var writer = connect()) {
            execute(observer, "SET logging_storage='memory'");
            execute(observer, "CALL enable_logging(['SibenchQuery','SibenchTransaction'])");
            String completed = "SELECT /*sib_completed*/ sum(range) FROM range(10000)";
            String failed = "SELECT /*sib_failed*/ missing FROM nonexistent";
            String abandoned = "SELECT /*sib_abandoned*/ range FROM range(1000000000)";
            assertEquals(49995000, number(writer, completed));
            assertThrows(SQLException.class, () -> execute(writer, failed));
            try (var statement = writer.createStatement(); var rows = statement.executeQuery(abandoned)) {
                assertTrue(rows.next()); assertEquals(0, rows.getLong(1));
            }
            var outcomes = new HashMap<String, String>();
            try (var statement = observer.createStatement(); var rows = statement.executeQuery(
                    "SELECT b.sql_text, b.query_id, b.root_query, e.outcome, e.submission_end, e.error "
                    + "FROM duckdb_logs_parsed('SibenchQuery') b JOIN duckdb_logs_parsed('SibenchQuery') e "
                    + "ON b.query_id=e.query_id WHERE b.event='begin' AND e.event='end'")) {
                while (rows.next()) {
                    String sql = rows.getString(1);
                    if (!List.of(completed, failed, abandoned).contains(sql)) continue;
                    assertNull("Duplicate terminal query event", outcomes.put(sql, rows.getString(4)));
                    assertEquals(rows.getLong(2), rows.getLong(3)); assertTrue(rows.getBoolean(5));
                    assertEquals(sql.equals(failed), !rows.getString(6).isEmpty());
                }
            }
            assertEquals(java.util.Map.of(completed, "completed", failed, "failed", abandoned, "abandoned"), outcomes);
            assertEquals(42, number(writer, "SELECT 42"));
        }
    }

    @Test public void mutationFactsDoNotMarkAConflictingWriteComplete() throws Exception {
        try (var observer = connect(); var writer = connect(); var competitor = connect()) {
            execute(observer, "CREATE TABLE items(id BIGINT PRIMARY KEY, value BIGINT)");
            execute(observer, "INSERT INTO items VALUES(1,10)");
            execute(observer, "SET logging_storage='memory'");
            execute(observer, "CALL enable_logging(['SibenchMutation','SibenchTransaction','SibenchQuery'])");
            writer.setAutoCommit(false); competitor.setAutoCommit(false);
            execute(writer, "UPDATE items SET value=20 WHERE id=1");
            assertThrows(SQLException.class, () -> execute(competitor, "UPDATE items SET value=99 WHERE id=1"));
            competitor.rollback(); writer.rollback();
            assertEquals(10, number(observer, "SELECT value FROM items WHERE id=1"));
            assertEquals(2, number(observer, "SELECT count(*) FROM duckdb_logs_parsed('SibenchMutation') "
                    + "WHERE table_name='items' AND kind='update' AND event='begin'"));
            assertEquals(1, number(observer, "SELECT count(*) FROM duckdb_logs_parsed('SibenchMutation') "
                    + "WHERE table_name='items' AND kind='update' AND event='complete' AND affected_rows=1"));
            assertEquals(1, number(observer, "SELECT count(*) FROM duckdb_logs_parsed('SibenchMutation') "
                    + "WHERE table_name='items' AND kind='update' AND event='failed' AND affected_rows=-1"));
        }
    }

    @Test public void abortedInsertRetainsItsNativeIdentityFactButNoCommittedRow() throws Exception {
        try (var database = connect()) {
            execute(database, "CREATE TABLE items(id BIGINT PRIMARY KEY, value VARCHAR)");
            execute(database, "INSERT INTO items VALUES(1,'A')");
            database.setAutoCommit(false);
            NativeHistory.initialize(database, org.sibench.config.Engine.DUCKDB); NativeHistory.install(database, org.sibench.config.Engine.DUCKDB, 1, "main", "items", "id");
            database.commit(); database.setAutoCommit(true);
            execute(database, "SET logging_storage='memory'");
            execute(database, "CALL enable_logging(['SibenchMutation','SibenchTransaction','SibenchQuery'])");
            database.setAutoCommit(false);
            execute(database, "INSERT INTO items VALUES(2,'discarded')");
            long identity = number(database, "SELECT _sib_row_id FROM sibench_history.identities_1 WHERE id=2");
            long transaction = number(database, "SELECT current_transaction_id()");
            database.rollback();
            assertEquals(0, number(database, "SELECT count(*) FROM items WHERE id=2"));
            assertEquals(0, number(database, "SELECT count(*) FROM sibench_history.identities_1 WHERE id=2"));
            assertEquals(identity, number(database, "SELECT CAST(rows[1].values_sql[1] AS BIGINT) "
                    + "FROM duckdb_logs_parsed('SibenchMutation') WHERE schema_name='sibench_history' "
                    + "AND table_name='identities_1' AND event='begin' AND transaction_id=" + transaction));
            assertEquals(1, number(database, "SELECT count(*) FROM duckdb_logs_parsed('SibenchTransaction') "
                    + "WHERE transaction_id=" + transaction + " AND event='rollback'"));
            execute(database, "INSERT INTO items VALUES(3,'committed')");
            long committed = number(database, "SELECT current_transaction_id()");
            database.commit();
            assertEquals(1, number(database, "SELECT count(*) FROM duckdb_logs_parsed('SibenchTransaction') "
                    + "WHERE transaction_id=" + committed + " AND event='commit' AND commit_stamp>0"));
        }
    }

    @Test public void failedBatchKeepsUnknownPhysicalWritesSeparateFromEarlierSuccessfulStatements() throws Exception {
        try (var observer = connect(); var writer = connect()) {
            execute(observer, "CREATE TABLE items(id BIGINT PRIMARY KEY, k BIGINT UNIQUE)");
            execute(observer, "INSERT INTO items VALUES(1,10)");
            execute(observer, "CALL enable_logging(['SibenchMutation','SibenchQuery','SibenchTransaction'])");
            writer.setAutoCommit(false);
            execute(writer, "INSERT INTO items VALUES(3,30)");
            long transaction = number(writer, "SELECT current_transaction_id()");
            assertEquals(30, number(writer, "SELECT k FROM items WHERE id=3"));
            assertThrows(SQLException.class, () -> execute(writer, "INSERT INTO items VALUES(2,20),(4,20)"));
            writer.rollback();
            assertEquals(1, number(observer, "SELECT count(*) FROM items"));
            assertEquals(10, number(observer, "SELECT k FROM items WHERE id=1"));
            String sameTransaction = "transaction_id=" + transaction;
            assertEquals(1, number(observer, "SELECT count(*) FROM duckdb_logs_parsed('SibenchMutation') WHERE "
                    + sameTransaction + " AND table_name='items' AND event='complete' AND affected_rows=1"));
            assertEquals(1, number(observer, "SELECT count(*) FROM duckdb_logs_parsed('SibenchMutation') WHERE "
                    + sameTransaction + " AND table_name='items' AND event='failed' AND affected_rows=-1 AND requested_rows=2"));
            assertEquals(1, number(observer, "SELECT count(*) FROM duckdb_logs_parsed('SibenchQuery') WHERE "
                    + sameTransaction + " AND event='end' AND submission_end AND outcome='failed'"));
            assertEquals(1, number(observer, "SELECT count(*) FROM duckdb_logs_parsed('SibenchTransaction') WHERE "
                    + sameTransaction + " AND event='rollback'"));
        }
    }

    @Test public void existingPathsAreNeverReplaced() throws Exception {
        String executable = System.getProperty("sibench.duckdb.host");
        for (Path directory : List.of(root.resolve("data"), root.resolve("second-data"))) {
            var rejected = new ProcessBuilder(executable, directory.toString(), socket.toString(), "256MiB", "2")
                    .redirectErrorStream(true).start();
            try {
                assertTrue(rejected.waitFor(5, TimeUnit.SECONDS));
                assertEquals(1, rejected.exitValue());
                assertTrue(new String(rejected.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8)
                        .contains(directory.endsWith("data") ? "data directory already exists" : "cannot bind local socket"));
            } finally { rejected.destroyForcibly(); }
        }
        try (var observer = connect()) { assertEquals(42, number(observer, "SELECT 42")); }
    }

    @Test public void terminationInterruptsQueriesAndRollsBackIdleTransactions() throws Exception {
        var writer = connect(); var worker = connect();
        try {
            execute(writer, "CREATE TABLE items(value INTEGER)");
            writer.setAutoCommit(false); execute(writer, "INSERT INTO items VALUES(99)");
            var running = CompletableFuture.supplyAsync(() -> {
                try { number(worker, "SELECT sum(range) FROM range(1000000000000)"); return false; }
                catch (SQLException e) { return true; }
            });
            Thread.sleep(100);
            stop(); host = null;
            assertTrue(running.get(5, TimeUnit.SECONDS));
            var readOnly = new java.util.Properties(); readOnly.setProperty("duckdb.read_only", "true");
            try (var reader = java.sql.DriverManager.getConnection("jdbc:duckdb:" + DuckDbDatabase.file(root.resolve("data")), readOnly)) {
                assertEquals(0, number(reader, "SELECT count(*) FROM items"));
            }
            assertFalse(Files.exists(socket));
        } finally {
            for (var connection : List.of(writer, worker)) {
                try { connection.close(); } catch (SQLException ignored) { /* The host has already closed this socket. */ }
            }
        }
    }
}
