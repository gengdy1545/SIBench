package org.sibench.capture;

import static org.junit.Assert.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import org.junit.Test;
import org.sibench.generation.DuckDbHostFixture;
import org.sibench.generation.DuckDbDatabase;
import org.sibench.generation.NativeHistory;

/** Explicit integration with the instrumented engine's registered message types. */
public class DuckDbFactsCheck extends DuckDbHostFixture {
    private static final String HEADER = "context_id,scope,connection_id,transaction_id,query_id,thread_id,timestamp,type,log_level,message\n";

    private static void execute(Connection database, String sql) throws Exception {
        try (var statement = database.createStatement()) { statement.execute(sql); }
    }

    private static long number(Connection database, String sql) throws Exception {
        try (var statement = database.createStatement(); var rows = statement.executeQuery(sql)) {
            assertTrue(rows.next()); long value = rows.getLong(1); assertFalse(rows.next()); return value;
        }
    }

    @Test public void importsNativeReadsWritesFailedStatementAndAbortWithoutInventingCounts() throws Exception {
        Path capture = root.resolve("native.csv");
        long transaction, connection, marker;
        try (var observer = connect()) {
            execute(observer, "CREATE TABLE items(id BIGINT PRIMARY KEY, k BIGINT UNIQUE)");
            execute(observer, "INSERT INTO items SELECT range, range FROM range(512)");
            execute(observer, "CALL enable_logging(['SibenchQuery','SibenchTransaction','SibenchMutation',"
                    + "'SibenchIndex','SibenchIndexRows','SibenchConsumption'], storage_path='" + capture + "')");
            assertThrows(SQLException.class, () -> DuckDbFacts.load(observer, capture));
            try (var writer = connect()) {
                writer.setAutoCommit(false);
                try (var query = writer.createStatement(); var row = query.executeQuery(
                        "SELECT current_transaction_id(), current_connection_id(), current_query_id()")) {
                    assertTrue(row.next()); transaction = row.getLong(1); connection = row.getLong(2); marker = row.getLong(3);
                }
                execute(writer, "INSERT INTO items VALUES(1000,1000)");
                assertEquals(1000, number(writer, "SELECT k FROM items WHERE id=1000"));
                // The embedded newline and quoted field survive both CSV and native STRUCT parsing.
                assertEquals(1000, number(writer, "SELECT id FROM items WHERE k=1000 /* quoted 'line\nsecond' */ LIMIT 1"));
                assertThrows(SQLException.class, () -> execute(writer, "INSERT INTO items VALUES(1001,2000),(1002,2000)"));
                writer.rollback();
            }
            execute(observer, "CALL disable_logging()");
            var counts = DuckDbFacts.load(observer, capture);
            assertEquals(6, counts.size());
            assertTrue(counts.get("query") > 0);
            assertTrue(counts.get("index") > 0);
            assertEquals(1, number(observer, "SELECT count(*) FROM " + DuckDbFacts.table("transaction")
                    + " WHERE transaction_id=" + transaction + " AND event='rollback' AND commit_stamp=0"));
            assertEquals(1, number(observer, "SELECT count(*) FROM " + DuckDbFacts.table("mutation")
                    + " WHERE event='failed' AND requested_rows=2 AND affected_rows=-1"));
            assertEquals(1, number(observer, "SELECT count(*) FROM " + DuckDbFacts.table("query")
                    + " WHERE event='end' AND outcome='failed' AND submission_end"));
            assertEquals(1, number(observer, "SELECT count(*) FROM " + DuckDbFacts.table("query")
                    + " WHERE contains(sql_text, chr(10))"));
            assertEquals(512, number(observer, "SELECT count(*) FROM items"));
            assertThrows(SQLException.class, () -> DuckDbStatements.bind(observer,
                    List.of(new DuckDbStatements.Session(connection, marker, 3))));
            execute(observer, "CREATE TEMP TABLE missing_end AS SELECT * FROM " + DuckDbFacts.table("query")
                    + " WHERE connection_id=" + connection + " AND query_id>" + marker + " AND event='end' LIMIT 1");
            execute(observer, "DELETE FROM " + DuckDbFacts.table("query")
                    + " WHERE raw_record IN (SELECT raw_record FROM missing_end)");
            assertThrows(SQLException.class, () -> DuckDbStatements.bind(observer,
                    List.of(new DuckDbStatements.Session(connection, marker, 4))));
            execute(observer, "INSERT INTO " + DuckDbFacts.table("query") + " SELECT * FROM missing_end");
            execute(observer, "INSERT INTO " + DuckDbFacts.table("query") + " SELECT * FROM missing_end");
            assertThrows(SQLException.class, () -> DuckDbStatements.bind(observer,
                    List.of(new DuckDbStatements.Session(connection, marker, 4))));
            execute(observer, "DELETE FROM " + DuckDbFacts.table("query")
                    + " WHERE raw_record IN (SELECT raw_record FROM missing_end)");
            execute(observer, "INSERT INTO " + DuckDbFacts.table("query") + " SELECT * FROM missing_end");
            execute(observer, "UPDATE " + DuckDbFacts.table("mutation") + " SET query_id=query_id+1000000");
            assertThrows(SQLException.class, () -> DuckDbStatements.bind(observer,
                    List.of(new DuckDbStatements.Session(connection, marker, 4))));
            execute(observer, "UPDATE " + DuckDbFacts.table("mutation") + " SET query_id=query_id-1000000");
            DuckDbStatements.bind(observer, List.of(new DuckDbStatements.Session(connection, marker, 4)));
            DuckDbTransactions.bind(observer, "data");
            assertEquals(1, number(observer, "SELECT count(*) FROM temp.main.sibench_transactions "
                    + "WHERE outcome='rollback' AND version=1 AND read_version=0"));
            assertEquals(4, number(observer, "SELECT count(*) FROM temp.main.sibench_statements"));
            assertEquals(1, number(observer, "SELECT count(*) FROM temp.main.sibench_mutation_statements "
                    + "WHERE event='failed' AND statement_outcome='failed' AND affected_rows=-1"));
            assertEquals(1, number(observer, "SELECT count(*) FROM temp.main.sibench_mutation_statements "
                    + "WHERE event='complete' AND statement_outcome='completed' AND affected_rows=1"));
            assertThrows(SQLException.class, () -> DuckDbFacts.load(observer, capture));
            assertEquals((long) counts.get("query"), number(observer, "SELECT count(*) FROM " + DuckDbFacts.table("query")));
            try (var other = connect()) {
                assertThrows(SQLException.class, () -> number(other, "SELECT count(*) FROM " + DuckDbFacts.table("query")));
            }
        }
    }

    private static long[] marker(Connection database) throws Exception {
        try (var query = database.createStatement(); var rows = query.executeQuery(
                "SELECT current_connection_id(), current_query_id()")) {
            assertTrue(rows.next()); return new long[] {rows.getLong(1), rows.getLong(2)};
        }
    }

    @Test public void mapsConcurrentNativeSnapshotsAndSealsHistoryWithoutReplayingDataWrites() throws Exception {
        Path capture = root.resolve("snapshots.csv");
        long oldTransaction, oldLastQuery, rowOne, rowThree;
        try (var observer = connect()) {
            execute(observer, "CREATE TABLE items(id BIGINT PRIMARY KEY, k BIGINT)");
            execute(observer, "INSERT INTO items SELECT range,0 FROM range(1,1025)");
            observer.setAutoCommit(false); NativeHistory.initialize(observer, org.sibench.config.Engine.DUCKDB);
            NativeHistory.install(observer, org.sibench.config.Engine.DUCKDB, 1, "main", "items", "id"); observer.commit(); observer.setAutoCommit(true);
            rowOne = number(observer, "SELECT _sib_row_id FROM sibench_history.identities_1 WHERE id=1");
            rowThree = number(observer, "SELECT _sib_row_id FROM sibench_history.identities_1 WHERE id=3");
            execute(observer, "CALL enable_logging(['SibenchQuery','SibenchTransaction','SibenchMutation',"
                    + "'SibenchIndex','SibenchIndexRows','SibenchConsumption'], storage_path='" + capture + "')");
            long[] a, b, c;
            try (var old = connect(); var committed = connect(); var aborted = connect()) {
                a = marker(old); b = marker(committed); c = marker(aborted);
                old.setAutoCommit(false); committed.setAutoCommit(false); aborted.setAutoCommit(false);
                assertEquals(0, number(old, "SELECT k FROM items WHERE id=1"));
                execute(committed, "UPDATE items SET k=1 WHERE id=1"); committed.commit();
                execute(aborted, "UPDATE items SET k=20 WHERE id=2");
                assertEquals(20, number(aborted, "SELECT k FROM items WHERE id=2")); aborted.rollback();
                execute(old, "UPDATE items SET k=30 WHERE id=3");
                assertEquals(0, number(old, "SELECT k FROM items WHERE id=1"));
                assertEquals(30, number(old, "SELECT k FROM items WHERE id=3")); old.commit();
                committed.setAutoCommit(true);
                assertEquals(31, number(committed, "SELECT sum(k) FROM items"));
            }
            execute(observer, "CALL disable_logging()");
            DuckDbFacts.load(observer, capture);
            DuckDbStatements.bind(observer, List.of(new DuckDbStatements.Session(a[0],a[1],4),
                    new DuckDbStatements.Session(b[0],b[1],2), new DuckDbStatements.Session(c[0],c[1],2)));
            DuckDbTransactions.bind(observer, "data");
            assertEquals(4, number(observer, "SELECT count(*) FROM sibench_transactions"));
            assertEquals(1, number(observer, "SELECT count(*) FROM sibench_transactions WHERE connection_id=" + a[0]
                    + " AND version=3 AND read_version=0 AND outcome='commit'"));
            assertEquals(1, number(observer, "SELECT count(*) FROM sibench_transactions WHERE connection_id=" + c[0]
                    + " AND version=2 AND read_version=1 AND outcome='rollback'"));
            assertEquals(1, number(observer, "SELECT count(*) FROM sibench_transactions WHERE connection_id=" + b[0]
                    + " AND version=4 AND read_version=3 AND outcome='commit'"));
            oldTransaction = number(observer, "SELECT transaction_id FROM sibench_transactions WHERE version=3");
            oldLastQuery = number(observer, "SELECT max(query_id) FROM sibench_read_points WHERE version=3");
            var commits = new java.util.ArrayList<long[]>();
            try (var query = observer.createStatement(); var rows = query.executeQuery(
                    "SELECT transaction_id,version FROM sibench_transactions WHERE outcome='commit' ORDER BY version")) {
                while (rows.next()) commits.add(new long[] {rows.getLong(1),rows.getLong(2)});
            }
            observer.setAutoCommit(false);
            for (var commit : commits) NativeHistory.recordCommit(observer, commit[0], commit[1]);
            NativeHistory.seal(observer); observer.commit();
        }
        stop(); host = null;
        try (var reader = DuckDbDatabase.openReadOnly(root.resolve("data"))) {
            assertEquals(0L, NativeHistory.readRow(reader,1,rowOne,0,0,0).orElseThrow().get("k"));
            assertEquals(1L, NativeHistory.readRow(reader,1,rowOne,1,0,0).orElseThrow().get("k"));
            assertEquals(0L, NativeHistory.readRow(reader,1,rowThree,0,0,0).orElseThrow().get("k"));
            assertEquals(30L, NativeHistory.readRow(reader,1,rowThree,0,oldTransaction,oldLastQuery).orElseThrow().get("k"));
            assertEquals(30L, NativeHistory.readRow(reader,1,rowThree,3,0,0).orElseThrow().get("k"));
        }
    }

    @Test public void completedInternalBatchesDoNotMakeAFailedStatementSuccessful() throws Exception {
        Path capture = root.resolve("failed-batches.csv");
        long connection, marker;
        try (var observer = connect()) {
            execute(observer, "CREATE TABLE items(id BIGINT PRIMARY KEY, k BIGINT UNIQUE)");
            execute(observer, "CALL enable_logging(['SibenchQuery','SibenchTransaction','SibenchMutation'], storage_path='"
                    + capture + "')");
            try (var writer = connect()) {
                writer.setAutoCommit(false);
                try (var query = writer.createStatement(); var row = query.executeQuery(
                        "SELECT current_connection_id(), current_query_id()")) {
                    assertTrue(row.next()); connection = row.getLong(1); marker = row.getLong(2);
                }
                execute(writer, "INSERT INTO items VALUES(1,1)");
                assertThrows(SQLException.class, () -> execute(writer, "INSERT INTO items SELECT 2000+range, "
                        + "CASE WHEN range=4999 THEN 14998 ELSE 10000+range END FROM range(5000)"));
                writer.rollback();
            }
            execute(observer, "CALL disable_logging()");
            DuckDbFacts.load(observer, capture);
            DuckDbStatements.bind(observer, List.of(new DuckDbStatements.Session(connection, marker, 2)));
            var diagnostics=new com.google.gson.Gson().toJsonTree(DuckDbDiagnostics.summarize(observer,
                    org.sibench.config.BenchmarkSelection.Mode.CAPTURE)).getAsJsonObject();
            var failed=diagnostics.getAsJsonArray("mutation_scopes").asList().stream()
                    .map(com.google.gson.JsonElement::getAsJsonObject)
                    .filter(row->row.get("statement_outcome").getAsString().equals("failed")).findFirst().orElseThrow();
            assertEquals(5000,failed.get("requested_rows").getAsInt());
            assertTrue(failed.get("completed_scope_rows").getAsInt()>0);
            assertTrue(failed.get("completed_scope_rows").getAsInt()<5000);
            assertEquals(1,failed.get("scopes_without_completion").getAsInt());
            assertEquals(1,failed.get("failed_records").getAsInt());
            assertTrue(number(observer, "SELECT count(*) FROM temp.main.sibench_mutation_statements "
                    + "WHERE event='complete' AND statement_outcome='failed' AND affected_rows>0") > 0);
            assertEquals(1, number(observer, "SELECT count(*) FROM temp.main.sibench_mutation_statements "
                    + "WHERE event='complete' AND statement_outcome='completed' AND affected_rows=1"));
            assertEquals(0, number(observer, "SELECT count(*) FROM items"));
        }
    }

    @Test public void rejectsUnknownTypesAndMissingFieldsWithoutLeavingPartialTables() throws Exception {
        try (var database = connect()) {
            for (String message : new String[] {
                    row("Unexpected", "anything"), row("SibenchTransaction", "{'event': begin}"),
                    row("SibenchTransaction", "{'engine_transaction': invalid}"),
                    "1,CONNECTION,1,1,1,,2026-11-04 00:00:00+00,SibenchQuery,INFO,\"unterminated",
                    "1,CONNECTION,1,1,1,,2026-11-04 00:00:00+00,SibenchQuery,INFO,\"unterminated\n"}) {
                Path capture = root.resolve("invalid.csv");
                Files.writeString(capture, HEADER + message);
                assertThrows(message, SQLException.class, () -> DuckDbFacts.load(database, capture));
                assertTrue(database.getAutoCommit());
                assertEquals(0, number(database, "SELECT count(*) FROM duckdb_tables() WHERE temporary AND table_name LIKE 'sibench_%'"));
            }
            Path empty = root.resolve("empty.csv"); Files.writeString(empty, HEADER);
            assertEquals(0, DuckDbFacts.load(database, empty).values().stream().mapToLong(Long::longValue).sum());
        }
    }

    private static String row(String type, String message) {
        return "1,CONNECTION,1,1,1,,2026-11-04 00:00:00+00," + type + ",INFO,\""
                + message.replace("\"", "\"\"") + "\"\n";
    }
}
