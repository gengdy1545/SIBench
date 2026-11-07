package org.sibench.capture;

import static org.junit.Assert.*;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import org.junit.Test;
import org.sibench.generation.NativeHistory;
import org.sibench.generation.DuckDbHostFixture;

public class DuckDbIdentitiesCheck extends DuckDbHostFixture {
    private static void execute(Connection database, String sql) throws Exception {
        try (var statement = database.createStatement()) { statement.execute(sql); }
    }
    private static long number(Connection database, String sql) throws Exception {
        try (var statement = database.createStatement(); var rows = statement.executeQuery(sql)) {
            assertTrue(rows.next()); long value=rows.getLong(1); assertFalse(rows.next()); return value;
        }
    }

    private static final class Client implements AutoCloseable {
        final Connection database;
        final long connection, marker;
        long calls;
        Client(Connection database) throws Exception {
            this.database=database;
            try (var query=database.createStatement(); var rows=query.executeQuery(
                    "SELECT current_connection_id(),current_query_id()")) {
                assertTrue(rows.next()); connection=rows.getLong(1); marker=rows.getLong(2); assertFalse(rows.next());
            }
            database.setAutoCommit(false);
        }
        void sql(String sql) throws Exception { calls++; execute(database,sql); }
        void insertKey(String key) throws Exception {
            calls++;
            try (var query=database.prepareStatement("INSERT INTO items VALUES(?)")) { query.setString(1,key); query.executeUpdate(); }
        }
        long scalar(String sql) throws Exception { calls++; return number(database,sql); }
        DuckDbStatements.Session receipt() { return new DuckDbStatements.Session(connection,marker,calls); }
        @Override public void close() throws Exception { database.close(); }
    }

    private Path logging(Connection observer) throws Exception {
        Path file=root.resolve("native.csv");
        execute(observer,"CALL enable_logging(['SibenchQuery','SibenchTransaction','SibenchMutation',"
                + "'SibenchIndex','SibenchIndexRows','SibenchConsumption'], storage_path='"+file+"')");
        return file;
    }
    private void compile(Connection observer, Path file, List<DuckDbStatements.Session> sessions) throws Exception {
        execute(observer,"CALL disable_logging()"); DuckDbFacts.load(observer,file);
        DuckDbStatements.bind(observer,sessions); DuckDbTransactions.bind(observer,"data");
        DuckDbIdentities.bind(observer);
    }

    @Test public void preservesActualRowIdsAcrossReinsertAndAbort() throws Exception {
        try (var observer=connect()) {
            execute(observer,"CREATE TABLE items(id BIGINT PRIMARY KEY, k BIGINT)");
            execute(observer,"INSERT INTO items VALUES(1,10)");
            observer.setAutoCommit(false); NativeHistory.initialize(observer, org.sibench.config.Engine.DUCKDB);
            NativeHistory.install(observer, org.sibench.config.Engine.DUCKDB,1,"main","items","id"); observer.commit(); observer.setAutoCommit(true);
            long baseline=number(observer,"SELECT _sib_row_id FROM sibench_history.identities_1 WHERE id=1");
            Path raw=logging(observer);
            long replacement, aborted, afterInsert;
            DuckDbStatements.Session receipt;
            try (var client=new Client(connect())) {
                client.sql("UPDATE items SET k=11 WHERE id=1"); client.database.commit();
                client.sql("DELETE FROM items WHERE id=1"); client.sql("INSERT INTO items VALUES(1,12)");
                replacement=client.scalar("SELECT max(_sib_row_id) FROM sibench_history.identities_1 WHERE id=1");
                client.database.commit();
                client.sql("DELETE FROM items WHERE id=1"); client.sql("INSERT INTO items VALUES(1,13)");
                aborted=client.scalar("SELECT max(_sib_row_id) FROM sibench_history.identities_1 WHERE id=1");
                assertEquals(13,client.scalar("SELECT k FROM items WHERE id=1"));
                afterInsert=client.scalar("SELECT current_query_id()");
                client.database.rollback(); receipt=client.receipt();
            }
            assertNotEquals(baseline,replacement); assertNotEquals(replacement,aborted);
            assertEquals(12,number(observer,"SELECT k FROM items WHERE id=1"));
            compile(observer,raw,List.of(receipt));
            assertEquals(baseline,DuckDbIdentities.observedRow(observer,1,"1",0,0,0));
            assertEquals(baseline,DuckDbIdentities.observedRow(observer,1,"1",1,0,0));
            assertEquals(replacement,DuckDbIdentities.observedRow(observer,1,"1",2,0,0));
            assertEquals(aborted,DuckDbIdentities.observedRow(observer,1,"1",2,3,afterInsert));
            assertEquals(replacement,DuckDbIdentities.observedRow(observer,1,"1",3,0,0));
            assertEquals(2,number(observer,"SELECT count(*) FROM sibench_insert_identities"));
            assertEquals(1,number(observer,"SELECT count(*) FROM sibench_insert_identities WHERE outcome='rollback'"));
            assertEquals(0,number(observer,"SELECT count(*) FROM sibench_history.identities_1 WHERE _sib_row_id="+aborted));
        }
    }

    @Test public void preservesNativeVarcharTokensWithoutExecutingCapturedSql() throws Exception {
        try (var observer=connect()) {
            execute(observer,"CREATE TABLE items(id VARCHAR PRIMARY KEY)");
            for (String key:List.of("quoted'雪","","back\\slash","line\nnext")) {
                try (var insert=observer.prepareStatement("INSERT INTO items VALUES(?)")) { insert.setString(1,key); insert.executeUpdate(); }
            }
            observer.setAutoCommit(false); NativeHistory.initialize(observer, org.sibench.config.Engine.DUCKDB);
            NativeHistory.install(observer, org.sibench.config.Engine.DUCKDB,1,"main","items","id"); observer.commit(); observer.setAutoCommit(true);
            Path raw=logging(observer);
            DuckDbStatements.Session receipt;
            try (var client=new Client(connect())) {
                client.sql("DELETE FROM items");
                for (String key:List.of("quoted'雪","","back\\slash","line\nnext")) client.insertKey(key);
                client.sql("INSERT INTO items VALUES('literal'';DROP TABLE items;--')"); client.database.commit();
                receipt=client.receipt();
            }
            compile(observer,raw,List.of(receipt));
            for (String key:List.of("quoted'雪","","back\\slash","line\nnext","literal';DROP TABLE items;--")) {
                long expected;
                try (var query=observer.prepareStatement("SELECT max(_sib_row_id) FROM sibench_history.identities_1 WHERE id=?")) {
                    query.setString(1,key); try(var rows=query.executeQuery()){assertTrue(rows.next());expected=rows.getLong(1);}
                }
                assertEquals(expected,DuckDbIdentities.observedRow(observer,1,DuckDbIdentities.textKey(key),1,0,0));
            }
            assertThrows(SQLException.class,()->DuckDbIdentities.observedRow(observer,1,"'absent'",1,0,0));
            assertEquals(5,number(observer,"SELECT count(*) FROM items"));
        }
    }

    @Test public void rejectsUnsupportedEmbeddedNulCaptureInsteadOfPublishingATruncatedKey() throws Exception {
        try (var observer=connect()) {
            execute(observer,"CREATE TABLE items(id VARCHAR PRIMARY KEY)");
            observer.setAutoCommit(false); NativeHistory.initialize(observer, org.sibench.config.Engine.DUCKDB);
            NativeHistory.install(observer, org.sibench.config.Engine.DUCKDB,1,"main","items","id"); observer.commit(); observer.setAutoCommit(true);
            Path raw=logging(observer);
            try (var client=new Client(connect())) { client.insertKey("x\0y"); client.database.commit(); }
            execute(observer,"CALL disable_logging()");
            assertThrows(SQLException.class,()->DuckDbFacts.load(observer,raw));
            assertThrows(IllegalArgumentException.class,()->DuckDbIdentities.textKey("x\0y"));
            assertEquals(1,number(observer,"SELECT count(*) FROM items WHERE id='x'||chr(0)||'y'"));
        }
    }

    @Test public void excludesCompletedIdentityWritesOfAFailedStatementButKeepsEarlierAbortedWrites() throws Exception {
        try (var observer=connect()) {
            execute(observer,"CREATE TABLE items(id BIGINT PRIMARY KEY)");
            execute(observer,"CREATE TABLE guard(id BIGINT)");
            observer.setAutoCommit(false); NativeHistory.initialize(observer, org.sibench.config.Engine.DUCKDB);
            NativeHistory.install(observer, org.sibench.config.Engine.DUCKDB,1,"main","items","id"); observer.commit(); observer.setAutoCommit(true);
            // Native AFTER triggers run by name; this fails after the history identity append has completed.
            execute(observer,"CREATE TRIGGER z_reject AFTER INSERT ON items REFERENCING NEW TABLE AS inserted "
                    + "FOR EACH STATEMENT INSERT INTO guard SELECT CASE WHEN id=2 THEN error('late failure') ELSE id END FROM inserted");
            Path raw=logging(observer);
            DuckDbStatements.Session receipt;
            long first;
            try (var client=new Client(connect())) {
                client.sql("INSERT INTO items VALUES(1)");
                first=client.scalar("SELECT _sib_row_id FROM sibench_history.identities_1 WHERE id=1");
                assertThrows(SQLException.class,()->client.sql("INSERT INTO items VALUES(2)"));
                client.database.rollback(); receipt=client.receipt();
            }
            compile(observer,raw,List.of(receipt));
            assertEquals(1,number(observer,"SELECT count(*) FROM sibench_mutation_statements WHERE "
                    + "schema_name='sibench_history' AND table_name='identities_1' AND event='complete' AND statement_outcome='failed'"));
            assertEquals(1,number(observer,"SELECT count(*) FROM sibench_insert_identities WHERE row_id="+first+" AND outcome='rollback'"));
            assertEquals(1,number(observer,"SELECT count(*) FROM sibench_insert_identities"));
            assertEquals(0,number(observer,"SELECT count(*) FROM items"));
            assertThrows(SQLException.class,()->DuckDbIdentities.observedRow(observer,1,"2",0,1,Long.MAX_VALUE));
        }
    }
}
