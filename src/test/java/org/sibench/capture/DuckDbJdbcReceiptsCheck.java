package org.sibench.capture;

import static org.junit.Assert.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.junit.Test;
import org.sibench.generation.DuckDbHostDriver;
import org.sibench.generation.DuckDbHostFixture;

/** Real native statement boundaries, including streaming completion and deferred errors. */
public class DuckDbJdbcReceiptsCheck extends DuckDbHostFixture {
    private static void sql(Connection db,String text)throws SQLException {
        try(var q=db.createStatement()){q.execute(text);}
    }
    private static long number(Connection db,String text)throws SQLException {
        try(var q=db.createStatement();var r=q.executeQuery(text)) {
            assertTrue(r.next());long value=r.getLong(1);assertFalse(r.next());return value;
        }
    }
    private Path logging(Connection db)throws Exception {
        Path raw=root.resolve("native.csv");
        sql(db,"CALL enable_logging(['SibenchQuery','SibenchTransaction','SibenchMutation'],storage_path='"+raw+"')");
        return raw;
    }
    private List<DuckDbReceipts.Call> bind(Connection db,Path raw,Path receipts)throws Exception {
        sql(db,"CALL disable_logging()");
        List<Path> files;try(var paths=Files.list(receipts)){files=paths.sorted().toList();}
        var calls=new ArrayList<DuckDbReceipts.Call>();
        for(Path file:files)DuckDbReceipts.read(file,(connection,call)->calls.add(call));
        var sessions=DuckDbReceipts.load(db,files,root.resolve("staged"));
        DuckDbFacts.load(db,raw);DuckDbStatements.bind(db,sessions);DuckDbReceipts.bind(db);
        DuckDbTransactions.bind(db,"data");
        return calls;
    }
    @Test public void recordsSqlIndependentlyOfNativeCommitAndAbort()throws Exception {
        try(var observer=connect()) {
            sql(observer,"CREATE TABLE items(id BIGINT PRIMARY KEY,k BIGINT)");
            Path raw=logging(observer),receipts=Files.createDirectory(root.resolve("clients"));
            var properties=new Properties();properties.setProperty("receipts",receipts.toString());
            try(var worker=DriverManager.getConnection(DuckDbHostDriver.PREFIX+socket,properties)) {
                worker.setAutoCommit(false);
                sql(worker,"INSERT INTO items VALUES(1,10)");
                assertEquals(10,number(worker,"SELECT k FROM items WHERE id=1"));
                assertThrows(SQLException.class,()->sql(worker,"INSERT INTO items VALUES(1,99)"));
                worker.rollback();worker.rollback();
                sql(worker,"INSERT INTO items VALUES(2,20)");worker.commit();worker.commit();
                sql(worker,"UPDATE items SET k=30 WHERE id=2");worker.setAutoCommit(true);
                try(var q=worker.prepareStatement("SELECT k FROM items WHERE id=?")) {
                    q.setLong(1,999);
                    try(var r=q.executeQuery()){assertFalse(r.next());}
                }
            }
            var calls=bind(observer,raw,receipts);
            assertEquals(6,calls.size());
            assertEquals(List.of(1L,1L,-1L,1L,1L,0L),calls.stream().map(DuckDbReceipts.Call::rows).toList());
            assertTrue(calls.get(2).errorCode()>0);
            assertEquals(1,number(observer,"SELECT count(*) FROM sibench_transactions WHERE outcome='rollback'"));
            assertEquals(30,number(observer,"SELECT k FROM items WHERE id=2"));
            assertEquals(0,number(observer,"SELECT count(*) FROM items WHERE id=1"));
        }
    }
    @Test public void recordsExplicitImplicitAndConnectionResultCloseAsPartialReads()throws Exception {
        try(var observer=connect()) {
            Path raw=logging(observer),receipts=Files.createDirectory(root.resolve("clients"));
            try(var worker=DuckDbHostDriver.open(socket,receipts)) {
                try(var q=worker.createStatement();var r=q.executeQuery("SELECT range FROM range(1000000000)")) {
                    assertTrue(r.next());assertTrue(r.next());
                }
                try(var q=worker.createStatement()) {
                    var r=q.executeQuery("SELECT range FROM range(1000000000)");assertTrue(r.next());
                    assertEquals(42,number(worker,"SELECT 42"));assertTrue(r.isClosed());
                }
                worker.setAutoCommit(false);
                var q=worker.createStatement();var r=q.executeQuery("SELECT range FROM range(1000000000)");
                assertTrue(r.next());
            }
            var calls=bind(observer,raw,receipts);
            assertEquals(List.of(2L,1L,1L,1L),calls.stream().map(DuckDbReceipts.Call::rows).toList());
            assertEquals(3,number(observer,"SELECT count(*) FROM sibench_sql_submissions WHERE outcome='abandoned'"));
            assertEquals(1,number(observer,"SELECT count(*) FROM sibench_sql_submissions WHERE outcome='completed'"));
        }
    }
    @Test public void deferredFetchErrorKeepsUnknownRowCountAndConnectionCanRecover()throws Exception {
        try(var observer=connect()) {
            Path raw=logging(observer),receipts=Files.createDirectory(root.resolve("clients"));
            try(var worker=DuckDbHostDriver.open(socket,receipts)) {
                sql(worker,"SET max_streaming_buffer_size='64KiB'");
                try(var q=worker.createStatement();var r=q.executeQuery(
                        "SELECT CASE WHEN range>100000 THEN error('late fetch failure') ELSE range END FROM range(1000000)")) {
                    long[] consumed={0};
                    SQLException failure=assertThrows(SQLException.class,()->{while(r.next())consumed[0]++;});
                    assertTrue(failure.getMessage().contains("late fetch failure"));assertTrue(consumed[0]>0);
                }
                assertEquals(42,number(worker,"SELECT 42"));
            }
            var calls=bind(observer,raw,receipts);
            assertEquals(3,calls.size());assertTrue(calls.get(1).errorCode()>0);assertEquals(-1,calls.get(1).rows());
            assertEquals(1,number(observer,"SELECT count(*) FROM sibench_sql_submissions WHERE outcome='failed'"));
        }
    }
}
