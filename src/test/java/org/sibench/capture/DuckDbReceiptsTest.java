package org.sibench.capture;

import static org.junit.Assert.*;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class DuckDbReceiptsTest {
    @Rule public TemporaryFolder temporary=new TemporaryFolder();
    private static final String HEADER="event,operation,kind,connection_id,after_query,error_code,rows\n";
    private static final String OPEN="open,0,session,9007199254740993,10,0,0\n";
    private static final String CALLS="begin,1,query,,,0,0\nend,1,query,,,0,5\n"
            +"begin,2,query,,,0,0\nend,2,query,,,18,-1\n"
            +"begin,3,rollback,,,0,0\nend,3,rollback,,,0,0\n";
    private static final String CLOSE="close,3,session,,,0,0\n";
    private static void sql(Connection db,String text) throws SQLException {
        try(var q=db.createStatement()){q.execute(text);}
    }
    private java.nio.file.Path file(String text) throws IOException {
        var file=temporary.newFile().toPath();Files.writeString(file,text);return file;
    }
    @Test public void preservesFailedSqlAndRecoveryCallsWithExactSessionIdentity() throws Exception {
        var calls=new ArrayList<DuckDbReceipts.Call>();
        var session=DuckDbReceipts.read(file(HEADER+OPEN+CALLS+CLOSE),(connection,call)->{
            assertEquals(9007199254740993L,connection);calls.add(call);
        });
        assertEquals(new DuckDbStatements.Session(9007199254740993L,10,3),session);
        assertEquals(List.of(new DuckDbReceipts.Call(1,"query",0,5),new DuckDbReceipts.Call(2,"query",18,-1),
                new DuckDbReceipts.Call(3,"rollback",0,0)),calls);
    }
    @Test public void rejectsMissingMismatchedAndAmbiguousReceipts() throws Exception {
        String valid=HEADER+OPEN+CALLS+CLOSE;
        for(String bad:List.of(valid.replace(HEADER,""),valid.replace(CLOSE,""),valid+CLOSE,
                valid.replace("begin,1,","begin,2,"),valid.replace("end,2,query","end,2,rollback"),
                valid.replace("18,-1","18,0"),valid.replace("0,5","0,-1"),valid.replace("18,-1","2147483648,-1"),
                valid.replace("close,3,","close,2,"),valid.replace("9007199254740993","9223372036854775808"),
                valid.replace("begin,1,query,,,0,0","begin,1,query,7,,0,0"),valid.replace("end,1,","end,1.0,"))) {
            assertThrows(bad,IOException.class,()->DuckDbReceipts.read(file(bad),(connection,call)->{}));
        }
    }
    private Connection fixture() throws Exception {
        var db=DriverManager.getConnection("jdbc:duckdb:");
        DuckDbReceipts.load(db,List.of(file(HEADER+OPEN+CALLS+CLOSE)),temporary.newFolder().toPath().resolve("receipts"));
        sql(db,"CREATE TEMP TABLE sibench_statements(connection_id UBIGINT,root_query BIGINT,query_id BIGINT,"
                +"submission_end BOOLEAN,outcome VARCHAR,sql_text VARCHAR)");
        // The second submission completed its first statement before a later statement failed.
        sql(db,"INSERT INTO sibench_statements VALUES(9007199254740993,11,11,true,'completed','SELECT 5'),"
                +"(9007199254740993,12,12,false,'completed','INSERT INTO t VALUES(1)'),"
                +"(9007199254740993,12,13,true,'failed','INSERT INTO t VALUES(1)'),"
                +"(9007199254740993,14,14,true,'completed','ROLLBACK')");
        return db;
    }
    @Test public void bindsWholeSubmissionsWithoutDroppingEarlierSuccessfulStatements() throws Exception {
        try(var db=fixture()) {
            DuckDbReceipts.bind(db);
            try(var q=db.createStatement();var r=q.executeQuery("SELECT query_id,operation,outcome,error_code FROM sibench_sql_statements ORDER BY query_id")) {
                assertTrue(r.next());assertEquals(1,r.getLong(2));assertEquals(0,r.getInt(4));
                assertTrue(r.next());assertEquals(12,r.getLong(1));assertEquals(2,r.getLong(2));assertEquals("completed",r.getString(3));assertEquals(18,r.getInt(4));
                assertTrue(r.next());assertEquals("failed",r.getString(3));
                assertTrue(r.next());assertEquals(3,r.getLong(2));assertFalse(r.next());
            }
        }
    }
    @Test public void refusesMissingNativeEndsWrongOutcomesAndFalseRecoveryLabels() throws Exception {
        for(String corrupt:List.of("DELETE FROM sibench_statements WHERE query_id=14",
                "UPDATE sibench_statements SET outcome='completed' WHERE query_id=13",
                "UPDATE sibench_statements SET sql_text='SELECT 1' WHERE query_id=14",
                "INSERT INTO sibench_sql_calls SELECT * FROM sibench_sql_calls WHERE operation=1")) {
            try(var db=fixture()) {
                sql(db,corrupt);assertThrows(corrupt,SQLException.class,()->DuckDbReceipts.bind(db));
                assertTrue(db.getAutoCommit());
                try(var q=db.createStatement();var r=q.executeQuery("SELECT count(*) FROM duckdb_tables() WHERE table_name='sibench_sql_submissions'")) {
                    assertTrue(r.next());assertEquals(0,r.getLong(1));
                }
            }
        }
    }
    @Test public void preservesNativeAbandonmentWithoutCallingItCompleted() throws Exception {
        try(var db=fixture()) {
            sql(db,"UPDATE sibench_statements SET outcome='abandoned' WHERE query_id=11");
            DuckDbReceipts.bind(db);
            try(var q=db.createStatement();var r=q.executeQuery("SELECT outcome FROM sibench_sql_statements WHERE query_id=11")) {
                assertTrue(r.next());assertEquals("abandoned",r.getString(1));assertFalse(r.next());
            }
        }
    }
    @Test public void acceptsIdleSessionButRejectsDuplicateConnectionBeforeImport() throws Exception {
        var empty=file(HEADER+OPEN+"close,0,session,,,0,0\n");
        try(var db=DriverManager.getConnection("jdbc:duckdb:")) {
            assertThrows(IOException.class,()->DuckDbReceipts.load(db,List.of(empty,empty),temporary.getRoot().toPath().resolve("duplicate")));
            assertEquals(0,DuckDbReceipts.load(db,List.of(empty),temporary.getRoot().toPath().resolve("empty")).get(0).submissions());
        }
    }
}
