package org.sibench.capture;

import static org.junit.Assert.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import org.junit.After;
import org.junit.Test;
import org.sibench.generation.DuckDbData;
import org.sibench.generation.NativeHistory;
import org.sibench.generation.DuckDbHostDriver;
import org.sibench.generation.DuckDbHostFixture;

/** Explicit source/close/move/reader check against the instrumented native host. */
public class DuckDbDataCheck extends DuckDbHostFixture {
    private Path retainedOutput;
    @After public void closeAndRetainSource()throws Exception {
        if(retainedOutput==null||host==null)return;
        stop();host=null;
        Files.copy(errors,retainedOutput.resolve("host.log"));
        Path destination=Files.createDirectory(retainedOutput.resolve("data"));
        Files.move(org.sibench.generation.DuckDbDatabase.file(root.resolve("data")),org.sibench.generation.DuckDbDatabase.file(destination));
    }
    private record Point(long connection,long query) {}
    private static void sql(Connection db,String text)throws SQLException {try(var q=db.createStatement()){q.execute(text);}}
    private static long number(Connection db,String text)throws SQLException {
        try(var q=db.createStatement();var r=q.executeQuery(text)) {assertTrue(r.next());long value=r.getLong(1);assertFalse(r.next());return value;}
    }
    private static Point observe(Connection db,String expected)throws SQLException {
        try(var q=db.createStatement();var r=q.executeQuery("SELECT current_connection_id(),current_query_id(),value FROM items WHERE id=7")) {
            assertTrue(r.next());Point point=new Point(r.getLong(1),r.getLong(2));assertEquals(expected,r.getString(3));assertFalse(r.next());return point;
        }
    }
    @Test public void reopensMovedDataAtNativeSnapshotsAndStatementStarts()throws Exception {
        String retained=System.getProperty("sibench.duckdb.data.output");
        Path output=Files.createDirectory(retained==null?root.resolve("published"):Path.of(retained));
        retainedOutput=output;
        Path raw=output.resolve("native.csv"),receipts=Files.createDirectory(output.resolve("clients"));
        Point committedB,committedC,oldSnapshot,beforeAbortWrite,afterAbortWrite;long original,replacement;
        try(var db=connect()) {
            sql(db,"CREATE TABLE items(id BIGINT PRIMARY KEY,value VARCHAR)");sql(db,"INSERT INTO items VALUES(7,'A')");
            db.setAutoCommit(false);NativeHistory.initialize(db, org.sibench.config.Engine.DUCKDB);NativeHistory.install(db, org.sibench.config.Engine.DUCKDB,1,"main","items","id");db.commit();db.setAutoCommit(true);
            original=number(db,"SELECT _sib_row_id FROM sibench_history.identities_1 WHERE id=7");
            sql(db,"CALL enable_logging(['SibenchQuery','SibenchTransaction','SibenchMutation',"
                    +"'SibenchIndex','SibenchIndexRows','SibenchConsumption'],storage_path='"+raw+"')");
            try(var writer=DuckDbHostDriver.open(socket,receipts);var reader=DuckDbHostDriver.open(socket,receipts)) {
                writer.setAutoCommit(false);reader.setAutoCommit(false);
                oldSnapshot=observe(reader,"A");
                sql(writer,"UPDATE items SET value='B' WHERE id=7");committedB=observe(writer,"B");
                sql(writer,"UPDATE items SET value='C' WHERE id=7");committedC=observe(writer,"C");writer.commit();
                observe(reader,"A");reader.rollback();
                beforeAbortWrite=observe(writer,"C");sql(writer,"UPDATE items SET value='discarded' WHERE id=7");
                afterAbortWrite=observe(writer,"discarded");writer.rollback();
                sql(writer,"DELETE FROM items WHERE id=7");writer.commit();
                sql(writer,"INSERT INTO items VALUES(7,'replacement')");
                replacement=number(writer,"SELECT _sib_row_id FROM sibench_history.identities_1 WHERE id=7 ORDER BY _sib_row_id DESC LIMIT 1");
                writer.commit();
            }
            sql(db,"CALL disable_logging()");
            List<Path> inputs;try(var files=Files.list(receipts)){inputs=files.sorted().toList();}
            var sessions=DuckDbReceipts.load(db,inputs,output.resolve("receipts"));
            DuckDbFacts.load(db,raw);DuckDbStatements.bind(db,sessions);DuckDbReceipts.bind(db);DuckDbTransactions.bind(db,"data");
            assertEquals(5,number(db,"SELECT count(*) FROM sibench_transactions"));
            DuckDbTransactions.seal(db);
            assertThrows(SQLException.class,()->sql(db,"DELETE FROM items WHERE id=7"));
        }
        closeAndRetainSource();
        Path destination=output.resolve("data");
        var inventory=DuckDbData.inventory(destination);assertEquals(5,inventory.lastResolvedVersion());
        var gson=new com.google.gson.GsonBuilder().setPrettyPrinting().create();
        Files.writeString(output.resolve("data-inventory.json"),gson.toJson(inventory));
        var parsed=gson.fromJson(Files.readString(output.resolve("data-inventory.json")),DuckDbData.Inventory.class);
        try(var data=DuckDbData.open(destination,parsed)) {
            assertEquals("A",data.snapshotRow(1,original,0).orElseThrow().get("value"));
            assertEquals("C",data.snapshotRow(1,original,1).orElseThrow().get("value"));
            assertEquals("C",data.snapshotRow(1,original,3).orElseThrow().get("value"));
            assertTrue(data.snapshotRow(1,original,4).isEmpty());assertTrue(data.snapshotRow(1,original,5).isEmpty());
            assertTrue(data.snapshotRow(1,replacement,0).isEmpty());
            assertEquals("replacement",data.snapshotRow(1,replacement,5).orElseThrow().get("value"));
            assertEquals("A",data.rowBeforeStatement(1,original,oldSnapshot.connection(),oldSnapshot.query()).orElseThrow().get("value"));
            assertEquals("B",data.rowBeforeStatement(1,original,committedB.connection(),committedB.query()).orElseThrow().get("value"));
            assertEquals("C",data.rowBeforeStatement(1,original,committedC.connection(),committedC.query()).orElseThrow().get("value"));
            assertEquals("C",data.rowBeforeStatement(1,original,beforeAbortWrite.connection(),beforeAbortWrite.query()).orElseThrow().get("value"));
            var unsupported=assertThrows(SQLException.class,()->data.rowBeforeStatement(1,original,afterAbortWrite.connection(),afterAbortWrite.query()));
            assertTrue(unsupported.getMessage().contains("not retained"));
        }
        assertEquals(inventory,DuckDbData.inventory(destination));
        Files.writeString(output.resolve("result.json"),gson.toJson(java.util.Map.of("status","passed","resolution_slots",5,
                "original_row_id",original,"replacement_row_id",replacement,"statement_points",List.of(oldSnapshot,committedB,committedC,beforeAbortWrite,afterAbortWrite))));
    }
}
