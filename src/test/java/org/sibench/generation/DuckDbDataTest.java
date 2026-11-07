package org.sibench.generation;

import static org.junit.Assert.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.capture.DuckDbTransactions;

public class DuckDbDataTest {
    @Rule public TemporaryFolder temporary=new TemporaryFolder();
    private static void sql(Connection db,String text)throws SQLException {try(var q=db.createStatement()){q.execute(text);}}
    private static void inputs(Connection db)throws SQLException {
        sql(db,"CREATE TEMP TABLE sibench_transactions(transaction_id UBIGINT,version BIGINT,read_version BIGINT,outcome VARCHAR,commit_stamp UBIGINT)");
        sql(db,"CREATE TEMP TABLE sibench_read_points(connection_id BIGINT,query_id BIGINT,transaction_id UBIGINT)");
        sql(db,"CREATE TEMP TABLE sibench_mutation_statements(transaction_id UBIGINT,query_id BIGINT,schema_name VARCHAR,"
                +"table_name VARCHAR,statement_outcome VARCHAR,event VARCHAR,affected_rows BIGINT)");
    }
    private Path baseline(String name)throws Exception {
        Path path=temporary.getRoot().toPath().resolve(name);
        try(var db=DuckDbDatabase.create(path)) {
            sql(db,"CREATE TABLE items(id BIGINT PRIMARY KEY,value VARCHAR)");sql(db,"INSERT INTO items VALUES(7,'baseline')");
            db.setAutoCommit(false);NativeHistory.initialize(db, org.sibench.config.Engine.DUCKDB);NativeHistory.install(db, org.sibench.config.Engine.DUCKDB,1,"main","items","id");
            db.commit();db.setAutoCommit(true);inputs(db);DuckDbTransactions.seal(db);
            assertThrows(SQLException.class,()->sql(db,"UPDATE items SET value='changed' WHERE id=7"));
        }
        return path;
    }
    @Test public void carriesItsInventoryToAnIndependentReadOnlyLocation()throws Exception {
        Path original=baseline("original");var inventory=DuckDbData.inventory(original);
        var gson=new com.google.gson.Gson();
        var decoded=gson.fromJson(gson.toJson(inventory),DuckDbData.Inventory.class);
        assertEquals(inventory,decoded);assertEquals(0,inventory.lastResolvedVersion());
        Path moved=temporary.getRoot().toPath().resolve("moved");Files.move(original,moved);
        try(var data=DuckDbData.open(moved,decoded)) {
            assertEquals("baseline",data.snapshotRow(1,1,0).orElseThrow().get("value"));
            assertTrue(data.snapshotRow(1,999,0).isEmpty());
            assertThrows(IllegalArgumentException.class,()->data.snapshotRow(1,1,1));
            assertThrows(SQLException.class,()->data.rowBeforeStatement(1,1,1,1));
        }
        assertEquals(inventory,DuckDbData.inventory(moved));
    }
    @Test public void rejectsAlteredFilesIncompleteDependenciesAndReaderMismatch()throws Exception {
        Path path=baseline("bound");var inventory=DuckDbData.inventory(path);
        var wrongHash=new DuckDbData.Inventory(inventory.engine(),inventory.bytes(),"0".repeat(64),0);
        assertThrows(java.io.IOException.class,()->DuckDbData.open(path,wrongHash));
        var wrongReader=new DuckDbData.Inventory(new DuckDbDatabase.Identity("other",inventory.engine().sourceId()),inventory.bytes(),inventory.sha256(),0);
        assertThrows(java.io.IOException.class,()->DuckDbData.open(path,wrongReader));
        var wrongVersion=new DuckDbData.Inventory(inventory.engine(),inventory.bytes(),inventory.sha256(),1);
        assertThrows(java.io.IOException.class,()->DuckDbData.open(path,wrongVersion));
        Path wal=path.resolve("data.duckdb.wal");Files.writeString(wal,"uncheckpointed");
        assertThrows(java.io.IOException.class,()->DuckDbData.inventory(path));
        assertThrows(java.io.IOException.class,()->DuckDbData.open(path,inventory));Files.delete(wal);
        byte[] bytes=Files.readAllBytes(DuckDbDatabase.file(path));bytes[bytes.length-1]^=1;
        Files.write(DuckDbDatabase.file(path),bytes);
        assertThrows(java.io.IOException.class,()->DuckDbData.open(path,inventory));
    }
    @Test public void doesNotSealUnattributedHistoryOrLeavePartialPublicationMetadata()throws Exception {
        Path path=temporary.getRoot().toPath().resolve("unattributed");
        try(var db=DuckDbDatabase.create(path)) {
            sql(db,"CREATE TABLE items(id BIGINT PRIMARY KEY,value VARCHAR)");sql(db,"INSERT INTO items VALUES(7,'baseline')");
            db.setAutoCommit(false);NativeHistory.initialize(db, org.sibench.config.Engine.DUCKDB);NativeHistory.install(db, org.sibench.config.Engine.DUCKDB,1,"main","items","id");
            db.commit();db.setAutoCommit(true);sql(db,"UPDATE items SET value='missing source commit'");inputs(db);
            assertThrows(SQLException.class,()->DuckDbTransactions.seal(db));
            try(var q=db.createStatement();var r=q.executeQuery("SELECT complete FROM sibench_history.state")) {assertTrue(r.next());assertFalse(r.getBoolean(1));}
            try(var q=db.createStatement();var r=q.executeQuery("SELECT count(*) FROM duckdb_tables() WHERE schema_name='sibench_history' AND table_name='transactions'")) {
                assertTrue(r.next());assertEquals(0,r.getLong(1));
            }
        }
        assertThrows(SQLException.class,()->DuckDbData.inventory(path));
    }
}
