package org.sibench.capture;

import static org.junit.Assert.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import org.junit.Test;

public class DuckDbAccessesTest {
    static final List<IndexCatalog.Definition> CATALOG=List.of(new IndexCatalog.Definition(1,"items",0,"items_pk","id","int"));
    private static void sql(Connection db,String text)throws SQLException {try(var q=db.createStatement()){q.execute(text);}}
    private Connection fixture()throws Exception {
        var db=DriverManager.getConnection("jdbc:duckdb:");
        sql(db,"CREATE TABLE items(id BIGINT PRIMARY KEY)");
        sql(db,"CREATE SCHEMA sibench_history");
        sql(db,"CREATE TABLE sibench_history.tables AS SELECT 1 AS table_id,'main' AS schema_name,'items' AS table_name,'id' AS primary_key");
        sql(db,"CREATE TEMP TABLE sibench_statements AS SELECT 1 AS connection_id,2 AS transaction_id,3 AS query_id,'completed' AS outcome");
        sql(db,"CREATE TEMP TABLE sibench_transactions AS SELECT 1 AS connection_id,2 AS transaction_id,1 AS version,0 AS read_version,'rollback' AS outcome");
        sql(db,"CREATE TEMP TABLE sibench_identities AS SELECT 1 AS table_id,'10' AS primary_key_sql,100 AS row_id,0 AS query_id,0 AS version,'commit' AS outcome");
        sql(db,"INSERT INTO sibench_identities VALUES(1,'20',200,2,1,'rollback')");
        sql(db,"CREATE TEMP TABLE sibench_index AS SELECT 1 AS access_id,1 AS connection_id,2 AS transaction_id,3 AS query_id,"
                +"'main' AS schema_name,'items' AS table_name,'opaque_native_primary_name' AS index_name,true AS index_primary,"
                +"true AS index_is_column,[{'name':'id','type':'BIGINT'}] AS index_columns,true AS adopted,1 AS scan_calls,1 AS observed_candidates,"
                +"[{'origin':'base','completed':true,'predicates':[{'comparison':'GREATERTHANOREQUALTO','value_type':'BIGINT','value_sql':'10'}]}] AS probes");
        sql(db,"CREATE TEMP TABLE sibench_index_rows AS SELECT 1 AS access_id,1 AS connection_id,2 AS transaction_id,3 AS query_id,"
                +"1 AS batch_id,'storage' AS phase,0 AS candidate_offset,1 AS candidates_read,[0::BIGINT] AS candidate_rows,"
                +"[0::BIGINT] AS physical_rows,'BIGINT' AS primary_key_type,['10'] AS primary_keys_sql,false AS source_drained");
        sql(db,"INSERT INTO sibench_index_rows VALUES(1,1,2,3,2,'local',0,0,[],[4611686018427387904],'BIGINT',['20'],false),"
                +"(1,1,2,3,0,'closed',0,0,[],[],'BIGINT',[],true)");
        sql(db,"CREATE TEMP TABLE sibench_consumption(access_id BIGINT,connection_id BIGINT,transaction_id BIGINT,query_id BIGINT,"
                +"batch_id BIGINT,stage VARCHAR,consumed_prefix BIGINT,input_rows BIGINT,direct_from_scan BOOLEAN)");
        return db;
    }
    @Test public void mapsNativePrimaryIdentityAndKeepsOwnRowsBeforeAbort()throws Exception {
        try(var db=fixture()) {
            DuckDbAccesses.bind(db,CATALOG);
            try(var q=db.createStatement();var r=q.executeQuery("SELECT key_sql,row_id FROM sibench_access_results ORDER BY key_sql")) {
                assertTrue(r.next());assertEquals("10",r.getString(1));assertEquals(100,r.getLong(2));
                assertTrue(r.next());assertEquals("20",r.getString(1));assertEquals(200,r.getLong(2));assertFalse(r.next());
            }
            try(var q=db.createStatement();var r=q.executeQuery("SELECT index_id,transaction_outcome FROM sibench_accesses")) {
                assertTrue(r.next());assertEquals(0,r.getInt(1));assertEquals("rollback",r.getString(2));
            }
        }
    }
    @Test public void preservesAbandonedAttemptWithoutInventingASearch()throws Exception {
        try(var db=fixture()) {
            sql(db,"DELETE FROM sibench_index_rows");sql(db,"UPDATE sibench_index SET adopted=false");
            DuckDbAccesses.bind(db,CATALOG);
            try(var q=db.createStatement();var r=q.executeQuery("SELECT (SELECT count(*) FROM sibench_index_attempts),(SELECT count(*) FROM sibench_accesses)")) {
                assertTrue(r.next());assertEquals(1,r.getInt(1));assertEquals(0,r.getInt(2));
            }
        }
    }
    @Test public void projectsAllIndexOutputEvenWhenDownstreamStopsOrDiscardsIt()throws Exception {
        for(String stage:List.of("stop","limit"))try(var db=fixture()) {
            sql(db,"INSERT INTO sibench_consumption VALUES(1,1,2,3,1,'"+stage+"',0,1,true)");
            if(stage.equals("limit"))sql(db,"INSERT INTO sibench_consumption VALUES(1,1,2,3,1,'limit',0,0,true)");
            DuckDbAccesses.bind(db,CATALOG);
            try(var q=db.createStatement();var r=q.executeQuery("SELECT count(*),sum(row_id) FROM sibench_access_results")) {
                assertTrue(r.next());assertEquals(2,r.getInt(1));assertEquals(300,r.getLong(2));
            }
        }
    }
    @Test public void rejectsIncompleteWrongIdentityAndUnsupportedConsumption()throws Exception {
        for(String corrupt:List.of("UPDATE sibench_index SET index_primary=false",
                "UPDATE sibench_index SET index_columns=[{'name':'wrong','type':'BIGINT'}]",
                "UPDATE sibench_index SET index_is_column=false",
                "UPDATE sibench_index SET adopted=false",
                "DELETE FROM sibench_index_rows WHERE phase='closed'",
                "UPDATE sibench_index_rows SET source_drained=false WHERE phase='closed'",
                "UPDATE sibench_index_rows SET batch_id=3 WHERE phase='local'",
                "UPDATE sibench_index SET observed_candidates=2",
                "UPDATE sibench_index_rows SET physical_rows=[] WHERE phase='local'",
                "UPDATE sibench_index_rows SET candidate_offset=1 WHERE phase='storage'",
                "DELETE FROM sibench_identities WHERE row_id=200",
                "UPDATE sibench_identities SET query_id=3 WHERE row_id=200",
                "INSERT INTO sibench_consumption VALUES(1,1,2,3,1,'unknown',0,1,true)",
                "INSERT INTO sibench_consumption VALUES(1,1,2,3,1,'limit',2,1,true)",
                "INSERT INTO sibench_consumption VALUES(1,1,2,3,1,'limit',1,2,true)",
                "INSERT INTO sibench_consumption VALUES(1,1,2,3,8,'limit',1,1,true)",
                "INSERT INTO sibench_index_rows SELECT * FROM sibench_index_rows WHERE phase='closed'")) {
            try(var db=fixture()) {
                sql(db,corrupt);var failure=assertThrows(corrupt,SQLException.class,()->DuckDbAccesses.bind(db,CATALOG));
                assertFalse(failure.getMessage(),failure.getMessage().contains("Binder Error"));
                assertTrue(db.getAutoCommit());
                try(var q=db.createStatement();var r=q.executeQuery("SELECT count(*) FROM duckdb_tables() WHERE table_name='sibench_accesses'")) {
                    assertTrue(r.next());assertEquals(0,r.getInt(1));
                }
            }
        }
    }

    @Test public void primaryEqualityMatchProvesCompleteOutputEvenWithoutTheFinalEmptyFetch()throws Exception {
        for(String corrupt:List.of("", "UPDATE sibench_index_rows SET primary_keys_sql=['20'] WHERE phase='storage'",
                "UPDATE sibench_index_rows SET primary_keys_sql=[],physical_rows=[] WHERE phase='storage'",
                "UPDATE sibench_index SET index_primary=false,index_name='items_secondary'")) {
            try(var db=fixture()) {
                sql(db,"CREATE INDEX items_secondary ON items(id)");
                sql(db,"DELETE FROM sibench_index_rows WHERE phase='local'");
                sql(db,"UPDATE sibench_index_rows SET source_drained=false WHERE phase='closed'");
                sql(db,"UPDATE sibench_index SET probes=[{'origin':'base','completed':true,"
                        +"'predicates':[{'comparison':'EQUAL','value_type':'BIGINT','value_sql':'10'}]}]");
                var catalog=List.of(CATALOG.get(0),new IndexCatalog.Definition(1,"items",1,"items_secondary","id","int"));
                if(!corrupt.isEmpty()) {
                    sql(db,corrupt);
                    var failure=assertThrows(SQLException.class,()->DuckDbAccesses.bind(db,catalog));
                    assertTrue(failure.getMessage(),failure.getMessage().contains("Unsupported ART early stop"));
                }else {
                    DuckDbAccesses.bind(db,catalog);
                    try(var q=db.createStatement();var r=q.executeQuery("SELECT key_sql,row_id FROM sibench_access_results")) {
                        assertTrue(r.next());assertEquals("10",r.getString(1));assertEquals(100,r.getLong(2));assertFalse(r.next());
                    }
                }
            }
        }
    }
}
