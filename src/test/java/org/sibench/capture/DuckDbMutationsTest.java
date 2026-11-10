package org.sibench.capture;

import static org.junit.Assert.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import org.junit.Test;

public class DuckDbMutationsTest {
    private static void sql(Connection db,String text)throws SQLException {try(var q=db.createStatement()){q.execute(text);}}
    private Connection fixture()throws Exception {
        var db=DriverManager.getConnection("jdbc:duckdb:");sql(db,"CREATE TABLE items(id BIGINT PRIMARY KEY,k BIGINT)");
        sql(db,"CREATE SCHEMA sibench_history");
        sql(db,"CREATE TABLE sibench_history.tables AS SELECT 1 AS table_id,'main' AS schema_name,'items' AS table_name,'id' AS primary_key");
        sql(db,"CREATE TEMP TABLE sibench_transactions AS SELECT 1 AS connection_id,1 AS transaction_id,1 AS version,0 AS read_version");
        sql(db,"CREATE TEMP TABLE sibench_index_catalog AS SELECT 1 AS table_id,'id' AS column_name UNION ALL SELECT 1,'k'");
        sql(db,"CREATE TEMP TABLE sibench_mutation_statements AS SELECT 1 AS mutation_id,1 AS connection_id,1 AS transaction_id,"
                +"1 AS query_id,1 AS raw_record,'main' AS schema_name,'items' AS table_name,'completed' AS statement_outcome,"
                +"'update' AS kind,'begin' AS event,1 AS requested_rows,-1 AS affected_rows,"
                +"[{'name':'id','type':'BIGINT'},{'name':'k','type':'BIGINT'}] AS columns,"
                +"[{'physical_row':0,'values_sql':['10','1']}] AS rows");
        sql(db,"INSERT INTO sibench_mutation_statements SELECT mutation_id,connection_id,transaction_id,query_id,2,schema_name,table_name,"
                +"statement_outcome,kind,'complete',requested_rows,1,columns,[{'physical_row':0,'values_sql':['10','2']}] FROM sibench_mutation_statements");
        sql(db,"INSERT INTO sibench_mutation_statements SELECT 2,connection_id,transaction_id,2,3,schema_name,table_name,"
                +"statement_outcome,'append','begin',1,-1,columns,[{'physical_row':100,'values_sql':['20','3']}] FROM sibench_mutation_statements WHERE event='begin'");
        sql(db,"INSERT INTO sibench_mutation_statements SELECT mutation_id,connection_id,transaction_id,query_id,4,schema_name,table_name,"
                +"statement_outcome,kind,'complete',1,1,columns,[] FROM sibench_mutation_statements WHERE mutation_id=2");
        // A successful internal append in a failed statement must not turn into a logical insert.
        sql(db,"INSERT INTO sibench_mutation_statements SELECT 3,connection_id,transaction_id,3,raw_record+10,schema_name,table_name,"
                +"'failed',kind,event,requested_rows,affected_rows,columns,rows FROM sibench_mutation_statements WHERE mutation_id=2");
        sql(db,"CREATE TEMP TABLE sibench_identities AS SELECT 1 AS table_id,'10' AS primary_key_sql,100 AS row_id,0 AS transaction_id,"
                +"0 AS query_id,0 AS version,'commit' AS outcome UNION ALL SELECT 1,'20',200,1,2,1,'rollback'");
        sql(db,"CREATE TEMP TABLE sibench_insert_identities AS SELECT * FROM sibench_identities WHERE version>0");
        sql(db,"CREATE TEMP TABLE sibench_access_batches(access_id BIGINT,connection_id BIGINT,transaction_id BIGINT,query_id BIGINT,raw_record BIGINT,physical_rows BIGINT[],phase VARCHAR)");
        sql(db,"CREATE TEMP TABLE sibench_accesses(access_id BIGINT,connection_id BIGINT,transaction_id BIGINT,query_id BIGINT,table_id INTEGER,raw_record BIGINT)");
        sql(db,"CREATE TEMP TABLE sibench_access_keys(access_id BIGINT,key_sql VARCHAR)");
        return db;
    }
    @Test public void keepsCompletedChangesAndExcludesFailedStatementInternals()throws Exception {
        try(var db=fixture()) {
            DuckDbMutations.bind(db);
            try(var q=db.createStatement();var r=q.executeQuery("SELECT kind,row_id,mutation_ordinal FROM sibench_changes ORDER BY mutation_ordinal")) {
                assertTrue(r.next());assertEquals("update",r.getString(1));assertEquals(100,r.getLong(2));assertEquals(1,r.getLong(3));
                assertTrue(r.next());assertEquals("insert",r.getString(1));assertEquals(200,r.getLong(2));assertEquals(2,r.getLong(3));assertFalse(r.next());
            }
        }
    }
    @Test public void rejectsIncompleteScopesIdentitiesColumnsAndRepeatedRowChanges()throws Exception {
        for(String corrupt:List.of("DELETE FROM sibench_mutation_statements WHERE mutation_id=1 AND event='begin'",
                "UPDATE sibench_mutation_statements SET affected_rows=0 WHERE event='complete' AND mutation_id=1",
                "DELETE FROM sibench_identities WHERE row_id=200",
                "UPDATE sibench_mutation_statements SET columns=[{'name':'id','type':'BIGINT'}] WHERE mutation_id=1",
                "UPDATE sibench_mutation_statements SET rows=[{'physical_row':0,'values_sql':['10']}] WHERE mutation_id=1 AND event='begin'",
                "INSERT INTO sibench_mutation_statements SELECT mutation_id+10,connection_id,transaction_id,query_id,raw_record+20,"
                        +"schema_name,table_name,statement_outcome,kind,event,requested_rows,affected_rows,columns,rows FROM sibench_mutation_statements WHERE mutation_id=1",
                "INSERT INTO sibench_insert_identities VALUES(1,'10',300,1,1,1,'rollback')")) {
            try(var db=fixture()) {
                sql(db,corrupt);var error=assertThrows(SQLException.class,()->DuckDbMutations.bind(db));
                assertFalse(error.getMessage(),error.getMessage().contains("Binder Error"));assertTrue(db.getAutoCommit());
            }
        }
    }
    @Test public void refusesAReadThatCouldObserveStatementInternalWrites()throws Exception {
        for(String visible:List.of("[0]","[]"))try(var db=fixture()) {
            sql(db,"INSERT INTO sibench_accesses VALUES(1,1,1,1,1,0)");sql(db,"INSERT INTO sibench_access_batches VALUES(1,1,1,1,5,"+visible+",'storage')");
            var error=assertThrows(SQLException.class,()->DuckDbMutations.bind(db));
            assertTrue(error.getMessage(),error.getMessage().contains("overlapping"));
        }
    }
}
