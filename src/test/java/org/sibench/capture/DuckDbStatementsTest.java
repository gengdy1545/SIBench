package org.sibench.capture;

import static org.junit.Assert.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import org.junit.Test;
import org.sibench.generation.DuckDbHostFixture;

public class DuckDbStatementsTest extends DuckDbHostFixture {
    private static final List<DuckDbStatements.Session> SESSIONS=List.of(new DuckDbStatements.Session(1,0,1));
    private static void sql(Connection db,String text)throws SQLException {try(var q=db.createStatement()){q.execute(text);}}
    private static long number(Connection db,String text)throws SQLException {
        try(var q=db.createStatement();var r=q.executeQuery(text)){assertTrue(r.next());return r.getLong(1);}
    }
    /** One completed UPDATE statement whose scope logs a before image at begin and an after image at complete. */
    private Connection fixture()throws Exception {
        var db=connect();
        sql(db,"CREATE TEMP TABLE sibench_query AS SELECT 1 AS raw_record,1 AS connection_id,1 AS transaction_id,1 AS query_id,"
                +"1 AS root_query,'begin' AS event,'running' AS outcome,false AS submission_end,'UPDATE items SET k=2' AS sql_text,'' AS error");
        sql(db,"INSERT INTO sibench_query VALUES(9,1,1,1,1,'end','completed',true,'','')");
        for(String kind:List.of("index","index_rows"))
            sql(db,"CREATE TEMP TABLE sibench_"+kind+"(connection_id BIGINT,transaction_id BIGINT,query_id BIGINT)");
        sql(db,"CREATE TEMP TABLE sibench_mutation AS SELECT 1 AS mutation_id,1 AS connection_id,1 AS transaction_id,1 AS query_id,"
                +"2 AS raw_record,'main' AS schema_name,'items' AS table_name,'update' AS kind,'begin' AS event,1 AS requested_rows,"
                +"-1 AS affected_rows,[{'name':'id','type':'BIGINT'},{'name':'k','type':'BIGINT'}] AS columns,"
                +"[{'physical_row':0,'values_sql':['10','1']}] AS rows");
        sql(db,"INSERT INTO sibench_mutation SELECT * REPLACE(3 AS raw_record,'complete' AS event,1 AS affected_rows,"
                +"[{'physical_row':0,'values_sql':['10','2']}] AS rows) FROM sibench_mutation");
        return db;
    }
    @Test public void admitsACompleteMutationScope()throws Exception {
        try(var db=fixture()) {
            DuckDbStatements.bind(db,SESSIONS);
            assertEquals(2,number(db,"SELECT count(*) FROM sibench_mutation_statements WHERE statement_outcome='completed'"));
        }
    }
    @Test public void rejectsInvalidOrIncompleteSuccessfulMutationScopesOnce()throws Exception {
        for(String corrupt:List.of("DELETE FROM sibench_mutation WHERE event='begin'",
                "DELETE FROM sibench_mutation WHERE event='complete'",
                "UPDATE sibench_mutation SET affected_rows=0 WHERE event='complete'",
                "UPDATE sibench_mutation SET event='failed' WHERE event='complete'",
                "UPDATE sibench_mutation SET kind='replace'",
                "UPDATE sibench_mutation SET rows=[] WHERE event='complete'",
                "UPDATE sibench_mutation SET raw_record=4 WHERE event='begin'",
                "UPDATE sibench_mutation SET columns=[{'name':'id','type':'BIGINT'}] WHERE event='complete'",
                "UPDATE sibench_mutation SET table_name='other' WHERE event='complete'",
                "INSERT INTO sibench_mutation SELECT * REPLACE(2 AS mutation_id,'append' AS kind,'rows' AS event) FROM sibench_mutation WHERE event='begin'")) {
            try(var db=fixture()) {
                sql(db,corrupt);
                var failure=assertThrows(corrupt,SQLException.class,()->DuckDbStatements.bind(db,SESSIONS));
                assertTrue(failure.getMessage(),failure.getMessage().contains("successful native mutation scope"));
                assertTrue(db.getAutoCommit());
                assertEquals(0,number(db,"SELECT count(*) FROM duckdb_tables() WHERE table_name='sibench_statements'"));
            }
        }
    }
}
