package org.sibench.capture;

import static org.junit.Assert.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import org.junit.Test;
import org.sibench.generation.NativeHistory;

public class DuckDbIdentitiesTest {
    private static void sql(Connection database,String text) throws Exception {
        try(var query=database.createStatement()){query.execute(text);}
    }
    private record Fixture(Connection database,long baseline,long inserted,long aborted,long beforeQuery) implements AutoCloseable {
        @Override public void close() throws SQLException { database.close(); }
    }
    private Fixture fixture() throws Exception {
        var database=DriverManager.getConnection("jdbc:duckdb:");
        sql(database,"CREATE TABLE items(id BIGINT PRIMARY KEY)"); sql(database,"INSERT INTO items VALUES(1)");
        database.setAutoCommit(false); NativeHistory.initialize(database, org.sibench.config.Engine.DUCKDB);
        NativeHistory.install(database, org.sibench.config.Engine.DUCKDB,1,"main","items","id"); database.commit();
        long baseline;
        try(var query=database.createStatement();var rows=query.executeQuery("SELECT _sib_row_id FROM sibench_history.identities_1")) {
            assertTrue(rows.next()); baseline=rows.getLong(1);
        }
        var observed=new java.util.ArrayList<long[]>();
        for(int key=2;key<=3;key++) {
            sql(database,"INSERT INTO items VALUES("+key+")");
            try(var query=database.createStatement();var rows=query.executeQuery(
                    "SELECT _sib_row_id,_sib_tx,_sib_query FROM sibench_history.identities_1 WHERE id="+key)) {
                assertTrue(rows.next());observed.add(new long[]{rows.getLong(1),rows.getLong(2),rows.getLong(3)});
            }
            if(key==2)database.commit();else database.rollback();
        }
        database.setAutoCommit(true);
        sql(database,"CREATE TEMP TABLE sibench_transactions(connection_id BIGINT,transaction_id BIGINT,version BIGINT,outcome VARCHAR)");
        sql(database,"CREATE TEMP TABLE sibench_mutation_statements(schema_name VARCHAR,table_name VARCHAR,statement_outcome VARCHAR,"
                + "mutation_id BIGINT,connection_id BIGINT,transaction_id BIGINT,query_id BIGINT,kind VARCHAR,event VARCHAR,"
                + "requested_rows BIGINT,affected_rows BIGINT,columns STRUCT(name VARCHAR,type VARCHAR)[],"
                + "rows STRUCT(physical_row BIGINT,values_sql VARCHAR[])[])");
        for(int i=0;i<2;i++) {
            var row=observed.get(i);int version=i+1,key=i+2;
            sql(database,"INSERT INTO sibench_transactions VALUES(1,"+row[1]+","+version+",'"+(i==0?"commit":"rollback")+"')");
            for(String event:List.of("begin","complete")) {
                sql(database,"INSERT INTO sibench_mutation_statements VALUES('sibench_history','identities_1','completed',"
                        +version+",1,"+row[1]+","+row[2]+",'append','"+event+"',1,"+(event.equals("begin")?-1:1)
                        +",[{'name':'_sib_row_id','type':'BIGINT'},{'name':'id','type':'BIGINT'}],"
                        +(event.equals("begin")?"[{'physical_row':100,'values_sql':['"+row[0]+"','"+key+"']}]":"[]")+")");
            }
        }
        return new Fixture(database,baseline,observed.get(0)[0],observed.get(1)[0],observed.get(1)[2]);
    }

    @Test public void onlyOwnPriorStatementsCanResolveAnAbortedInsert() throws Exception {
        try(var fixture=fixture()) {
            var database=fixture.database(); DuckDbIdentities.bind(database);
            assertEquals(fixture.baseline(),DuckDbIdentities.observedRow(database,1,"1",0,0,0));
            assertEquals(fixture.inserted(),DuckDbIdentities.observedRow(database,1,"2",1,0,0));
            assertEquals(fixture.aborted(),DuckDbIdentities.observedRow(database,1,"3",1,2,fixture.beforeQuery()+1));
            assertThrows(SQLException.class,()->DuckDbIdentities.observedRow(database,1,"3",2,0,0));
            assertThrows(SQLException.class,()->DuckDbIdentities.observedRow(database,1,"3",1,2,fixture.beforeQuery()));
        }
    }

    @Test public void refusesIncompleteScopesAndDisagreementWithNativeIdentities() throws Exception {
        for(String corrupt:List.of(
                "DELETE FROM sibench_mutation_statements WHERE mutation_id=1 AND event='complete'",
                "DELETE FROM sibench_mutation_statements WHERE mutation_id=1",
                "UPDATE sibench_mutation_statements SET statement_outcome='failed' WHERE mutation_id=1",
                "UPDATE sibench_mutation_statements SET affected_rows=0 WHERE mutation_id=1 AND event='complete'",
                "UPDATE sibench_mutation_statements SET columns=[] WHERE mutation_id=1",
                "UPDATE sibench_mutation_statements SET rows=[{'physical_row':100,'values_sql':['1','3']}] WHERE mutation_id=2 AND event='begin'",
                "UPDATE sibench_mutation_statements SET rows=[{'physical_row':100,'values_sql':['999','bad key']}] WHERE mutation_id=2 AND event='begin'")) {
            try(var fixture=fixture()) {
                sql(fixture.database(),corrupt);
                assertThrows(corrupt,SQLException.class,()->DuckDbIdentities.bind(fixture.database()));
                assertTrue(fixture.database().getAutoCommit());
                try(var query=fixture.database().createStatement();var rows=query.executeQuery(
                        "SELECT count(*) FROM duckdb_tables() WHERE table_name='sibench_insert_identities'")) {
                    assertTrue(rows.next());assertEquals(0,rows.getLong(1));
                }
            }
        }
    }

    @Test public void acceptsChunkedMergeScopesWithoutAssigningNewRowIds() throws Exception {
        try(var fixture=fixture()) {
            var database=fixture.database();
            sql(database,"INSERT INTO sibench_mutation_statements SELECT * REPLACE('rows' AS event) "
                    + "FROM sibench_mutation_statements WHERE event='begin'");
            sql(database,"UPDATE sibench_mutation_statements SET rows=[] WHERE event='begin'");
            sql(database,"UPDATE sibench_mutation_statements SET kind='merge'");
            DuckDbIdentities.bind(database);
            assertEquals(fixture.inserted(),DuckDbIdentities.observedRow(database,1,"2",1,0,0));
            assertEquals(fixture.aborted(),DuckDbIdentities.observedRow(database,1,"3",1,2,fixture.beforeQuery()+1));
        }
    }

    @Test public void quotesVarcharTokensAndRejectsUnsupportedNulKeys() {
        assertEquals("'quoted''雪'",DuckDbIdentities.textKey("quoted'雪"));
        assertEquals("'line\nnext'",DuckDbIdentities.textKey("line\nnext"));
        assertThrows(IllegalArgumentException.class,()->DuckDbIdentities.textKey("\0"));
        assertThrows(IllegalArgumentException.class,()->DuckDbIdentities.textKey("x\0y"));
    }
}
