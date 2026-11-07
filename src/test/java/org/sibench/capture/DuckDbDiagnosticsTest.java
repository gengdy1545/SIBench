package org.sibench.capture;

import static org.junit.Assert.*;
import com.google.gson.Gson;
import java.sql.Connection;
import java.sql.DriverManager;
import org.junit.Test;
import org.sibench.config.BenchmarkSelection.Mode;

public class DuckDbDiagnosticsTest {
    private static void sql(Connection db,String text)throws Exception {try(var q=db.createStatement()){q.execute(text);}}

    @Test public void separatesAbandonedProbesAndUnknownFailedPrefixesFromLogicalOperations()throws Exception {
        try(var db=DriverManager.getConnection("jdbc:duckdb:")) {
            sql(db,"CREATE TEMP TABLE sibench_statements(connection_id BIGINT,transaction_id BIGINT,query_id BIGINT,outcome VARCHAR)");
            sql(db,"INSERT INTO sibench_statements VALUES(1,1,1,'completed'),(1,1,2,'completed'),(1,1,3,'failed')");
            sql(db,"CREATE TEMP TABLE sibench_index(connection_id BIGINT,transaction_id BIGINT,query_id BIGINT,schema_name VARCHAR,"
                    +"adopted BOOLEAN,scan_calls BIGINT,observed_candidates BIGINT,probe_time_ns BIGINT)");
            sql(db,"INSERT INTO sibench_index VALUES(1,1,1,'main',true,1,2,10),(1,1,2,'main',false,2,7,30),"
                    +"(1,1,2,'main',false,1,3,5),(1,1,3,'sibench_history',true,10,100,1000),(9,9,9,'main',true,10,100,1000)");
            sql(db,"CREATE TEMP TABLE sibench_mutation_statements(connection_id BIGINT,transaction_id BIGINT,query_id BIGINT,"
                    +"mutation_id BIGINT,statement_outcome VARCHAR,schema_name VARCHAR,kind VARCHAR,event VARCHAR,requested_rows BIGINT,affected_rows BIGINT)");
            sql(db,"INSERT INTO sibench_mutation_statements VALUES"
                    +"(1,1,1,1,'completed','main','append','begin',1,-1),(1,1,1,1,'completed','main','append','complete',1,1),"
                    +"(1,1,3,2,'failed','main','append','begin',4,-1),(1,1,3,2,'failed','main','append','complete',4,4),"
                    +"(1,1,3,3,'failed','main','append','begin',2,-1),(1,1,3,3,'failed','main','append','failed',2,-1),"
                    +"(1,1,3,4,'failed','sibench_history','append','begin',100,-1)");
            var report=new Gson().toJsonTree(DuckDbDiagnostics.summarize(db,Mode.CAPTURE)).getAsJsonObject();
            var statements=report.getAsJsonArray("statements");
            assertEquals(2,statements.get(0).getAsJsonObject().get("statements").getAsInt());
            assertEquals(1,statements.get(0).getAsJsonObject().get("without_adopted_art").getAsInt());
            assertEquals(1,statements.get(1).getAsJsonObject().get("without_adopted_art").getAsInt());
            var attempts=report.getAsJsonArray("art_attempts");assertEquals(2,attempts.size());
            var abandoned=attempts.get(0).getAsJsonObject();assertFalse(abandoned.get("adopted").getAsBoolean());
            assertEquals(2,abandoned.get("attempts").getAsInt());assertEquals(3,abandoned.get("probe_calls").getAsInt());
            assertEquals(10,abandoned.get("sum_candidate_vector_maxima").getAsInt());assertEquals(35,abandoned.get("probe_time_ns").getAsInt());
            var failed=report.getAsJsonArray("mutation_scopes").get(1).getAsJsonObject();
            assertEquals("failed",failed.get("statement_outcome").getAsString());assertEquals(2,failed.get("scopes").getAsInt());
            assertEquals(6,failed.get("requested_rows").getAsInt());assertEquals(4,failed.get("completed_scope_rows").getAsInt());
            assertEquals(1,failed.get("scopes_without_completion").getAsInt());assertEquals(1,failed.get("failed_records").getAsInt());
            assertTrue(report.get("interpretation").toString().contains("Physical partial success is not measured"));
            sql(db,"DELETE FROM sibench_index");sql(db,"DELETE FROM sibench_statements");sql(db,"DELETE FROM sibench_mutation_statements");
            var empty=new Gson().toJsonTree(DuckDbDiagnostics.summarize(db,Mode.API)).getAsJsonObject();
            assertEquals("api",empty.get("mode").getAsString());assertEquals(0,empty.getAsJsonArray("statements").size());
            assertEquals(0,empty.getAsJsonArray("art_attempts").size());assertEquals(0,empty.getAsJsonArray("mutation_scopes").size());
        }
    }
}
