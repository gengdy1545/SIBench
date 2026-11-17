package org.sibench.generation;

import static org.junit.Assert.*;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import com.google.gson.JsonObject;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Explicit native check: -Dtest=PostgresHistoryCheck -Dsibench.postgres.runtime=/path/to/runtime. RowID identities
 * with their captured facts are checked by PostgresCaptureCheck.
 */
public class PostgresHistoryCheck {
    @Rule public TemporaryFolder temporary=new TemporaryFolder();
    private static void sql(Connection db,String text)throws SQLException {try(var q=db.createStatement()){q.execute(text);}}
    private static long number(Connection db,String text)throws SQLException {
        try(var q=db.createStatement();var r=q.executeQuery(text)){assertTrue(r.next());long value=r.getLong(1);assertFalse(r.next());return value;}
    }
    @Test public void loadedKeysNeedNoHeapFetchesAfterVacuum()throws Exception {
        String configured=System.getProperty("sibench.postgres.runtime");
        assertNotNull("Set sibench.postgres.runtime",configured);
        try(var cluster=PostgresDatabase.create(Path.of(configured),temporary.getRoot().toPath().resolve("visibility"),org.sibench.generation.PostgresTestSettings.SETTINGS);
                var setup=cluster.connect("visibility")) {
            sql(setup,"CREATE UNLOGGED TABLE wide_rows(id BIGINT PRIMARY KEY,payload TEXT)");
            sql(setup,"INSERT INTO wide_rows SELECT i,repeat('x',1000) FROM generate_series(1,10000) i");
            sql(setup,"CREATE SEQUENCE probe_ids");sql(setup,"ANALYZE wide_rows");
            String query="EXPLAIN (ANALYZE,FORMAT JSON) SELECT nextval('probe_ids'),id FROM wide_rows";
            assertEquals("Seq Scan",plan(setup,query).get("Node Type").getAsString());
            sql(setup,"VACUUM ANALYZE wide_rows");
            var visible=plan(setup,query);
            assertEquals("Index Only Scan",visible.get("Node Type").getAsString());
            assertEquals(10000,visible.get("Actual Rows").getAsLong());
            assertEquals(0,visible.get("Heap Fetches").getAsLong());
        }
    }
    private static JsonObject plan(Connection database,String sql)throws SQLException {
        try(var query=database.createStatement();var rows=query.executeQuery(sql)) {
            assertTrue(rows.next());
            return com.google.gson.JsonParser.parseString(rows.getString(1)).getAsJsonArray().get(0).getAsJsonObject().getAsJsonObject("Plan");
        }
    }
}
