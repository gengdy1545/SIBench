package org.sibench.capture;

import static org.junit.Assert.*;
import java.nio.file.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.config.Engine;
import org.sibench.generation.NativeHistory;
import org.sibench.generation.PostgresDatabase;

/** Native regression for a lookup prepared while the analyzed mapping table is still empty. */
public class PostgresVersionsCheck {
    @Rule public TemporaryFolder temporary=new TemporaryFolder();

    @Test public void growingCommitMappingsKeepAnIndexedPreparedLookup()throws Exception {
        String runtime=System.getProperty("sibench.postgres.runtime");assertNotNull(runtime);
        try(var cluster=PostgresDatabase.create(Path.of(runtime),temporary.getRoot().toPath().resolve("source"));
                var database=cluster.connect("projection");var query=database.createStatement()) {
            database.setAutoCommit(false);NativeHistory.initialize(database,Engine.POSTGRES);database.commit();
            database.setAutoCommit(true);query.execute("VACUUM ANALYZE");database.setAutoCommit(false);
            try(var versions=new PostgresVersions(database)) {
                query.execute("SET LOCAL plan_cache_mode=force_generic_plan");
                query.execute("PREPARE lookup(bigint) AS SELECT commit_version FROM sibench_history.commits WHERE transaction_id=$1");
                try(var rows=query.executeQuery("EXECUTE lookup(100000)")){assertFalse(rows.next());}
                query.execute("INSERT INTO sibench_history.commits SELECT n,n,n FROM generate_series(1,100000) n");
                try(var rows=query.executeQuery("EXECUTE lookup(100000)")) {
                    assertTrue(rows.next());assertEquals(100000,rows.getLong(1));assertFalse(rows.next());
                }
                try(var rows=query.executeQuery("EXPLAIN (ANALYZE,FORMAT JSON) EXECUTE lookup(100000)")) {
                    assertTrue(rows.next());
                    var plan=com.google.gson.JsonParser.parseString(rows.getString(1)).getAsJsonArray()
                            .get(0).getAsJsonObject().getAsJsonObject("Plan");
                    assertTrue(plan.toString(),plan.has("Index Name"));
                    assertEquals("commits_pkey",plan.get("Index Name").getAsString());
                    assertEquals(1,plan.get("Actual Rows").getAsInt());
                }
            }
            database.rollback();
            try(var source=cluster.connect("source-planner");var check=source.createStatement();
                    var rows=check.executeQuery("SHOW enable_seqscan")) {
                assertTrue(rows.next());assertEquals("on",rows.getString(1));
            }
        }
    }
}
