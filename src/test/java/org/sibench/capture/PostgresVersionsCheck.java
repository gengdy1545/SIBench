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

    /** An XID held by the projection for the whole workload would stop pruning of every benchmark table. */
    @Test public void projectionHoldsNoTransactionIdAcrossTheWorkload()throws Exception {
        String runtime=System.getProperty("sibench.postgres.runtime");assertNotNull(runtime);
        try(var cluster=PostgresDatabase.create(Path.of(runtime),temporary.getRoot().toPath().resolve("source"),org.sibench.generation.PostgresTestSettings.SETTINGS);
                var database=cluster.connect("projection");var observer=cluster.connect("observer");
                var transactions=new PostgresTransactions(temporary.getRoot().toPath().resolve("spool"),1L<<20,2048)) {
            database.setAutoCommit(false);NativeHistory.initialize(database,Engine.POSTGRES);database.commit();
            try(var versions=new PostgresVersions(database)) {
                assertNull(projectionXid(observer));
                for(long i=1;i<=1024;i++) {
                    var chunk=transactions.begin(1);chunk.csn=i;chunk.transaction=i;chunk.committed=true;
                    versions.begin(chunk);versions.finish(chunk);chunk.close();
                }
                assertNull(projectionXid(observer));
                try(var query=database.createStatement();var rows=query.executeQuery("SELECT count(*) FROM sibench_history.commits")) {
                    assertTrue(rows.next());assertEquals(1024,rows.getLong(1));
                }
            }
            database.commit();
        }
    }

    private static String projectionXid(java.sql.Connection observer)throws Exception {
        try(var query=observer.createStatement();var rows=query.executeQuery(
                "SELECT backend_xid::text FROM pg_stat_activity WHERE application_name='projection'")) {
            assertTrue(rows.next());String xid=rows.getString(1);assertFalse(rows.next());return xid;
        }
    }

    @Test public void growingCommitMappingsKeepAnIndexedPreparedLookup()throws Exception {
        String runtime=System.getProperty("sibench.postgres.runtime");assertNotNull(runtime);
        try(var cluster=PostgresDatabase.create(Path.of(runtime),temporary.getRoot().toPath().resolve("source"),org.sibench.generation.PostgresTestSettings.SETTINGS);
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
