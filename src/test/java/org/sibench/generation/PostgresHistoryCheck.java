package org.sibench.generation;

import static org.junit.Assert.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import com.google.gson.JsonObject;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.capture.PostgresFacts;
import org.sibench.config.Engine;

/** Explicit native check: -Dtest=PostgresHistoryCheck -Dsibench.postgres.runtime=/path/to/runtime. */
public class PostgresHistoryCheck {
    @Rule public TemporaryFolder temporary=new TemporaryFolder();
    private static void sql(Connection db,String text)throws SQLException {try(var q=db.createStatement()){q.execute(text);}}
    private static long number(Connection db,String text)throws SQLException {
        try(var q=db.createStatement();var r=q.executeQuery(text)){assertTrue(r.next());long value=r.getLong(1);assertFalse(r.next());return value;}
    }
    @Test public void loadedKeysNeedNoHeapFetchesAfterVacuum()throws Exception {
        String configured=System.getProperty("sibench.postgres.runtime");
        assertNotNull("Set sibench.postgres.runtime",configured);
        try(var cluster=PostgresDatabase.create(Path.of(configured),temporary.getRoot().toPath().resolve("visibility"));
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
    @Test public void historyAndCapturePreservePrefixesAbortsAndReinsertions()throws Exception {
        String configured=System.getProperty("sibench.postgres.runtime");
        assertNotNull("Set sibench.postgres.runtime to a freshly built instrumented runtime",configured);
        Path root=temporary.getRoot().toPath().resolve("source");long aborted,committed,first,original,replacement;
        try(var cluster=PostgresDatabase.create(Path.of(configured),root)) {
            try(var setup=cluster.connect("setup")) {
                assertEquals(1,number(setup,"SELECT (current_setting('wal_level')='minimal' "
                        +"AND current_setting('max_wal_senders')='0' AND current_setting('max_parallel_maintenance_workers')='0' "
                        +"AND current_setting('fsync')='off' "
                        +"AND current_setting('synchronous_commit')='off' AND current_setting('full_page_writes')='off')::int"));
                PostgresDatabase.unloggedTables(setup);
                sql(setup,"CREATE TABLE items(id BIGINT PRIMARY KEY,value BIGINT)");
                assertEquals(1,number(setup,"SELECT (relpersistence='u')::int FROM pg_class WHERE oid='items'::regclass"));
                sql(setup,"INSERT INTO items VALUES(1,10),(2,20)");
                PostgresDatabase.finishLoad(setup);
                assertEquals(0,number(setup,"SELECT count(*) FROM pg_event_trigger WHERE evtname='sibench_unlogged_tables'"));
                setup.setAutoCommit(false);NativeHistory.initialize(setup,Engine.POSTGRES);
                NativeHistory.install(setup,Engine.POSTGRES,1,"public","items","id");setup.commit();
                assertEquals(0,number(setup,"SELECT count(*) FROM pg_class c JOIN pg_namespace n ON c.relnamespace=n.oid WHERE n.nspname='sibench_history' AND c.relkind='r' AND c.relpersistence<>'u'"));
                original=number(setup,"SELECT _sib_row_id FROM sibench_history.identities_1 WHERE id=1");
            }
            try(var writer=cluster.connect("sibench:writer");var observer=cluster.connect("observer")) {
                sql(writer,"SET enable_seqscan=off");writer.setAutoCommit(false);
                sql(writer,"UPDATE items SET value=11 WHERE id=1");
                aborted=number(writer,"SELECT txid_current()");
                assertEquals(11,number(writer,"SELECT value FROM items WHERE id=1"));
                assertEquals(10,number(observer,"SELECT value FROM items WHERE id=1"));
                sql(writer,"INSERT INTO items VALUES(3,30)");
                assertEquals(30,number(writer,"SELECT value FROM items WHERE id=3"));
                assertEquals(0,number(observer,"SELECT count(*) FROM items WHERE id=3"));
                assertThrows(SQLException.class,()->sql(writer,"INSERT INTO items VALUES(4,40),(1,99)"));
                writer.rollback();
                assertEquals(0,number(observer,"SELECT count(*) FROM sibench_history.rows_1"));
                assertEquals(0,number(observer,"SELECT count(*) FROM items WHERE id=3"));
                sql(writer,"UPDATE items SET value=12 WHERE id=1");
                committed=number(writer,"SELECT txid_current()");
                first=number(writer,"SELECT _sib_query FROM sibench_history.rows_1 WHERE id=1");
                sql(writer,"UPDATE items SET value=13 WHERE id=1");writer.commit();
                NativeHistory.recordCommit(observer,committed,1);
                sql(writer,"DELETE FROM items WHERE id=1");long deleted=number(writer,"SELECT txid_current()");writer.commit();
                NativeHistory.recordCommit(observer,deleted,2);
                sql(writer,"INSERT INTO items VALUES(1,14)");long inserted=number(writer,"SELECT txid_current()");writer.commit();
                NativeHistory.recordCommit(observer,inserted,3);
                replacement=number(observer,"SELECT _sib_row_id FROM sibench_history.identities_1 WHERE id=1 AND _sib_tx<>0");
                assertNotEquals(original,replacement);
                assertThrows(SQLException.class,()->sql(writer,"UPDATE items SET id=3-id"));writer.rollback();
                observer.setAutoCommit(false);NativeHistory.seal(observer);observer.commit();
                assertEquals(10L,NativeHistory.readRow(observer,1,original,0,0,0).orElseThrow().get("value"));
                assertEquals(12L,NativeHistory.readRow(observer,1,original,0,committed,first).orElseThrow().get("value"));
                assertEquals(13L,NativeHistory.readRow(observer,1,original,1,0,0).orElseThrow().get("value"));
                assertTrue(NativeHistory.readRow(observer,1,original,2,0,0).isEmpty());
                assertTrue(NativeHistory.readRow(observer,1,original,3,0,0).isEmpty());
                assertEquals(14L,NativeHistory.readRow(observer,1,replacement,3,0,0).orElseThrow().get("value"));
                assertThrows(SQLException.class,()->sql(writer,"UPDATE items SET value=99 WHERE id=1"));writer.rollback();
            }
        }
        var events=new ArrayList<JsonObject>();
        try(var files=Files.list(root.resolve("capture"))) {
            for(Path file:files.toList())PostgresFacts.read(file,events::add);
        }
        assertTrue(events.stream().anyMatch(e->"row_change".equals(e.get("event").getAsString())&&e.get("xid").getAsLong()==aborted));
        for(var event:events)if(event.get("event").getAsString().equals("row_change"))
            for(String image:java.util.List.of("old","new"))if(!event.get(image).isJsonNull())
                assertEquals(java.util.Set.of("id"),event.getAsJsonObject(image).keySet());
        assertTrue(events.stream().anyMatch(e->"transaction_abort".equals(e.get("event").getAsString())&&e.get("xid").getAsLong()==aborted));
        assertTrue(events.stream().anyMatch(e->"row_identity".equals(e.get("event").getAsString())&&e.get("xid").getAsLong()==aborted
                &&e.get("primary_key").getAsString().equals("3")));
        assertFalse(events.stream().anyMatch(e->"row_identity".equals(e.get("event").getAsString())&&e.get("xid").getAsLong()==aborted
                &&e.get("primary_key").getAsString().equals("4")));
        assertTrue(events.stream().anyMatch(e->"scan_summary".equals(e.get("event").getAsString())&&e.get("xid").getAsLong()==aborted));
        long failedRequest=events.stream().filter(e->"executor_start".equals(e.get("event").getAsString())
                &&e.get("depth").getAsInt()==1&&e.get("sql").getAsString().contains("VALUES(4,40),(1,99)"))
                .findFirst().orElseThrow().get("req").getAsLong();
        assertTrue(events.stream().anyMatch(e->"index_insert_attempt".equals(e.get("event").getAsString())&&e.get("req").getAsLong()==failedRequest));
        assertFalse(events.stream().anyMatch(e->"executor_end".equals(e.get("event").getAsString())
                &&e.get("depth").getAsInt()==1&&e.get("req").getAsLong()==failedRequest));
        assertFalse(events.stream().anyMatch(e->"scan_summary".equals(e.get("event").getAsString())&&e.get("relation").getAsString().startsWith("history_")));
        assertFalse(Files.exists(root.resolve("data/postmaster.pid")));
        assertThrows(java.io.IOException.class,()->PostgresDatabase.create(Path.of(configured),root));
    }
}
