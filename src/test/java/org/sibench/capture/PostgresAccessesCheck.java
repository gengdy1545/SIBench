package org.sibench.capture;

import static org.junit.Assert.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.generation.PostgresDatabase;
import org.sibench.schema.SourceSchema;
import org.sibench.trace.IndexKey;

/** Native accesses are compared with hand-checkable rows, not SQL result cardinalities. */
public class PostgresAccessesCheck {
    @Rule public TemporaryFolder temporary=new TemporaryFolder();
    private static void sql(Connection db,String text)throws Exception{try(var q=db.createStatement()){q.execute(text);}}
    private static List<Long> values(Connection db,String text)throws Exception {
        var values=new ArrayList<Long>();try(var q=db.createStatement();var rows=q.executeQuery(text)){while(rows.next())values.add(rows.getLong(1));}return values;
    }
    @Test public void projectsIndexVisibleRowsBeforeFilterAggregationAndAbort()throws Exception {
        String runtime=System.getProperty("sibench.postgres.runtime");assertNotNull("Set sibench.postgres.runtime",runtime);
        Path root=temporary.getRoot().toPath().resolve("source");
        try(var cluster=PostgresDatabase.create(Path.of(runtime),root)) {
            try(var setup=cluster.connect("setup")) {
                sql(setup,"CREATE TABLE sbtest1(id INTEGER PRIMARY KEY,k INTEGER,c CHAR(120),pad CHAR(60))");
                sql(setup,"INSERT INTO sbtest1 SELECT i,i/4,'value','pad' FROM generate_series(1,20) i");
                sql(setup,"CREATE INDEX k_1 ON sbtest1(k)");sql(setup,"VACUUM ANALYZE sbtest1");
            }
            try(var client=cluster.connect("sibench:accesses")) {
                sql(client,"SET enable_seqscan=off");sql(client,"SET enable_bitmapscan=off");
                assertEquals(List.of(4L),values(client,"SELECT count(*) FROM sbtest1 WHERE id BETWEEN 5 AND 8"));
                assertEquals(List.of(6L,8L,10L),values(client,"SELECT id FROM sbtest1 WHERE id>=5 AND id%2=0 ORDER BY id LIMIT 3"));
                client.setAutoCommit(false);sql(client,"UPDATE sbtest1 SET k=9 WHERE id=5");
                assertEquals(List.of(5L),values(client,"SELECT id FROM sbtest1 WHERE k=9"));client.rollback();client.setAutoCommit(true);
                assertEquals(List.of(),values(client,"SELECT id FROM sbtest1 WHERE k=9"));
            }
        }
        var catalog=IndexCatalog.fromSchema(SourceSchema.of("sysbench",Path.of("benchmark/sysbench"),Map.of("tables",1L)));
        var accesses=new ArrayList<PostgresAccesses.Access>();
        try(var files=Files.list(root.resolve("capture"))) {
            for(Path file:files.toList())PostgresAccesses.read(file,catalog,true,accesses::add);
        }
        assertEquals(5,accesses.size());
        var aggregate=accesses.get(0).projection();assertEquals(4,aggregate.count());
        assertEquals(IndexKey.ofInt(5),aggregate.parts().get(0).range().lower());
        assertEquals(IndexKey.ofInt(8),aggregate.parts().get(0).range().upper());
        var early=accesses.get(1).projection();assertEquals(6,early.count());
        assertEquals(6,early.parts().get(0).range().limit());
        assertEquals(1,accesses.get(2).projection().count());
        assertEquals(1,accesses.get(3).projection().count());
        assertEquals(0,accesses.get(4).projection().count());
        assertEquals(1,accesses.get(3).projection().parts().get(0).index());
    }

    @Test public void statementSnapshotsAdvanceInReadCommittedAndRemainFixedInRepeatableRead()throws Exception {
        String runtime=System.getProperty("sibench.postgres.runtime");assertNotNull("Set sibench.postgres.runtime",runtime);
        Path root=temporary.getRoot().toPath().resolve("snapshots");
        try(var cluster=PostgresDatabase.create(Path.of(runtime),root);
                var writer=cluster.connect("setup");var rc=cluster.connect("sibench:rc");var rr=cluster.connect("sibench:rr")) {
            sql(writer,"CREATE TABLE sbtest1(id INTEGER PRIMARY KEY,k INTEGER)");sql(writer,"INSERT INTO sbtest1 VALUES(1,10)");
            sql(rc,"SET enable_seqscan=off");sql(rr,"SET enable_seqscan=off");
            rc.setAutoCommit(false);rr.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);rr.setAutoCommit(false);
            assertEquals(List.of(10L),values(rc,"SELECT k FROM sbtest1 WHERE id=1"));
            assertEquals(List.of(10L),values(rr,"SELECT k FROM sbtest1 WHERE id=1"));
            sql(writer,"UPDATE sbtest1 SET k=20 WHERE id=1");
            assertEquals(List.of(20L),values(rc,"SELECT k FROM sbtest1 WHERE id=1"));
            assertEquals(List.of(10L),values(rr,"SELECT k FROM sbtest1 WHERE id=1"));rc.commit();rr.commit();
        }
        var catalog=IndexCatalog.fromSchema(SourceSchema.of("sysbench",Path.of("benchmark/sysbench"),Map.of("tables",1L)));
        try(var files=Files.list(root.resolve("capture"))) {
            var paths=files.toList();assertEquals(2,paths.size());
            for(Path file:paths) {
                var accesses=new ArrayList<PostgresAccesses.Access>();PostgresAccesses.read(file,catalog,true,accesses::add);
                assertEquals(2,accesses.size());
                var statements=new java.util.HashMap<Long,Long>();String[] application={null};
                PostgresFacts.read(file,e->{
                    application[0]=e.get("application").getAsString();
                    if(e.get("event").getAsString().equals("executor_start")&&e.get("depth").getAsInt()==1)
                        statements.put(e.get("req").getAsLong(),e.get("snapshot_csn").getAsLong());
                });
                for(var access:accesses)assertEquals(statements.get(access.query()).longValue(),access.snapshotCsn());
                if(application[0].equals("sibench:rc"))assertTrue(accesses.get(0).snapshotCsn()<accesses.get(1).snapshotCsn());
                else {assertEquals("sibench:rr",application[0]);assertEquals(accesses.get(0).snapshotCsn(),accesses.get(1).snapshotCsn());}
            }
        }
    }
}
