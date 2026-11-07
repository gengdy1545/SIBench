package org.sibench.capture;

import static org.junit.Assert.*;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import org.junit.Test;
import org.sibench.generation.NativeHistory;
import org.sibench.generation.DuckDbHostFixture;

public class DuckDbAccessesCheck extends DuckDbHostFixture {
    private static final List<IndexCatalog.Definition> CATALOG=List.of(
            new IndexCatalog.Definition(1,"items",0,"canonical_primary_name","id","int"),
            new IndexCatalog.Definition(1,"items",1,"items_k","k","int"));
    private static void sql(Connection db,String text)throws Exception {try(var q=db.createStatement()){q.execute(text);}}
    private static long number(Connection db,String text)throws Exception {
        try(var q=db.createStatement();var r=q.executeQuery(text)){assertTrue(r.next());long n=r.getLong(1);assertFalse(r.next());return n;}
    }
    private static DuckDbStatements.Session marker(Connection db,long count)throws Exception {
        try(var q=db.createStatement();var r=q.executeQuery("SELECT current_connection_id(),current_query_id()")) {
            assertTrue(r.next());var session=new DuckDbStatements.Session(r.getLong(1),r.getLong(2),count);assertFalse(r.next());return session;
        }
    }
    private Path prepare(Connection db)throws Exception {
        return prepare(db,3);
    }
    private Path prepare(Connection db,int rows)throws Exception {
        sql(db,"CREATE TABLE items(id BIGINT PRIMARY KEY,k BIGINT)");
        sql(db,rows==3?"INSERT INTO items VALUES(10,1),(20,1),(30,2)":"INSERT INTO items SELECT range,1 FROM range("+rows+")");
        sql(db,"CREATE INDEX items_k ON items(k)");
        db.setAutoCommit(false);NativeHistory.initialize(db, org.sibench.config.Engine.DUCKDB);NativeHistory.install(db, org.sibench.config.Engine.DUCKDB,1,"main","items","id");db.commit();db.setAutoCommit(true);
        Path raw=root.resolve("native.csv");
        sql(db,"CALL enable_logging(['SibenchQuery','SibenchTransaction','SibenchMutation',"
                +"'SibenchIndex','SibenchIndexRows','SibenchConsumption'],storage_path='"+raw+"')");
        return raw;
    }
    private void bind(Connection db,Path raw,DuckDbStatements.Session session)throws Exception {
        sql(db,"CALL disable_logging()");DuckDbFacts.load(db,raw);DuckDbStatements.bind(db,List.of(session));
        DuckDbTransactions.bind(db,"data");DuckDbIdentities.bind(db);
    }
    @Test public void bindsPrimarySecondaryAndLocalRowsBeforeAbortWithoutUsingSqlAggregateCounts()throws Exception {
        try(var observer=connect()) {
            Path raw=prepare(observer);DuckDbStatements.Session session;long local;
            try(var worker=connect()) {
                session=marker(worker,6);worker.setAutoCommit(false);
                assertEquals(30,number(worker,"SELECT sum(id) FROM items WHERE k=1"));
                sql(worker,"UPDATE items SET k=1 WHERE id=30");sql(worker,"INSERT INTO items VALUES(40,1)");
                local=number(worker,"SELECT _sib_row_id FROM sibench_history.identities_1 WHERE id=40");
                assertEquals(100,number(worker,"SELECT sum(id) FROM items WHERE k=1"));worker.rollback();
                assertEquals(30,number(worker,"SELECT sum(id) FROM items WHERE k=1"));worker.commit();
            }
            bind(observer,raw,session);DuckDbAccesses.bind(observer,CATALOG);
            assertEquals(3,number(observer,"SELECT count(*) FROM sibench_accesses WHERE index_id=1"));
            try(var q=observer.createStatement();var r=q.executeQuery("SELECT a.version,count(r.key_sql),sum(CAST(r.key_sql AS BIGINT)) "
                    +"FROM sibench_accesses a LEFT JOIN sibench_access_keys r USING(access_id) WHERE a.index_id=1 "
                    +"GROUP BY a.access_id,a.version,a.query_id ORDER BY a.query_id")) {
                assertTrue(r.next());assertEquals(1,r.getLong(1));assertEquals(2,r.getLong(2));assertEquals(30,r.getLong(3));
                assertTrue(r.next());assertEquals(1,r.getLong(1));assertEquals(4,r.getLong(2));assertEquals(100,r.getLong(3));
                assertTrue(r.next());assertEquals(2,r.getLong(1));assertEquals(2,r.getLong(2));assertEquals(30,r.getLong(3));assertFalse(r.next());
            }
            assertEquals(local,DuckDbIdentities.observedRow(observer,1,"40",0,1,Long.MAX_VALUE));
            assertEquals(1,number(observer,"SELECT count(*) FROM sibench_accesses WHERE index_id=0"));
            assertEquals(1,number(observer,"SELECT count(*) FROM sibench_transactions WHERE outcome='rollback'"));
        }
    }
    @Test public void keepsCompleteIndexOutputWhenSqlLimitConsumesOnlyOneRow()throws Exception {
        try(var observer=connect()) {
            Path raw=prepare(observer);DuckDbStatements.Session session;
            try(var worker=connect()) {
                session=marker(worker,1);assertEquals(10,number(worker,"SELECT id FROM items WHERE k=1 LIMIT 1"));
            }
            bind(observer,raw,session);
            var diagnostics=DuckDbDiagnostics.summarize(observer,org.sibench.config.BenchmarkSelection.Mode.CAPTURE);
            assertFalse(((List<?>)diagnostics.get("art_attempts")).isEmpty());
            DuckDbAccesses.bind(observer,CATALOG);
            assertEquals(2,number(observer,"SELECT count(*) FROM sibench_access_keys"));
            assertEquals(30,number(observer,"SELECT sum(CAST(key_sql AS BIGINT)) FROM sibench_access_keys"));
        }
    }
    @Test public void rejectsUnreadBaseBatches()throws Exception {
        try(var observer=connect()) {
            Path raw=prepare(observer,5000);DuckDbStatements.Session session;
            try(var worker=connect()) {
                sql(worker,"SET threads=1");sql(worker,"SET index_scan_max_count=10000");
                session=marker(worker,1);assertEquals(0,number(worker,"SELECT id FROM items WHERE id<4000 LIMIT 1"));
            }
            bind(observer,raw,session);
            assertEquals(1,number(observer,"SELECT count(*) FROM sibench_index WHERE adopted AND schema_name='main'"));
            assertEquals(2048,number(observer,"SELECT sum(len(primary_keys_sql)) FROM sibench_index_rows WHERE phase='storage'"));
            var failure=assertThrows(SQLException.class,()->DuckDbAccesses.bind(observer,CATALOG));
            assertTrue(failure.getMessage(),failure.getMessage().contains("Unsupported ART early stop"));
        }
    }
    @Test public void keepsACompleteVectorWhenLimitRevisitsItsShortenedInput()throws Exception {
        try(var observer=connect()) {
            Path raw=prepare(observer,5000);DuckDbStatements.Session session;
            try(var worker=connect()) {
                sql(worker,"SET threads=1");sql(worker,"SET index_scan_max_count=10000");
                session=marker(worker,1);assertEquals(0,number(worker,"SELECT id FROM items WHERE id<2048 LIMIT 1"));
            }
            bind(observer,raw,session);
            assertTrue(number(observer,"SELECT count(*) FROM sibench_consumption WHERE stage='limit' AND input_rows=1")>0);
            DuckDbAccesses.bind(observer,CATALOG);
            assertEquals(2048,number(observer,"SELECT count(*) FROM sibench_access_keys"));
            assertEquals(2047L*2048/2,number(observer,"SELECT sum(CAST(key_sql AS BIGINT)) FROM sibench_access_keys"));
        }
    }
    @Test public void rejectsUnreadOwnWritesAfterCompleteBaseBatch()throws Exception {
        try(var observer=connect()) {
            Path raw=prepare(observer);DuckDbStatements.Session session;
            try(var worker=connect()) {
                session=marker(worker,2);worker.setAutoCommit(false);
                sql(worker,"INSERT INTO items VALUES(40,1)");
                assertEquals(10,number(worker,"SELECT id FROM items WHERE k=1 LIMIT 1"));worker.rollback();
            }
            bind(observer,raw,session);
            var failure=assertThrows(SQLException.class,()->DuckDbAccesses.bind(observer,CATALOG));
            assertTrue(failure.getMessage(),failure.getMessage().contains("Unsupported ART early stop"));
        }
    }

    @Test public void preservesTheUniquePrimaryMatchReadBeforeAWriteConflict()throws Exception {
        try(var observer=connect()) {
            Path raw=prepare(observer,5000);DuckDbStatements.Session first,second;
            sql(observer,"DROP INDEX items_k");
            try(var blocker=connect();var worker=connect()) {
                first=marker(blocker,1);second=marker(worker,2);
                blocker.setAutoCommit(false);worker.setAutoCommit(false);
                sql(worker,"INSERT INTO items VALUES(6000,1)");
                try(var update=blocker.prepareStatement("UPDATE items SET k=k+? WHERE id=?");
                        var conflict=worker.prepareStatement("UPDATE items SET k=k+? WHERE id=?")) {
                    update.setLong(1,2);update.setLong(2,10);update.executeUpdate();
                    conflict.setLong(1,3);conflict.setLong(2,10);
                    var failure=assertThrows(SQLException.class,conflict::executeUpdate);
                    assertTrue(failure.getMessage(),failure.getMessage().contains("Conflict on"));
                }
                worker.rollback();blocker.rollback();
            }
            sql(observer,"CALL disable_logging()");DuckDbFacts.load(observer,raw);
            DuckDbStatements.bind(observer,List.of(first,second));DuckDbTransactions.bind(observer,"data");DuckDbIdentities.bind(observer);
            assertTrue(number(observer,"SELECT count(*) FROM sibench_index_rows WHERE phase='closed' AND NOT source_drained")>0);
            DuckDbAccesses.bind(observer,List.of(CATALOG.get(0)));
            assertEquals(2,number(observer,"SELECT count(*) FROM sibench_access_keys WHERE key_sql='10'"));
            assertEquals(2,number(observer,"SELECT count(*) FROM sibench_transactions WHERE outcome='rollback'"));
            assertEquals(1,number(observer,"SELECT k FROM items WHERE id=10"));
        }
    }

    @Test public void parallelArtUpdateDoesNotReadItsOwnNewRow()throws Exception {
        try(var observer=connect()) {
            prepare(observer,5000);sql(observer,"DROP INDEX items_k");
            sql(observer,"CALL disable_logging()");sql(observer,"SET threads=8");
            var executor=java.util.concurrent.Executors.newFixedThreadPool(32);
            var start=new java.util.concurrent.CountDownLatch(1);var ready=new java.util.concurrent.CountDownLatch(32);
            var failed=new java.util.concurrent.atomic.AtomicBoolean();
            var tasks=new java.util.ArrayList<java.util.concurrent.Future<?>>();
            try {
                for(int client=0;client<32;client++) {
                    final int key=10+client;
                    tasks.add(executor.submit(()->{
                        try(var worker=connect();var update=worker.prepareStatement("UPDATE items SET k=k+? WHERE id=?")) {
                            sql(worker,"PRAGMA verify_parallelism");worker.setAutoCommit(false);ready.countDown();start.await();
                            for(int attempt=0;attempt<100&&!failed.get();attempt++) {
                                sql(worker,"INSERT INTO items VALUES("+(6000+key)+",1)");
                                update.setLong(1,1);update.setLong(2,key);
                                assertEquals("A statement must not revisit its own replacement row",1,update.executeUpdate());
                                assertEquals(2,number(worker,"SELECT k FROM items WHERE id="+key));worker.rollback();
                            }
                        }catch(Exception|AssertionError error){failed.set(true);throw new RuntimeException(error);}
                        return null;
                    }));
                }
                assertTrue(ready.await(5,java.util.concurrent.TimeUnit.SECONDS));start.countDown();
                for(var task:tasks)task.get(20,java.util.concurrent.TimeUnit.SECONDS);
            }finally {
                failed.set(true);start.countDown();executor.shutdownNow();
                assertTrue(executor.awaitTermination(5,java.util.concurrent.TimeUnit.SECONDS));
            }
        }
    }
}
