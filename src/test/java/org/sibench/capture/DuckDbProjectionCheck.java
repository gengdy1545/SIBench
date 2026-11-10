package org.sibench.capture;

import static org.junit.Assert.*;
import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.sibench.adapters.IndexExecutor;
import org.sibench.adapters.VersionedIndexBackend;
import org.sibench.adapters.proto.Status;
import org.sibench.adapters.proto.TraceRecord;
import org.sibench.adapters.proto.ValueKind;
import org.sibench.generation.NativeHistory;
import org.sibench.generation.DuckDbHostFixture;
import org.sibench.trace.IndexKey;

public class DuckDbProjectionCheck extends DuckDbHostFixture {
    private static final List<IndexCatalog.Definition> CATALOG=List.of(
            new IndexCatalog.Definition(1,"items",0,"canonical_primary","id","int"),
            new IndexCatalog.Definition(1,"items",1,"items_k","k","int"));
    private record Expected(long key,long row,long version) {}
    private static void sql(Connection db,String text)throws Exception {try(var q=db.createStatement()){q.execute(text);}}
    private static long number(Connection db,String text)throws Exception {
        try(var q=db.createStatement();var r=q.executeQuery(text)){assertTrue(r.next());long value=r.getLong(1);assertFalse(r.next());return value;}
    }
    @Test public void replaysNativeSearchesAndMutationsWithAbortAndHistoricalBaseline()throws Exception {
        try(var db=connect()) {
            sql(db,"CREATE TABLE items(id BIGINT PRIMARY KEY,k BIGINT)");sql(db,"INSERT INTO items VALUES(10,1),(20,1),(30,2)");
            sql(db,"CREATE INDEX items_k ON items(k)");db.setAutoCommit(false);NativeHistory.initialize(db, org.sibench.config.Engine.DUCKDB);
            NativeHistory.install(db, org.sibench.config.Engine.DUCKDB,1,"main","items","id");db.commit();db.setAutoCommit(true);
            long id10=number(db,"SELECT _sib_row_id FROM sibench_history.identities_1 WHERE id=10");
            long id20=number(db,"SELECT _sib_row_id FROM sibench_history.identities_1 WHERE id=20");
            long id30=number(db,"SELECT _sib_row_id FROM sibench_history.identities_1 WHERE id=30");
            var baseline=root.resolve("baseline.pb.zst");var baseWriter=new ProtoTrace.Writer(baseline);
            db.setAutoCommit(false);
            try(baseWriter){assertEquals(java.util.Map.of("items",3L),NativeBaseline.write(db,org.sibench.config.Engine.DUCKDB,CATALOG,baseWriter));}
            assertEquals(6,baseWriter.count());db.commit();db.setAutoCommit(true);
            var raw=root.resolve("native.csv");sql(db,"CALL enable_logging(['SibenchQuery','SibenchTransaction','SibenchMutation',"
                    +"'SibenchIndex','SibenchIndexRows','SibenchConsumption'],storage_path='"+raw+"')");
            DuckDbStatements.Session session;long id40;
            try(var worker=connect()) {
                try(var q=worker.createStatement();var r=q.executeQuery("SELECT current_connection_id(),current_query_id()")) {
                    assertTrue(r.next());session=new DuckDbStatements.Session(r.getLong(1),r.getLong(2),10);assertFalse(r.next());
                }
                worker.setAutoCommit(false);
                assertEquals(30,number(worker,"SELECT sum(id) FROM items WHERE k=1"));
                assertEquals(40,number(worker,"SELECT sum(id) FROM items WHERE id IN (10,30)"));
                sql(worker,"UPDATE items SET k=1 WHERE id=30");sql(worker,"INSERT INTO items VALUES(40,1)");
                id40=number(worker,"SELECT _sib_row_id FROM sibench_history.identities_1 WHERE id=40");
                assertEquals(100,number(worker,"SELECT sum(id) FROM items WHERE k=1"));worker.rollback();
                sql(worker,"UPDATE items SET k=9 WHERE id=10");worker.commit();
                sql(worker,"DELETE FROM items WHERE id=20");worker.commit();
                sql(worker,"INSERT INTO items VALUES(20,5)");worker.commit();
                assertEquals(0,number(worker,"SELECT count(*) FROM items WHERE k=1"));worker.commit();
            }
            sql(db,"CALL disable_logging()");DuckDbFacts.load(db,raw);DuckDbStatements.bind(db,List.of(session));
            DuckDbTransactions.bind(db,"data");DuckDbIdentities.bind(db);DuckDbAccesses.bind(db,CATALOG);DuckDbMutations.bind(db);
            assertEquals(5,number(db,"SELECT count(*) FROM sibench_changes"));
            assertEquals(2,number(db,"SELECT count(*) FROM sibench_changes WHERE version=1"));
            var expected=new HashMap<Long,List<Expected>>();
            try(var q=db.createStatement();var r=q.executeQuery("SELECT a.raw_record,a.version,s.sql_text,a.query_id FROM sibench_accesses a "
                    +"JOIN sibench_statements s USING(connection_id,transaction_id,query_id) ORDER BY a.query_id")) {
                int aggregate=0;
                while(r.next()) {
                    String text=r.getString(3);List<Expected> wanted;
                    if(text.contains(" IN "))wanted=List.of(new Expected(10,id10,0),new Expected(30,id30,0));
                    else if(text.startsWith("SELECT sum")) wanted=aggregate++==0?List.of(new Expected(10,id10,0),new Expected(20,id20,0)):
                            List.of(new Expected(10,id10,0),new Expected(20,id20,0),new Expected(30,id30,1),new Expected(40,id40,1));
                    else if(text.contains("id=30"))wanted=List.of(new Expected(30,id30,0));
                    else if(text.contains("id=10"))wanted=List.of(new Expected(10,id10,0));
                    else if(text.contains("id=20"))wanted=List.of(new Expected(20,id20,0));
                    else {assertTrue(text,text.startsWith("SELECT count"));wanted=List.of();}
                    expected.put(r.getLong(1),wanted);
                }
            }
            assertEquals(7,expected.size());
            var workload=root.resolve("workload.pb.zst");var writer=new ProtoTrace.Writer(workload);
            try(writer){assertEquals(30,DuckDbProjection.write(db,true,writer));}
            try(var backend=VersionedIndexBackend.create(root.resolve("index"),ValueKind.ROW_ID);var index=new IndexExecutor(backend)) {
                long[] requests={0},searches={0},aborts={0};
                var consumer=new IndexOperationTrace.Consumer(){
                    @Override public void accept(IndexOperationTrace.Operation operation){fail("Missing stored service request");}
                    @Override public void accept(TraceRecord record,List<IndexOperationTrace.Operation> operations)throws java.io.IOException {
                        if(record.hasBegin())return;
                        if(record.hasEnd()){if(record.getEnd().getAborted())aborts[0]++;return;}
                        try {
                            var request=record.getRequest().toBuilder().setId(++requests[0]).build();
                            var response=index.submit(request).get(5,TimeUnit.SECONDS);assertEquals(response.getMessage(),Status.OK,response.getStatus());
                            if(request.hasSearch()||request.hasGroup()) {
                                var wanted=expected.get(operations.get(0).sourceEvent());assertNotNull(wanted);assertEquals(wanted.size(),response.getResultsCount());
                                for(int i=0;i<wanted.size();i++) {
                                    var actual=response.getResults(i);var e=wanted.get(i);
                                    assertArrayEquals(IndexKey.ofInt(e.key()).bytes(),actual.getPrimaryKey().toByteArray());
                                    assertEquals(e.row(),actual.getValue().getRowId());assertEquals(e.version(),actual.getVersion());
                                }
                                searches[0]++;
                            }
                        }catch(Exception failure){throw new java.io.IOException(failure);}
                    }
                };
                ProtoTrace.read(baseline,baseWriter.sha256(),baseWriter.logicalSha256(),consumer);
                ProtoTrace.read(workload,writer.sha256(),writer.logicalSha256(),consumer);
                assertEquals(6,searches[0]);assertEquals(1,aborts[0]);assertEquals(19,requests[0]);
            }
        }
    }

    @Test public void retainsNativeFailedStatementFactsButExportsNoAbortedWritesOrLocalReads()throws Exception {
        try(var db=connect()) {
            sql(db,"CREATE TABLE items(id BIGINT PRIMARY KEY,k BIGINT)");sql(db,"CREATE INDEX items_k ON items(k)");
            sql(db,"INSERT INTO items VALUES(10,100),(20,200)");
            sql(db,"CREATE TABLE guard(id BIGINT)");db.setAutoCommit(false);NativeHistory.initialize(db, org.sibench.config.Engine.DUCKDB);
            NativeHistory.install(db, org.sibench.config.Engine.DUCKDB,1,"main","items","id");db.commit();db.setAutoCommit(true);
            sql(db,"CREATE TRIGGER z_reject AFTER INSERT ON items REFERENCING NEW TABLE AS inserted "
                    +"FOR EACH STATEMENT INSERT INTO guard SELECT CASE WHEN id=2 THEN error('late failure') ELSE id END FROM inserted");
            var raw=root.resolve("native.csv");sql(db,"CALL enable_logging(['SibenchQuery','SibenchTransaction','SibenchMutation',"
                    +"'SibenchIndex','SibenchIndexRows','SibenchConsumption'],storage_path='"+raw+"')");
            DuckDbStatements.Session session;long rowId;
            try(var worker=connect()) {
                try(var q=worker.createStatement();var r=q.executeQuery("SELECT current_connection_id(),current_query_id()")) {
                    assertTrue(r.next());session=new DuckDbStatements.Session(r.getLong(1),r.getLong(2),4);assertFalse(r.next());
                }
                worker.setAutoCommit(false);sql(worker,"INSERT INTO items VALUES(1,10)");
                rowId=number(worker,"SELECT _sib_row_id FROM sibench_history.identities_1 WHERE id=1");
                assertEquals(10,number(worker,"SELECT k FROM items WHERE id=1"));
                assertThrows(java.sql.SQLException.class,()->sql(worker,"INSERT INTO items VALUES(2,20)"));worker.rollback();
            }
            sql(db,"CALL disable_logging()");DuckDbFacts.load(db,raw);DuckDbStatements.bind(db,List.of(session));
            DuckDbTransactions.bind(db,"data");DuckDbIdentities.bind(db);DuckDbAccesses.bind(db,CATALOG);DuckDbMutations.bind(db);
            assertEquals(1,number(db,"SELECT count(*) FROM sibench_changes"));
            assertEquals(1,number(db,"SELECT count(*) FROM sibench_source_mutations WHERE statement_outcome='failed' AND event='complete'"));
            assertEquals(2,number(db,"SELECT count(*) FROM items"));
            var trace=root.resolve("workload.pb.zst");var writer=new ProtoTrace.Writer(trace);
            try(writer){assertEquals(5,DuckDbProjection.write(db,true,writer));}
            try(var backend=VersionedIndexBackend.create(root.resolve("index"),ValueKind.ROW_ID);var index=new IndexExecutor(backend)) {
                long[] searches={0},aborts={0},ids={0};
                ProtoTrace.read(trace,writer.sha256(),writer.logicalSha256(),new IndexOperationTrace.Consumer(){
                    @Override public void accept(IndexOperationTrace.Operation operation){fail("Missing stored request");}
                    @Override public void accept(TraceRecord record,List<IndexOperationTrace.Operation> operations)throws java.io.IOException {
                        if(record.hasBegin())return;
                        if(record.hasEnd()){if(record.getEnd().getAborted())aborts[0]++;return;}
                        try {
                            var request=record.getRequest().toBuilder().setId(++ids[0]).build();
                            var response=index.submit(request).get(5,TimeUnit.SECONDS);assertEquals(response.getMessage(),Status.OK,response.getStatus());
                            if(request.hasSearch()) {assertEquals(1,response.getResultsCount());assertEquals(rowId,response.getResults(0).getValue().getRowId());assertEquals(1,response.getResults(0).getVersion());searches[0]++;}
                        }catch(Exception error){throw new java.io.IOException(error);}
                    }
                });
                assertEquals(0,searches[0]);assertEquals(1,aborts[0]);assertEquals(0,ids[0]);
            }
        }
    }
}
