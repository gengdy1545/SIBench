package org.sibench.capture;

import org.sibench.adapters.IndexTestSupport;

import static org.junit.Assert.*;
import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.sibench.generation.NativeHistory;
import org.sibench.generation.DuckDbHostFixture;
import org.sibench.trace.IndexKey;

public class DuckDbProjectionCheck extends DuckDbHostFixture {
    private static final List<IndexCatalog.Definition> CATALOG=List.of(
            new IndexCatalog.Definition(1,"items",0,"canonical_primary","id","int"),
            new IndexCatalog.Definition(1,"items",1,"items_k","k","int"));
    private static void sql(Connection db,String text)throws Exception {try(var q=db.createStatement()){q.execute(text);}}
    private static long number(Connection db,String text)throws Exception {
        try(var q=db.createStatement();var r=q.executeQuery(text)){assertTrue(r.next());long value=r.getLong(1);assertFalse(r.next());return value;}
    }
    @Test public void nativeSearchesPreserveLocalVisibilityThroughServiceReplay()throws Exception {
        try(var db=connect()) {
            sql(db,"CREATE TABLE items(id BIGINT PRIMARY KEY,k BIGINT)");sql(db,"INSERT INTO items VALUES(10,1),(20,1),(30,2)");
            sql(db,"CREATE INDEX items_k ON items(k)");db.setAutoCommit(false);NativeHistory.initialize(db, org.sibench.config.Engine.DUCKDB);
            NativeHistory.install(db, org.sibench.config.Engine.DUCKDB,1,"main","items","id");db.commit();db.setAutoCommit(true);
            long id10=number(db,"SELECT _sib_row_id FROM sibench_history.identities_1 WHERE id=10");
            long id20=number(db,"SELECT _sib_row_id FROM sibench_history.identities_1 WHERE id=20");
            long id30=number(db,"SELECT _sib_row_id FROM sibench_history.identities_1 WHERE id=30");
            var baseline=root.resolve("baseline.pb.zst");var baseWriter=new ProtoTrace.Writer(baseline, true);
            db.setAutoCommit(false);
            try(baseWriter){assertEquals(java.util.Map.of("items",3L),NativeBaseline.write(db,org.sibench.config.Engine.DUCKDB,CATALOG,baseWriter));}
            assertEquals(6,baseWriter.count());db.commit();db.setAutoCommit(true);
            var raw=root.resolve("native.csv");sql(db,"CALL enable_logging(['SibenchQuery','SibenchTransaction','SibenchMutation',"
                    +"'SibenchIndex','SibenchIndexRows'],storage_path='"+raw+"')");
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
            sql(db,"CALL disable_logging()");DuckDbFacts.load(db,raw,0);DuckDbStatements.bind(db,List.of(session));
            DuckDbTransactions.bind(db,"data",0);DuckDbIdentities.bind(db);DuckDbAccesses.bind(db,CATALOG);DuckDbMutations.bind(db);
            assertEquals(5,number(db,"SELECT count(*) FROM sibench_changes"));
            assertEquals(2,number(db,"SELECT count(*) FROM sibench_changes WHERE version=1"));
            var expected=new HashMap<Long,List<Long>>();
            try(var q=db.createStatement();var r=q.executeQuery("SELECT a.raw_record,a.version,s.sql_text,a.query_id FROM sibench_accesses a "
                    +"JOIN sibench_statements s USING(connection_id,transaction_id,query_id) ORDER BY a.query_id")) {
                int aggregate=0;
                while(r.next()) {
                    String text=r.getString(3);List<Long> wanted;
                    if(text.contains(" IN "))wanted=List.of(id10,id30);
                    else if(text.startsWith("SELECT sum")) wanted=aggregate++==0?List.of(id10,id20):
                            List.of(id10,id20,id30,id40);
                    else if(text.contains("id=30"))wanted=List.of(id30);
                    else if(text.contains("id=10"))wanted=List.of(id10);
                    else if(text.contains("id=20"))wanted=List.of(id20);
                    else {assertTrue(text,text.startsWith("SELECT count"));wanted=List.of();}
                    expected.put(r.getLong(1),wanted);
                }
            }
            assertEquals(7,expected.size());
            var workload=root.resolve("workload.pb.zst");var writer=new ProtoTrace.Writer(workload, true);
            var operations=new java.util.ArrayList<IndexOperationTrace.Operation>();
            assertEquals(30,DuckDbProjection.write(db,true,operations::add));
            try(var reference=new IndexReferenceReplay(true,true)) {
                ProtoTrace.read(baseline,baseWriter.sha256(),baseWriter.logicalSha256(),reference::accept);
                var actual=new java.util.LinkedHashSet<Long>();long reads=0;
                for(var operation:operations) {
                    if(IndexOperationTrace.isControl(operation)&&operation.kind()!=IndexOperationTrace.TRANSACTION_ABORT)continue;
                    reference.accept(operation);
                    if(operation.kind()!=IndexOperationTrace.SEARCH)continue;
                    if(!operation.grouped()||operation.expansionPosition()==0)actual.clear();
                    var member=new java.util.LinkedHashSet<Long>();
                    // Searches carry no version; a transaction-local search reads its own writes at the transaction's slot.
                    long local=(operation.flags()&IndexOperationTrace.TRANSACTION_LOCAL_SEARCH)!=0?operation.transactionId():0;
                    for(var row:reference.search(operation.table(),operation.index(),operation.range(),operation.readTimestamp(),
                            local,operation.transactionId(),operation.mutationOrdinal()))member.add(row.value());
                    if((operation.flags()&IndexOperationTrace.INTERSECT_GROUP)!=0&&operation.expansionPosition()>0)actual.retainAll(member);
                    else actual.addAll(member);
                    if(!operation.grouped()||operation.expansionPosition()+1==operation.expansionSize()) {
                        assertEquals(expected.get(operation.sourceEvent()),List.copyOf(actual));reads++;
                    }
                }
                assertEquals(7,reads);
            }
            try(writer){for(var operation:operations)writer.accept(operation);}
            var requests=new org.sibench.adapters.TraceRequests(org.sibench.adapters.ValueKind.ROW_ID);
            try(var backend=org.sibench.adapters.VersionedIndexBackend.create(root.resolve("index"),org.sibench.adapters.ValueKind.ROW_ID)) {
                long[] reads={0};
                var consumer=new IndexOperationTrace.Consumer() {
                    @Override public void accept(IndexOperationTrace.Operation operation){fail("Missing stored request");}
                    @Override public void accept(org.sibench.adapters.proto.TraceRecord record,List<IndexOperationTrace.Operation> views)throws java.io.IOException {
                        if(!record.hasOperation())return;
                        var request=record.getOperation();var operation=views.get(0);List<com.google.protobuf.ByteString> values;
                        if(request.hasReadGroup()) {
                            var answers=new java.util.ArrayList<List<com.google.protobuf.ByteString>>();
                            for(var member:request.getReadGroup().getRequestsList()) {
                                var response=IndexTestSupport.execute(backend,org.sibench.adapters.TraceRequests.read(member));
                                assertEquals(response.message(),org.sibench.adapters.IndexStatus.OK,response.status());answers.add(response.values());
                            }
                            values=org.sibench.adapters.TraceRequests.combine(request.getReadGroup(),answers);
                        } else {
                            var response=IndexTestSupport.execute(backend,requests.forward(request,operation));
                            assertEquals(response.message(),org.sibench.adapters.IndexStatus.OK,response.status());values=response.values();
                        }
                        if(operation.kind()==IndexOperationTrace.SEARCH) {
                            assertEquals(expected.get(operation.sourceEvent()),values.stream().map(v->v.asReadOnlyByteBuffer().getLong()).toList());reads[0]++;
                        }
                    }
                };
                ProtoTrace.read(baseline,baseWriter.sha256(),baseWriter.logicalSha256(),consumer);
                ProtoTrace.read(workload,writer.sha256(),writer.logicalSha256(),consumer);
                assertEquals(7,reads[0]);
            }
        }
    }

    @Test public void failedStatementsDoNotEraseEarlierSuccessfulLocalReads()throws Exception {
        try(var db=connect()) {
            sql(db,"CREATE TABLE items(id BIGINT PRIMARY KEY,k BIGINT)");sql(db,"CREATE INDEX items_k ON items(k)");
            sql(db,"INSERT INTO items VALUES(10,100),(20,200)");
            sql(db,"CREATE TABLE guard(id BIGINT)");db.setAutoCommit(false);NativeHistory.initialize(db, org.sibench.config.Engine.DUCKDB);
            NativeHistory.install(db, org.sibench.config.Engine.DUCKDB,1,"main","items","id");db.commit();db.setAutoCommit(true);
            sql(db,"CREATE TRIGGER z_reject AFTER INSERT ON items REFERENCING NEW TABLE AS inserted "
                    +"FOR EACH STATEMENT INSERT INTO guard SELECT CASE WHEN id=2 THEN error('late failure') ELSE id END FROM inserted");
            var raw=root.resolve("native.csv");sql(db,"CALL enable_logging(['SibenchQuery','SibenchTransaction','SibenchMutation',"
                    +"'SibenchIndex','SibenchIndexRows'],storage_path='"+raw+"')");
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
            sql(db,"CALL disable_logging()");DuckDbFacts.load(db,raw,0);DuckDbStatements.bind(db,List.of(session));
            DuckDbTransactions.bind(db,"data",0);DuckDbIdentities.bind(db);DuckDbAccesses.bind(db,CATALOG);DuckDbMutations.bind(db);
            assertEquals(1,number(db,"SELECT count(*) FROM sibench_changes"));
            assertEquals(1,number(db,"SELECT count(*) FROM sibench_source_mutations WHERE statement_outcome='failed' AND event='complete'"));
            assertEquals(2,number(db,"SELECT count(*) FROM items"));
            var trace=root.resolve("workload.pb.zst");var writer=new ProtoTrace.Writer(trace, true);
            var operations=new java.util.ArrayList<IndexOperationTrace.Operation>();
            assertEquals(5,DuckDbProjection.write(db,true,operations::add));
            assertTrue(operations.stream().anyMatch(o->o.kind()==IndexOperationTrace.SEARCH
                    &&(o.flags()&IndexOperationTrace.TRANSACTION_LOCAL_SEARCH)!=0&&o.expectedCount()==1));
            try(writer){for(var operation:operations)writer.accept(operation);}
            var exported=new java.util.ArrayList<IndexOperationTrace.Operation>();
            ProtoTrace.read(trace,writer.sha256(),writer.logicalSha256(),exported::add);
            assertEquals(1,exported.size());assertEquals(IndexOperationTrace.SEARCH,exported.get(0).kind());
            assertEquals(rowId,exported.get(0).localWrites().get(0).rowId());
            try(var reference=new IndexReferenceReplay(true,true)) {
                reference.acceptCommitted(exported.get(0));assertEquals(1,reference.queries());
                assertNull(reference.lookupPrimary(1,IndexKey.ofInt(1),0));
            }
        }
    }
}
