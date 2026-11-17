package org.sibench.capture;

import static org.junit.Assert.*;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Test;
import org.sibench.adapters.ServiceTarget;
import org.sibench.adapters.ServiceReplay;
import org.sibench.adapters.ValueKind;
import org.sibench.generation.NativeHistory;
import org.sibench.generation.DuckDbHostDriver;
import org.sibench.generation.DuckDbHostFixture;
import org.sibench.replay.ReplayConfig;
import org.sibench.replay.ReplayInput;
import org.sibench.schema.SourceSchema;
import org.sibench.trace.TraceManifest;

/** Explicit native publication and official consumer check for both input modes. */
public class DuckDbTraceCheck extends DuckDbHostFixture {
    private Path output;
    private static final ReplayConfig REPLAY=new ReplayConfig(true,0,0,0,0,"operation",2,16,5, 1);
    private static final org.sibench.analysis.UpdateLocality.Config LOCALITY=new org.sibench.analysis.UpdateLocality.Config(256,4096,10000,1000,1<<30,10000);

    private void output(String name)throws Exception {
        String parent=System.getProperty("sibench.duckdb.trace.output");
        output=Files.createDirectory((parent==null?root:Path.of(parent)).resolve(name));
    }
    @After public void retainSource()throws Exception {
        if(output==null||host==null)return;
        stop();host=null;Files.copy(errors,output.resolve("host.log"));
        Path data=Files.createDirectory(output.resolve("data"));
        for(Path file:files(root.resolve("data")))Files.move(file,data.resolve(file.getFileName()));
    }
    private static void sql(Connection db,String text)throws Exception {try(var q=db.createStatement()){q.execute(text);}}
    private static long number(Connection db,String text)throws Exception {
        try(var q=db.createStatement();var r=q.executeQuery(text)) {
            assertTrue(r.next());long value=r.getLong(1);assertFalse(r.next());return value;
        }
    }
    private void logging(Connection db)throws Exception {
        sql(db,"CALL enable_logging(['SibenchQuery','SibenchTransaction','SibenchMutation',"
                +"'SibenchIndex','SibenchIndexRows'],storage_path='"+output.resolve("native.csv")+"')");
    }
    private static List<Path> files(Path directory)throws Exception {
        try(var files=Files.list(directory)){return files.sorted().toList();}
    }
    private static void history(Connection db,String table,String key)throws Exception {
        db.setAutoCommit(false);NativeHistory.initialize(db, org.sibench.config.Engine.DUCKDB);NativeHistory.install(db, org.sibench.config.Engine.DUCKDB,1,"main",table,key);
        db.commit();db.setAutoCommit(true);
    }
    private Path publish(DuckDbTrace.Draft draft)throws Exception {
        assertThrows(IOException.class,()->ReplayInput.check(draft.directory(),REPLAY));
        retainSource();
        draft.publish();assertThrows(IOException.class,draft::publish);
        Path moved=Files.move(draft.directory(),output.resolve("moved"));
        var manifest=TraceManifest.read(moved);
        manifest.addProperty("source_directory","/missing/generation-checkout");
        Files.writeString(moved.resolve("manifest.json"),new com.google.gson.Gson().toJson(manifest));
        assertEquals("trace_integrity_and_supply_valid",ReplayInput.check(moved,REPLAY).get("status"));
        // Nothing listens on port 1: the unsupported value kind is refused before any service call.
        var unsupported=new ServiceTarget("127.0.0.1:1",ValueKind.LOCATOR);
        var failure=assertThrows(IOException.class,()->ServiceReplay.run(moved,REPLAY,unsupported,output.resolve("unsupported.json")));
        assertTrue(failure.getMessage(),failure.getMessage().contains("does not provide LOCATOR"));
        assertEquals("passed",org.sibench.adapters.IndexTestSupport.replay(moved,REPLAY,output.resolve("index"),
                ValueKind.ROW_ID,output.resolve("replay.json")).get("status"));
        var reference=org.sibench.replay.ReferenceReplay.run(moved,REPLAY,output.resolve("reference.json"));
        assertEquals("passed",reference.get("status"));
        assertEquals(1,((List<?>)reference.get("results")).size());
        assertThrows(IOException.class,()->org.sibench.replay.ReferenceReplay.run(moved,REPLAY,List.of(false),output.resolve("locator-reference.json")));
        assertFalse(Files.exists(output.resolve("locator-reference.json")));
        assertFalse(Files.exists(moved.resolve("data")));
        assertFalse(TraceManifest.read(moved).has("native_data"));
        return moved;
    }

    private void verifiesCoverageAndDeferredBaseline(Path trace)throws Exception {
        Path baseline=trace.resolve("baseline.pb.zst"), held=output.resolve("baseline-held");
        Files.move(baseline,held);
        ReplayInput.Input input;
        try {
            input=ReplayInput.prepare(trace,REPLAY);
            assertEquals("workload_integrity_and_supply_valid",input.preflight().get("status"));
            assertFalse(input.preflight().containsKey("baseline"));
            assertThrows(IOException.class,()->input.baseline().read(ignored->{}));
            var shortage=assertThrows(IOException.class,()->ReplayInput.prepare(trace,new ReplayConfig(true,1,1000000,0,0, "operation", 1, 4096, 60, 1)));
            assertTrue(shortage.getMessage().contains("Insufficient single-pass"));
        }finally {Files.move(held,baseline);}
        var manifest=TraceManifest.read(trace);
        assertEquals(manifest.get("baseline_operations").getAsLong(),input.baseline().read(ignored->{}));
        assertEquals("trace_integrity_and_supply_valid",input.preflight().get("status"));
        var failure=assertThrows(IOException.class,()->input.baseline().read(ignored->{throw new IOException("consumer failed");}));
        assertEquals("consumer failed",failure.getMessage());
        assertEquals("workload_integrity_and_supply_valid",input.preflight().get("status"));
        assertFalse(input.preflight().containsKey("baseline"));
        Path path=trace.resolve("manifest.json");String original=Files.readString(path);
        try {
            for(String name:List.of("baseline_rows","source_work_units","baseline_operations","workload_operations"))
                for(String invalid:List.of("missing","null","\"1\"","-1","1.5","9223372036854775808")) {
                    var changed=manifest.deepCopy();
                    if(invalid.equals("missing"))changed.remove(name);else changed.add(name,com.google.gson.JsonParser.parseString(invalid));
                    Files.writeString(path,changed.toString());
                    assertTrue(assertThrows(IOException.class,()->ReplayInput.prepare(trace,REPLAY)).getMessage().contains(name));
                }
            // Both consumers use TraceManifest.read; one case verifies the analysis entry point.
            var missingCount=manifest.deepCopy();missingCount.remove("baseline_rows");
            Files.writeString(path,missingCount.toString());
            Path invalidAnalysis=output.resolve("invalid-locality");
            assertTrue(assertThrows(IOException.class,()->org.sibench.analysis.UpdateLocality.run(trace,
                    LOCALITY,invalidAnalysis)).getMessage().contains("baseline_rows"));
            assertTrue(Files.exists(invalidAnalysis.resolve("INCOMPLETE")));
            assertFalse(Files.exists(invalidAnalysis.resolve("summary.json")));
            for(String name:List.of("output","baseline_trace_sha256","index_definitions")) {
                var changed=manifest.deepCopy();
                if(name.equals("output"))changed.remove(name);
                else if(name.equals("baseline_trace_sha256"))changed.addProperty(name,"0".repeat(64));
                else changed.getAsJsonArray(name).remove(1);
                Files.writeString(path,changed.toString());
                assertThrows(IOException.class,()->ReplayInput.check(trace,REPLAY));
            }
            var unsupported=manifest.deepCopy();unsupported.addProperty("output","full");
            Files.writeString(path,unsupported.toString());
            assertThrows(IOException.class,()->TraceManifest.read(trace));
        }finally {Files.writeString(path,original);}
        Path analysis=output.resolve("locality");org.sibench.analysis.UpdateLocality.run(trace,LOCALITY,analysis);
        var summary=com.google.gson.JsonParser.parseString(Files.readString(analysis.resolve("summary.json"))).getAsJsonObject();
        assertEquals(1,summary.getAsJsonObject("analysis").getAsJsonObject("business_changes").get("update").getAsInt());
    }

    @Test public void capturePublishesAbortAndHistoricalRowsThroughOfficialConsumers()throws Exception {
        output("capture");Path receipts=Files.createDirectory(output.resolve("clients"));
        var source=SourceSchema.of("sysbench",Path.of("benchmark/sysbench"),Map.of("tables",1));
        DuckDbTrace.Draft draft;
        try(var db=connect()) {
            sql(db,"CREATE TABLE sbtest1(id INTEGER PRIMARY KEY,k INTEGER NOT NULL,c VARCHAR NOT NULL,pad VARCHAR NOT NULL)");
            sql(db,"CREATE INDEX k_1 ON sbtest1(k)");sql(db,"INSERT INTO sbtest1 VALUES(10,1,'A',''),(20,1,'B',''),(30,2,'C','')");
            history(db,"sbtest1","id");
            var trace=new DuckDbTrace(source,output.resolve("trace"),true,null);
            db.setAutoCommit(false);trace.baseline(db,Map.of("sbtest1",3L));db.commit();db.setAutoCommit(true);
            logging(db);
            try(var worker=DuckDbHostDriver.open(socket,receipts);var older=DuckDbHostDriver.open(socket,receipts)) {
                older.setAutoCommit(false);assertEquals(30,number(older,"SELECT sum(id) FROM sbtest1 WHERE k=1"));
                worker.setAutoCommit(false);
                assertEquals(30,number(worker,"SELECT sum(id) FROM sbtest1 WHERE k=1"));
                sql(worker,"UPDATE sbtest1 SET k=1 WHERE id=30");
                assertEquals(60,number(worker,"SELECT sum(id) FROM sbtest1 WHERE k=1"));worker.rollback();
                sql(worker,"UPDATE sbtest1 SET c='changed' WHERE id=10");worker.commit();
                assertEquals(30,number(worker,"SELECT sum(id) FROM sbtest1 WHERE k=1"));worker.commit();
                older.commit();
            }
            sql(db,"CALL disable_logging()");DuckDbFacts.load(db,output.resolve("native.csv"),0);
            var sessions=DuckDbReceipts.load(db,files(receipts),output.resolve("receipts"));
            DuckDbStatements.bind(db,sessions);DuckDbReceipts.bind(db);DuckDbTransactions.bind(db,"data",0);
            DuckDbIdentities.bind(db);DuckDbAccesses.bind(db,IndexCatalog.fromSchema(source).definitions());DuckDbMutations.bind(db);
            try(trace){trace.append(db);draft=trace.finish(Map.of("input","hand-controlled capture"));}
            assertEquals(1L,draft.manifest().get("aborted_transactions"));
        }
        Path moved=publish(draft);
        verifiesCoverageAndDeferredBaseline(moved);
        org.sibench.replay.ReplayMatrixValidation.validate(moved,output.resolve("matrix"));
    }

    @Test public void incrementalWindowsPreserveVersionsAndDeletedRowIdentities() throws Exception {
        output("incremental");
        var source=SourceSchema.of("sysbench",Path.of("benchmark/sysbench"),Map.of("tables",1));
        DuckDbTrace.Draft draft;
        try(var db=connect()) {
            sql(db,"CREATE TABLE sbtest1(id INTEGER PRIMARY KEY,k INTEGER NOT NULL,c VARCHAR NOT NULL,pad VARCHAR NOT NULL)");
            sql(db,"CREATE INDEX k_1 ON sbtest1(k)");
            sql(db,"INSERT INTO sbtest1 VALUES(10,1,'original','')");
            history(db,"sbtest1","id");
            try(var trace=new DuckDbTrace(source,output.resolve("trace"),true,null)) {
                db.setAutoCommit(false);trace.baseline(db,Map.of("sbtest1",1L));db.commit();db.setAutoCommit(true);
                long rawPrefix=0;
                for(int window=0;window<2;window++) {
                    Path directory=Files.createDirectory(output.resolve("window-"+window));
                    Path receipts=Files.createDirectory(directory.resolve("clients")), raw=directory.resolve("native.csv");
                    sql(db,"CALL enable_logging(['SibenchQuery','SibenchTransaction','SibenchMutation',"
                            +"'SibenchIndex','SibenchIndexRows'],storage_path='"+raw+"')");
                    try(var worker=DuckDbHostDriver.open(socket,receipts)) {
                        worker.setAutoCommit(false);
                        assertEquals(1,number(worker,"SELECT count(*) FROM sbtest1 WHERE id=10"));
                        if(window==0) sql(worker,"UPDATE sbtest1 SET c='committed' WHERE id=10");
                        else {
                            sql(worker,"DELETE FROM sbtest1 WHERE id=10");
                            sql(worker,"INSERT INTO sbtest1 VALUES(10,2,'replacement','')");
                        }
                        worker.commit();
                        sql(worker,"UPDATE sbtest1 SET k=99 WHERE id=10");
                        assertEquals(1,number(worker,"SELECT count(*) FROM sbtest1 WHERE k=99"));
                        worker.rollback();
                    }
                    sql(db,"CALL disable_logging()");
                    sql(db,"SET logging_storage='memory'");
                    try(var capture=connect()) {
                        var counts=DuckDbFacts.load(capture,raw,rawPrefix);
                        rawPrefix+=counts.values().stream().mapToLong(Long::longValue).sum();
                        var sessions=DuckDbReceipts.load(capture,files(receipts),directory.resolve("receipts"));
                        DuckDbStatements.bind(capture,sessions);DuckDbReceipts.bind(capture);
                        DuckDbTransactions.bind(capture,"data",trace.lastVersion());
                        if(window==1) assertEquals(1,number(capture,"SELECT min(read_version) FROM sibench_transactions"));
                        DuckDbIdentities.bind(capture);DuckDbAccesses.bind(capture,IndexCatalog.fromSchema(source).definitions());
                        DuckDbMutations.bind(capture);trace.append(capture);
                    }
                }
                assertEquals(4,trace.lastVersion());
                draft=trace.finish(Map.of("input","two drained native windows"));
            }
        }
        Path moved=publish(draft);
        // The update keeps baseline RowID 1; delete and reinsert in one transaction publish a new RowID.
        var operations=new java.util.ArrayList<IndexOperationTrace.Operation>();
        ProtoTrace.source(moved,TraceManifest.read(moved),"workload").read(operations::add);
        var rowIds=operations.stream().filter(o->IndexOperationTrace.isMutation(o)&&o.index()==0)
                .map(IndexOperationTrace.Operation::rowId).toList();
        assertEquals(2,rowIds.size());assertEquals(1L,(long)rowIds.get(0));assertNotEquals(1L,(long)rowIds.get(1));
    }

}
