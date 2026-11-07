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
import org.sibench.adapters.ServiceConfig;
import org.sibench.adapters.ServiceReplay;
import org.sibench.adapters.proto.ValueKind;
import org.sibench.config.BenchmarkSelection.Mode;
import org.sibench.generation.DuckDbDatabase;
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
    private static final ReplayConfig REPLAY=new ReplayConfig(true,0,0,0,0,"operation",2,16,5);

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
                +"'SibenchIndex','SibenchIndexRows','SibenchConsumption'],storage_path='"+output.resolve("native.csv")+"')");
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
        retainSource();Files.move(output.resolve("data"),draft.directory().resolve("data"));
        var wrong=new java.util.LinkedHashMap<>(draft.manifest());wrong.put("last_resolved_version",999L);
        assertThrows(IOException.class,()->new DuckDbTrace.Draft(draft.directory(),wrong).publish());
        assertFalse(Files.exists(draft.directory().resolve("manifest.json")));
        draft.publish();assertThrows(IOException.class,draft::publish);
        Path moved=Files.move(draft.directory(),output.resolve("moved"));
        var manifest=TraceManifest.read(moved);
        manifest.addProperty("source_directory","/missing/generation-checkout");
        Files.writeString(moved.resolve("manifest.json"),new com.google.gson.Gson().toJson(manifest));
        assertEquals("trace_integrity_and_supply_valid",ReplayInput.check(moved,REPLAY).get("status"));
        var unsupported=new ServiceConfig("rocksdb",null,output.resolve("unsupported-index"),0,ValueKind.LOCATOR,null,false);
        var failure=assertThrows(IOException.class,()->ServiceReplay.run(moved,REPLAY,unsupported,output.resolve("unsupported.json")));
        assertTrue(failure.getMessage(),failure.getMessage().contains("does not provide LOCATOR"));
        assertFalse(Files.exists(unsupported.path()));
        var service=new ServiceConfig("rocksdb",null,output.resolve("index"),0,ValueKind.ROW_ID,null,false);
        assertEquals("passed",ServiceReplay.run(moved,REPLAY,service,output.resolve("replay.json")).get("status"));
        var reference=org.sibench.replay.ReferenceReplay.run(moved,REPLAY,output.resolve("reference.json"));
        assertEquals("passed",reference.get("status"));
        assertEquals(1,((List<?>)reference.get("results")).size());
        assertThrows(IOException.class,()->org.sibench.replay.ReferenceReplay.run(moved,REPLAY,List.of(false),output.resolve("locator-reference.json")));
        assertFalse(Files.exists(output.resolve("locator-reference.json")));
        assertEquals("passed",org.sibench.replay.SnapshotLookupValidation.run(moved,REPLAY,
                output.resolve("native-oracle.json")).get("status"));
        assertEquals("passed",org.sibench.replay.FullSnapshotValidation.run(moved,
                output.resolve("full-native-oracle")).get("status"));
        rejectsWrongBaselineRowIds(moved);
        assertTrue(Files.isRegularFile(DuckDbDatabase.file(moved.resolve("data"))));
        return moved;
    }

    private void rejectsWrongBaselineRowIds(Path trace)throws Exception {
        Path wrong=Files.createDirectory(output.resolve("wrong-row-ids"));
        try(var files=Files.walk(trace)) {
            for(Path file:files.sorted().toList()) {
                Path target=wrong.resolve(trace.relativize(file));
                if(file.equals(trace))continue;
                if(Files.isDirectory(file))Files.createDirectory(target);
                else if(!file.getFileName().toString().equals("baseline.pb.zst"))Files.copy(file,target);
            }
        }
        var manifest=TraceManifest.read(trace);
        var writer=new ProtoTrace.Writer(wrong.resolve("baseline.pb.zst"));
        try(writer) {
            ProtoTrace.source(trace,manifest,"baseline").read(o->writer.accept(IndexOperationTrace.write(
                    o.sourceEvent(),o.kind(),o.table(),o.flags(),o.key(),o.version(),o.file(),o.position(),o.rowId()+100000)
                    .withIndex(o.index(),o.indexedValue(),o.kind()).withExpansion(o.expansionPosition(),o.expansionSize())
                    .withWorkUnitId(o.workUnitId())));
        }
        var files=new com.google.gson.JsonArray();files.add(new com.google.gson.Gson().toJsonTree(ProtoTrace.file("baseline.pb.zst",writer)));
        manifest.getAsJsonObject("proto_files").add("baseline",files);
        manifest.addProperty("baseline_trace_sha256",ProtoTrace.identity(files));
        Files.writeString(wrong.resolve("manifest.json"),manifest.toString());
        Path report=output.resolve("wrong-row-ids-report.json");
        var failure=assertThrows(IOException.class,()->org.sibench.replay.SnapshotLookupValidation.run(wrong,REPLAY,report));
        assertTrue(failure.toString(),failure.getMessage().contains("Historical primary mismatch"));
        assertFalse(Files.exists(report));
        Path full=output.resolve("wrong-full-row-ids");
        assertThrows(IOException.class,()->org.sibench.replay.FullSnapshotValidation.run(wrong,full));
        assertTrue(Files.exists(full.resolve("INCOMPLETE")));
        assertFalse(Files.exists(full.resolve("report.json")));
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
            var shortage=assertThrows(IOException.class,()->ReplayInput.prepare(trace,new ReplayConfig(true,1,1000000,0,0)));
            assertTrue(shortage.getMessage().contains("Insufficient single-pass"));
        }finally {Files.move(held,baseline);}
        var manifest=TraceManifest.read(trace);
        assertEquals(manifest.get("baseline_operations").getAsLong(),input.baseline().read(ignored->{}));
        assertEquals("trace_integrity_and_supply_valid",input.preflight().get("status"));
        var failure=assertThrows(IOException.class,()->input.baseline().read(ignored->{throw new IOException("consumer failed");}));
        assertEquals("consumer failed",failure.getMessage());
        assertEquals("workload_integrity_and_supply_valid",input.preflight().get("status"));
        assertFalse(input.preflight().containsKey("baseline"));
        Path path=trace.resolve("manifest.json");String original=Files.readString(path);int number=0;
        try {
            for(String name:List.of("baseline_rows","source_work_units","baseline_operations","workload_operations"))
                for(String invalid:List.of("missing","null","\"1\"","-1","1.5","9223372036854775808")) {
                    var changed=manifest.deepCopy();
                    if(invalid.equals("missing"))changed.remove(name);else changed.add(name,com.google.gson.JsonParser.parseString(invalid));
                    Files.writeString(path,changed.toString());
                    assertTrue(assertThrows(IOException.class,()->ReplayInput.prepare(trace,REPLAY)).getMessage().contains(name));
                    Path analysis=output.resolve("invalid-locality-"+number++);
                    assertTrue(assertThrows(IOException.class,()->org.sibench.analysis.UpdateLocality.run(trace,
                            Path.of("config/sibench.yaml"),analysis)).getMessage().contains(name));
                }
            for(String name:List.of("output","baseline_trace_sha256","index_definitions")) {
                var changed=manifest.deepCopy();
                if(name.equals("output"))changed.remove(name);
                else if(name.equals("baseline_trace_sha256"))changed.addProperty(name,"0".repeat(64));
                else changed.getAsJsonArray(name).remove(1);
                Files.writeString(path,changed.toString());
                assertThrows(IOException.class,()->ReplayInput.check(trace,REPLAY));
            }
            var retired=manifest.deepCopy();retired.addProperty("output","full");
            Files.writeString(path,retired.toString());
            assertThrows(IOException.class,()->TraceManifest.read(trace));
        }finally {Files.writeString(path,original);}
        Path analysis=output.resolve("locality");org.sibench.analysis.UpdateLocality.run(trace,Path.of("config/sibench.yaml"),analysis);
        var summary=com.google.gson.JsonParser.parseString(Files.readString(analysis.resolve("summary.json"))).getAsJsonObject();
        assertEquals(1,summary.getAsJsonObject("analysis").getAsJsonObject("business_changes").get("update").getAsInt());
    }

    @Test public void capturePublishesAbortAndHistoricalRowsThroughOfficialConsumers()throws Exception {
        output("capture");Path receipts=Files.createDirectory(output.resolve("clients"));
        var source=SourceSchema.of("sysbench",Path.of("benchmark/sysbench"),Map.of("tables",1));
        DuckDbTrace.Draft draft;long row;
        try(var db=connect()) {
            sql(db,"CREATE TABLE sbtest1(id INTEGER PRIMARY KEY,k INTEGER NOT NULL,c VARCHAR NOT NULL,pad VARCHAR NOT NULL)");
            sql(db,"CREATE INDEX k_1 ON sbtest1(k)");sql(db,"INSERT INTO sbtest1 VALUES(10,1,'A',''),(20,1,'B',''),(30,2,'C','')");
            history(db,"sbtest1","id");row=number(db,"SELECT _sib_row_id FROM sibench_history.identities_1 WHERE id=10");
            var trace=new DuckDbTrace(Mode.CAPTURE,source,output.resolve("trace"));
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
            sql(db,"CALL disable_logging()");DuckDbFacts.load(db,output.resolve("native.csv"));
            var sessions=DuckDbReceipts.load(db,files(receipts),output.resolve("receipts"));
            DuckDbStatements.bind(db,sessions);DuckDbReceipts.bind(db);DuckDbTransactions.bind(db,"data");
            DuckDbIdentities.bind(db);DuckDbAccesses.bind(db,IndexCatalog.duckDbDefinitions(source));DuckDbMutations.bind(db);
            draft=trace.workload(db,Map.of("input","hand-controlled capture"));
            assertEquals(1L,draft.manifest().get("aborted_transactions"));
        }
        Path moved=publish(draft);
        assertEquals(1L,CapturePlan.writeRatio(Path.of("."),Path.of("config/benchmark/sysbench.yaml"),moved,1)
                .get("calibration_row_writes"));
        verifiesCoverageAndDeferredBaseline(moved);
        org.sibench.replay.ReplayMatrixValidation.validate(moved,output.resolve("matrix"));
        try(var data=TraceManifest.openData(moved)) {
            assertEquals("A",data.snapshotRow(1,row,0).orElseThrow().get("c"));
            assertEquals("A",data.snapshotRow(1,row,1).orElseThrow().get("c"));
            assertEquals("changed",data.snapshotRow(1,row,3).orElseThrow().get("c"));
        }
    }

}
