package org.sibench.replay;

import com.google.gson.GsonBuilder;
import java.io.*;
import java.nio.file.*;
import java.util.*;
import org.sibench.capture.*;
import org.sibench.generation.DuckDbDatabase;
import org.sibench.state.ExternalSort;
import org.sibench.trace.IndexKey;
import org.sibench.trace.TraceManifest;

/** Full-data acceptance with external state/sorts and one native primary-key history in memory. */
public final class FullSnapshotValidation {
    public static void main(String[] args)throws Exception {
        if(args.length!=2)throw new IllegalArgumentException("FullSnapshotValidation TRACE OUTPUT");
        run(Path.of(args[0]),Path.of(args[1]));
    }

    public static Map<String,Object> run(Path trace,Path output)throws Exception {
        Files.createDirectory(output);
        Files.writeString(output.resolve("INCOMPLETE"),"Full native snapshot acceptance pending\n");
        var config=new ReplayConfig(true,0,0,0,0,"operation",1,4096,60);
        var input=ReplayInput.prepare(trace,config);var manifest=TraceManifest.read(trace);
        var catalog=IndexCatalog.fromManifest(manifest);long last=manifest.get("last_resolved_version").getAsLong();
        long budget=Long.getLong("sibench.reference.sort_bytes",64L<<20);
        var settings=new IndexReferenceReplay.Settings(output.resolve("reference"),
                Long.getLong("sibench.reference.baseline_entries",1_000_000L),
                Integer.getInteger("sibench.reference.overlay_entries",500_000),budget);
        long[] counts=new long[5];long started=System.nanoTime();
        try(var verified=TraceManifest.openData(trace)) { /* Bind the native file before reading it independently. */ }
        try(var actual=new ExternalSort(output.resolve("actual"),budget);
                var expected=new ExternalSort(output.resolve("expected"),budget);
                var model=new IndexReferenceReplay(true,true,settings)) {
            try(var load=new ReplayProjection(config,false,operation->{
                    model.accept(operation);entry(actual,0,operation.table(),operation.index(),operation.key(),
                            operation.indexedValue(),0,operation.rowId());
                });var replay=new ReplayProjection(config,true,model::accept)) {
                input.baseline().read(load);load.finish();input.workload().read(replay);replay.finish();
            }
            model.scanVisible(last,(table,index,key,value,result)->
                    entry(actual,1,table,index,key,value,result.commitVersion(),result.value()));
            try(var database=DuckDbDatabase.openReadOnly(trace.resolve("data"));var sql=database.createStatement()) {
                String memory=System.getProperty("sibench.oracle.duckdb_memory","1GiB");
                if(!memory.matches("[1-9][0-9]*(MiB|GiB)"))throw new IOException("Invalid native oracle memory limit");
                sql.execute("SET memory_limit='"+memory+"'");sql.execute("SET threads=4");
                sql.execute("SET temp_directory='"+output.toAbsolutePath().resolve("native-temp").toString().replace("'","''")+"'");
                NativeRowHistories.read(database,catalog,(table,key,history)->{
                    counts[4]=Math.max(counts[4],history.commits.size());
                    if(history.baseline!=null){counts[0]++;row(expected,0,table,key,history.baseline);}
                    var current=history.at(last);if(current!=null){counts[1]++;row(expected,1,table,key,current);}
                    if(history.commits.isEmpty())return;
                    counts[2]++;var times=new TreeSet<Long>(List.of(0L,last));
                    for(long version:history.commits.keySet()) {
                        if(version>last)throw new IOException("Native history exceeds resolution watermark");
                        times.add(version-1);times.add(version);
                    }
                    for(long time:times) {
                        var wanted=history.at(time);
                        var primary=wanted==null?null:wanted.result(key,true);
                        if(!Objects.equals(primary,model.lookupPrimary(table,key,time)))
                            throw new IOException("Historical primary mismatch: "+table+"/"+key+" at "+time);
                        counts[3]++;
                        for(var index:history.indexedValues.entrySet())for(var value:index.getValue()) {
                            var match=wanted!=null&&Objects.equals(wanted.indexes().get(index.getKey()),value)?primary:null;
                            if(!Objects.equals(match,model.lookupEntry(table,index.getKey(),key,value,time)))
                                throw new IOException("Historical secondary mismatch: "+table+"/"+key+" at "+time);
                            counts[3]++;
                        }
                    }
                });
            }
            if(counts[0]!=manifest.get("baseline_rows").getAsLong())throw new IOException("Native baseline row count mismatch");
            try(var a=actual.sorted();var e=expected.sorted()) {
                while(a.hasNext()&&e.hasNext()) {
                    if(!Arrays.equals(a.peekKey(),e.peekKey())||!Arrays.equals(a.peekValue(),e.peekValue()))
                        throw new IOException("Full baseline/final index state differs from native rows");
                    a.advance();e.advance();
                }
                if(a.hasNext()||e.hasNext())throw new IOException("Missing or extra baseline/final index entries");
            }
            var report=new LinkedHashMap<String,Object>();report.put("status","passed");report.put("preflight",input.preflight());
            report.put("baseline_rows",counts[0]);report.put("final_rows",counts[1]);report.put("changed_primary_keys",counts[2]);
            report.put("historical_entry_probes",counts[3]);report.put("maximum_retained_key_commits",counts[4]);
            report.put("compared_index_entries",actual.count());report.put("workload_searches",model.queries());
            report.put("elapsed_seconds",(System.nanoTime()-started)/1e9);
            report.put("scope","Exact full baseline and final primary/secondary key, version and RowID multisets against native rows;"
                    +" every changed primary key and native secondary identity before/at each per-key commit and at final;"
                    +" all Trace queries replayed by the independent reference. Native reconstruction uses no Trace writes."
                    +" External state/sorts; one primary key history retained. No aborted local row contents, arbitrary derived range"
                    +" ordering, RPC concurrency, recovery or performance certification.");
            Files.writeString(output.resolve("report.json"),new GsonBuilder().setPrettyPrinting().create().toJson(report)+"\n");
            Files.delete(output.resolve("INCOMPLETE"));return report;
        }
    }

    private static void row(ExternalSort sort,int phase,int table,IndexKey key,SnapshotLookupValidation.Row row)throws IOException {
        entry(sort,phase,table,0,key,null,row.version(),row.rowId());
        for(var index:row.indexes().entrySet())entry(sort,phase,table,index.getKey(),key,index.getValue(),row.version(),row.rowId());
    }

    private static void entry(ExternalSort sort,int phase,int table,int index,IndexKey key,IndexKey value,long version,long rowId)throws IOException {
        var bytes=new ByteArrayOutputStream();var out=new DataOutputStream(bytes);
        out.writeByte(phase);out.writeInt(table);out.writeInt(index);byte[] pk=key.bytes();out.writeInt(pk.length);out.write(pk);
        if(value!=null)out.write(value.bytes());
        sort.add(bytes.toByteArray(),java.nio.ByteBuffer.allocate(16).putLong(version).putLong(rowId).array());
    }
}
