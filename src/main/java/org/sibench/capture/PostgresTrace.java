package org.sibench.capture;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.sibench.schema.SourceSchema;

/** Writes final Trace streams alongside load and workload; publication only seals their inventories. */
final class PostgresTrace {
    private final Path directory;
    private final boolean secondaryIndexes;
    private final RequestBudget budget;
    private volatile long projectedTransactions;
    private final IndexCatalog catalog;
    private final Map<String,Object> manifest;

    PostgresTrace(Path directory,SourceSchema source,boolean secondaryIndexes)throws IOException {
        this(directory,source,secondaryIndexes,null);
    }

    PostgresTrace(Path directory,SourceSchema source,boolean secondaryIndexes,RequestBudget budget)throws IOException {
        this.budget=budget;
        this.secondaryIndexes=secondaryIndexes;this.directory=directory;catalog=IndexCatalog.fromSchema(source);
        manifest=new LinkedHashMap<>(IndexCatalog.provenance(source,catalog.definitions()));
        IndexCatalog.fromManifest(new Gson().toJsonTree(manifest).getAsJsonObject());
        Files.createDirectory(directory);
        Files.writeString(directory.resolve("INCOMPLETE"),"Native generation/publication pending\n",StandardOpenOption.CREATE_NEW);
        manifest.put("output","proto");manifest.put("value_kinds",List.of("row_id"));
        manifest.put("mode","capture");manifest.put("generation","native_postgres");
        manifest.put("secondary_index_coverage",secondaryIndexes?"all_source_definitions":"primary_only");
    }

    void baseline(Connection database,Map<String,Long> expected)throws Exception {
        var writer=new ProtoTrace.Writer(directory.resolve("baseline.pb.zst"),secondaryIndexes);
        try(writer){Generation.requireRows(expected,NativeBaseline.write(database,org.sibench.config.Engine.POSTGRES,catalog.definitions().stream().filter(d->secondaryIndexes||d.indexId()==0).toList(),catalog::postgresKey,writer::accept));}
        manifest.put("baseline_rows",expected.values().stream().mapToLong(Long::longValue).sum());
        stream("baseline",writer);
    }

    /** One consumer runs concurrently with native clients; source counts are checked before publication. */
    void workload(Connection database,PostgresCapture capture)throws Exception {
        var units=WorkUnits.create(directory);
        var writer=new ProtoTrace.Writer(directory.resolve("workload.pb.zst"),secondaryIndexes,budget);
        try(var lines=units.lines();writer;var versions=new PostgresVersions(database);
                var projection=new PostgresProjection(database,catalog,directory,versions,units,secondaryIndexes,writer::accept)) {
            for(PostgresTransactions.Chunk next;(next=capture.transactions.next(()->capture.watermark(database)))!=null;)
                try(var chunk=next){
                    if (projection.accept(chunk)) projectedTransactions++;
                    capture.updateHold(database);
                }
            var counts=projection.counts();
            long requests=0,commits=0,aborts=0;
            for(var session:capture.reports()) {
                var source=org.sibench.config.ConfigValues.object(session,"counts");
                requests+=((Number)source.get("requests")).longValue();
                commits+=((Number)source.get("commits")).longValue();aborts+=((Number)source.get("aborts")).longValue();
            }
            if(requests!=((Number)counts.get("source_work_units")).longValue()
                    ||commits!=((Number)counts.get("committed_transactions")).longValue()
                    ||aborts!=((Number)counts.get("aborted_transactions")).longValue()
                    ||versions.version()!=commits+aborts)throw new IOException("Incomplete PG source projection");
            manifest.putAll(counts);manifest.put("source_statements",requests);
            manifest.put("last_resolved_version",writer.lastCommittedVersion());
            manifest.put("capture_queue",capture.transactions.metrics());
        }
        database.commit();
        manifest.put("work_units_sha256",HexFormat.of().formatHex(units.digest().digest()));
        manifest.put("work_unit_rule","one native statement origin; numbered in native transaction completion order");
        stream("workload",writer);
    }

    void awaitProjection(PostgresCapture capture, java.time.Duration timeout) throws Exception {
        long expected=0;
        for(var session:capture.completedSessions()) {
            var counts=org.sibench.config.ConfigValues.object(session,"counts");
            expected+=((Number)counts.get("commits")).longValue()+((Number)counts.get("aborts")).longValue();
        }
        long deadline=System.nanoTime()+timeout.toNanos();
        while(projectedTransactions<expected) {
            capture.check();
            if(System.nanoTime()>=deadline)throw new IOException("PG budget projection drain timed out");
            Thread.sleep(10);
        }
        if(projectedTransactions!=expected)throw new IOException("PG projected outcome count differs from source");
    }

    private void stream(String name,ProtoTrace.Writer writer)throws IOException {
        manifest.put(name+"_operations",writer.count());
        var files=List.of(ProtoTrace.file(name+".pb.zst",writer));
        @SuppressWarnings("unchecked") var streams=(Map<String,Object>)manifest.computeIfAbsent("proto_files",ignored->new LinkedHashMap<String,Object>());
        streams.put(name,files);
        manifest.put(name+"_trace_sha256",ProtoTrace.identity(new Gson().toJsonTree(files).getAsJsonArray()));
    }

    Map<String,Object> publish(Map<String,Object> evidence)throws Exception {
        if(!manifest.containsKey("workload_trace_sha256"))throw new IOException("PG workload did not finish");
        manifest.put("source_evidence",evidence);manifest.put("status","complete_for_declared_projection");
        Path partial=directory.resolve("manifest.json.partial");
        Files.writeString(partial,new GsonBuilder().setPrettyPrinting().create().toJson(manifest),StandardOpenOption.CREATE_NEW);
        Files.move(partial,directory.resolve("manifest.json"),StandardCopyOption.ATOMIC_MOVE);
        Files.delete(directory.resolve("INCOMPLETE"));return Map.copyOf(manifest);
    }
}
