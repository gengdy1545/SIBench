package org.sibench.capture;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.HexFormat;
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
    private final PostgresQueries coverage;

    PostgresTrace(Path directory,SourceSchema source,boolean secondaryIndexes,RequestBudget budget)throws IOException {
        this.budget=budget;
        this.secondaryIndexes=secondaryIndexes;this.directory=directory;catalog=IndexCatalog.fromSchema(source);
        coverage=new PostgresQueries(source,secondaryIndexes);
        manifest=TraceDirectory.create(directory,source,catalog.definitions(),"capture","native_postgres",secondaryIndexes,List.of("row_id"));
    }

    Map<String,Long> iqQueries(){return coverage.successful();}
    Map<String,Object> queryCoverage(){return coverage.report();}

    void baseline(Connection database,Map<String,Long> expected)throws Exception {
        var writer=new ProtoTrace.Writer(directory.resolve("baseline.pb.zst"),secondaryIndexes);
        try(writer){Generation.requireRows(expected,NativeBaseline.write(database,org.sibench.config.Engine.POSTGRES,catalog.definitions().stream().filter(d->secondaryIndexes||d.indexId()==0).toList(),catalog::postgresKey,writer::accept));}
        org.sibench.trace.TraceManifest.baselineRows(manifest,expected);
        TraceDirectory.stream(manifest,"baseline",ProtoTrace.file(writer));
    }

    /** One consumer runs concurrently with native clients; source counts are checked before publication. */
    void workload(Connection database,PostgresCapture capture)throws Exception {
        var units=WorkUnits.create(directory);
        var writer=new ProtoTrace.Writer(directory.resolve("workload.pb.zst"),secondaryIndexes,budget,coverage::exported);
        try(var lines=units.lines();writer;var versions=new PostgresVersions(database);
                var projection=new PostgresProjection(database,catalog,directory,versions,units,secondaryIndexes,writer::accept,coverage)) {
            for(PostgresTransactions.Chunk next;(next=capture.transactions.next(()->capture.watermark(database)))!=null;)
                {
                    var chunk=next;
                    try {
                        if(chunk.hasRequests&&!chunk.committed)writer.expectAbort();
                        if (projection.accept(chunk)) projectedTransactions++;
                        capture.updateHold(database);
                    } catch(Exception|Error failure) {
                        capture.fail(failure);
                        throw failure;
                    }
                    chunk.close();
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
            manifest.putAll(counts);
            manifest.put("query_coverage",coverage.report());
            manifest.put("last_resolved_version",writer.lastCommittedVersion());
            manifest.put("capture_queue",capture.transactions.metrics());
        }
        database.commit();
        manifest.put("work_units_sha256",HexFormat.of().formatHex(units.digest().digest()));
        manifest.put("work_unit_rule","one native statement origin; numbered in native transaction completion order");
        TraceDirectory.stream(manifest,"workload",ProtoTrace.file(writer));
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

    Map<String,Object> publish(Map<String,Object> evidence)throws Exception {
        return TraceDirectory.publish(directory,manifest,evidence);
    }
}
