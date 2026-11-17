package org.sibench.capture;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import org.sibench.schema.SourceSchema;

/** Both native inputs publish the same baseline and workload contract. */
public final class DuckDbTrace implements AutoCloseable {
    private final Path directory;
    private final boolean secondaryIndexes;
    private final List<IndexCatalog.Definition> catalog;
    private final Map<String,Object> manifest;
    private ProtoTrace.Writer base;
    private final ProtoTrace.Writer workload;
    private final WorkUnits.Output units;
    private long unitCount,commits,aborts,statements;
    private int windows;
    private boolean finished;

    long lastVersion() { return commits+aborts; }


    DuckDbTrace(SourceSchema source,Path directory,boolean secondaryIndexes,RequestBudget budget)throws IOException {
        this.secondaryIndexes=secondaryIndexes;this.directory=directory;catalog=IndexCatalog.fromSchema(source).definitions();
        manifest=TraceDirectory.create(directory,source,catalog,"capture","native_duckdb",secondaryIndexes,List.of("row_id"));
        units=WorkUnits.create(directory);
        workload=new ProtoTrace.Writer(directory.resolve("workload.pb.zst"),secondaryIndexes,budget);
    }

    public void baseline(Connection database,Map<String,Long> expected)throws SQLException,IOException {
        var writer=new ProtoTrace.Writer(directory.resolve("baseline.pb.zst"),secondaryIndexes);base=writer;
        try(writer) {
            var counts=NativeBaseline.write(database,org.sibench.config.Engine.DUCKDB,catalog.stream().filter(d->secondaryIndexes||d.indexId()==0).toList(),writer);
            Generation.requireRows(expected,counts);
            org.sibench.trace.TraceManifest.baselineRows(manifest,counts);
        }
    }

    /** The caller closes the host and publishes; native data is discarded only after publication. */
    public record Draft(Path directory,Map<String,Object> manifest,Map<String,Object> evidence) {
        public Draft {manifest=Map.copyOf(manifest);evidence=Map.copyOf(evidence);}
        public Map<String,Object> publish()throws IOException {
            return TraceDirectory.publish(directory,manifest,evidence);
        }
    }

    /** A closed source window has its own temporary bindings; only its new facts are projected. */
    public void append(Connection database)throws SQLException,IOException {
        if(finished || !manifest.containsKey("baseline_rows"))throw new IOException("Trace is not accepting workload output");
        long baseUnit=unitCount;
        long[] origins=origins(database);
        // The projection emits one outcome per sibench_transactions row, so lastVersion() advances by that count.
        DuckDbProjection.write(database,secondaryIndexes,new DuckDbProjection.Output() {
            @Override public void accept(IndexOperationTrace.Operation operation)throws IOException {
                if(!IndexOperationTrace.isControl(operation)) {
                    int unit=Arrays.binarySearch(origins,1,origins.length,operation.sourceEvent());
                    if(unit<1)throw new IOException("Operation has no exact native source origin");
                    operation=operation.withWorkUnitId(baseUnit+unit);
                }
                if(operation.kind()==IndexOperationTrace.TRANSACTION_COMMIT)commits++;
                if(operation.kind()==IndexOperationTrace.TRANSACTION_ABORT)aborts++;
                workload.accept(operation);
            }
            @Override public void expectAbort()throws IOException { workload.expectAbort(); }
        });
        statements+=number(database,"SELECT count(*) FROM temp.main.sibench_statements");
        DuckDbTransactions.retain(database,windows++==0);
    }

    public Draft finish(Map<String,Object> evidence)throws IOException {
        if(finished || windows==0)throw new IOException("Trace workload is not ready for publication");
        workload.close();units.lines().close();
        manifest.put("work_units_sha256",HexFormat.of().formatHex(units.digest().digest()));
        manifest.put("source_work_units",unitCount);
        manifest.put("work_unit_rule","one native operation origin; numbered by capture-window and raw event order");
        manifest.put("committed_transactions",commits);
        manifest.put("aborted_transactions",aborts);
        TraceDirectory.stream(manifest,"baseline",ProtoTrace.file(base));
        TraceDirectory.stream(manifest,"workload",ProtoTrace.file(workload));
        manifest.put("source_statements",statements);manifest.put("last_resolved_version",workload.lastCommittedVersion());
        finished=true;
        return new Draft(directory,manifest,evidence);
    }

    @Override public void close()throws IOException {
        try(workload;var lines=units.lines()) { }
    }

    private long[] origins(Connection database)throws SQLException,IOException {
        String source="SELECT raw_record AS source_event,connection_id,query_id FROM temp.main.sibench_accesses UNION "
                +"SELECT source_event,connection_id,query_id FROM temp.main.sibench_changes";
        long[] events=new long[1024];int count=0;long previous=0;
        try(var query=database.createStatement();var rows=query.executeQuery(
                "SELECT DISTINCT source_event,connection_id,query_id FROM ("+source+") ORDER BY source_event")) {
            while(rows.next()) {
                long event=rows.getLong(1),connection=rows.getLong(2),sourceQuery=rows.getLong(3);
                if(event<=previous||connection<0||sourceQuery<0||unitCount>=Integer.MAX_VALUE-8)
                    throw new IOException("Invalid native operation origin");
                previous=event;count++;unitCount++;
                if(count>=events.length)events=Arrays.copyOf(events,Math.toIntExact(Math.min(Integer.MAX_VALUE-8L,events.length*2L)));
                events[count]=event;
                units.lines().write("{\"work_unit_id\":"+unitCount+",\"source_event\":"+event+"}\n");
            }
        }
        return Arrays.copyOf(events,count+1);
    }

    private static long number(Connection database,String sql)throws SQLException {
        try(var query=database.createStatement();var rows=query.executeQuery(sql)) {
            if(!rows.next())throw new SQLException("Missing native source count");return rows.getLong(1);
        }
    }
}
