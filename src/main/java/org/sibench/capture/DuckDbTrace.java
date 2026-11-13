package org.sibench.capture;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.sibench.config.BenchmarkSelection.Mode;
import org.sibench.schema.SourceSchema;

/** Both native inputs publish the same baseline and workload contract. */
public final class DuckDbTrace implements AutoCloseable {
    private final Mode mode;
    private final Path directory;
    private final boolean secondaryIndexes;
    private final List<IndexCatalog.Definition> catalog;
    private final LinkedHashMap<String,Object> manifest;
    private ProtoTrace.Writer base;
    private final ProtoTrace.Writer workload;
    private final WorkUnits.Output units;
    private final Map<String,Long> shapes = new LinkedHashMap<>();
    private long unitCount,commits,aborts,localSearches,statements;
    private int windows;
    private boolean finished;
    private boolean queryStage;

    long lastVersion() { return commits+aborts; }


    public DuckDbTrace(Mode mode,SourceSchema source,Path directory,boolean secondaryIndexes)throws IOException {
        this(mode,source,directory,secondaryIndexes,false,null);
    }

    DuckDbTrace(Mode mode,SourceSchema source,Path directory,boolean secondaryIndexes,boolean exportQueries,RequestBudget budget)throws IOException {
        if(mode!=Mode.CAPTURE || source.benchmark().equals("ycsb"))
            throw new IllegalArgumentException("Unsupported native benchmark mode");
        this.secondaryIndexes=secondaryIndexes;this.mode=mode;this.directory=directory;catalog=IndexCatalog.duckDbDefinitions(source);
        manifest=new LinkedHashMap<>(IndexCatalog.provenance(source,catalog));
        IndexCatalog.fromManifest(new Gson().toJsonTree(manifest).getAsJsonObject());
        Files.createDirectory(directory);
        Files.writeString(directory.resolve("INCOMPLETE"),"Native trace generation/publication pending\n",StandardOpenOption.CREATE_NEW);
        units=WorkUnits.create(directory);
        workload=new ProtoTrace.Writer(directory.resolve("workload.pb.zst"),secondaryIndexes,exportQueries,budget);
    }

    void queryStage(boolean value) { queryStage=value; }

    public void baseline(Connection database,Map<String,Long> expected)throws SQLException,IOException {
        var writer=new ProtoTrace.Writer(directory.resolve("baseline.pb.zst"),secondaryIndexes);base=writer;
        try(writer) {
            var counts=NativeBaseline.write(database,org.sibench.config.Engine.DUCKDB,catalog.stream().filter(d->secondaryIndexes||d.indexId()==0).toList(),writer);
            Generation.requireRows(expected,counts);
            manifest.put("baseline_rows",counts.values().stream().mapToLong(Long::longValue).sum());
            manifest.put("baseline_operations",base.count());
        }
    }

    /** The caller closes the host and publishes; native data is discarded only after publication. */
    public record Draft(Path directory,Map<String,Object> manifest) {
        public Draft {manifest=Map.copyOf(manifest);}
        public Map<String,Object> publish()throws IOException,SQLException {
            if(!Files.isRegularFile(directory.resolve("INCOMPLETE"))||Files.exists(directory.resolve("manifest.json")))
                throw new IOException("Trace publication is not pending");
            var result=new LinkedHashMap<>(manifest);
            result.put("status","complete_for_declared_projection");
            Path pending=directory.resolve("manifest.json.partial");
            Files.writeString(pending,new GsonBuilder().setPrettyPrinting().create().toJson(result),StandardOpenOption.CREATE_NEW);
            Files.move(pending,directory.resolve("manifest.json"),StandardCopyOption.ATOMIC_MOVE);
            Files.delete(directory.resolve("INCOMPLETE"));
            return java.util.Collections.unmodifiableMap(result);
        }
    }

    /** A closed source window has its own temporary bindings; only its new facts are projected. */
    public void append(Connection database)throws SQLException,IOException {
        if(finished || !manifest.containsKey("baseline_rows"))throw new IOException("Trace is not accepting workload output");
        long before=lastVersion(),baseUnit=unitCount;
        long[] origins=origins(database);
        IndexOperationTrace.Consumer output=operation->{
            if(queryStage&&operation.kind()==IndexOperationTrace.TRANSACTION_BEGIN)workload.expectQuery();
            if(!IndexOperationTrace.isControl(operation)) {
                int unit=Arrays.binarySearch(origins,1,origins.length,operation.sourceEvent());
                if(unit<1)throw new IOException("Operation has no exact native source origin");
                operation=operation.withWorkUnitId(baseUnit+unit);
            }
            if(operation.kind()==IndexOperationTrace.TRANSACTION_COMMIT)commits++;
            if(operation.kind()==IndexOperationTrace.TRANSACTION_ABORT)aborts++;
            if(operation.kind()==IndexOperationTrace.SEARCH&&operation.mutationOrdinal()>0)localSearches++;
            if(operation.grouped()&&operation.expansionPosition()==0)
                shapes.merge((operation.flags()&IndexOperationTrace.INTERSECT_GROUP)!=0?"intersection":"union",1L,Long::sum);
            workload.accept(operation);
        };
        DuckDbProjection.write(database,secondaryIndexes,output);
        if(lastVersion()-before!=number(database,"SELECT count(*) FROM temp.main.sibench_transactions"))
            throw new IOException("Incomplete native transaction projection");
        statements+=number(database,"SELECT count(*) FROM temp.main.sibench_statements");
        DuckDbTransactions.retain(database,windows++==0);
    }

    public Draft finish(Connection database,Map<String,Object> evidence)throws SQLException,IOException {
        if(finished || windows==0)throw new IOException("Trace workload is not ready for publication");
        workload.close();units.lines().close();
        DuckDbTransactions.finish(database);
        manifest.put("work_units_sha256",HexFormat.of().formatHex(units.digest().digest()));
        manifest.put("source_work_units",unitCount);
        manifest.put("work_unit_rule","one native operation origin; numbered by capture-window and raw event order");
        manifest.put("output","proto");manifest.put("value_kinds",List.of("row_id"));
        manifest.put("mode",mode.name().toLowerCase(java.util.Locale.ROOT));
        manifest.put("generation","native_duckdb");manifest.put("secondary_index_coverage",secondaryIndexes?"all_source_definitions":"primary_only");
        manifest.put("workload_operations",workload.count());manifest.put("committed_transactions",commits);
        manifest.put("aborted_transactions",aborts);manifest.put("transaction_local_searches",localSearches);
        if(!shapes.isEmpty())manifest.put("searched_accesses_by_shape",shapes);
        var baselineFiles=List.of(ProtoTrace.file("baseline.pb.zst",base));
        var workloadFiles=List.of(ProtoTrace.file("workload.pb.zst",workload));
        manifest.put("proto_files",Map.of("baseline",baselineFiles,"workload",workloadFiles));
        var gson=new Gson();
        manifest.put("baseline_trace_sha256",ProtoTrace.identity(gson.toJsonTree(baselineFiles).getAsJsonArray()));
        manifest.put("workload_trace_sha256",ProtoTrace.identity(gson.toJsonTree(workloadFiles).getAsJsonArray()));
        manifest.put("source_statements",statements);manifest.put("last_resolved_version",workload.lastCommittedVersion());
        manifest.put("source_evidence",Map.copyOf(evidence));
        finished=true;
        return new Draft(directory,manifest);
    }

    public Draft workload(Connection database,Map<String,Object> evidence)throws SQLException,IOException {
        try(this) { append(database);return finish(database,evidence); }
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
