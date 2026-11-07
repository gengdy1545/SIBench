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
import org.sibench.generation.DuckDbData;
import org.sibench.replay.ReplayConfig;
import org.sibench.replay.ReplayProjection;
import org.sibench.schema.SourceSchema;

/** Both native inputs publish the same baseline, workload and sealed data contract. */
public final class DuckDbTrace {
    private final Mode mode;
    private final Path directory;
    private final List<IndexCatalog.Definition> catalog;
    private final LinkedHashMap<String,Object> manifest;
    private ProtoTrace.Writer base;

    public DuckDbTrace(Mode mode,SourceSchema source,Path directory)throws IOException {
        if(mode!=Mode.CAPTURE || source.benchmark().equals("ycsb"))
            throw new IllegalArgumentException("Unsupported native benchmark mode");
        this.mode=mode;this.directory=directory;catalog=IndexCatalog.duckDbDefinitions(source);
        manifest=new LinkedHashMap<>(IndexCatalog.provenance(source,catalog));
        IndexCatalog.fromManifest(new Gson().toJsonTree(manifest).getAsJsonObject());
        Files.createDirectory(directory);
        Files.writeString(directory.resolve("INCOMPLETE"),"Native trace generation/publication pending\n",StandardOpenOption.CREATE_NEW);
    }

    public void baseline(Connection database,Map<String,Long> expected)throws SQLException,IOException {
        var writer=new ProtoTrace.Writer(directory.resolve("baseline.pb.zst"));base=writer;
        try(writer) {
            var counts=NativeBaseline.write(database,org.sibench.config.Engine.DUCKDB,catalog,writer);
            Generation.requireRows(expected,counts);
            manifest.put("baseline_rows",counts.values().stream().mapToLong(Long::longValue).sum());
            manifest.put("baseline_operations",base.count());
        }
    }

    /** The caller closes the host and moves its native data directory here before publishing. */
    public record Draft(Path directory,Map<String,Object> manifest) {
        public Draft {manifest=Map.copyOf(manifest);}
        public Map<String,Object> publish()throws IOException,SQLException {
            if(!Files.isRegularFile(directory.resolve("INCOMPLETE"))||Files.exists(directory.resolve("manifest.json")))
                throw new IOException("Trace publication is not pending");
            var result=new LinkedHashMap<>(manifest);
            var data=DuckDbData.inventory(directory.resolve("data"));
            if(data.lastResolvedVersion()!=((Number)manifest.get("last_resolved_version")).longValue())
                throw new IOException("Native data and Trace resolution watermarks differ");
            result.put("native_data",data);
            result.put("status","complete_for_declared_projection");
            Path pending=directory.resolve("manifest.json.partial");
            Files.writeString(pending,new GsonBuilder().setPrettyPrinting().create().toJson(result),StandardOpenOption.CREATE_NEW);
            Files.move(pending,directory.resolve("manifest.json"),StandardCopyOption.ATOMIC_MOVE);
            Files.delete(directory.resolve("INCOMPLETE"));
            return java.util.Collections.unmodifiableMap(result);
        }
    }

    /** Facts and mode-specific projection bindings must be complete; source sessions must have closed. */
    public Draft workload(Connection database,Map<String,Object> evidence)throws SQLException,IOException {
        if(!manifest.containsKey("baseline_rows"))throw new IOException("Baseline export is incomplete");
        try {
            var origins=origins(database,directory);
            manifest.put("work_units_sha256",origins.sha256());manifest.put("source_work_units",origins.count());
            manifest.put("work_unit_rule","one native operation origin; numbered by raw event identity, not transaction resolution order");
            var policy=new ReplayConfig(true,0,0,0,0,"operation");
            long[] commits={0},aborts={0},localSearches={0};
            var shapes=new LinkedHashMap<String,Long>();
            var workload=new ProtoTrace.Writer(directory.resolve("workload.pb.zst"));
            try(workload;var validation=new ReplayProjection(policy,true,operation->{})) {
                IndexOperationTrace.Consumer output=operation->{
                    if(!IndexOperationTrace.isControl(operation)) {
                        int unit=Arrays.binarySearch(origins.firstEvents(),1,(int)origins.count()+1,operation.sourceEvent());
                        if(unit<1)throw new IOException("Operation has no exact native source origin");
                        operation=operation.withWorkUnitId(unit);
                    }
                    if(operation.kind()==IndexOperationTrace.TRANSACTION_COMMIT)commits[0]++;
                    if(operation.kind()==IndexOperationTrace.TRANSACTION_ABORT)aborts[0]++;
                    if(operation.kind()==IndexOperationTrace.SEARCH&&operation.mutationOrdinal()>0)localSearches[0]++;
                    if(operation.grouped()&&operation.expansionPosition()==0)
                        shapes.merge((operation.flags()&IndexOperationTrace.INTERSECT_GROUP)!=0?"intersection":"union",1L,Long::sum);
                    validation.accept(operation);workload.accept(operation);
                };
                DuckDbProjection.write(database,output);
                validation.finish();
            }
            manifest.put("output","proto");manifest.put("value_kinds",List.of("row_id"));
            manifest.put("mode",mode.name().toLowerCase(java.util.Locale.ROOT));
            manifest.put("generation","native_duckdb");manifest.put("secondary_index_coverage","all_source_definitions");
            manifest.put("workload_operations",workload.count());manifest.put("committed_transactions",commits[0]);
            manifest.put("aborted_transactions",aborts[0]);manifest.put("transaction_local_searches",localSearches[0]);
            if(!shapes.isEmpty())manifest.put("searched_accesses_by_shape",shapes);
            var baselineFiles=List.of(ProtoTrace.file("baseline.pb.zst",base));
            var workloadFiles=List.of(ProtoTrace.file("workload.pb.zst",workload));
            manifest.put("proto_files",Map.of("baseline",baselineFiles,"workload",workloadFiles));
            var gson=new Gson();
            manifest.put("baseline_trace_sha256",ProtoTrace.identity(gson.toJsonTree(baselineFiles).getAsJsonArray()));
            manifest.put("workload_trace_sha256",ProtoTrace.identity(gson.toJsonTree(workloadFiles).getAsJsonArray()));
            manifest.put("source_statements",number(database,"SELECT count(*) FROM temp.main.sibench_statements"));
            long resolved=number(database,"SELECT count(*) FROM temp.main.sibench_transactions");
            if(commits[0]+aborts[0]!=resolved)throw new IOException("Incomplete native transaction projection");
            manifest.put("last_resolved_version",resolved);
            manifest.put("source_evidence",Map.copyOf(evidence));
            DuckDbTransactions.seal(database);
            return new Draft(directory,manifest);
        }catch(SQLException|IOException|RuntimeException failure) {
            try{Files.writeString(directory.resolve("failure.txt"),failure.toString()+"\n",StandardOpenOption.CREATE_NEW);}
            catch(IOException diagnostic){failure.addSuppressed(diagnostic);}throw failure;
        }
    }

    private static WorkUnits.Catalog origins(Connection database,Path directory)throws SQLException,IOException {
        String origins="SELECT raw_record AS source_event,connection_id,query_id FROM temp.main.sibench_accesses UNION "
                +"SELECT source_event,connection_id,query_id FROM temp.main.sibench_changes";
        long[] events=new long[1024];int count=0;long previous=0;
        var output=WorkUnits.create(directory);
        try(var lines=output.lines();var query=database.createStatement();var rows=query.executeQuery(
                "SELECT DISTINCT source_event,connection_id,query_id FROM ("+origins+") ORDER BY source_event")) {
            while(rows.next()) {
                long event=rows.getLong(1),connection=rows.getLong(2),sourceQuery=rows.getLong(3);
                if(event<=previous||connection<0||sourceQuery<0||count>=Integer.MAX_VALUE-8)
                    throw new IOException("Invalid native operation origin");
                previous=event;count++;
                if(count>=events.length)events=Arrays.copyOf(events,Math.toIntExact(Math.min(Integer.MAX_VALUE-8L,events.length*2L)));
                events[count]=event;
                lines.write("{\"work_unit_id\":"+count+",\"source_event\":"+event+",\"connection_id\":"+connection+",\"query_id\":"+sourceQuery+"}\n");
            }
        }
        return new WorkUnits.Catalog(HexFormat.of().formatHex(output.digest().digest()),events,count);
    }

    private static long number(Connection database,String sql)throws SQLException {
        try(var query=database.createStatement();var rows=query.executeQuery(sql)) {
            if(!rows.next())throw new SQLException("Missing native source count");return rows.getLong(1);
        }
    }
}
