package org.sibench.capture;

import java.util.*;
import org.sibench.adapters.proto.TraceRecord;
import org.sibench.schema.SourceSchema;

/** Labels native IQ executions for coverage; SQL text never determines index operations. */
final class PostgresQueries {
    private final Map<String,String> templates=new HashMap<>();
    private final Map<String,Stats> totals=new LinkedHashMap<>();
    private final IdentityHashMap<PostgresAccessUnits.Request,Observation> requests=new IdentityHashMap<>();
    private final Map<Long,Observation> units=new HashMap<>();
    private final boolean secondaryIndexes;

    private static final class Stats {
        long executed,successful,indexed,exported,sourceLookups,lookups;
        final Map<String,Long> indexes=new TreeMap<>(),omissions=new TreeMap<>();
        Map<String,Object> report() {
            return Map.of("executed_queries",executed,"successful_queries",successful,"failed_queries",executed-successful,
                    "queries_with_index_access",indexed,"queries_with_exported_lookups",exported,
                    "source_lookup_requests",sourceLookups,"exported_lookup_requests",lookups,
                    "indexes",Map.copyOf(indexes),"omitted_query_reasons",Map.copyOf(omissions));
        }
    }
    private static final class Observation {
        final Stats stats;
        boolean successful;
        long sourceLookups,lookups;
        final Map<String,Long> indexes=new TreeMap<>();
        Observation(Stats stats){this.stats=stats;}
    }
    PostgresQueries(SourceSchema source,boolean secondaryIndexes) {
        this.secondaryIndexes=secondaryIndexes;
        if(!source.benchmark().equals("hybench"))return;
        var sql=new com.moandjiezana.toml.Toml().read(source.source().resolve("conf/stmt_postgres.toml").toFile());
        for(int i=1;i<=6;i++) {
            String name="IQ-"+i;totals.put(name,new Stats());templates.put(normalize(sql.getString(name+".sql")),name);
        }
        totals.put("IQ-5.1",new Stats());templates.put(normalize(sql.getString("IQ-5.1.sql")),"IQ-5.1");
    }
    private static String normalize(String sql) {
        return sql.strip().replaceFirst(";\\s*$","").replaceAll("\\$[0-9]+","?").replaceAll("\\s+"," ").toLowerCase(Locale.ROOT);
    }
    void start(PostgresAccessUnits.Request request,long unit) {
        String name=templates.get(normalize(request.sql));
        if(name==null)return;
        var observation=new Observation(totals.get(name));requests.put(request,observation);units.put(unit,observation);
    }
    void index(PostgresAccessUnits.Unit unit) {
        var observation=requests.get(unit.request);
        if(observation!=null&&unit.source!=PostgresAccessUnits.Source.PLANNER_PROBE&&(unit.calls>0||unit.bitmapEnded))
            observation.indexes.merge(unit.scan.index,1L,Long::sum);
    }
    void projected(PostgresAccessUnits.Request request,int members) {
        var observation=requests.get(request);if(observation!=null)observation.sourceLookups+=members;
    }
    void finish(PostgresAccessUnits.Request request,boolean successful) {
        var observation=requests.get(request);if(observation!=null)observation.successful=successful;
    }
    void exported(TraceRecord record) {
        if(!record.hasOperation())return;
        var operation=record.getOperation();var observation=units.get(operation.getMetadata().getWorkUnitId());
        if(observation==null)return;
        if(operation.hasGet()||operation.hasScan())observation.lookups++;
        else if(operation.hasReadGroup())observation.lookups+=operation.getReadGroup().getRequestsCount();
    }
    void endTransaction() {
        for(var observation:units.values()) {
            if(observation.lookups>observation.sourceLookups)
                throw new IllegalStateException("Exported IQ reads lack certified native access evidence");
            var stats=observation.stats;stats.executed++;
            if(observation.successful)stats.successful++;
            if(!observation.indexes.isEmpty())stats.indexed++;
            if(observation.lookups>0)stats.exported++;
            stats.sourceLookups+=observation.sourceLookups;stats.lookups+=observation.lookups;
            observation.indexes.forEach((index,count)->stats.indexes.merge(index,count,Long::sum));
            if(observation.lookups==0) {
                String reason=!observation.successful?"failed_statement":observation.indexes.isEmpty()?"no_executed_index_access":
                        !secondaryIndexes?"secondary_scope":"no_exported_access";
                stats.omissions.merge(reason,1L,Long::sum);
            }
        }
        requests.clear();units.clear();
    }
    Map<String,Long> successful() {
        var result=new LinkedHashMap<String,Long>();
        totals.forEach((name,stats)->{if(!name.equals("IQ-5.1"))result.put(name,stats.successful);});
        return result;
    }
    Map<String,Object> report() {
        var result=new LinkedHashMap<String,Object>();long executed=0,successful=0,indexed=0,exported=0;
        for(var entry:totals.entrySet()) {
            result.put(entry.getKey(),entry.getValue().report());
            if(entry.getKey().equals("IQ-5.1"))continue;
            var s=entry.getValue();executed+=s.executed;successful+=s.successful;indexed+=s.indexed;exported+=s.exported;
        }
        return Map.of("iq",result,"executed_iq_queries",executed,"successful_iq_queries",successful,
                "iq_queries_with_index_access",indexed,"iq_queries_with_exported_lookups",exported,
                "indexed_iq_coverage",indexed==0?0.0:(double)exported/indexed,
                "all_iq_coverage",executed==0?0.0:(double)exported/executed,
                "scope","Native IQ main SQL executions; IQ-5.1 parameter sampling is reported separately. Index access coverage is not full SQL or HTAP coverage.");
    }
}
