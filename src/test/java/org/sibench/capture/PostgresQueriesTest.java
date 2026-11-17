package org.sibench.capture;

import static org.junit.Assert.*;
import static org.sibench.config.ConfigValues.*;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.sibench.adapters.proto.*;
import org.sibench.schema.SourceSchema;

public class PostgresQueriesTest {
    private static PostgresAccessUnits.Request request(int iq) {
        var sql=new com.moandjiezana.toml.Toml().read(Path.of("benchmark/HyBench-2024/conf/stmt_postgres.toml").toFile());
        return new PostgresAccessUnits.Request(iq,"source",sql.getString("IQ-"+iq+".sql").replace("?","$1"));
    }
    private static TraceRecord lookup(long unit) {
        return TraceRecord.newBuilder().setOperation(TraceOperation.newBuilder().setGet(GetRequest.getDefaultInstance())
                .setMetadata(TraceMetadata.newBuilder().setWorkUnitId(unit))).build();
    }
    @Test public void coverageCountsSuccessfulNativeQueriesAndActualExportedMembers() throws Exception {
        var coverage=new PostgresQueries(SourceSchema.hybench(Path.of("benchmark/HyBench-2024")),true);
        var first=request(1);coverage.start(first,11);
        var unit=new PostgresAccessUnits.Unit(new PostgresAccessUnits.Scan("scan","idx_transfer_1"),
                PostgresAccessUnits.Source.EXECUTOR,new com.google.gson.JsonArray(),first,1);
        unit.calls=1;coverage.index(unit);coverage.projected(first,2);coverage.finish(first,true);
        coverage.exported(lookup(11));coverage.exported(lookup(11));coverage.endTransaction();
        var sequential=request(2);coverage.start(sequential,12);coverage.finish(sequential,true);coverage.endTransaction();
        var failed=request(3);coverage.start(failed,13);coverage.finish(failed,false);coverage.endTransaction();
        var report=coverage.report();var iq=object(report,"iq");
        assertEquals(3L,report.get("executed_iq_queries"));assertEquals(2L,report.get("successful_iq_queries"));
        assertEquals(1L,report.get("iq_queries_with_index_access"));assertEquals(1L,report.get("iq_queries_with_exported_lookups"));
        assertEquals(2L,object(iq,"IQ-1").get("exported_lookup_requests"));
        assertEquals(Map.of("idx_transfer_1",1L),object(iq,"IQ-1").get("indexes"));
        assertEquals(Map.of("no_executed_index_access",1L),object(iq,"IQ-2").get("omitted_query_reasons"));
        assertEquals(Map.of("failed_statement",1L),object(iq,"IQ-3").get("omitted_query_reasons"));
        var batch=Map.<String,Object>of("client",Map.of("xp",Map.of("iq1",1L,"iq2",1L,"iq3",0L,"iq4",0L,"iq5",0L,"iq6",0L)));
        PostgresGeneration.requireIqCounts(List.of(batch),coverage.successful());
        assertThrows(java.io.IOException.class,()->PostgresGeneration.requireIqCounts(List.of(batch),Map.of()));
    }
    @Test public void completedAccessesDoNotCertifyTheirFailedEnclosingQuery() throws Exception {
        var coverage=new PostgresQueries(SourceSchema.hybench(Path.of("benchmark/HyBench-2024")),true);
        var failed=request(1);coverage.start(failed,7);coverage.projected(failed,1);coverage.exported(lookup(7));coverage.finish(failed,false);
        coverage.endTransaction();
        var report=object(object(coverage.report(),"iq"),"IQ-1");
        assertEquals(0L,report.get("successful_queries"));assertEquals(1L,report.get("failed_queries"));
        assertEquals(1L,report.get("exported_lookup_requests"));
        var invalid=request(1);coverage.start(invalid,8);coverage.exported(lookup(8));
        assertThrows(IllegalStateException.class,coverage::endTransaction);
    }
}
