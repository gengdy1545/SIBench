package org.sibench.capture;

import static org.junit.Assert.*;
import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.sibench.schema.SourceSchema;
import org.sibench.trace.IndexKey;

public class PostgresAccessProjectionTest {
    private IndexCatalog catalog()throws Exception {
        return IndexCatalog.fromSchema(SourceSchema.of("sysbench",Path.of("benchmark/sysbench"),Map.of("tables",1L)));
    }
    private PostgresAccessUnits.Unit scan(String name,String kind) {
        var scan=new PostgresAccessUnits.Scan("scan",name);scan.kind=kind;
        JsonArray keys=JsonParser.parseString("[{\"attribute\":1,\"strategy\":4,\"flags\":0,\"type_oid\":23,\"value\":\"1\"}]").getAsJsonArray();
        return new PostgresAccessUnits.Unit(scan,PostgresAccessUnits.Source.EXECUTOR,keys,
                new PostgresAccessUnits.Request(1,"sibench:test","source",1L),1);
    }
    @Test public void stoppedSecondaryTiesUseObservedPrimaryKeysNotAnInventedKvOrder()throws Exception {
        var unit=scan("k_1","index");unit.calls=5;unit.found=5;unit.heapVisible=5;unit.lastFound=true;unit.forward=true;
        unit.rowsBeforeLastRun=3;unit.lastValue=new JsonPrimitive("5");
        unit.lastPks=List.of(new JsonPrimitive("9"),new JsonPrimitive("3"));
        var result=PostgresAccessProjection.decide(unit,catalog(),true);
        assertTrue(result.projected());assertEquals(5,result.count());assertEquals(3,result.parts().size());
        var prefix=result.parts().get(0);assertEquals(3,prefix.count());assertEquals(IndexKey.ofInt(5),prefix.range().upper());
        assertFalse(prefix.range().upperInclusive());assertEquals(0,prefix.range().limit());
        assertEquals(List.of(IndexKey.ofInt(9),IndexKey.ofInt(3)),result.parts().subList(1,3).stream().map(PostgresAccessProjection.Part::pinned).toList());
        unit.lastPks=List.of(new JsonPrimitive("9"),new JsonPrimitive("9"));
        assertEquals("row_returned_twice",PostgresAccessProjection.decide(unit,catalog(),true).reason());
        unit.identityMissing=true;
        assertEquals("stopped_scan_without_row_identity",PostgresAccessProjection.decide(unit,catalog(),true).reason());
    }
    @Test public void refusesUnknownConsumptionAndLossyOrUnresolvedBitmapCandidates()throws Exception {
        var unit=scan("sbtest1_pkey","index");unit.calls=1;unit.lastFound=true;unit.forward=true;
        assertEquals("stopped_without_visible_row",PostgresAccessProjection.decide(unit,catalog(),true).reason());
        unit.backward=true;
        assertEquals("scan_direction_changed",PostgresAccessProjection.decide(unit,catalog(),true).reason());
        var bitmap=scan("k_1","bitmap");bitmap.bitmapEnded=true;bitmap.bitmapLossy=true;
        assertEquals("bitmap_heap_lossy_page",PostgresAccessProjection.decide(bitmap,catalog(),true).reason());
        bitmap.bitmapLossy=false;bitmap.summarizedUnresolved=1;
        assertEquals("bitmap_heap_stopped_early",PostgresAccessProjection.decide(bitmap,catalog(),true).reason());
    }
}
