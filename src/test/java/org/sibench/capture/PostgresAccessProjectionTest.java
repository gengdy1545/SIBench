package org.sibench.capture;

import static org.junit.Assert.*;
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
        return scan(name,kind,"[{\"attribute\":1,\"strategy\":4,\"flags\":0,\"type_oid\":23,\"value\":\"1\"}]");
    }
    private PostgresAccessUnits.Unit scan(String name,String kind,String keys) {
        var scan=new PostgresAccessUnits.Scan("scan",name);scan.kind=kind;
        return new PostgresAccessUnits.Unit(scan,PostgresAccessUnits.Source.EXECUTOR,JsonParser.parseString(keys).getAsJsonArray(),
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
    @Test public void bitmapAndStopsAtAnEmptyIntersectionButOtherMissingInputsAreRefused()throws Exception {
        var first=scan("sbtest1_pkey","bitmap");first.bitmapEnded=true;
        var second=scan("k_1","bitmap");var third=scan("k_1","bitmap");
        var group=new PostgresAccessUnits.BitmapGroup(false);group.summarizedIntersection=false;
        group.members.addAll(List.of(first,second,third));
        var result=PostgresAccessProjection.decideGroup(group,catalog(),true);
        // Only the input that ran searched; PostgreSQL skipped the rest after its empty bitmap.
        assertTrue(result.projected());assertEquals(0,result.count());assertEquals(1,result.parts().size());
        assertEquals(0,result.parts().get(0).index());
        group.visible=1;
        assertEquals("bitmap_and_input_without_bitmap",PostgresAccessProjection.decideGroup(group,catalog(),true).reason());
        group.visible=0;third.bitmapEnded=true;
        assertEquals("bitmap_and_input_without_bitmap",PostgresAccessProjection.decideGroup(group,catalog(),true).reason());
        // No input fetched at all: a rescan without a search, which is excluded rather than projected.
        third.bitmapEnded=false;first.bitmapEnded=false;
        var rescanned=PostgresAccessProjection.decideGroup(group,catalog(),true);
        assertTrue(rescanned.excluded());assertEquals("rescan_without_fetch",rescanned.reason());
        second.bitmapTids=1;
        assertEquals("bitmap_and_input_without_bitmap",PostgresAccessProjection.decideGroup(group,catalog(),true).reason());
    }
    @Test public void postgresCharKeysCompareWithoutTrailingBlanksOnlyOnCharIndexes()throws Exception {
        var catalog=IndexCatalog.fromSchema(SourceSchema.hybench(Path.of("benchmark/HyBench-2024")));
        var type=catalog.named("idx_transfer_3");var category=catalog.named("idx_company_1");
        assertTrue(catalog.blankPadded(type));assertFalse(catalog.blankPadded(category));
        assertEquals(type.encode("salary"),catalog.postgresKey(type,"salary    "));
        assertEquals(category.encode("tech  "),catalog.postgresKey(category,"tech  "));
        String equal="[{\"attribute\":1,\"strategy\":3,\"flags\":0,\"type_oid\":%d,\"value\":\"salary\"}]";
        var unit=scan("idx_transfer_3","index",equal.formatted(1042));unit.calls=1;unit.found=1;unit.heapVisible=1;unit.forward=true;
        var result=PostgresAccessProjection.decide(unit,catalog,true);
        assertTrue(result.projected());assertEquals(type.encode("salary"),result.parts().get(0).range().lower());
        // A text operand would compare with the padding the CHAR(n) column ignores.
        unit=scan("idx_transfer_3","index",equal.formatted(25));unit.calls=1;unit.found=1;unit.heapVisible=1;unit.forward=true;
        assertEquals("unsupported_key_type",PostgresAccessProjection.decide(unit,catalog,true).reason());
    }
}
