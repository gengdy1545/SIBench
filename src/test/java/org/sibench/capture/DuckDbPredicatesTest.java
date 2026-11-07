package org.sibench.capture;

import static org.junit.Assert.*;
import java.io.IOException;
import java.util.List;
import org.junit.Test;
import org.sibench.trace.IndexKey;

public class DuckDbPredicatesTest {
    private static DuckDbPredicates.Predicate predicate(String operator,String value) {
        return new DuckDbPredicates.Predicate(operator,"BIGINT",value);
    }
    @Test public void encodesNativeValuesWithoutEvaluatingSqlTokens()throws Exception {
        assertEquals(IndexKey.ofInt(Long.MIN_VALUE),DuckDbKeys.parse("BIGINT",Long.toString(Long.MIN_VALUE)));
        assertEquals(IndexKey.ofText("雪'\n\\;DROP TABLE items;--"),DuckDbKeys.parse("VARCHAR","'雪''\n\\;DROP TABLE items;--'"));
        assertEquals(IndexKey.ofText("a".repeat(32000)),DuckDbKeys.parse("VARCHAR","'"+"a".repeat(32000)+"'"));
        assertEquals(IndexKey.NULL,DuckDbKeys.parse("INTEGER","NULL::INTEGER"));
        assertEquals(IndexKey.parse("timestamp","2026-01-02 03:04:05.123456"),DuckDbKeys.parse("TIMESTAMP","'2026-01-02 03:04:05.123456'::TIMESTAMP"));
        assertEquals(IndexKey.parse("timestamp","2026-01-02 00:00:00"),DuckDbKeys.parse("DATE","'2026-01-02'::DATE"));
        assertEquals(IndexKey.parse("timestamp","infinity"),DuckDbKeys.parse("DATE","'infinity'::DATE"));
        for(var bad:List.of(new String[]{"INTEGER","2147483648"},new String[]{"TINYINT","128"},
                new String[]{"BIGINT","+1"},new String[]{"BIGINT","-0"},new String[]{"VARCHAR","'a';SELECT 1;--'"},
                new String[]{"VARCHAR","chr(0)"},new String[]{"TIMESTAMP","'2026-01-02'"},new String[]{"DECIMAL(10,2)","1.25"}))
            assertThrows(IOException.class,()->DuckDbKeys.parse(bad[0],bad[1]));
    }
    @Test public void retainsInclusiveExclusiveBoundsAndChecksDeltaPredicates()throws Exception {
        var conditions=List.of(predicate("GREATERTHAN","10"),predicate("LESSTHANOREQUALTO","20"));
        var result=DuckDbPredicates.ranges("int",List.of(new DuckDbPredicates.Probe("base",conditions),
                new DuckDbPredicates.Probe("deleted_rows",conditions)));
        assertEquals(List.of(new IndexOperationTrace.Range(IndexKey.ofInt(10),false,IndexKey.ofInt(20),true,false,0)),result);
        assertThrows(IOException.class,()->DuckDbPredicates.ranges("int",List.of(new DuckDbPredicates.Probe("base",conditions),
                new DuckDbPredicates.Probe("deleted_rows",List.of(predicate("EQUAL","10"))))));
    }
    @Test public void nativeOrProbesBecomeADeduplicatedPointUnion()throws Exception {
        var probes=List.of(new DuckDbPredicates.Probe("base",List.of(predicate("EQUAL","20"))),
                new DuckDbPredicates.Probe("base",List.of(predicate("EQUAL","10"))),
                new DuckDbPredicates.Probe("base",List.of(predicate("EQUAL","20"))));
        assertEquals(List.of(IndexOperationTrace.Range.equal(IndexKey.ofInt(10)),IndexOperationTrace.Range.equal(IndexKey.ofInt(20))),
                DuckDbPredicates.ranges("int",probes));
    }
    @Test public void refusesUnsupportedAndContradictoryAdoptedConditions()throws Exception {
        for(var predicates:List.of(List.of(predicate("NOTEQUAL","1")),List.of(predicate("EQUAL","NULL::BIGINT")),
                List.of(predicate("GREATERTHAN","10"),predicate("LESSTHAN","10")),
                List.of(predicate("GREATERTHAN","20"),predicate("LESSTHAN","10"))))
            assertThrows(IOException.class,()->DuckDbPredicates.ranges("int",List.of(new DuckDbPredicates.Probe("base",predicates))));
        assertThrows(IOException.class,()->DuckDbPredicates.ranges("varchar",List.of(new DuckDbPredicates.Probe("base",List.of(predicate("EQUAL","1"))))));
        assertThrows(IOException.class,()->DuckDbPredicates.ranges("int",List.of(
                new DuckDbPredicates.Probe("base",List.of(predicate("GREATERTHAN","1"))),
                new DuckDbPredicates.Probe("base",List.of(predicate("EQUAL","0"))))));
    }
}
