package org.sibench.capture;

import static org.junit.Assert.*;
import static org.sibench.config.ConfigValues.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.Test;

public class DuckDbGenerationTest {
    static Map<String,Object> profile(String benchmark)throws Exception {
        var document=parse(Files.readString(Path.of("config/benchmark/"+benchmark+".yaml")));
        var nativeConfig=object(document,"native");
        nativeConfig.put("timeout_minutes",1);document.put("native",nativeConfig);return document;
    }
    @Test public void nativeOptionsAreExplicitAndDoNotSilentlyIgnoreReplacedSettings()throws Exception {
        for(String benchmark:new String[]{"ycsb","sysbench","hybench"}) {
            var n=object(profile(benchmark),"native");Generation.validateNative(benchmark,n);
            var bad=new LinkedHashMap<>(n);bad.put("jdbc_table_creator","JdbcDBCreateTable");
            assertThrows(IllegalArgumentException.class,()->Generation.validateNative(benchmark,bad));
            bad.remove("jdbc_table_creator");bad.put("timeout_minutes",0);
            assertThrows(IllegalArgumentException.class,()->Generation.validateNative(benchmark,bad));
        }
    }
    @Test public void baselineCountsRejectMissingBatchesAndBalancedTableErrors() throws Exception {
        var expected=Map.of("sbtest1",1000L,"sbtest2",1000L);
        Generation.requireRows(expected,Map.of("sbtest2",1000L,"sbtest1",1000L));
        for(var actual:java.util.List.of(Map.of("sbtest1",999L,"sbtest2",1001L),
                Map.of("sbtest1",1000L),Map.of("sbtest1",1000L,"sbtest2",1000L,"extra",1L)))
            assertThrows(java.io.IOException.class,()->Generation.requireRows(expected,actual));
        for(long count:new long[]{999,1001})
            assertThrows(java.io.IOException.class,()->Generation.requireRows(
                    Map.of("usertable",1000L),Map.of("usertable",count)));
        var profile=parse(Files.readString(Path.of("config/benchmark/hybench.yaml")));
        var rows=org.sibench.workloads.ScaleInspector.nativeRows(Path.of(""),profile);
        assertEquals(87060000,rows.values().stream().mapToLong(Long::longValue).sum());
        var balanced=new LinkedHashMap<>(rows);balanced.put("loanapps",5999999L);balanced.put("loantrans",6000001L);
        assertThrows(java.io.IOException.class,()->Generation.requireRows(rows,balanced));
    }

    @Test public void rejectsAlternativeSysbenchExecutables() throws Exception {
        var nativeConfig=object(profile("sysbench"),"native");
        for(String path:new String[]{"another-client","/usr/bin/true","../../../outside"}) {
            nativeConfig.put("binary",path);
            assertThrows(IllegalArgumentException.class,()->Generation.validateNative("sysbench",nativeConfig));
        }
    }
    @Test public void engineBuildPathCannotEscapeRepository() throws Exception {
        Path root=Files.createTempDirectory("sibench-engine-path-");
        try {
            assertEquals(root.resolve("build/services/duckdb"),NativeBuildCheck.engineDirectory(root,
                    Map.of("duckdb",Map.of("build_directory","build/services/duckdb"))));
            for(String path:new String[]{"build/services","../outside","build/services/../../source"})
                assertThrows(java.io.IOException.class,()->NativeBuildCheck.engineDirectory(root,
                        Map.of("duckdb",Map.of("build_directory",path))));
        }finally{Files.delete(root);}
    }
    @Test public void rejectsMissingWorkloadsAndOverridesOfConnectionOrScale()throws Exception {
        var n=object(profile("ycsb"),"native");n.put("workloads",Map.of());
        assertThrows(IllegalArgumentException.class,()->Generation.validateNative("ycsb",n));
        n.put("workloads",Map.of("workloada",Map.of("operationcount",1,"properties",Map.of("sibench.socket","elsewhere"))));
        assertThrows(IllegalArgumentException.class,()->Generation.validateNative("ycsb",n));
        n.put("workloads",Map.of("workloada",Map.of("operationcount",1)));n.put("insertcount",1);
        assertThrows(IllegalArgumentException.class,()->Generation.validateNative("ycsb",n));
    }
}
