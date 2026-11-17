package org.sibench.capture;

import static org.junit.Assert.*;
import static org.sibench.config.ConfigValues.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.Test;

public class DuckDbGenerationTest {
    @org.junit.Rule public org.junit.rules.TemporaryFolder temporary = new org.junit.rules.TemporaryFolder();

    @Test public void outputAliasesCannotHideOverlapOrEscapeCaptureDirectory() throws Exception {
        Path root=temporary.getRoot().toPath().toRealPath();
        Path capture=Files.createDirectories(root.resolve("results/capture"));
        Path target=Files.createDirectory(capture.resolve("target"));
        Files.createSymbolicLink(capture.resolve("alias"),target);
        assertEquals(Generation.outputDirectory(root,"results/capture/target/run"),
                Generation.outputDirectory(root,"results/capture/alias/run"));
        Files.createSymbolicLink(capture.resolve("outside"),Files.createDirectory(root.resolve("source")));
        assertThrows(IllegalArgumentException.class,()->Generation.outputDirectory(root,"results/capture/outside/run"));
        Files.createSymbolicLink(capture.resolve("dangling"),root.resolve("missing"));
        assertThrows(IllegalArgumentException.class,()->Generation.outputDirectory(root,"results/capture/dangling"));
    }

    @Test public void exportDefaultsToPrimaryWithoutChangingSourceIndexes() throws Exception {
        assertFalse(Generation.secondaryIndexes(Map.of()));
        for (String benchmark : new String[]{"ycsb", "sysbench", "hybench"}) {
            var document = profile(benchmark);
            assertFalse(Generation.secondaryIndexes(document));
            document.put("trace", Map.of("secondary_indexes", true));
            assertTrue(Generation.secondaryIndexes(document));
        }
        for (var trace : java.util.List.of(Map.of("secondary_indexes","false"),
                Map.of("secondary_indexes",false,"export_queries",true), Map.of()))
            assertThrows(IllegalArgumentException.class, () -> Generation.secondaryIndexes(Map.of("trace",trace)));
    }
    @Test public void requestTargetsAreExplicitPositiveAndSharedAcrossProfiles() throws Exception {
        assertEquals(0,Generation.requestTarget(Map.of()));
        for(String benchmark:java.util.List.of("ycsb","sysbench","hybench")) {
            var document=profile(benchmark);
            document.put("trace",Map.of("workload_requests",17));
            assertEquals(17,Generation.requestTarget(document));
        }
        for(Object invalid:java.util.List.of(0,-1,"60000000",1.5,true))
            assertThrows(IllegalArgumentException.class,()->Generation.requestTarget(Map.of("trace",Map.of("workload_requests",invalid))));
    }

    @Test public void hybenchWorkloadConfigurationOwnsTheQueryClientCount() throws Exception {
        var nativeConfig=object(profile("hybench"),"native");
        nativeConfig.put("xp_query_clients",0);
        Generation.validateNative("hybench",nativeConfig);
        for(Object invalid:java.util.List.of(-1,257,"0",true)) {
            nativeConfig.put("xp_query_clients",invalid);
            assertThrows(IllegalArgumentException.class,()->Generation.validateNative("hybench",nativeConfig));
        }
    }
    static Map<String,Object> profile(String benchmark)throws Exception {
        var document=parse(Files.readString(Path.of("config/benchmark/"+benchmark+".yaml")));
        if (document.containsKey("trace")) { var trace=object(document,"trace"); trace.remove("workload_requests"); document.put("trace",trace); }
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
                    Map.of("build_directory","build/services/duckdb"),"duckdb"));
            for(String path:new String[]{"build/services","../outside","build/services/../../source"})
                assertThrows(java.io.IOException.class,()->NativeBuildCheck.engineDirectory(root,
                        Map.of("build_directory",path),"duckdb"));
        }finally{Files.delete(root);}
    }
    @Test public void rejectsMissingWorkloadsAndOverridesOfConnectionOrScale()throws Exception {
        var n=object(profile("ycsb"),"native");n.put("workloads",Map.of());
        assertThrows(IllegalArgumentException.class,()->Generation.validateNative("ycsb",n));
        n.put("workloads",Map.of("workloada",Map.of("operationcount",1,"properties",Map.of("sibench.socket","elsewhere"))));
        assertThrows(IllegalArgumentException.class,()->Generation.validateNative("ycsb",n));
        n.put("workloads",Map.of("workloada",Map.of("operationcount",1)));n.put("insertcount",1);
        assertThrows(IllegalArgumentException.class,()->Generation.validateNative("ycsb",n));
        // The primary key and k indexes are fixed by the Trace schema, not selectable sysbench options.
        for(var option:Map.<String,Object>of("secondary",true,"create-secondary",false,"pgsql-variant","redshift",
                "create-table-options","WITH (fillfactor=50)","binary","src/sysbench").entrySet()) {
            var sysbench=object(profile("sysbench"),"native");sysbench.put(option.getKey(),option.getValue());
            assertThrows(option.getKey(),IllegalArgumentException.class,()->Generation.validateNative("sysbench",sysbench));
        }
        // A workload is a named run of one script; each zipfian run carries its own exponent.
        var base=object(profile("sysbench"),"native");
        var workloads=object(base,"workloads");
        assertEquals(java.util.Set.of("oltp_read_write_zipf080","oltp_read_write_zipf099"),workloads.keySet());
        assertEquals(java.util.List.of("--secondary=off","--create-secondary=on"),Generation.sysbenchOptions(base,"oltp_read_write_zipf080",true).subList(0,2));
        var low=Generation.sysbenchOptions(base,"oltp_read_write_zipf080",false);
        var high=Generation.sysbenchOptions(base,"oltp_read_write_zipf099",false);
        assertTrue(low.containsAll(java.util.List.of("--rand-type=zipfian","--rand-zipfian-exp=0.8")));
        assertTrue(high.contains("--rand-zipfian-exp=0.99")&&!high.contains("--rand-zipfian-exp=0.8"));
        assertFalse("Loading draws no skewed values",Generation.sysbenchOptions(base,"oltp_read_write_zipf080",true).stream().anyMatch(o->o.startsWith("--rand-zipfian-exp")));
        assertEquals("oltp_read_write",Generation.sysbenchScript(base,"oltp_read_write_zipf099",false));
        assertEquals("oltp_read_write",Generation.sysbenchScript(base,"anything",true));
        // Upstream rejects "special" at run time, an exponent must not be silently omitted or ignored, and scripts are pinned.
        var mutations=java.util.List.<java.util.function.Consumer<Map<String,Object>>>of(
                cfg->cfg.put("rand-type","special"),
                cfg->cfg.put("rand-type","uniform"),
                cfg->cfg.put("rand-zipfian-exp",0.8),
                cfg->live(cfg,"oltp_read_write_zipf080").remove("rand-zipfian-exp"),
                cfg->live(cfg,"oltp_read_write_zipf080").put("rand-zipfian-exp",-0.1),
                cfg->live(cfg,"oltp_read_write_zipf080").put("rand-zipfian-exp","0.8"),
                cfg->live(cfg,"oltp_read_write_zipf080").put("rand-zipfian-exp",Double.NaN),
                cfg->live(cfg,"oltp_read_write_zipf080").put("script","oltp_unknown"),
                cfg->live(cfg,"oltp_read_write_zipf080").remove("script"),
                cfg->{var all=(Map<String,Object>)cfg.get("workloads");all.put("Bad-Name",all.remove("oltp_read_write_zipf080"));});
        for(int i=0;i<mutations.size();i++) {
            var invalid=object(profile("sysbench"),"native");
            mutations.get(i).accept(invalid);
            assertThrows("mutation "+i,IllegalArgumentException.class,()->Generation.validateNative("sysbench",invalid));
        }
        var uniform=object(profile("sysbench"),"native");uniform.put("rand-type","uniform");
        for(var workload:((Map<String,Map<String,Object>>)uniform.get("workloads")).values())workload.remove("rand-zipfian-exp");
        Generation.validateNative("sysbench",uniform);
        assertFalse(Generation.sysbenchOptions(uniform,"oltp_read_write_zipf080",false).stream().anyMatch(o->o.startsWith("--rand-zipfian-exp")));
    }
    @SuppressWarnings("unchecked")
    private static Map<String,Object> live(Map<String,Object> native_,String workload) {
        return (Map<String,Object>)((Map<String,Object>)native_.get("workloads")).get(workload);
    }
}
