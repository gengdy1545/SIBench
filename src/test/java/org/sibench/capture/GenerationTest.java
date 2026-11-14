package org.sibench.capture;

import static org.junit.Assert.*;
import java.io.IOException;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** The closed native database is discarded only after its Trace is published. */
public class GenerationTest {
    @Rule public TemporaryFolder temporary=new TemporaryFolder();

    @Test public void preparationSelectsHybenchWithoutRequiringItsNativeOrPixelsEngine()throws Exception {
        var root=temporary.getRoot().toPath();
        Files.writeString(root.resolve("hybench.yaml"),"benchmark: hybench\n");
        var selected=Generation.hybenchSelection(root,Map.of("benchmarks",java.util.List.of(
                Map.of("config","hybench.yaml","mode","capture","engine","postgres"))));
        assertEquals("hybench",selected.benchmark());
        assertEquals(org.sibench.config.Engine.POSTGRES,selected.engine());
    }

    @Test public void configuredQueryAndMixedStagesAreAlwaysRetained() throws Exception {
        for(String benchmark:java.util.List.of("hybench","sysbench","ycsb")) {
            var document=DuckDbGenerationTest.profile(benchmark);
            var expected=switch(benchmark) {
                case "hybench"->java.util.List.of("xp");
                case "sysbench"->java.util.List.of("oltp_read_write");
                default->java.util.List.of("workloada","workloadb","workloadc","workloadf","workloadd","workloade");
            };
            assertEquals(expected,Generation.workloads(document));
            document.remove("trace");
            assertEquals(expected,Generation.workloads(document));
        }
    }

    @Test public void queryOnlyWorkloadsRemainEligibleForTheRequestTarget() throws Exception {
        var document=DuckDbGenerationTest.profile("sysbench");
        var nativeConfig=org.sibench.config.ConfigValues.object(document,"native");
        nativeConfig.put("workloads",Map.of("oltp_point_select",Map.of("events",10)));
        document.put("native",nativeConfig);
        document.put("trace",Map.of("secondary_indexes",false,"workload_requests",10));
        assertEquals(java.util.List.of("oltp_point_select"),Generation.workloads(document));
        assertEquals(10,Generation.requestTarget(document));
    }

    @Test public void effectiveConfigurationRecordsIndexScopeWithoutChangingTheInput() {
        var selection=new org.sibench.config.BenchmarkSelection(java.nio.file.Path.of("profile"),"sysbench",
                org.sibench.config.BenchmarkSelection.Mode.CAPTURE,org.sibench.config.Engine.POSTGRES);
        for(var document:java.util.List.of(Map.<String,Object>of(),
                Map.<String,Object>of("trace",Map.of("secondary_indexes",false,"workload_requests",10)))) {
            var input=new Generation.Input(selection,document,null,null,null,Map.of(),Map.of(),Map.of());
            var plan=new Generation.Plan(java.nio.file.Path.of(""),Map.of(),Map.of(org.sibench.config.Engine.POSTGRES,
                    new Generation.NativeBuild(null,Map.of(),Map.of())),java.util.List.of(input));
            var snapshot=Generation.effectiveConfig(plan,input);
            var benchmark=org.sibench.config.ConfigValues.object(snapshot,"benchmark");
            assertFalse(org.sibench.config.ConfigValues.object(benchmark,"trace").containsKey("export_queries"));
            assertFalse(Generation.secondaryIndexes(benchmark));
            assertTrue(!document.containsKey("trace")||!org.sibench.config.ConfigValues.object(document,"trace").containsKey("export_queries"));
        }
    }

    @Test public void failedPublicationRetainsTheClosedDatabase()throws Exception {
        var database=Files.createDirectories(temporary.getRoot().toPath().resolve("database/data"));
        Files.writeString(database.resolve("page"),"state");
        var timings=new LinkedHashMap<String,Double>();
        var failure=assertThrows(IOException.class,()->Generation.publishThenDiscard(()->{throw new IOException("manifest");},
                database.getParent(),timings));
        assertEquals("manifest",failure.getMessage());
        assertTrue(Files.isRegularFile(database.resolve("page")));
        assertTrue(timings.isEmpty());
    }

    @Test public void publishedTraceDiscardsTheDatabase()throws Exception {
        var database=Files.createDirectories(temporary.getRoot().toPath().resolve("database/data"));
        Files.writeString(database.resolve("page"),"state");
        var timings=new LinkedHashMap<String,Double>();
        boolean[] published={false};
        assertEquals(Map.of("status","published"),Generation.publishThenDiscard(()->{
            assertTrue("publication precedes deletion",Files.exists(database));published[0]=true;
            return Map.of("status","published");
        },database.getParent(),timings));
        assertTrue(published[0]);
        assertFalse(Files.exists(database.getParent()));
        assertEquals(java.util.List.of("publication_seconds","database_cleanup_seconds"),java.util.List.copyOf(timings.keySet()));
    }

    @Test public void cleanupFailureReportsTheRemainingDatabaseAfterPublication()throws Exception {
        var missing=temporary.getRoot().toPath().resolve("absent");
        var failure=assertThrows(IOException.class,()->Generation.publishThenDiscard(()->"published",missing,new LinkedHashMap<>()));
        assertTrue(failure.getMessage().startsWith("Artifact published; closed database remains at "+missing));
    }
}
