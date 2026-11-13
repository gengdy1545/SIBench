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

    @Test public void aFilteredQueryOnlySelectionCannotPromisePositiveExportSupply() throws Exception {
        var root=java.nio.file.Path.of("").toRealPath();
        var document=DuckDbGenerationTest.profile("sysbench");
        var nativeConfig=org.sibench.config.ConfigValues.object(document,"native");
        nativeConfig.put("workloads",Map.of("oltp_point_select",Map.of("events",10)));
        document.put("native",nativeConfig);
        document.put("trace",Map.of("secondary_indexes",false,"export_queries",false,"workload_requests",10));
        var profile=temporary.getRoot().toPath().resolve("query.yaml");
        Files.writeString(profile,new org.yaml.snakeyaml.Yaml().dump(document));
        var selection=new org.sibench.config.BenchmarkSelection(profile,"sysbench",
                org.sibench.config.BenchmarkSelection.Mode.CAPTURE,org.sibench.config.Engine.POSTGRES);
        var failure=assertThrows(IllegalArgumentException.class,()->Generation.inputs(root,Map.of(),
                java.util.List.of(selection),Map.of(),null));
        assertTrue(failure.getMessage().contains("cannot supply the exported request target"));
    }

    @Test public void effectiveConfigurationRecordsTheResolvedQuerySwitchWithoutChangingTheInput() {
        var selection=new org.sibench.config.BenchmarkSelection(java.nio.file.Path.of("profile"),"sysbench",
                org.sibench.config.BenchmarkSelection.Mode.CAPTURE,org.sibench.config.Engine.POSTGRES);
        for(var document:java.util.List.of(Map.<String,Object>of(),
                Map.<String,Object>of("trace",Map.of("secondary_indexes",false,"workload_requests",10)))) {
            var input=new Generation.Input(selection,document,null,null,null,Map.of(),Map.of(),Map.of());
            var plan=new Generation.Plan(java.nio.file.Path.of(""),Map.of(),Map.of(org.sibench.config.Engine.POSTGRES,
                    new Generation.NativeBuild(null,Map.of(),Map.of())),java.util.List.of(input));
            var snapshot=Generation.effectiveConfig(plan,input);
            var benchmark=org.sibench.config.ConfigValues.object(snapshot,"benchmark");
            assertEquals(false,org.sibench.config.ConfigValues.object(benchmark,"trace").get("export_queries"));
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
