package org.sibench.capture;

import static org.junit.Assert.*;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** The closed native database is discarded only after its Trace is published. */
public class GenerationTest {
    @Rule public TemporaryFolder temporary=new TemporaryFolder();

    @Test public void disabledGenerationLeavesExistingTraceAndNeedsNoEngineOrCredentials() throws Exception {
        var root=temporary.getRoot().toPath();
        Files.writeString(root.resolve("hybench.yaml"),"benchmark: hybench\n");
        var trace=InputStoreTest.traceFixture(root.resolve("trace"));
        byte[] before=Files.readAllBytes(trace.resolve("manifest.json"));
        var entry=org.sibench.config.BenchmarkFixtures.entry(root.resolve("hybench.yaml"),"postgres",trace,"xp");
        entry.put("trace",Map.of("generate",false,"directory",trace.toString(),"upload",Map.of("enabled",true,"path","cos://bucket/traces/hybench")));
        var config=root.resolve("runtime.yaml");
        Files.writeString(config,new org.yaml.snakeyaml.Yaml().dump(Map.of("schema_version",1,"purpose","sibench","benchmarks",Map.of("hybench",entry))));
        assertTrue(Generation.run(root,config).isEmpty());
        assertArrayEquals(before,Files.readAllBytes(trace.resolve("manifest.json")));
    }

    @Test public void publicationPreflightRejectsSymlinkOverlapBeforeCreatingOutputs() throws Exception {
        var root=temporary.getRoot().toPath();
        var actual=Files.createDirectory(root.resolve("actual"));
        Files.createSymbolicLink(root.resolve("alias"),actual);
        var plan=publicationPlan(root.resolve("alias/run/trace"),actual.resolve("run"),null,Map.of());
        assertTrue(assertThrows(IllegalArgumentException.class,()->Generation.validatePublication(plan))
                .getMessage().contains("overlap"));
        assertFalse(Files.exists(actual.resolve("run")));
    }

    @Test public void publicationPreflightRejectsDifferentFilesystemsAndMissingCredentials() throws Exception {
        var root=temporary.getRoot().toPath();
        try(var other=java.nio.file.FileSystems.newFileSystem(root.resolve("other.zip"),Map.of("create","true"))) {
            var plan=publicationPlan(other.getPath("/trace"),root.resolve("working"),null,Map.of());
            assertTrue(assertThrows(IllegalArgumentException.class,()->Generation.validatePublication(plan))
                    .getMessage().contains("same filesystem"));
            assertFalse(Files.exists(other.getPath("/trace")));
        }
        var connection=Map.of("endpoint","http://127.0.0.1:1","region","test","addressing","path",
                "access_key_env","SIBENCH_TEST_MISSING_KEY","secret_key_env","SIBENCH_TEST_MISSING_SECRET");
        var plan=publicationPlan(root.resolve("trace"),root.resolve("working"),"cos://bucket/traces/a",
                Map.of("object_storage",Map.of("cos",connection)));
        assertTrue(assertThrows(IOException.class,()->Generation.validatePublication(plan)).getMessage().contains("not set"));
        assertFalse(Files.exists(root.resolve("trace")));assertFalse(Files.exists(root.resolve("working")));
    }

    private Generation.Plan publicationPlan(java.nio.file.Path trace,java.nio.file.Path working,String upload,Map<String,Object> runtime) {
        var root=temporary.getRoot().toPath();
        var selection=new org.sibench.config.BenchmarkSelection(root.resolve("profile"),"hybench",
                org.sibench.config.BenchmarkSelection.Mode.CDC,org.sibench.config.Engine.PIXELS,"hybench","xp",trace,true,upload);
        var input=new Generation.Input(selection,Map.of(),null,null,working,Map.of(),Map.of(),Map.of());
        return new Generation.Plan(root,runtime,Map.of(),java.util.List.of(input),java.util.List.of(selection));
    }

    @Test public void completedTraceMovesToItsSelectedDirectoryWithoutOverwriting() throws Exception {
        var root=temporary.getRoot().toPath();var source=Files.createDirectory(root.resolve("working"));
        var trace=InputStoreTest.traceFixture(source.resolve("trace"));
        byte[] before=Files.readAllBytes(trace.resolve("manifest.json"));
        var destination=root.resolve("persistent/ycsb-a");
        var selection=new org.sibench.config.BenchmarkSelection(root.resolve("profile"),"ycsb",
                org.sibench.config.BenchmarkSelection.Mode.API,org.sibench.config.Engine.ROCKSDB,
                "ycsb-a","workloada",destination,true,null);
        var plan=new Generation.Plan(root,Map.of(),Map.of(),java.util.List.of(),java.util.List.of(selection));
        Generation.persist(plan,selection,source);
        assertArrayEquals(before,Files.readAllBytes(destination.resolve("manifest.json")));
        assertFalse(Files.exists(trace));
        assertTrue(Files.exists(source.resolve("trace-publication.json")));
        assertThrows(IllegalArgumentException.class,()->Generation.persist(plan,selection,source));
        assertArrayEquals(before,Files.readAllBytes(destination.resolve("manifest.json")));
    }

    @Test public void uploadSetupFailureRetainsThePersistentLocalBundleAndDiagnostic() throws Exception {
        var root=temporary.getRoot().toPath();var source=Files.createDirectory(root.resolve("working"));
        InputStoreTest.traceFixture(source.resolve("trace"));
        var destination=root.resolve("persistent");
        var selection=new org.sibench.config.BenchmarkSelection(root.resolve("profile"),"ycsb",
                org.sibench.config.BenchmarkSelection.Mode.API,org.sibench.config.Engine.ROCKSDB,
                "ycsb-a","workloada",destination,true,"cos://bucket/trace/a");
        var plan=new Generation.Plan(root,Map.of(),Map.of(),java.util.List.of(),java.util.List.of(selection));
        assertThrows(IllegalArgumentException.class,()->Generation.persist(plan,selection,source));
        org.sibench.trace.TraceManifest.read(destination);
        var receipt=org.sibench.config.ConfigValues.parse(Files.readString(source.resolve("trace-publication.json")));
        assertEquals("failed",receipt.get("status"));assertEquals(true,receipt.get("local_trace_published"));
    }

    @Test public void preparationSelectsHybenchWithoutRequiringItsNativeOrPixelsEngine()throws Exception {
        var root=temporary.getRoot().toPath();
        Files.writeString(root.resolve("hybench.yaml"),"benchmark: hybench\n");
        var entry=org.sibench.config.BenchmarkFixtures.entry(Path.of("hybench.yaml"),"postgres",root.resolve("trace"),"xp");
        var selected=Generation.preparationSelection(root,Map.of("benchmarks",Map.of("hybench",entry)),"hybench");
        assertEquals("hybench",selected.benchmark());
        assertEquals(org.sibench.config.Engine.POSTGRES,selected.engine());
        var other=org.sibench.config.BenchmarkFixtures.entry(Path.of("hybench.yaml"),"postgres",root.resolve("other-trace"),"tp");
        var ambiguous=Map.<String,Object>of("benchmarks",Map.of("first",entry,"second",other));
        assertThrows(IllegalArgumentException.class,()->Generation.preparationSelection(root,ambiguous,"hybench"));
        assertThrows(IllegalArgumentException.class,()->Generation.preparationSelection(root,ambiguous,"hybench","sysbench"));
        Files.writeString(root.resolve("sysbench.yaml"),"benchmark: sysbench\nnative:\n  workloads:\n    writes: {}\n");
        var sysbench=org.sibench.config.BenchmarkFixtures.entry(Path.of("sysbench.yaml"),"postgres",root.resolve("sysbench-trace"),"writes");
        var runtime=Map.<String,Object>of("benchmarks",Map.of("sysbench",sysbench));
        assertEquals("sysbench",Generation.preparationSelection(root,runtime,"hybench","sysbench").benchmark());
        assertThrows(IllegalArgumentException.class,()->Generation.preparationSelection(root,runtime,"hybench"));
    }

    @Test public void ycsbRunsEveryConfiguredWorkloadWithOrWithoutARequestTarget() throws Exception {
        var document=DuckDbGenerationTest.profile("ycsb");
        var expected=java.util.List.of("workloada","workloadb","workloadc","workloadf","workloadd","workloade");
        assertEquals(expected,RocksGeneration.workloads(input("ycsb","workloada",document)));
        document.remove("trace");
        assertEquals(expected,RocksGeneration.workloads(input("ycsb","workloada",document)));
    }

    @Test public void queryOnlyWorkloadsRemainEligibleForTheRequestTarget() throws Exception {
        var document=DuckDbGenerationTest.profile("sysbench");
        var nativeConfig=org.sibench.config.ConfigValues.object(document,"native");
        nativeConfig.put("rand-type","uniform");
        nativeConfig.put("workloads",Map.of("oltp_point_select",Map.of("script","oltp_point_select","events",10)));
        document.put("native",nativeConfig);
        document.put("trace",Map.of("secondary_indexes",false,"workload_requests",10));
        Generation.validateNative("sysbench",nativeConfig);
        assertEquals(10,Generation.requestTarget(document));
    }

    private Generation.Input input(String benchmark,String workload,Map<String,Object> document) {
        var selection=new org.sibench.config.BenchmarkSelection(java.nio.file.Path.of("profile"),benchmark,
                org.sibench.config.BenchmarkSelection.Mode.CAPTURE,org.sibench.config.Engine.POSTGRES,
                benchmark,workload,java.nio.file.Path.of("unused-trace"),true,null);
        return new Generation.Input(selection,document,null,null,null,Map.of(),Map.of(),Map.of());
    }

    @Test public void effectiveConfigurationRecordsIndexScopeWithoutChangingTheInput() {
        var selection=new org.sibench.config.BenchmarkSelection(java.nio.file.Path.of("profile"),"sysbench",
                org.sibench.config.BenchmarkSelection.Mode.CAPTURE,org.sibench.config.Engine.POSTGRES,"sysbench","",java.nio.file.Path.of("unused-trace"),false,null);
        for(var document:java.util.List.of(Map.<String,Object>of(),
                Map.<String,Object>of("trace",Map.of("secondary_indexes",false,"workload_requests",10)))) {
            var input=new Generation.Input(selection,document,null,null,null,Map.of(),Map.of(),Map.of());
            var plan=new Generation.Plan(java.nio.file.Path.of(""),Map.of(),Map.of(org.sibench.config.Engine.POSTGRES,
                    new Generation.NativeBuild(null,Map.of(),Map.of())),java.util.List.of(input),java.util.List.of(selection));
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
