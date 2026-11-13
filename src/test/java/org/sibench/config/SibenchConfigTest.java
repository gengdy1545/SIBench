package org.sibench.config;

import static org.junit.Assert.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.adapters.ServiceConfig;
import org.sibench.analysis.UpdateLocality;
import org.sibench.replay.ReplayConfig;
import org.yaml.snakeyaml.Yaml;

public class SibenchConfigTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test public void remoteHostPortMustFitTheNetworkPortRange() throws Exception {
        var root = Path.of("").toAbsolutePath();
        var settings = new java.util.LinkedHashMap<>(ConfigValues.parse(Files.readString(root.resolve("config/engine/pixels/pixels.yaml"))));
        settings.remove("schema_version"); settings.remove("purpose"); settings.remove("engine");
        settings.put("pixels_host", "host:65536");
        var failure = assertThrows(IllegalArgumentException.class, () -> PixelsConfig.read(root.resolve("config/engine/pixels"), settings));
        assertEquals("Invalid Pixels host port", failure.getMessage());
    }

    @Test public void pixelsStorageProfilesResolveWithoutReadingOrEmbeddingCredentials() throws Exception {
        Path root = Path.of("").toAbsolutePath();
        var settings = new java.util.LinkedHashMap<>(ConfigValues.parse(Files.readString(root.resolve("config/engine/pixels/pixels.yaml"))));
        settings.keySet().removeAll(java.util.Set.of("schema_version", "purpose", "engine"));
        settings.put("data_storage", Map.of("config", "../../index/storage.yaml", "profile", "cos", "prefix", "sibench/pixels"));
        var resolved = PixelsConfig.read(root.resolve("config/engine/pixels"), settings);
        var profile = ConfigValues.object(ConfigValues.object(resolved, "data_storage"), "profile");
        assertEquals("COS_SECRET_KEY", profile.get("secret_key_env"));
        settings.put("data_storage", Map.of("config", "../../index/storage.yaml", "profile", "cos", "prefix", "sibench/../outside"));
        assertThrows(IllegalArgumentException.class, () -> PixelsConfig.read(root.resolve("config/engine/pixels"), settings));
        assertThrows(IllegalArgumentException.class, () -> PixelsConfig.pixelsProperties(Map.of("cos.secret.key", "not-allowed")));
    }

    @Test public void objectPathsStayBelowTheProfilePrefixAtAPathBoundary() {
        assertTrue(ObjectStore.below("sibench/pixels", "sibench/"));
        assertTrue(ObjectStore.below("sibench/pixels", "sibench"));
        assertFalse(ObjectStore.below("sibenchX/pixels", "sibench"));
        assertFalse(ObjectStore.below("sibench/", "sibench/"));
        assertFalse(ObjectStore.below("sibench", "sibench"));
        assertTrue(ObjectStore.below("any/key", ""));
    }

    @Test public void pixelsServicePortsCanBeFixedWithoutExposingOtherOwnedProperties() {
        assertEquals(Map.of("metadata.server.port", "18801", "retina.server.port", "18802"),
                PixelsConfig.pixelsProperties(Map.of("metadata.server.port", "18801", "retina.server.port", "18802")));
        for (String invalid : java.util.List.of("0", "-1", "65536", "18801\nother=1", "abc"))
            assertThrows(IllegalArgumentException.class, () -> PixelsConfig.pixelsProperties(Map.of("metadata.server.port", invalid)));
        assertThrows(IllegalArgumentException.class, () -> PixelsConfig.pixelsProperties(
                Map.of("metadata.server.port", "18801", "retina.server.port", "18801")));
        for (String owned : java.util.List.of("metadata.server.host", "unknown.server.port", "etcd.hosts", "metadata.db.url", "index.rocksdb.data.path"))
            assertThrows(IllegalArgumentException.class, () -> PixelsConfig.pixelsProperties(Map.of(owned, "elsewhere")));
    }

    @Test
    public void replayServiceAndAnalysisUseTheSameConfiguration() throws Exception {
        Path root = Path.of("").toAbsolutePath();
        Path config = root.resolve("config/sibench.yaml");
        var document = SibenchConfig.read(config);
        assertEquals(64, ReplayConfig.read(root, config).clientCount());
        assertEquals("127.0.0.1:50551", ServiceConfig.ofReplay(root, document).endpoint());
        var analysis = UpdateLocality.Config.read(config);
        assertEquals(256, analysis.entriesPerBlock());
        assertEquals(4096, analysis.blocksPerFile());
        assertEquals(10000, analysis.businessChangesPerWindow());
    }

    @Test
    public void publicGenerationSelectsThreeNativeModesAndOwnsEngineBuildLocations() throws Exception {
        Path root = Path.of("").toAbsolutePath();
        var document = SibenchConfig.read(root.resolve("config/sibench.yaml"));
        var selected = BenchmarkSelection.read(root, document);
        assertEquals(java.util.List.of("ycsb", "sysbench", "hybench"),
                selected.stream().map(BenchmarkSelection::benchmark).toList());
        assertEquals(java.util.List.of(BenchmarkSelection.Mode.API, BenchmarkSelection.Mode.CAPTURE,
                BenchmarkSelection.Mode.CAPTURE), selected.stream().map(BenchmarkSelection::mode).toList());
        assertEquals(java.util.List.of(Engine.ROCKSDB, Engine.POSTGRES, Engine.POSTGRES),
                selected.stream().map(BenchmarkSelection::engine).toList());
        var postgres = SibenchConfig.engine(root, document, Engine.POSTGRES);
        assertEquals("build/services/postgres", ConfigValues.string(postgres, "build_directory"));
        assertEquals("off", SibenchConfig.postgresSettings(ConfigValues.object(postgres, "settings")).get("fsync"));
        assertEquals("build/services/rocksdb", ConfigValues.string(SibenchConfig.engine(root, document, Engine.ROCKSDB), "build_directory"));
        assertEquals("16GiB", ConfigValues.string(SibenchConfig.engine(root, document, Engine.DUCKDB), "memory_limit"));
        assertFalse(document.containsKey("generation"));
        // Engine settings live in their own files; the runtime only selects them.
        assertThrows(IllegalArgumentException.class, () -> SibenchConfig.validate(
                Map.of("schema_version", 1, "purpose", "sibench", "postgres", Map.of("build_directory", "build/services/postgres"))));
        assertThrows(IllegalArgumentException.class, () -> SibenchConfig.validate(
                Map.of("schema_version", 1, "purpose", "sibench", "engines", Map.of("mysql", "config/engine/mysql.yaml"))));
    }

    @Test
    public void engineConfigsRejectUnknownOptionsAndGeneratorOwnedPostgresSettings() throws Exception {
        Path root = temporary.getRoot().toPath();
        Files.writeString(root.resolve("postgres.yaml"), "schema_version: 1\npurpose: engine\nengine: postgres\n"
                + "build_directory: build/services/postgres\nsettings:\n  port: 5432\n");
        var runtime = Map.<String, Object>of("engines", Map.of("postgres", "postgres.yaml", "duckdb", "postgres.yaml"));
        assertThrows(IllegalArgumentException.class, () -> SibenchConfig.engine(root, runtime, Engine.POSTGRES));
        assertThrows(IllegalArgumentException.class, () -> SibenchConfig.engine(root, runtime, Engine.DUCKDB));
        assertThrows(IllegalArgumentException.class, () -> SibenchConfig.engine(root, runtime, Engine.ROCKSDB));
        Files.writeString(root.resolve("postgres.yaml"), "schema_version: 1\npurpose: engine\nengine: postgres\n"
                + "build_directory: build/services/postgres\nsettings: {}\ndata_directory: data\n");
        assertThrows(IllegalArgumentException.class, () -> SibenchConfig.engine(root, runtime, Engine.POSTGRES));
    }

    @Test public void postgresSettingsCannotRedirectWorkingStateOrDisableSourceTriggers() {
        var accepted=new java.util.ArrayList<String>();
        for (String key:java.util.List.of("data_directory","hba_file","ident_file","external_pid_file","session_replication_role")) {
            try { SibenchConfig.postgresSettings(Map.of(key,"external")); accepted.add(key); }
            catch (IllegalArgumentException expected) { }
        }
        assertEquals(java.util.List.of(),accepted);
    }

    @Test public void nativeSelectionCannotStartPixels() throws Exception {
        var root=temporary.getRoot().toPath();
        Files.writeString(root.resolve("hybench.yaml"),"benchmark: hybench\n");
        assertThrows(IllegalArgumentException.class,()->BenchmarkSelection.read(root,Map.of("benchmarks",java.util.List.of(
                Map.of("config","hybench.yaml","mode","cdc","engine","pixels")))));
    }

    @Test public void nativeConfigurationDoesNotValidateUnusedPixelsExperimentSettings() {
        var runtime=Map.<String,Object>of("schema_version",1,"purpose","sibench",
                "pixels_experiment",Map.of("timeout_seconds",-1));
        SibenchConfig.validate(runtime);
        assertThrows(IllegalArgumentException.class,()->PixelsExperimentConfig.settings(runtime));
    }

    @Test public void cdcSourceDoesNotReadTargetStorageOrPixelsProperties() throws Exception {
        Path root=temporary.getRoot().toPath();
        Files.writeString(root.resolve("sink.properties"),"sink.proto.maxRecords=2\n");
        var settings=new java.util.LinkedHashMap<String,Object>(Map.of("schema_version",1,"purpose","engine","engine","pixels",
                "build_directory","source-build","source",Map.of("settings",Map.of("shared_buffers","64MB")),
                "memory",Map.of("sink_heap","1g"),"sink_properties","sink.properties",
                "properties","missing.properties","data_storage",Map.of("config","missing-storage.yaml")));
        Files.writeString(root.resolve("pixels.yaml"),new Yaml().dump(settings));
        var runtime=Map.<String,Object>of("engines",Map.of("pixels","pixels.yaml"));
        var source=SibenchConfig.cdcSource(root,runtime);
        assertEquals("2",ConfigValues.object(source,"sink_properties").get("sink.proto.maxRecords"));
        assertFalse(source.containsKey("data_storage"));
        settings.put("source",Map.of("settings",Map.of("wal_level","minimal")));
        Files.writeString(root.resolve("pixels.yaml"),new Yaml().dump(settings));
        assertThrows(IllegalArgumentException.class,()->SibenchConfig.cdcSource(root,runtime));
    }

    @Test
    public void rejectsOtherPurposesAndUnknownTopLevelOptions() {
        for (String purpose : new String[] {"index_service"})
            assertThrows(IllegalArgumentException.class, () -> SibenchConfig.validate(
                    Map.of("schema_version", 1, "purpose", purpose)));
        assertThrows(IllegalArgumentException.class, () -> SibenchConfig.validate(
                Map.of("schema_version", 1, "purpose", "sibench", "replaay", Map.of())));
    }
}
