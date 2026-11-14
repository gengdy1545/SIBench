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
        var failure = assertThrows(IllegalArgumentException.class, () -> PixelsConfig.read(root.resolve("config/engine/pixels"), settings, SibenchConfig.read(root.resolve("config/sibench.yaml"))));
        assertEquals("Invalid Pixels host port", failure.getMessage());
    }

    @Test public void pixelsStorageProfilesResolveWithoutReadingOrEmbeddingCredentials() throws Exception {
        Path root = Path.of("").toAbsolutePath();
        var settings = new java.util.LinkedHashMap<>(ConfigValues.parse(Files.readString(root.resolve("config/engine/pixels/pixels.yaml"))));
        settings.keySet().removeAll(java.util.Set.of("schema_version", "purpose", "engine"));
        settings.put("data_storage", "cos://bucket/sibench/pixels");
        var resolved = PixelsConfig.read(root.resolve("config/engine/pixels"), settings, SibenchConfig.read(root.resolve("config/sibench.yaml")));
        var profile = ConfigValues.object(ConfigValues.object(resolved, "data_storage"), "profile");
        assertEquals("COS_SECRET_KEY", profile.get("secret_key_env"));
        settings.put("data_storage", "cos://bucket/sibench/../outside");
        assertThrows(IllegalArgumentException.class, () -> PixelsConfig.read(root.resolve("config/engine/pixels"), settings, SibenchConfig.read(root.resolve("config/sibench.yaml"))));
        settings.put("data_storage", "s3://bucket/sibench/pixels");
        assertTrue(assertThrows(IllegalArgumentException.class, () -> PixelsConfig.read(root.resolve("config/engine/pixels"), settings,
                SibenchConfig.read(root.resolve("config/sibench.yaml")))).getMessage().contains("AWS S3 is not integrated"));
        assertThrows(IllegalArgumentException.class, () -> PixelsConfig.pixelsProperties(Map.of("cos.secret.key", "not-allowed")));
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
        ReplayConfig.read(root, config);
        ServiceConfig.ofReplay(root, document);
        UpdateLocality.Config.read(config);
        document.put("replay", Map.of("dispatch_mode", "operation", "client_count", 3,
                "max_in_flight", 16, "baseline_batch_operations", 4, "request_timeout_seconds", 5));
        document.put("target", Map.of("secondary_indexes", Map.of("enabled", false),
                "service", Map.of("adapter", "grpc", "endpoint", "test-host:50599", "value", "row_id")));
        document.put("analysis", Map.of("layout", Map.of("entries_per_block", 4, "blocks_per_file", 8, "sort_memory_bytes", 1024),
                "window", Map.of("business_changes", 3, "heatmap_bins", 16, "l0_index_modifications", 5)));
        config = temporary.newFile("runtime.yaml").toPath();
        Files.writeString(config, new Yaml().dump(document));
        assertEquals(3, ReplayConfig.read(root, config).clientCount());
        assertEquals("test-host:50599", ServiceConfig.ofReplay(root, SibenchConfig.read(config)).endpoint());
        var analysis = UpdateLocality.Config.read(config);
        assertEquals(4, analysis.entriesPerBlock());
        assertEquals(8, analysis.blocksPerFile());
        assertEquals(3, analysis.businessChangesPerWindow());
    }

    @Test
    public void publicGenerationSelectsThreeNativeModesAndOwnsEngineBuildLocations() throws Exception {
        Path root = Path.of("").toAbsolutePath();
        var document = SibenchConfig.read(root.resolve("config/sibench.yaml"));
        var selected = BenchmarkSelection.read(root, document);
        assertEquals(java.util.List.of("hybench", "sysbench", "ycsb-a", "ycsb-b", "ycsb-c", "ycsb-d", "ycsb-e", "ycsb-f"),
                selected.stream().map(BenchmarkSelection::name).toList());
        assertEquals(6, selected.stream().filter(s -> s.engine() == Engine.ROCKSDB).count());
        assertEquals(2, selected.stream().filter(s -> s.engine() == Engine.POSTGRES).count());
        var postgres = SibenchConfig.engine(root, document, Engine.POSTGRES);
        assertEquals("build/services/postgres", ConfigValues.string(postgres, "build_directory"));
        SibenchConfig.postgresSettings(ConfigValues.object(postgres, "settings"));
        assertEquals("build/services/rocksdb", ConfigValues.string(SibenchConfig.engine(root, document, Engine.ROCKSDB), "build_directory"));
        SibenchConfig.engine(root, document, Engine.DUCKDB);
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
