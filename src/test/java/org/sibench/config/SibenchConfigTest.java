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
                BenchmarkSelection.Mode.CDC), selected.stream().map(BenchmarkSelection::mode).toList());
        assertEquals(java.util.List.of(Engine.ROCKSDB, Engine.POSTGRES, Engine.PIXELS),
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

    @Test
    public void rejectsOldPurposesAndUnknownTopLevelOptions() {
        for (String purpose : new String[] {"index_replay", "update_locality"})
            assertThrows(IllegalArgumentException.class, () -> SibenchConfig.validate(
                    Map.of("schema_version", 1, "purpose", purpose)));
        assertThrows(IllegalArgumentException.class, () -> SibenchConfig.validate(
                Map.of("schema_version", 1, "purpose", "sibench", "replaay", Map.of())));
    }

    @Test
    public void rejectsRetiredPgGenerationSettings() {
        assertThrows(IllegalArgumentException.class, () -> SibenchConfig.validate(Map.of(
                "schema_version", 1, "purpose", "sibench", "generation", Map.of(
                        "pending_transaction_bytes", 1000, "pending_transactions", 10,
                        "operation_target", 100, "output_byte_target", 1000))));
    }
}
