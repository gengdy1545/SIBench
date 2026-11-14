package org.sibench.capture;

import static org.junit.Assert.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class CdcPreparationTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test public void rejectsMissingHybenchSelectionBeforeLookingForNativeBuilds() throws Exception {
        Path root = Path.of("").toRealPath(), config = temporary.newFile().toPath();
        var entry=org.sibench.config.BenchmarkFixtures.entry(Path.of("config/benchmark/ycsb.yaml"),"api","rocksdb",temporary.getRoot().toPath().resolve("trace"));
        entry.put("workload","workloada");
        Files.writeString(config, new org.yaml.snakeyaml.Yaml().dump(Map.of("schema_version", 1, "purpose", "sibench",
                "benchmarks", Map.of("ycsb-a",entry),
                "engines", Map.of("rocksdb", "/missing/engine-config.yaml"))));
        var failure = assertThrows(IllegalArgumentException.class, () -> CdcPreparation.run(root, config, Path.of("/missing/csv-input")));
        assertTrue(failure.getMessage(), failure.getMessage().contains("requires one HyBench benchmark selection"));
    }

    @Test public void protoFileLimitIsAValidatedEngineOverride() {
        assertEquals(Map.of("sink.proto.maxRecords", "2"), org.sibench.config.PixelsConfig.sinkProperties(Map.of("sink.proto.maxRecords", "2")));
        for (String value : List.of("0", "-1", "2147483648"))
            assertThrows(IllegalArgumentException.class,
                    () -> org.sibench.config.PixelsConfig.sinkProperties(Map.of("sink.proto.maxRecords", value)));
    }
}
