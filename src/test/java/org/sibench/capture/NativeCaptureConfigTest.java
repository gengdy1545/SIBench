package org.sibench.capture;

import static org.junit.Assert.*;

import static org.sibench.config.ConfigValues.*;

import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

import java.nio.file.*;
import java.util.Map;

public class NativeCaptureConfigTest {
    /** Representative SF1 row count used to exercise configurable source scales. */
    private static final long SF1_ROWS = 8_706_000;

    private final Path root = Path.of("").toAbsolutePath();

    @Test
    public void captureBaselinesMatchTheHybenchRowCount() throws Exception {
        var ycsbDocument = parse(Files.readString(root.resolve("src/test/resources/config/ycsb_1k.yaml")));
        var ycsbParameters = object(ycsbDocument, "native");
        assertEquals(1_000, positive(ycsbParameters, "recordcount"));
        assertEquals(1_000, positive(ycsbParameters, "insertcount"));
        ycsbParameters.put("recordcount", SF1_ROWS);
        ycsbParameters.put("insertcount", SF1_ROWS);
        ycsbParameters.put("load_threads", 16);
        ycsbParameters.put("timeout_minutes", 360);
        ycsbDocument.put("native", ycsbParameters);
        var ycsb = readProfile(ycsbDocument);
        assertEquals(SF1_ROWS, positive(ycsb.nativeConfig(), "recordcount"));
        assertEquals(SF1_ROWS, positive(ycsb.nativeConfig(), "insertcount"));
        assertEquals(16, ycsb.loadThreads());
        assertEquals(java.time.Duration.ofMinutes(360), ycsb.timeout());
        var sysbenchDocument = parse(Files.readString(root.resolve("src/test/resources/config/sysbench_2k.yaml")));
        var sysbenchParameters = object(sysbenchDocument, "native");
        assertEquals(2_000, Math.multiplyExact(positive(sysbenchParameters, "tables"),
                positive(sysbenchParameters, "table-size")));
        sysbenchParameters.put("tables", 8);
        sysbenchParameters.put("table-size", 1_088_250);
        sysbenchDocument.put("native", sysbenchParameters);
        var sysbench = readProfile(sysbenchDocument);
        assertEquals(
                SF1_ROWS,
                positive(sysbench.nativeConfig(), "tables")
                        * positive(sysbench.nativeConfig(), "table-size"));
    }

    @Test
    public void rejectsOutOfRangeClientCountsAndTimeouts() throws Exception {
        String template = Files.readString(root.resolve("src/test/resources/config/ycsb_1k.yaml"));
        for (var limit : Map.of("threads", 257, "load_threads", 65, "timeout_minutes", 1441).entrySet()) {
            var document = parse(template);
            var parameters = object(document, "native");
            parameters.put(limit.getKey(), limit.getValue());
            document.put("native", parameters);
            assertThrows(IllegalArgumentException.class, () -> readProfile(document));
        }
    }

    @Test
    public void rejectsAPartialInitialPopulation() throws Exception {
        var document = parse(Files.readString(root.resolve("src/test/resources/config/ycsb_1k.yaml")));
        var parameters = object(document, "native");
        parameters.put("insertcount", 500);
        document.put("native", parameters);
        assertTrue(assertThrows(IllegalArgumentException.class, () -> readProfile(document))
                .getMessage().contains("insertcount to equal recordcount"));
    }

    private NativeCaptureConfig readProfile(Map<String, Object> document) throws Exception {
        Path profile = Files.createTempFile(root.resolve("build/sibench"), "native-", ".yaml");
        try {
            Files.writeString(profile, new Yaml().dump(document));
            return NativeCaptureConfig.read(root, profile);
        } finally {
            Files.delete(profile);
        }
    }
}
