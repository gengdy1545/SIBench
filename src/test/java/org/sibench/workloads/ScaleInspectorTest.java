package org.sibench.workloads;

import static org.junit.Assert.*;
import static org.sibench.config.ConfigValues.*;

import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;

public class ScaleInspectorTest {
    private static final Path ROOT = Path.of("").toAbsolutePath();

    private String ycsb() throws Exception {
        return Files.readString(ROOT.resolve("src/test/resources/config/ycsb_1k.yaml"));
    }

    private java.util.Map<String, Object> inspect(String text) throws Exception {
        Path profile = Files.createTempFile(ROOT.resolve("build/sibench"), "native-profile-", ".yaml");
        try {
            Files.writeString(profile, text);
            return ScaleInspector.inspect(ROOT, profile);
        } finally {
            Files.delete(profile);
        }
    }

    @Test
    public void nativeDefaultsHaveTheSameHandCheckedRowCount() throws Exception {
        for (String profile : ScaleInspector.DEFAULT_PROFILES) {
            var report = ScaleInspector.inspect(ROOT, Path.of(profile));
            assertEquals(87_060_000L, report.get("baseline_rows"));
            assertEquals("native", report.get("generation_mode"));
            assertFalse((Boolean) report.get("generation_verified"));
        }
    }

    @Test
    public void rejectsUnsupportedPurposeAndVersion() throws Exception {
        String source = ycsb();
        for (String invalid : new String[] {
                source.replace("schema_version: 1", "schema_version: 2"),
                source.replace("purpose: native_capture", "purpose: baseline_generation")})
            assertThrows(IllegalArgumentException.class, () -> inspect(invalid));
    }

    @Test
    public void preservesNativeInt64CountsAndRejectsOverflow() throws Exception {
        assertEquals(3_000_000_000L, inspect(ycsb().replace("recordcount: 1000", "recordcount: 3000000000"))
                .get("baseline_rows"));
        assertThrows(ArithmeticException.class, () -> inspect(ycsb()
                .replace("recordcount: 1000", "recordcount: 9223372036854775808")));
        String sysbench = Files.readString(ROOT.resolve("src/test/resources/config/sysbench_2k.yaml"));
        assertThrows(ArithmeticException.class, () -> inspect(sysbench
                .replace("table-size: 1000", "table-size: 9223372036854775807")));
    }

    @Test
    public void rejectsInvalidCountsAndScalarTypes() throws Exception {
        String source = ycsb();
        for (String invalid : new String[] {"0", "true", "1.5"})
            assertThrows(IllegalArgumentException.class, () -> inspect(source
                    .replace("recordcount: 1000", "recordcount: " + invalid)));
    }

    @Test
    public void rejectsSourcePinChanges() throws Exception {
        String source = ycsb();
        String commit = string(object(parse(source), "source"), "commit");
        assertThrows(IllegalArgumentException.class, () -> inspect(source.replace(commit, "0".repeat(40))));
    }
}
