package org.sibench.workloads;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public class ScaleInspectorTest {
    private static final Path ROOT = Path.of("").toAbsolutePath();

    private String ycsb() throws Exception {
        return Files.readString(ROOT.resolve(ScaleInspector.DEFAULT_PROFILES.get(1)));
    }

    @Test
    public void profilesPreserveLargeIntegerCounts() throws Exception {
        long[] counts = {8706000000L, 1000000000L, 1000000000L};
        for (int i = 0; i < counts.length; i++) {
            Map<String, Object> report =
                    ScaleInspector.inspect(ROOT, Path.of(ScaleInspector.DEFAULT_PROFILES.get(i)));
            assertEquals(counts[i], report.get("baseline_rows"));
            assertEquals(ScaleInspector.DEFAULT_PROFILES.get(i), report.get("profile"));
            assertFalse((Boolean) report.get("generation_verified"));
            assertTrue(report.containsKey("estimated_parquet_bytes"));
            assertNull(report.get("estimated_parquet_bytes"));
        }
    }

    @Test
    public void rejectsInvalidCountsAndScalarTypes() throws Exception {
        String text = ycsb();
        for (String invalid : new String[] {"0", "true", "1.5"}) {
            assertThrows(
                    IllegalArgumentException.class,
                    () ->
                            ScaleInspector.validate(
                                    ScaleInspector.parse(
                                            text.replace(
                                                    "total_rows: 1000000000",
                                                    "total_rows: " + invalid))));
        }
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        ScaleInspector.validate(
                                ScaleInspector.parse(
                                        text.replace(
                                                "recordcount: 1000000000", "recordcount: 1"))));
    }

    @Test
    public void rejectsPayloadOverflow() throws Exception {
        String text = ycsb().replace("1000000000", "9223372036854775807");
        assertThrows(
                ArithmeticException.class,
                () -> ScaleInspector.validate(ScaleInspector.parse(text)));
        String outOfRange =
                ycsb().replace("total_rows: 1000000000", "total_rows: 9223372036854775808");
        assertThrows(
                ArithmeticException.class,
                () -> ScaleInspector.validate(ScaleInspector.parse(outOfRange)));
    }

    @Test
    public void rejectsSysbenchSeedAndIdMismatch() throws Exception {
        String text = Files.readString(ROOT.resolve(ScaleInspector.DEFAULT_PROFILES.get(2)));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        ScaleInspector.validate(
                                ScaleInspector.parse(
                                        text.replace("rand-seed: 20261019", "rand-seed: 0"))));
        String tooLarge =
                text.replace("total_rows: 1000000000", "total_rows: 300000000000")
                        .replace("table-size: 10000000", "table-size: 3000000000");
        assertThrows(
                IllegalArgumentException.class,
                () -> ScaleInspector.validate(ScaleInspector.parse(tooLarge)));
    }

    @Test
    public void rejectsSourceFingerprintChanges() throws Exception {
        Path temporary = Files.createTempFile(ROOT.resolve("build/sibench"), "profile-", ".yaml");
        try {
            String text = ycsb();
            Map<String, Object> source =
                    ScaleInspector.object(ScaleInspector.parse(text), "source");
            Files.writeString(
                    temporary,
                    text.replace((String) source.get("definition_sha256"), "0".repeat(64)));
            assertThrows(
                    IllegalArgumentException.class, () -> ScaleInspector.inspect(ROOT, temporary));
            Files.writeString(
                    temporary, text.replace((String) source.get("commit"), "0".repeat(40)));
            assertThrows(
                    IllegalArgumentException.class, () -> ScaleInspector.inspect(ROOT, temporary));
        } finally {
            Files.deleteIfExists(temporary);
        }
    }
}
