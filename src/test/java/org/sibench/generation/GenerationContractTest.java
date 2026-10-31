package org.sibench.generation;

import static org.junit.Assert.*;
import static org.sibench.config.ConfigValues.*;

import com.google.gson.JsonParser;

import org.junit.Test;
import org.sibench.cli.Sibench;
import org.sibench.workloads.ScaleInspector;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/** No test invokes a row producer, Parquet writer or generation command. */
public class GenerationContractTest {
    private static final Path ROOT = Path.of("").toAbsolutePath();
    private static final java.util.List<String> DERIVED_PROFILES = java.util.List.of(
            "src/test/resources/config/hybench_sf1000.yaml",
            "src/test/resources/config/ycsb_1b.yaml",
            "src/test/resources/config/sysbench_1b.yaml");

    @Test
    public void dryRunResolvesAllSchemasWithoutCreatingOutput() throws Exception {
        int[] counts = {8, 1, 100};
        for (int i = 0; i < counts.length; i++) {
            var prepared =
                    BaselineGenerator.prepare(
                            ROOT, Path.of(DERIVED_PROFILES.get(i)));
            boolean before = Files.exists(prepared.config().output());
            assertEquals(counts[i], prepared.tables().size());
            assertEquals(
                    positive(object(prepared.config().document(), "scale"), "total_rows"),
                    TableData.totalRows(prepared.tables()));
            long next = 1;
            for (var table : prepared.tables()) {
                assertEquals(next, table.rowIdStart());
                next = Math.addExact(next, table.rows());
                assertTrue(table.schema().containsField(table.primaryKey()));
                assertTrue(table.schema().containsField("_sib_row_id"));
                assertTrue(table.schema().containsField("_sib_timestamp"));
            }
            assertEquals(
                    "configuration_valid_not_generated",
                    BaselineGenerator.describe(prepared).get("status"));
            assertEquals(before, Files.exists(prepared.config().output()));
        }
    }

    @Test
    public void configurationRejectsUnsupportedAndEscapingOptions() throws Exception {
        String text = Files.readString(ROOT.resolve(DERIVED_PROFILES.get(1)));
        for (String invalid :
                new String[] {
                    text.replace("row_group_max_rows: 1048576", "row_group_max_rows: 0"),
                    text.replace("file_size_max_overshoot_percent: 5", "file_size_max_overshoot_percent: 0"),
                    text.replace("file_size_max_overshoot_percent: 5", "file_size_max_overshoot_percent: 101"),
                    text.replace("file_size_max_overshoot_percent: 5", "file_size_max_overshoot_percent: 1.5"),
                    text.replace("target_file_size_bytes: 536870912", "target_file_size_bytes: 9223372036854775807"),
                    text.replace("compression: zstd", "compression: gzip"),
                    text.replace("compression_level: 3", "compression_level: 30"),
                    text.replace("partitioning: none", "partitioning: date"),
                    text.replace("bucket_function: ordinal_mod", "bucket_function: random"),
                    text.replace("results/datasets/tests/ycsb_1b", "../outside"),
                    text.replace("mode: deterministic_derived", "mode: native")
                }) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> GenerationConfig.from(ROOT, parse(invalid)));
        }
    }

    @Test
    public void groupBoundariesRespectEitherLimit() {
        assertFalse(ParquetDataFile.shouldFlush(9, 99, 10, 100));
        assertTrue(ParquetDataFile.shouldFlush(10, 99, 10, 100));
        assertTrue(ParquetDataFile.shouldFlush(1, 100, 10, 100));
        assertTrue(ParquetDataFile.shouldFlush(11, 101, 10, 100));
    }

    @Test
    public void rngIsAddressedByRowAndDomain() {
        var a = new DeterministicRandom(7, "table", 8);
        var b = new DeterministicRandom(7, "table", 8);
        for (int i = 0; i < 100; i++) assertEquals(a.next(), b.next());
        assertNotEquals(
                new DeterministicRandom(7, "table", 8).next(),
                new DeterministicRandom(7, "table", 9).next());
        assertNotEquals(
                new DeterministicRandom(7, "table", 8).next(),
                new DeterministicRandom(7, "other", 8).next());
        assertThrows(IllegalArgumentException.class, () -> a.between(2, 2));
    }

    @Test
    public void cliDryRunIsReadOnly() throws Exception {
        var output = new ByteArrayOutputStream();
        var errors = new ByteArrayOutputStream();
        try (var out = new PrintStream(output, true, StandardCharsets.UTF_8);
                var err = new PrintStream(errors, true, StandardCharsets.UTF_8)) {
            assertEquals(
                    0,
                    Sibench.run(
                            new String[] {
                                "prepare",
                                "--dry-run",
                                "--root",
                                ROOT.toString(),
                                DERIVED_PROFILES.get(1)
                            },
                            out,
                            err));
            assertEquals(
                    "configuration_valid_not_generated",
                    JsonParser.parseString(output.toString(StandardCharsets.UTF_8))
                            .getAsJsonObject()
                            .get("status")
                            .getAsString());
            assertEquals(1, Sibench.run(new String[] {"prepare", "--dry-run"}, out, err));
        }
    }
}
