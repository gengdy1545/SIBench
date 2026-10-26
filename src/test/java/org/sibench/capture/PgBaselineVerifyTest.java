package org.sibench.capture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.config.ConfigValues;
import org.sibench.schema.SourceSchema;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The baseline is verified row group by row group, each against the PG rows of its own key range or
 * of its stretch of one key-ordered stream (tables whose heap does not follow the key); either way
 * no PG row escapes, whatever the thread count.
 */
public class PgBaselineVerifyTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    /** One integer-keyed (sysbench) and one text-keyed (YCSB) schema. */
    private static SourceSchema schema(String benchmark) throws Exception {
        var document = ConfigValues.parse(Files.readString(Path.of("config/" + benchmark + "_large.yaml")));
        var nativeConfig = ConfigValues.object(document, "native");
        if (benchmark.equals("ycsb")) nativeConfig.put("fieldcount", 2);
        if (benchmark.equals("sysbench")) nativeConfig.put("tables", 2);
        document.put("native", nativeConfig);
        return SourceSchema.read(Path.of(""), document);
    }

    private static JsonObject row(SourceSchema.Table table, long key) {
        var row = new JsonObject();
        var primary = SourceRow.primary(table);
        for (var column : table.columns()) {
            if (column == primary) continue;
            if (column.type().equals("integer")) row.addProperty(column.name(), key % 7);
            else row.addProperty(column.name(), "v" + key);
        }
        // Text keys sort in C order, so zero padding keeps them in numeric order.
        if (primary.type().equals("integer")) row.addProperty(primary.name(), key);
        else row.addProperty(primary.name(), String.format("user%04d", key));
        return row;
    }

    /** Keys 10, 20, ..., 10 * count in every table: gaps on both sides of every row. */
    private static Map<String, List<JsonObject>> rows(SourceSchema schema, int count) {
        var result = new LinkedHashMap<String, List<JsonObject>>();
        for (var table : schema.tables()) {
            var rows = new ArrayList<JsonObject>();
            for (long key = 1; key <= count; key++) rows.add(row(table, 10 * key));
            result.put(table.name(), rows);
        }
        return result;
    }

    /** Correlations that read every table by key range, and as one stream. */
    private static final double[] READINGS = {1.0, 0.0};

    private Path verified(SourceSchema schema, Map<String, List<JsonObject>> baseline,
            Map<String, List<JsonObject>> pg, int threads, double correlation) throws Exception {
        Path fixture = temporary.newFolder().toPath();
        Path directory = RowAuditTestSupport.baseline(fixture, schema, baseline, 3);
        // Three rows per group: ten rows are four groups, so ranges meet inside every file.
        for (var table : schema.tables())
            try (var reader = org.apache.parquet.hadoop.ParquetFileReader.open(
                    org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(
                            new org.apache.hadoop.fs.Path(directory.resolve(table.name() + ".parquet").toUri()),
                            new org.apache.hadoop.conf.Configuration(false)))) {
                assertEquals(4, reader.getFooter().getBlocks().size());
            }
        String previous = System.getProperty("sibench.baseline.threads");
        System.setProperty("sibench.baseline.threads", Integer.toString(threads));
        try {
            PgBaselineVerify.verify(RowAuditTestSupport.connections(schema, pg, new int[1], correlation), schema, directory);
        } finally {
            if (previous == null) System.clearProperty("sibench.baseline.threads");
            else System.setProperty("sibench.baseline.threads", previous);
        }
        return directory.resolve("verified-manifest.json");
    }

    private static Map<String, List<JsonObject>> copy(Map<String, List<JsonObject>> rows) {
        var result = new LinkedHashMap<String, List<JsonObject>>();
        rows.forEach((table, list) -> result.put(table, new ArrayList<>(list)));
        return result;
    }

    @Test
    public void rowGroupsVerifiedConcurrentlyGiveTheSerialReport() throws Exception {
        for (String benchmark : List.of("sysbench", "ycsb")) {
            var schema = schema(benchmark);
            var rows = rows(schema, 10);
            String serial = Files.readString(verified(schema, rows, rows, 1, 1.0));
            // The report does not depend on the thread count or on how PG was read.
            for (int threads : new int[] {1, 4})
                for (double correlation : READINGS)
                    assertEquals(serial, Files.readString(verified(schema, rows, rows, threads, correlation)));
            // An uncorrelated table read as one key-ordered stream, row by row, reports the same.
            orderedStream(() -> assertEquals(serial, Files.readString(verified(schema, rows, rows, 4, 0.0))));
            var report = JsonParser.parseString(serial).getAsJsonObject();
            assertEquals("verified_complete", report.get("status").getAsString());
            assertEquals(10L * schema.tables().size(), report.get("rows").getAsLong());
            for (var file : report.getAsJsonArray("files"))
                assertTrue(file.getAsJsonObject().has("sha256"));
        }
    }

    @Test
    public void aPgRowInAnyGapOrMissingFromAnyGroupFails() throws Exception {
        for (String benchmark : List.of("sysbench", "ycsb"))
            for (double correlation : READINGS) mismatches(benchmark, correlation);
        for (String benchmark : List.of("sysbench", "ycsb")) orderedStream(() -> mismatches(benchmark, 0.0));
    }

    /** Parquet keys out of order fail, though PG holds the same rows (an unordered comparison). */
    @Test
    public void baselineKeysOutOfOrderFail() throws Exception {
        for (String benchmark : List.of("sysbench", "ycsb")) {
            var schema = schema(benchmark);
            var table = schema.tables().get(0);
            var rows = rows(schema, 10);
            var swapped = copy(rows);
            var list = swapped.get(table.name());
            var first = list.get(1);
            list.set(1, list.get(2));
            list.set(2, first);
            for (double correlation : READINGS)
                assertThrows(benchmark + " " + correlation, Exception.class, () -> verified(schema, swapped, rows, 4, correlation));
        }
    }

    @FunctionalInterface
    private interface Check {
        void run() throws Exception;
    }

    private static void orderedStream(Check check) throws Exception {
        System.setProperty(PgBaselineVerify.ORDERED_STREAM, "true");
        try {
            check.run();
        } finally {
            System.clearProperty(PgBaselineVerify.ORDERED_STREAM);
        }
    }

    private void mismatches(String benchmark, double correlation) throws Exception {
        {
            var schema = schema(benchmark);
            var table = schema.tables().get(0);
            var rows = rows(schema, 10);
            // Below the first key, between two row groups (30 | 40), inside one, and above the last.
            for (long extra : new long[] {5, 35, 15, 105}) {
                var pg = copy(rows);
                pg.get(table.name()).add(row(table, extra));
                assertThrows(benchmark + " extra " + extra, Exception.class, () -> verified(schema, rows, pg, 4, correlation));
            }
            // The first and last row of a middle group, and the table's first and last row.
            for (int missing : new int[] {3, 5, 0, 9}) {
                var pg = copy(rows);
                pg.get(table.name()).remove(missing);
                assertThrows(benchmark + " missing " + missing, Exception.class, () -> verified(schema, rows, pg, 4, correlation));
            }
            var changed = copy(rows);
            var value = changed.get(table.name()).get(4).deepCopy();
            var column = table.columns().stream().filter(c -> c != SourceRow.primary(table)).findFirst().orElseThrow();
            if (column.type().equals("integer")) value.addProperty(column.name(), 99L);
            else value.addProperty(column.name(), "other");
            changed.get(table.name()).set(4, value);
            assertThrows(benchmark + " changed value", Exception.class, () -> verified(schema, rows, changed, 4, correlation));
        }
    }
}
