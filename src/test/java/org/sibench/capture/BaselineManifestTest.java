package org.sibench.capture;

import com.google.gson.JsonObject;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.config.ConfigValues;
import org.sibench.schema.SourceSchema;

import java.nio.file.*;
import java.util.*;
import static org.junit.Assert.*;

public class BaselineManifestTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    private SourceSchema schema() throws Exception {
        var document = ConfigValues.parse(Files.readString(Path.of("src/test/resources/config/ycsb_1b.yaml")));
        return SourceSchema.read(Path.of(""), document);
    }

    @Test
    public void exportIdentityDoesNotClaimIndependentVerificationAndRejectsChangedData() throws Exception {
        var schema = schema();
        var table = schema.tables().get(0);
        var row = new JsonObject();
        for (var column : table.columns()) row.addProperty(column.name(), "value");
        Path baseline = RowAuditTestSupport.baseline(temporary.newFolder().toPath(), schema, Map.of(table.name(), List.of(row)));
        assertEquals(1L, BaselineManifest.verify(schema, baseline).get("rows").getAsLong());
        assertFalse(Files.exists(baseline.resolve("verified-manifest.json")));
        Files.writeString(baseline.resolve("INCOMPLETE"), "unfinished");
        assertThrows(java.io.IOException.class, () -> BaselineManifest.verifyFiles(baseline));
        Files.delete(baseline.resolve("INCOMPLETE"));
        Path data = baseline.resolve(table.name() + ".parquet");
        Files.write(data, new byte[] {1, 2, 3});
        assertThrows(java.io.IOException.class, () -> BaselineManifest.verifyFiles(baseline));
    }

    @Test
    public void missingWholeTableCannotPublishEvenWithoutIndependentAudit() throws Exception {
        var schema = schema();
        Path baseline = temporary.newFolder().toPath();
        Files.writeString(baseline.resolve("files.jsonl"), "");
        assertThrows(java.io.IOException.class, () -> BaselineManifest.publish(schema, baseline,
                Map.of(schema.tables().get(0).name(), 1L), TraceOutput.FULL));
        assertFalse(Files.exists(baseline.resolve(BaselineManifest.FILE)));
        BaselineManifest.publish(schema, baseline, Map.of(schema.tables().get(0).name(), 0L), TraceOutput.FULL);
        assertEquals(0L, BaselineManifest.verify(schema, baseline).get("rows").getAsLong());
    }
    @Test
    public void readRejectsChangedTotalsAndTableCoverage() throws Exception {
        var schema = schema();
        var table = schema.tables().get(0);
        var row = new JsonObject();
        for (var column : table.columns()) row.addProperty(column.name(), "value");
        Path baseline = RowAuditTestSupport.baseline(temporary.newFolder().toPath(), schema, Map.of(table.name(), List.of(row)));
        Path receipt = baseline.resolve(BaselineManifest.FILE);
        String original = Files.readString(receipt);
        for (String defect : List.of("total", "table_count", "missing_table", "extra_table", "missing_output", "unsupported_output", "non_string_output")) {
            var changed = com.google.gson.JsonParser.parseString(original).getAsJsonObject();
            switch (defect) {
                case "total" -> changed.addProperty("rows", 2);
                case "missing_output" -> changed.remove("output");
                case "unsupported_output" -> changed.addProperty("output", "raw");
                case "non_string_output" -> changed.addProperty("output", 1);
                case "table_count" -> changed.getAsJsonObject("tables").addProperty(table.name(), 2);
                case "missing_table" -> changed.getAsJsonObject("tables").remove(table.name());
                case "extra_table" -> changed.getAsJsonObject("tables").addProperty("unknown", 0);
            }
            Files.writeString(receipt, changed.toString());
            assertThrows(defect, java.io.IOException.class, () -> BaselineManifest.verifyFiles(baseline));
        }
        Files.writeString(receipt, original);
        assertEquals(1L, BaselineManifest.verifyFiles(baseline).get("rows").getAsLong());
    }

    @Test
    public void duplicateFileIdentitiesCannotPublish() throws Exception {
        var schema = schema();
        String table = schema.tables().get(0).name();
        Path baseline = temporary.newFolder().toPath();
        var first = new JsonObject();
        first.addProperty("table", table);
        first.addProperty("path", "first.parquet");
        first.addProperty("file_id", 1);
        first.addProperty("rows", 1);
        var second = first.deepCopy();
        second.addProperty("path", "second.parquet");
        Files.write(baseline.resolve("first.parquet"), new byte[] {1});
        Files.write(baseline.resolve("second.parquet"), new byte[] {2});
        Files.write(baseline.resolve("files.jsonl"), List.of(first.toString(), second.toString()));
        assertThrows(java.io.IOException.class, () -> BaselineManifest.publish(schema, baseline, Map.of(table, 2L), TraceOutput.FULL));
        assertFalse(Files.exists(baseline.resolve(BaselineManifest.FILE)));
    }

}
