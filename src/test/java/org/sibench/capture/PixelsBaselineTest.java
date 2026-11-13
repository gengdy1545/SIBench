package org.sibench.capture;

import static org.junit.Assert.*;
import com.google.gson.*;
import java.nio.file.*;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class PixelsBaselineTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test public void baselineBindsTheSealedCsvAndRejectsAnotherCapture() throws Exception {
        Path root = fixture();
        var baseline = PixelsBaseline.open(root);
        var cdc = baseline.manifest().deepCopy();
        var evidence = new JsonObject(); evidence.add("csv_manifest_sha256", baseline.manifest().get("csv_manifest_sha256"));
        cdc.add("source_evidence", evidence);
        PixelsBaseline.match(baseline, cdc);
        evidence.addProperty("csv_manifest_sha256", "f".repeat(64));
        assertThrows(java.io.IOException.class, () -> PixelsBaseline.match(baseline, cdc));
        assertEquals("passed", PixelsBaseline.check(root).get("status"));
    }

    /** Binding checks only descriptors; the single conversion read must still reject a same-size change before use. */
    @Test public void conversionRejectsACsvChangedAfterBinding() throws Exception {
        var input = CsvInputTest.fixture(temporary.newFolder("csv").toPath());
        var csv = CsvInput.bind(input.output());
        var ddl = csv.manifest().getAsJsonObject("source_ddl");
        var source = org.sibench.schema.SourceSchema.captured(csv.directory().resolve(ddl.get("path").getAsString()),
                csv.manifest().getAsJsonObject("source_evidence").getAsJsonObject("benchmark_source").get("commit").getAsString(),
                ddl.get("sha256").getAsString());
        PixelsBaseline.convert(csv, source, temporary.getRoot().toPath().resolve("ok"), 1000);
        Path file = input.output().resolve("native/Data_1x/customer.csv");
        String text = Files.readString(file);
        int digit = text.indexOf('1');
        Files.writeString(file, text.substring(0, digit) + '2' + text.substring(digit + 1));
        var changed = CsvInput.bind(input.output());
        var failure = assertThrows(java.io.IOException.class, () -> PixelsBaseline.convert(changed, source,
                temporary.getRoot().toPath().resolve("changed"), 1000));
        assertTrue(failure.getMessage(), failure.getMessage().contains("CSV changed"));
    }

    @Test public void invalidObjectOwnershipCountsAndTimestampsAreRejected() throws Exception {
        Path root = fixture();
        String original = Files.readString(root.resolve("manifest.json"));
        for (String field : List.of("uri", "rows", "max_timestamp", "duplicate", "fraction", "string")) {
            var document = JsonParser.parseString(original).getAsJsonObject();
            var files = document.getAsJsonObject("inventory").getAsJsonArray("files");
            var first = files.get(0).getAsJsonObject();
            switch (field) {
                case "uri" -> first.addProperty("uri", "cos://bucket/outside.pixels");
                case "rows" -> first.addProperty("rows", 2);
                case "max_timestamp" -> first.addProperty("max_timestamp", 11);
                case "duplicate" -> files.add(first.deepCopy());
                case "fraction" -> first.addProperty("bytes", 0.5);
                case "string" -> first.addProperty("bytes", "100");
            }
            Files.writeString(root.resolve("manifest.json"), document.toString());
            assertThrows(field, java.io.IOException.class, () -> PixelsBaseline.open(root));
        }
    }

    @Test public void differentStorageAndReaderAreRejected() throws Exception {
        var baseline = PixelsBaseline.open(fixture());
        var profile = org.sibench.config.ConfigValues.parse(baseline.manifest().get("storage").toString());
        var settings = java.util.Map.<String, Object>of("data_storage", java.util.Map.of("profile", profile));
        var identity = org.sibench.config.ConfigValues.parse(baseline.manifest().get("engine").toString());
        PixelsBaseline.deployment(baseline, settings, identity);
        profile.put("bucket", "another-bucket");
        assertThrows(java.io.IOException.class, () -> PixelsBaseline.deployment(baseline, settings, identity));
        profile.put("bucket", "bucket");
        var changed = new java.util.LinkedHashMap<>(identity);
        changed.put("java", java.util.Map.of("pixels_commit", "c".repeat(40), "pixels_patch_sha256", "b".repeat(64)));
        assertThrows(java.io.IOException.class, () -> PixelsBaseline.deployment(baseline, settings, changed));
    }

    @Test public void schemaTamperingAndUnpublishedInputsFail() throws Exception {
        Path root = fixture();
        Files.writeString(root.resolve("pixels-schema.tsv"), "wrong");
        assertThrows(java.io.IOException.class, () -> PixelsBaseline.open(root));
        Files.delete(root.resolve("manifest.json"));
        assertThrows(java.io.IOException.class, () -> PixelsBaseline.open(root));
    }

    private Path fixture() throws Exception {
        return fixture(temporary.newFolder("csv").toPath(), temporary.newFolder("baseline").toPath());
    }

    static Path fixture(Path csvDirectory, Path root) throws Exception {
        var csv = CsvInput.open(CsvInputTest.fixture(csvDirectory).output());
        var document = new JsonObject();
        document.addProperty("kind", "pixels_baseline_input"); document.addProperty("status", "complete");
        document.addProperty("csv_manifest_sha256", csv.sha256());
        String base = "cos://bucket/sibench/pixels/runs/test/data";
        document.addProperty("data_uri", base);
        document.add("engine", new Gson().toJsonTree(java.util.Map.of("java", java.util.Map.of("pixels_commit", "a".repeat(40), "pixels_patch_sha256", "b".repeat(64)))));
        document.add("storage", new Gson().toJsonTree(java.util.Map.of("endpoint", "https://cos.example.com", "region", "test", "bucket", "bucket")));
        for (String field : List.of("schema", "source_ddl")) {
            var file = csv.manifest().getAsJsonObject(field);
            Files.copy(csv.directory().resolve(file.get("path").getAsString()), root.resolve(file.get("path").getAsString()));
            document.add(field, file);
        }
        document.add("baseline_rows", csv.manifest().get("baseline_rows"));
        document.add("source_evidence", csv.manifest().get("source_evidence"));
        var inventory = new JsonObject(); inventory.addProperty("max_timestamp", 10); inventory.addProperty("index_buckets", 8);
        var files = new JsonArray();
        for (String table : csv.manifest().getAsJsonObject("baseline_rows").keySet()) {
            var file = new JsonObject(); file.addProperty("table", table);
            file.addProperty("uri", base + "/postgres/" + table + "/v-0-ordered/part.pixels");
            file.addProperty("sha256", "a".repeat(64)); file.addProperty("bytes", 100);
            file.addProperty("rows", 1); file.addProperty("row_groups", 1); file.addProperty("max_timestamp", 10);
            files.add(file);
        }
        inventory.add("files", files); document.add("inventory", inventory);
        Files.writeString(root.resolve("manifest.json"), document.toString());
        return root;
    }
}
