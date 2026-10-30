package org.sibench.capture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.gson.JsonObject;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.config.ConfigValues;
import org.sibench.generation.GenerationConfig;
import org.sibench.schema.SourceSchema;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** One export per still database; every capture's baseline links to it. */
public class SharedBaselineTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void theFirstCaptureExportsAndEveryCaptureLinks() throws Exception {
        var document = ConfigValues.parse(Files.readString(Path.of("config/sysbench_large.yaml")));
        var nativeConfig = ConfigValues.object(document, "native");
        nativeConfig.put("tables", 1);
        document.put("native", nativeConfig);
        var schema = SourceSchema.read(Path.of(""), document);
        var table = schema.tables().get(0);
        var rows = new LinkedHashMap<String, List<JsonObject>>();
        var list = new ArrayList<JsonObject>();
        for (long key = 1; key <= 5; key++) {
            var row = new JsonObject();
            for (var column : table.columns())
                if (column.type().equals("integer")) row.addProperty(column.name(), key);
                else row.addProperty(column.name(), "v" + key);
            list.add(row);
        }
        rows.put(table.name(), list);
        Path root = temporary.newFolder().toPath();
        // The capture script creates the still directory when it builds the still cluster.
        Files.createDirectories(StillBase.directory(root, "base"));
        int[] exports = {0};
        SharedBaseline.Export export = layout -> {
            exports[0]++;
            Path built = RowAuditTestSupport.baseline(temporary.newFolder().toPath(), schema, rows);
            Files.move(built, layout.output());
            PgBaselineVerify.verify(RowAuditTestSupport.connections(schema, rows, new int[1]), schema, layout.output());
            assertTrue(Files.exists(BaselineDigests.location(layout.output())));
        };
        GenerationConfig first = layout(root, "first");
        SharedBaseline.link(root, "base", first, export);
        SharedBaseline.link(root, "base", layout(root, "second"), export);
        assertEquals(1, exports[0]);
        Path shared = StillBase.directory(root, "base").resolve("baseline");
        assertFalse(Files.exists(BaselineDigests.location(shared.resolveSibling("baseline.partial"))));
        var publishedDigests = BaselineDigests.of(shared);
        try (var files = Files.list(shared)) {
            for (Path file : files.toList()) {
                assertTrue(Files.isSameFile(file, root.resolve("second").resolve(file.getFileName())));
                if (file.toString().endsWith(".parquet")) assertTrue(publishedDigests.get(file) != null);
            }
        }
        // A mutation after PG verification but before publication invalidates the cached identity.
        Files.createDirectories(StillBase.directory(root, "changed"));
        assertThrows(java.io.IOException.class, () -> SharedBaseline.link(root, "changed", layout(root, "changed-output"), output -> {
            export.into(output);
            try (var paths = Files.list(output.output())) {
                Path file = paths.filter(p -> p.toString().endsWith(".parquet")).findFirst().orElseThrow();
                byte[] content = Files.readAllBytes(file);
                content[content.length / 2] ^= 1;
                Files.write(file, content);
                Files.setLastModifiedTime(file, java.nio.file.attribute.FileTime.fromMillis(System.currentTimeMillis() + 5_000));
            }
        }));
        // The second capture's check used what the first recorded, and records every data file.
        Path second = root.resolve("second");
        var recorded = BaselineDigests.of(second);
        try (var files = Files.list(second)) {
            for (Path file : files.filter(f -> f.toString().endsWith(".parquet")).toList())
                assertTrue(file.toString(), recorded.get(file) != null);
        }
        // A data file that changed is read again, whatever was recorded for its earlier content.
        Path data;
        try (var files = Files.list(shared)) {
            data = files.filter(f -> f.toString().endsWith(".parquet")).findFirst().orElseThrow();
        }
        byte[] bytes = Files.readAllBytes(data);
        bytes[bytes.length / 2] ^= 1;
        Files.write(data, bytes);
        Files.setLastModifiedTime(data, java.nio.file.attribute.FileTime.fromMillis(System.currentTimeMillis() + 5_000));
        assertThrows(java.io.IOException.class, () -> PgBaselineVerify.verifyFiles(second));

        var other = CaptureConfig.read(Path.of(""), Path.of("config/hybench_sf1.yaml")).layout();
        var changed = new GenerationConfig(other.root(), other.document(), other.benchmark(), other.seed(), other.targetBytes(), other.maxOvershootPercent(),
                other.maxRows() / 2, other.groupBytes(), other.pageBytes(), other.dictionaryBytes(), other.dictionary(),
                other.codec(), other.codecLevel(), other.buckets(), other.baselineTimestamp(), root.resolve("third"), other.minFreeBytes());
        var failure = assertThrows(IllegalStateException.class, () -> SharedBaseline.link(root, "base", changed, export));
        assertTrue(failure.getMessage(), failure.getMessage().contains("data_layout differs"));
    }

    private static GenerationConfig layout(Path root, String output) throws Exception {
        return SharedBaseline.withOutput(CaptureConfig.read(Path.of(""), Path.of("config/hybench_sf1.yaml")).layout(), root.resolve(output));
    }
}
