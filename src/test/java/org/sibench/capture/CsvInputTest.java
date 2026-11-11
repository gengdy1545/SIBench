package org.sibench.capture;

import static org.junit.Assert.*;

import com.google.gson.JsonParser;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.config.BenchmarkSelection;
import org.sibench.config.Engine;
import org.sibench.schema.SourceSchema;

public class CsvInputTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test public void independentConsumersBindTheSamePublishedInput() throws Exception {
        var fixture = fixture();
        var csv = CsvInput.open(fixture.output());
        CsvInput.requireCompatible(csv, fixture);
        var pool = Executors.newFixedThreadPool(2);
        try {
            Path capture = temporary.newFolder("capture").toPath(), pixels = temporary.newFolder("pixels").toPath();
            var first = pool.submit(() -> { CsvInput.materialize(csv, capture); return capture; });
            var second = pool.submit(() -> { CsvInput.materialize(csv, pixels); return pixels; });
            for (Path consumer : java.util.List.of(first.get(), second.get())) {
                var binding = JsonParser.parseString(Files.readString(consumer.resolve("csv-input.json"))).getAsJsonObject();
                assertEquals(csv.sha256(), binding.get("manifest_sha256").getAsString());
                CsvInput.validateBaseline(consumer, csv.manifest(), true);
                assertFalse(Files.exists(consumer.resolve("database")));
                assertFalse(Files.exists(consumer.resolve("cdc")));
            }
            Generation.deleteTree(capture);
            assertEquals(csv.sha256(), CsvInput.open(fixture.output()).sha256());
            CsvInput.validateBaseline(pixels, csv.manifest(), true);
        } finally { pool.shutdownNow(); }
    }

    @Test public void corruptOrSubstitutedInputsCannotBeConsumed() throws Exception {
        var fixture = fixture();
        Path file = fixture.output().resolve("native/Data_1x/customer.csv");
        byte[] original = Files.readAllBytes(file);
        Files.writeString(file, "wrong data\n");
        assertThrows(java.io.IOException.class, () -> CsvInput.open(fixture.output()));
        Files.write(file, original);
        Path outside = temporary.newFile("outside.csv").toPath();
        Files.write(outside, original);
        Files.delete(file);
        Files.createSymbolicLink(file, outside);
        assertThrows(java.io.IOException.class, () -> CsvInput.open(fixture.output()));
    }

    @Test public void captureParametersMustMatchTheGenerator() throws Exception {
        var fixture = fixture();
        var csv = CsvInput.open(fixture.output());
        var changed = new Generation.Input(fixture.selection(), fixture.document(), fixture.source(), fixture.build(),
                fixture.output(), fixture.buildIdentity(), fixture.baselineRows(), Map.of("customer_number", 2L));
        assertThrows(java.io.IOException.class, () -> CsvInput.requireCompatible(csv, changed));
        Path parameters = fixture.output().resolve("native/parameters/parameters.toml");
        Files.writeString(parameters, "[1x]\ncustomer_number = 2\n");
        assertThrows(java.io.IOException.class, () -> CsvInput.open(fixture.output()));
    }

    @Test public void anUnpublishedDirectoryIsNotAnInput() throws Exception {
        var fixture = fixture();
        Files.delete(fixture.output().resolve("manifest.json"));
        assertThrows(java.io.IOException.class, () -> CsvInput.open(fixture.output()));
    }

    private Generation.Input fixture() throws Exception {
        return fixture(temporary.newFolder("input").toPath());
    }

    static Generation.Input fixture(Path output) throws Exception {
        Path root = Path.of("").toRealPath();
        var source = SourceSchema.hybench(root.resolve("benchmark/HyBench-2024"));
        Path data = Files.createDirectories(output.resolve("native/Data_1x"));
        HybenchTest.fixture(data);
        for (String name : HybenchInputs.RUNTIME_INPUTS) Files.write(data.resolve(name), new byte[0]);
        Files.createDirectory(output.resolve("load"));
        HybenchInputs.record(data, source.source().resolve("conf/load_data_pg.sql"), output.resolve("load/inputs.json"));
        Files.copy(source.source().resolve("conf/ddl_pg.sql"), output.resolve("schema.sql"));
        Files.writeString(output.resolve("pixels-schema.tsv"), PixelsInputs.inspect(source, data).schema().text());
        var rows = new LinkedHashMap<String, Long>();
        source.tables().forEach(table -> rows.put(table.name(), 1L));
        Map<String, Object> parameters = Map.of("customer_number", 1L);
        Files.createDirectory(output.resolve("native/parameters"));
        Files.writeString(output.resolve("native/parameters/parameters.toml"),
                new com.moandjiezana.toml.TomlWriter().write(Map.of("1x", parameters)));
        var selection = new BenchmarkSelection(root.resolve("config/benchmark/hybench.yaml"), "hybench", BenchmarkSelection.Mode.CDC, Engine.PIXELS);
        var input = new Generation.Input(selection, Map.of("native", Map.of("sf", "1x")), source, root, output, Map.of(), rows, parameters);
        CsvInput.publish(output, input, Map.of("benchmark_source", Map.of("commit", source.sourceCommit(), "files", source.fingerprints())));
        return input;
    }
}
