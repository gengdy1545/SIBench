package org.sibench.capture;

import static org.junit.Assert.*;

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
        HybenchInputs.requireCompatible(csv, fixture);
        var pool = Executors.newFixedThreadPool(2);
        try {
            Path capture = temporary.newFolder("capture").toPath(), pixels = temporary.newFolder("pixels").toPath();
            var first = pool.submit(() -> { CsvInput.materialize(csv, capture); return capture; });
            var second = pool.submit(() -> { CsvInput.materialize(csv, pixels); return pixels; });
            for (Path consumer : java.util.List.of(first.get(), second.get())) {
                // Consumers bind the CSV identity in their own published manifest, not in a side file.
                assertFalse(Files.exists(consumer.resolve("csv-input.json")));
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

    @Test public void publicationRejectsInspectedTableBytesThatDifferFromTheLoadInventory() throws Exception {
        Path output = temporary.newFolder("changed").toPath();
        var input = fixture(output);
        var inspected = new java.util.HashMap<>(PixelsInputs.inspect(input.source(), output.resolve("native/Data_1x")).sha256());
        Files.delete(output.resolve("manifest.json"));
        HybenchInputs.publish(output, input, inspected, Map.of("benchmark_source", Map.of("commit", input.source().sourceCommit(), "files", input.source().fingerprints())));
        // A table changed between the inventory and inspection reads produces another digest.
        Files.delete(output.resolve("manifest.json"));
        inspected.put("customer", "0".repeat(64));
        var failure = assertThrows(java.io.IOException.class, () -> HybenchInputs.publish(output, input, inspected, Map.of()));
        assertTrue(failure.getMessage(), failure.getMessage().contains("changed after source loading"));
        assertFalse(Files.exists(output.resolve("manifest.json")));
    }

    @Test public void captureParametersMustMatchTheGenerator() throws Exception {
        var fixture = fixture();
        var csv = CsvInput.open(fixture.output());
        var changed = new Generation.Input(fixture.selection(), fixture.document(), fixture.source(), fixture.build(),
                fixture.output(), fixture.buildIdentity(), fixture.baselineRows(), Map.of("customer_number", 2L));
        assertThrows(java.io.IOException.class, () -> HybenchInputs.requireCompatible(csv, changed));
        Path parameters = fixture.output().resolve("native/parameters/parameters.toml");
        Files.writeString(parameters, "[1x]\ncustomer_number = 2\n");
        assertThrows(java.io.IOException.class, () -> CsvInput.open(fixture.output()));
    }

    @Test public void benchmarkIdentityMustBeExplicitOrBoundToRetainedHybenchDdl() throws Exception {
        var fixture = fixture();
        var document = CsvInput.open(fixture.output()).manifest();
        var evidence = document.getAsJsonObject("source_evidence");
        assertEquals("hybench", evidence.get("benchmark").getAsString());
        evidence.addProperty("benchmark", "sysbench");
        Path manifest = fixture.output().resolve("manifest.json");
        Files.writeString(manifest, document.toString());
        assertThrows(java.io.IOException.class, () -> CsvInput.open(fixture.output()));
        // The retained SF10 artifact has no benchmark tag, but binds the original HyBench DDL.
        evidence.remove("benchmark");
        Files.writeString(manifest, document.toString());
        assertEquals("hybench", CsvInput.source(fixture.output(), CsvInput.open(fixture.output()).manifest()).benchmark());
        evidence.getAsJsonObject("benchmark_source").getAsJsonObject("files").addProperty("conf/ddl_pg.sql", "0".repeat(64));
        Files.writeString(manifest, document.toString());
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
        var inspected = PixelsInputs.inspect(source, data);
        Files.writeString(output.resolve("pixels-schema.tsv"), inspected.schema().text());
        var rows = new LinkedHashMap<String, Long>();
        source.tables().forEach(table -> rows.put(table.name(), 1L));
        Map<String, Object> parameters = Map.of("customer_number", 1L);
        Files.createDirectory(output.resolve("native/parameters"));
        Files.writeString(output.resolve("native/parameters/parameters.toml"),
                new com.moandjiezana.toml.TomlWriter().write(Map.of("1x", parameters)));
        var selection = new BenchmarkSelection(root.resolve("config/benchmark/hybench.yaml"), "hybench", BenchmarkSelection.Mode.CDC, Engine.PIXELS, "hybench", "xp", root.resolve("unused-trace"), false, null);
        var input = new Generation.Input(selection, Map.of("native", Map.of("sf", "1x")), source, root, output, Map.of(), rows, parameters);
        HybenchInputs.publish(output, input, inspected.sha256(), Map.of("benchmark_source", Map.of("commit", source.sourceCommit(), "files", source.fingerprints())));
        return input;
    }
}
