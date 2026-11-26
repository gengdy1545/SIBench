package org.sibench.capture;

import static org.junit.Assert.*;
import static org.sibench.config.ConfigValues.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.config.BenchmarkSelection;
import org.sibench.config.Engine;
import org.sibench.schema.SourceSchema;

public class SysbenchInputsTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test public void querySwitchesAreExplicitValidatedAndPassedToBothDrivers() throws Exception {
        var document = DuckDbGenerationTest.profile("sysbench");
        var n = object(document, "native");
        var workload = new LinkedHashMap<String,Object>(Map.of("script", "oltp_read_write", "events", 10,
                "rand-zipfian-exp", 0.99, "point-selects", 0, "range-selects", false));
        n.put("rand-type", "zipfian"); n.put("workloads", Map.of("writes", workload));
        document.put("native", n);
        Generation.validateNative("sysbench", n);
        var input = input(temporary.getRoot().toPath(), document);
        SysbenchInputs.requireWorkload(input);
        var args = Generation.sysbenchOptions(n, "writes", false);
        assertTrue(args.containsAll(java.util.List.of("--point-selects=0", "--range-selects=off", "--rand-zipfian-exp=0.99")));
        assertFalse(Generation.sysbenchOptions(n, "writes", true).stream().anyMatch(a -> a.startsWith("--point-selects")));
        workload.put("point-selects", 1);
        assertThrows(IllegalArgumentException.class, () -> SysbenchInputs.requireWorkload(input));
        workload.put("point-selects", -1);
        assertThrows(IllegalArgumentException.class, () -> Generation.validateNative("sysbench", n));
        workload.put("point-selects", 0); workload.put("range-selects", true);
        assertThrows(IllegalArgumentException.class, () -> SysbenchInputs.requireWorkload(input));
        workload.remove("range-selects");
        assertThrows(IllegalArgumentException.class, () -> SysbenchInputs.requireWorkload(input));
    }

    @Test public void portableBaselineKeepsSysbenchIdentityAndRejectsTampering() throws Exception {
        Path output = temporary.newFolder("csv").toPath();
        Files.createDirectory(output.resolve("baseline"));
        // Independent two-row fixture, including fixed-width padding and a CSV quote.
        Files.writeString(output.resolve("baseline/sbtest1.csv"), "1,7,\"a,b\",pad\n2,3,other,\"\"\n");
        Files.writeString(output.resolve("schema.sql"),
                "CREATE TABLE sbtest1 (id integer PRIMARY KEY, k integer NOT NULL, c char(120) NOT NULL, pad char(60) NOT NULL);\n");
        var input = input(output, Map.of("native", Map.of("tables", 1, "table-size", 2, "auto-inc", true, "rand-seed", 7)));
        var inspected = PixelsInputs.prepare(input.source(), Map.of("sbtest1", output.resolve("baseline/sbtest1.csv")), null, 10);
        Files.writeString(output.resolve("pixels-schema.tsv"), inspected.schema().text());
        var manifest = SysbenchInputs.manifest(output, input, inspected.sha256());
        Path path = output.resolve("manifest.json");
        Files.writeString(path, manifest.toString());
        var csv = CsvInput.open(output);
        assertEquals("sysbench", CsvInput.benchmark(csv.manifest()));
        var ddl = manifest.getAsJsonObject("source_ddl");
        var source = SourceSchema.captured(output.resolve("schema.sql"), input.source().sourceCommit(),
                ddl.get("sha256").getAsString(), CsvInput.benchmark(manifest));
        assertEquals("sysbench", source.benchmark());
        assertEquals("character(120)", source.tables().get(0).columns().get(2).type());
        manifest.getAsJsonObject("source_evidence").remove("benchmark");
        Files.writeString(path, manifest.toString());
        assertThrows(java.io.IOException.class, () -> CsvInput.open(output));
        manifest.getAsJsonObject("source_evidence").addProperty("benchmark", "sysbench");
        manifest.getAsJsonObject("data_parameters").addProperty("table-size", 3);
        Files.writeString(path, manifest.toString());
        assertThrows(java.io.IOException.class, () -> CsvInput.open(output));
        manifest.getAsJsonObject("data_parameters").addProperty("table-size", 2);
        Files.writeString(path, manifest.toString());
        var file = output.resolve("baseline/sbtest1.csv");
        Files.writeString(file, Files.readString(file).replace("1,7", "1,9"));
        assertThrows(java.io.IOException.class, () -> CsvInput.open(output));
    }

    private Generation.Input input(Path output, Map<String,Object> document) throws Exception {
        Path root = Path.of("").toRealPath();
        var n = object(document, "native");
        var source = SourceSchema.of("sysbench", root.resolve("benchmark/sysbench"), n);
        var rows = new LinkedHashMap<String,Long>();
        for (var table : source.tables()) rows.put(table.name(), ((Number)n.get("table-size")).longValue());
        var selection = new BenchmarkSelection(root.resolve("config/benchmark/sysbench.yaml"), "sysbench",
                BenchmarkSelection.Mode.CDC, Engine.PIXELS, "sysbench", "writes", output.resolve("unused"), false, null);
        return new Generation.Input(selection, document, source, root, output, Map.of(), rows, Map.of());
    }
}
