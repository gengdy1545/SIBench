package org.sibench.config;

import static org.junit.Assert.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class BenchmarkSelectionTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test public void resolvesOnlyListedProfilesInTheirConfiguredOrder() throws Exception {
        Path root = temporary.getRoot().toPath();
        profile("ycsb.yaml", "ycsb");
        profile("sysbench.yaml", "sysbench");
        Files.writeString(root.resolve("hybench.yaml"), "deliberately invalid: [");
        var document = runtime(List.of(selection("sysbench.yaml", "capture"), selection("ycsb.yaml", "api")));
        BenchmarkSelection.validate(document.get("benchmarks"));
        var selected = BenchmarkSelection.read(root, document);
        assertEquals(List.of("sysbench", "ycsb"), selected.stream().map(BenchmarkSelection::benchmark).toList());
        assertEquals(root.resolve("sysbench.yaml").toRealPath(), selected.get(0).config());
        assertEquals(BenchmarkSelection.Mode.CAPTURE, selected.get(0).mode());
        assertEquals(BenchmarkSelection.Mode.API, selected.get(1).mode());
        assertEquals(Engine.DUCKDB,selected.get(0).engine());
        assertEquals(Engine.ROCKSDB,selected.get(1).engine());
        assertThrows(UnsupportedOperationException.class, () -> selected.clear());
        assertFalse(Files.exists(root.resolve("data.duckdb")));
    }

    @Test public void rejectsAmbiguousOrMalformedSelections() throws Exception {
        for (Object selections : new Object[] {List.of(), "ycsb", List.of("ycsb"),
                java.util.Collections.singletonList(null), List.of(Map.of("config", "ycsb.yaml")),
                List.of(Map.of("config","ycsb.yaml","mode","api")),
                List.of(Map.of("config","sysbench.yaml","mode","capture","engine","auto")),
                List.of(selection(" ", "api")), List.of(selection("ycsb.yaml", "API")),
                List.of(Map.of("config", "ycsb.yaml", "mode", "api", "benchmark", "ycsb")),
                List.of(selection("ycsb.yaml", "api"), selection("ycsb.yaml", "api"))}) {
            assertThrows(IllegalArgumentException.class, () -> BenchmarkSelection.validate(selections));
        }
        assertThrows(IllegalArgumentException.class,
                () -> BenchmarkSelection.read(temporary.getRoot().toPath(), Map.of()));
    }

    @Test public void runtimeAcceptsExplicitSelectionsAndRejectsAnEmptySelection() {
        SibenchConfig.validate(runtime(List.of(selection("ycsb.yaml", "api"))));
        assertThrows(IllegalArgumentException.class,()->SibenchConfig.validate(runtime(List.of())));
    }

    @Test public void postgresIsAnExplicitEngineIndependentOfCaptureMode()throws Exception {
        profile("sysbench.yaml","sysbench");
        var selected=BenchmarkSelection.read(temporary.getRoot().toPath(),runtime(List.of(
                Map.of("config","sysbench.yaml","mode","capture","engine","postgres"))));
        assertEquals(Engine.POSTGRES,selected.get(0).engine());
        assertEquals(BenchmarkSelection.Mode.CAPTURE,selected.get(0).mode());
    }

    @Test public void ycsbRequiresTheNativeKvEngine() throws Exception {
        profile("ycsb.yaml", "ycsb"); profile("sysbench.yaml", "sysbench");
        for (String engine : List.of("postgres", "duckdb"))
            assertThrows(IllegalArgumentException.class, () -> BenchmarkSelection.read(temporary.getRoot().toPath(), runtime(List.of(
                    Map.of("config", "ycsb.yaml", "mode", "api", "engine", engine)))));
        assertThrows(IllegalArgumentException.class, () -> BenchmarkSelection.read(temporary.getRoot().toPath(), runtime(List.of(
                Map.of("config", "sysbench.yaml", "mode", "capture", "engine", "rocksdb")))));
    }

    @Test public void rejectsDuplicateBenchmarksAliasesAndUnsupportedModesBeforeExecution() throws Exception {
        Path root = temporary.getRoot().toPath();
        profile("ycsb.yaml", "ycsb");
        profile("second.yaml", "ycsb");
        profile("sysbench.yaml", "sysbench");
        profile("unknown.yaml", "unknown");
        for (var list : List.of(
                List.of(selection("ycsb.yaml", "api"), selection("./ycsb.yaml", "api")),
                List.of(selection("ycsb.yaml", "api"), selection("second.yaml", "api")),
                List.of(selection("ycsb.yaml", "capture")),
                List.of(selection("sysbench.yaml", "api")),
                List.of(selection("unknown.yaml", "api")))) {
            assertThrows(IllegalArgumentException.class, () -> BenchmarkSelection.read(root, runtime(list)));
        }
        assertThrows(java.io.IOException.class,
                () -> BenchmarkSelection.read(root, runtime(List.of(selection("missing.yaml", "api")))));
    }

    private void profile(String filename, String benchmark) throws Exception {
        Files.writeString(temporary.getRoot().toPath().resolve(filename), "benchmark: " + benchmark + "\n");
    }

    private static Map<String, String> selection(String config, String mode) {
        return Map.of("config", config, "mode", mode,"engine",mode.equals("api") ? "rocksdb" : "duckdb");
    }

    private static Map<String, Object> runtime(Object selections) {
        return Map.of("schema_version", 1, "purpose", "sibench", "benchmarks", selections);
    }
}
