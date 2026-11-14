package org.sibench.config;

import static org.junit.Assert.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class BenchmarkSelectionTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    private Map<String, Object> entry(String file, String mode, String engine, String name) {
        return BenchmarkFixtures.entry(Path.of(file), mode, engine, Path.of("traces", name));
    }
    private Map<String, Object> runtime(Object entries) {
        return Map.of("schema_version", 1, "purpose", "sibench", "benchmarks", entries);
    }
    private void profiles() throws Exception {
        Files.writeString(temporary.getRoot().toPath().resolve("ycsb.yaml"), "benchmark: ycsb\nnative:\n  workloads: {workloada: {}, workloadb: {}}\n");
        Files.writeString(temporary.getRoot().toPath().resolve("sysbench.yaml"), "benchmark: sysbench\nnative:\n  workloads: {oltp_read_write: {}}\n");
        Files.writeString(temporary.getRoot().toPath().resolve("hybench.yaml"), "deliberately invalid: [");
    }

    @Test public void namedWorkloadsShareAProfileAndKeepTheirOwnPathsAndSwitches() throws Exception {
        profiles();
        var entries = new LinkedHashMap<String, Object>();
        var a = entry("ycsb.yaml", "api", "rocksdb", "a"); a.put("workload", "workloada");
        var b = entry("ycsb.yaml", "api", "rocksdb", "b"); b.put("workload", "workloadb");
        b.put("trace", Map.of("generate", false, "directory", "traces/b", "upload", Map.of("enabled", false)));
        entries.put("ycsb-a", a); entries.put("ycsb-b", b);
        var root = temporary.getRoot().toPath();
        var selected = BenchmarkSelection.read(root, runtime(entries));
        assertEquals(List.of("ycsb-a", "ycsb-b"), selected.stream().map(BenchmarkSelection::name).toList());
        assertEquals(selected.get(0).config(), selected.get(1).config());
        assertEquals(List.of("workloada", "workloadb"), selected.stream().map(BenchmarkSelection::workload).toList());
        assertTrue(selected.get(0).generate()); assertFalse(selected.get(1).generate());
        assertEquals(root.resolve("traces/b"), BenchmarkSelection.directory(root, runtime(entries), "ycsb-b"));
        assertNull(selected.get(0).upload());
        assertThrows(UnsupportedOperationException.class, () -> selected.clear());
    }

    @Test public void consumersNeedNoSourceCheckoutOrCloudCredentials() {
        var entry = entry("missing.yaml", "capture", "postgres", "hybench");
        entry.put("trace", Map.of("generate", true, "directory", "traces/hybench", "upload", Map.of("enabled", true, "path", "cos://bucket/traces/hybench")));
        var runtime = runtime(Map.of("hybench", entry));
        assertEquals(temporary.getRoot().toPath().resolve("traces/hybench"), BenchmarkSelection.directory(temporary.getRoot().toPath(), runtime, "hybench"));
        assertThrows(IllegalArgumentException.class, () -> BenchmarkSelection.directory(temporary.getRoot().toPath(), runtime, "missing"));
    }

    @Test public void rejectsMalformedSelectionsAndOldListShape() {
        for (Object entries : new Object[] {Map.of(), List.of(), List.of(entry("ycsb.yaml", "api", "rocksdb", "a")), "ycsb", Map.of("a", Map.of("config", "a"))})
            assertThrows(IllegalArgumentException.class, () -> BenchmarkSelection.validate(entries));
        var a = entry("ycsb.yaml", "api", "rocksdb", "a");
        a.put("trace", Map.of("generate", true, "directory", "traces/a", "upload", Map.of("enabled", true)));
        assertThrows(IllegalArgumentException.class, () -> BenchmarkSelection.validate(Map.of("a", a)));
    }

    @Test public void duplicateWorkloadsAndOverlappingOutputsAreRejected() throws Exception {
        profiles(); var root = temporary.getRoot().toPath();
        var a = entry("ycsb.yaml", "api", "rocksdb", "a"); a.put("workload", "workloada");
        var b = entry("./ycsb.yaml", "api", "rocksdb", "b"); b.put("workload", "workloada");
        assertThrows(IllegalArgumentException.class, () -> BenchmarkSelection.read(root, runtime(Map.of("a", a, "b", b))));
        b.put("workload", "unknown");
        assertThrows(IllegalArgumentException.class, () -> BenchmarkSelection.read(root, runtime(Map.of("b", b))));
        b.put("workload", "workloadb"); b.put("trace", a.get("trace"));
        assertThrows(IllegalArgumentException.class, () -> BenchmarkSelection.directories(root, runtime(Map.of("a", a, "b", b))));
        b.put("trace", Map.of("generate", false, "directory", "traces/a/child", "upload", Map.of("enabled", false)));
        assertThrows(IllegalArgumentException.class, () -> BenchmarkSelection.directories(root, runtime(Map.of("a", a, "b", b))));
    }

    @Test public void sourceModeAndEngineRemainExplicit() throws Exception {
        profiles();var root = temporary.getRoot().toPath();
        for (String engine : List.of("postgres", "duckdb")) {
            var ycsb = entry("ycsb.yaml", "api", engine, "a"); ycsb.put("workload", "workloada");
            assertThrows(IllegalArgumentException.class, () -> BenchmarkSelection.read(root, runtime(Map.of("a", ycsb))));
        }
        var selected = BenchmarkSelection.read(root, runtime(Map.of("sysbench", entry("sysbench.yaml", "capture", "postgres", "sysbench"))));
        assertEquals(Engine.POSTGRES, selected.get(0).engine());
        assertThrows(IllegalArgumentException.class, () -> BenchmarkSelection.read(root, runtime(Map.of("sysbench", entry("sysbench.yaml", "api", "rocksdb", "sysbench")))));
        assertThrows(IllegalArgumentException.class, () -> BenchmarkSelection.validate(Map.of("a", entry("ycsb.yaml", "cdc", "pixels", "a"))));
    }

    @Test public void conflictingMissingAndUnknownSourcesRemainRejected() throws Exception {
        profiles();var root = temporary.getRoot().toPath();
        Files.copy(root.resolve("ycsb.yaml"), root.resolve("second.yaml"));
        var a = entry("ycsb.yaml", "api", "rocksdb", "a"); a.put("workload", "workloada");
        var b = entry("second.yaml", "api", "rocksdb", "b"); b.put("workload", "workloadb");
        assertTrue(assertThrows(IllegalArgumentException.class, () -> BenchmarkSelection.read(root, runtime(Map.of("a", a, "b", b))))
                .getMessage().contains("Conflicting source"));
        assertThrows(java.io.IOException.class, () -> BenchmarkSelection.read(root, runtime(Map.of("a", entry("missing.yaml", "api", "rocksdb", "a")))));
        Files.writeString(root.resolve("unknown.yaml"), "benchmark: unknown\n");
        assertTrue(assertThrows(IllegalArgumentException.class, () -> BenchmarkSelection.read(root, runtime(Map.of("a", entry("unknown.yaml", "api", "rocksdb", "a")))))
                .getMessage().contains("Unsupported benchmark"));
    }
}
