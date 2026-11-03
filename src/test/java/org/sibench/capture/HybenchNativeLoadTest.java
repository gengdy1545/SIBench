package org.sibench.capture;

import static org.junit.Assert.*;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;

/** The base is built from the pinned upstream scripts exactly as they are written. */
public class HybenchNativeLoadTest {
    private static final Path SOURCE = Path.of("benchmark/HyBench-2024");

    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    /** A missing or changed runtime input fails the base stage, not the clients mid-run. */
    @Test
    public void runtimeInputsMustBePresentAndMatchTheLoadManifest() throws Exception {
        Path work = temporary.newFolder().toPath(), data = Files.createDirectory(work.resolve("Data_1x"));
        Path manifest = work.resolve("load.json");
        assertThrows(java.nio.file.NoSuchFileException.class, () -> HybenchNativeLoad.requireRuntimeInputs(data, manifest));
        Files.writeString(manifest, "{\"data_sha256\":{}}");
        var failure = assertThrows(IllegalStateException.class, () -> HybenchNativeLoad.requireRuntimeInputs(data, manifest));
        assertTrue(failure.getMessage(), failure.getMessage().contains("Related_transfer_bids"));
        var digests = new com.google.gson.JsonObject();
        for (String name : HybenchNativeLoad.RUNTIME_INPUTS) {
            Files.writeString(data.resolve(name), name);
            digests.addProperty(name, org.sibench.config.Digests.sha256(data.resolve(name)));
        }
        var report = new com.google.gson.JsonObject();
        report.add("data_sha256", digests);
        Files.writeString(manifest, report.toString());
        HybenchNativeLoad.requireRuntimeInputs(data, manifest);
        Files.writeString(data.resolve("Related_checking_bids"), "regenerated");
        failure = assertThrows(IllegalStateException.class, () -> HybenchNativeLoad.requireRuntimeInputs(data, manifest));
        assertTrue(failure.getMessage(), failure.getMessage().contains("differs from the load manifest"));
        // Cleanup deletes only upstream COPY inputs, preserving unrelated diagnostic files.
        Files.writeString(data.resolve("transfer.csv"), "1,2,3");
        Files.writeString(data.resolve("diagnostic.txt"), "unique evidence");
        HybenchNativeLoad.dropLoadInputs(data, SOURCE.resolve("conf/load_data_pg.sql"));
        try (var left = Files.list(data)) {
            var expected = new java.util.TreeSet<>(HybenchNativeLoad.RUNTIME_INPUTS);
            expected.add("diagnostic.txt");
            assertEquals(expected,
                    left.map(f -> f.getFileName().toString()).collect(java.util.stream.Collectors.toCollection(java.util.TreeSet::new)));
        }
    }

    @Test
    public void upstreamDdlIsEightTables() throws Exception {
        var statements = HybenchNativeLoad.statements(SOURCE.resolve("conf/ddl_pg.sql"));
        assertEquals(8, statements.size());
        for (String sql : statements) assertTrue(sql, sql.toUpperCase().startsWith("CREATE TABLE"));
    }

    @Test
    public void everyUpstreamLoadStatementIsRecognised() throws Exception {
        long statements = Files.readAllLines(SOURCE.resolve("conf/load_data_pg.sql")).stream()
                .filter(line -> !line.isBlank())
                .peek(line -> assertTrue(line, HybenchNativeLoad.copyStatement(line) != null))
                .count();
        assertEquals(8, statements);
        assertArrayEquals(new String[] {"loanTrans", "loanTrans.csv"},
                HybenchNativeLoad.copyStatement("\\copy loanTrans from 'Data_1x/loanTrans.csv' CSV DELIMITER ',' ;"));
        assertNull(HybenchNativeLoad.copyStatement("\\copy loanTrans from 'elsewhere.csv' CSV"));
    }
}
