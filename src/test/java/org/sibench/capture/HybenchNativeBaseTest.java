package org.sibench.capture;

import static org.junit.Assert.*;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;

/** The base is built from the pinned upstream scripts exactly as they are written. */
public class HybenchNativeBaseTest {
    private static final Path SOURCE = Path.of("benchmark/HyBench-2024");

    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    /** A missing or changed runtime input fails the base stage, not the clients mid-run. */
    @Test
    public void runtimeInputsMustBePresentAndMatchTheBaseManifest() throws Exception {
        Path work = temporary.newFolder().toPath(), data = Files.createDirectory(work.resolve("Data_1x"));
        Path manifest = work.resolve("base.json");
        var failure = assertThrows(IllegalStateException.class, () -> HybenchNativeBase.requireRuntimeInputs(data, manifest));
        assertTrue(failure.getMessage(), failure.getMessage().contains("Related_transfer_bids"));
        var digests = new com.google.gson.JsonObject();
        for (String name : HybenchNativeBase.RUNTIME_INPUTS) {
            Files.writeString(data.resolve(name), name);
            digests.addProperty(name, org.sibench.generation.BaselineGenerator.sha256(data.resolve(name)));
        }
        // Without a manifest presence is all that can be checked.
        HybenchNativeBase.requireRuntimeInputs(data, manifest);
        var report = new com.google.gson.JsonObject();
        report.add("data_sha256", digests);
        Files.writeString(manifest, report.toString());
        HybenchNativeBase.requireRuntimeInputs(data, manifest);
        Files.writeString(data.resolve("Related_checking_bids"), "regenerated");
        failure = assertThrows(IllegalStateException.class, () -> HybenchNativeBase.requireRuntimeInputs(data, manifest));
        assertTrue(failure.getMessage(), failure.getMessage().contains("differs from the base manifest"));
        // Once loaded, only the runtime inputs stay.
        Files.writeString(data.resolve("transfer.csv"), "1,2,3");
        HybenchNativeBase.dropLoadInputs(data);
        try (var left = Files.list(data)) {
            assertEquals(new java.util.TreeSet<>(HybenchNativeBase.RUNTIME_INPUTS),
                    left.map(f -> f.getFileName().toString()).collect(java.util.stream.Collectors.toCollection(java.util.TreeSet::new)));
        }
    }

    @Test
    public void upstreamDdlIsEightTables() throws Exception {
        var statements = HybenchNativeBase.statements(SOURCE.resolve("conf/ddl_pg.sql"));
        assertEquals(8, statements.size());
        for (String sql : statements) assertTrue(sql, sql.toUpperCase().startsWith("CREATE TABLE"));
    }

    @Test
    public void everyUpstreamLoadStatementIsRecognised() throws Exception {
        long statements = Files.readAllLines(SOURCE.resolve("conf/load_data_pg.sql")).stream()
                .filter(line -> !line.isBlank())
                .peek(line -> assertTrue(line, HybenchNativeBase.copyStatement(line) != null))
                .count();
        assertEquals(8, statements);
        assertArrayEquals(new String[] {"loanTrans", "loanTrans.csv"},
                HybenchNativeBase.copyStatement("\\copy loanTrans from 'Data_1x/loanTrans.csv' CSV DELIMITER ',' ;"));
        assertNull(HybenchNativeBase.copyStatement("\\copy loanTrans from 'elsewhere.csv' CSV"));
    }
}
