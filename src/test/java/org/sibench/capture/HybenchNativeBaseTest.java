package org.sibench.capture;

import static org.junit.Assert.*;

import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;

/** The base is built from the pinned upstream scripts exactly as they are written. */
public class HybenchNativeBaseTest {
    private static final Path SOURCE = Path.of("benchmark/HyBench-2024");

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
