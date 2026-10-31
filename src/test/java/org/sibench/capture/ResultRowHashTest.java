package org.sibench.capture;

import static org.junit.Assert.*;

import org.junit.Test;

import java.sql.Types;
import java.util.Arrays;
import java.util.List;

public class ResultRowHashTest {
    private static String hash(int type, String... values) {
        return ResultRowHash.of(Arrays.asList(values), java.util.Collections.nCopies(values.length, type));
    }

    /** What a JDBC client reads back differs from the server's text only in representation. */
    @Test
    public void representationsOfOneValueHashAlike() {
        assertEquals(hash(Types.REAL, "2000"), hash(Types.REAL, "2000.0"));
        assertEquals(hash(Types.DOUBLE, "1e3"), hash(Types.DOUBLE, "1000.0"));
        assertEquals(hash(Types.NUMERIC, "1.50"), hash(Types.NUMERIC, "1.5"));
        assertEquals(hash(Types.NUMERIC, "100.000"), hash(Types.NUMERIC, "100"));
        assertEquals(hash(Types.TIMESTAMP, "2023-11-01 10:00:00"), hash(Types.TIMESTAMP, "2023-11-01 10:00:00.0"));
        assertEquals(hash(Types.CHAR, "abc   "), hash(Types.CHAR, "abc"));
    }

    @Test
    public void differentValuesAndShapesHashApart() {
        assertNotEquals(hash(Types.REAL, "2000"), hash(Types.REAL, "2000.5"));
        assertNotEquals(hash(Types.NUMERIC, "100"), hash(Types.NUMERIC, "10"));
        // Text keeps its trailing spaces; only char drops its padding.
        assertNotEquals(hash(Types.VARCHAR, "abc "), hash(Types.VARCHAR, "abc"));
        // Null is not the empty string, and column boundaries count.
        assertNotEquals(hash(Types.VARCHAR, (String) null), hash(Types.VARCHAR, ""));
        assertNotEquals(hash(Types.VARCHAR, "ab", "c"), hash(Types.VARCHAR, "a", "bc"));
    }

    /**
     * Hashes the PG instrumentation (sibench_capture.c) computed for real HyBench rows, against
     * the text a JDBC client can read back for them, binary-transfer forms included.
     */
    @Test
    public void matchesTheServerEncoding() {
        assertEquals("cbf29ce484222325", ResultRowHash.of(List.of(), List.of()));
        assertEquals(
                "03309c217c7d10f6",
                ResultRowHash.of(
                        Arrays.asList("41514", "41514", "2000.0", "1", "2017-06-27 14:58:58.685", null),
                        List.of(Types.INTEGER, Types.INTEGER, Types.REAL, Types.INTEGER, Types.TIMESTAMP, Types.TIMESTAMP)));
        assertEquals(
                "9093cb8072a5aa66",
                ResultRowHash.of(
                        List.of("5", "300795", "male", "Guo Yihan", "34", "15894229212", "Hong_Kong", "Hong_Kong",
                                "10000.0", "0", "0", "0", "0", "2015-11-20", "2015-11-20 20:00:15.611"),
                        List.of(Types.INTEGER, Types.INTEGER, Types.CHAR, Types.CHAR, Types.INTEGER, Types.CHAR,
                                Types.CHAR, Types.CHAR, Types.REAL, Types.INTEGER, Types.INTEGER, Types.INTEGER,
                                Types.INTEGER, Types.DATE, Types.TIMESTAMP)));
        assertEquals(
                "29eb599383e2286f",
                ResultRowHash.of(List.of("research300003-350895717", "197.13"), List.of(Types.VARCHAR, Types.REAL)));
        assertEquals("89497709985320be", hash(Types.BIGINT, "39"));
    }
}
