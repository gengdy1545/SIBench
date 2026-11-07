package org.sibench.capture;

import static org.junit.Assert.*;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class DuckDbFactsTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test public void rejectsWrongHeadersAndAnActiveCallerTransaction() throws Exception {
        var file = temporary.newFile("capture.csv").toPath();
        try (var database = DriverManager.getConnection("jdbc:duckdb:")) {
            for (String header : new String[] {"", "message,type\n", "context_id,scope,connection_id\n"}) {
                Files.writeString(file, header);
                assertThrows(IOException.class, () -> DuckDbFacts.load(database, file));
                assertTrue(database.getAutoCommit());
            }
            database.setAutoCommit(false);
            assertThrows(SQLException.class, () -> DuckDbFacts.load(database, file));
            assertFalse(database.getAutoCommit());
            database.rollback();
        }
    }

    @Test public void sourceRuntimeParsesCaptureTypesAndRollsBackUnknownFacts() throws Exception {
        var file = temporary.newFile("capture.csv").toPath();
        String header = "context_id,scope,connection_id,transaction_id,query_id,thread_id,timestamp,type,log_level,message\n";
        Files.writeString(file, header + "1,CONNECTION,1,1,1,1,2026-01-01 00:00:00+00,Unknown,INFO,{}\n");
        try (var database = DriverManager.getConnection("jdbc:duckdb:")) {
            var error = assertThrows(SQLException.class, () -> DuckDbFacts.load(database, file));
            assertTrue(error.getMessage(), error.getMessage().contains("Unexpected capture scope, level or log type"));
            assertTrue(database.getAutoCommit());
            try (var statement = database.createStatement(); var rows = statement.executeQuery(
                    "SELECT count(*) FROM duckdb_tables() WHERE temporary AND table_name LIKE 'sibench_%'")) {
                assertTrue(rows.next()); assertEquals(0, rows.getInt(1));
            }
            Files.writeString(file, header);
            var counts = DuckDbFacts.load(database, file);
            assertEquals(java.util.Set.of("query", "transaction", "mutation", "index", "index_rows", "consumption"), counts.keySet());
            assertTrue(counts.values().stream().allMatch(count -> count == 0));
        }
    }
}
