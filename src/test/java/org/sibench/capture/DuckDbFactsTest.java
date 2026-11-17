package org.sibench.capture;

import static org.junit.Assert.*;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.SQLException;
import org.junit.Test;
import org.sibench.generation.DuckDbHostFixture;

public class DuckDbFactsTest extends DuckDbHostFixture {

    @Test public void rejectsWrongHeadersAndAnActiveCallerTransaction() throws Exception {
        var file = temporary.newFile("capture.csv").toPath();
        try (var database = connect()) {
            for (String header : new String[] {"", "message,type\n", "context_id,scope,connection_id\n"}) {
                Files.writeString(file, header);
                assertThrows(IOException.class, () -> DuckDbFacts.load(database, file,0));
                assertTrue(database.getAutoCommit());
            }
            database.setAutoCommit(false);
            assertThrows(SQLException.class, () -> DuckDbFacts.load(database, file,0));
            assertFalse(database.getAutoCommit());
            database.rollback();
        }
    }

    @Test public void sourceRuntimeRejectsMalformedFactsWithoutLeavingPartialTables() throws Exception {
        var file = temporary.newFile("capture.csv").toPath();
        String header = "context_id,scope,connection_id,transaction_id,query_id,thread_id,timestamp,type,log_level,message\n";
        try (var database = connect()) {
            for (String message : new String[] {
                    row("Unexpected", "anything"), row("SibenchTransaction", "{'event': begin}"),
                    row("SibenchTransaction", "{'engine_transaction': invalid}"),
                    "1,CONNECTION,1,1,1,,2026-11-04 00:00:00+00,SibenchQuery,INFO,\"unterminated",
                    "1,CONNECTION,1,1,1,,2026-11-04 00:00:00+00,SibenchQuery,INFO,\"unterminated\n"}) {
                Files.writeString(file, header + message);
                var error = assertThrows(message, SQLException.class, () -> DuckDbFacts.load(database, file,0));
                if (message.contains(",Unexpected,"))
                    assertTrue(error.getMessage(), error.getMessage().contains("Unexpected capture scope, level or log type"));
                assertTrue(database.getAutoCommit());
                try (var statement = database.createStatement(); var rows = statement.executeQuery(
                        "SELECT count(*) FROM duckdb_tables() WHERE temporary AND table_name LIKE 'sibench_%'")) {
                    assertTrue(rows.next()); assertEquals(0, rows.getInt(1));
                }
            }
            Files.writeString(file, header);
            var counts = DuckDbFacts.load(database, file,0);
            assertEquals(java.util.Set.of("query", "transaction", "mutation", "index", "index_rows"), counts.keySet());
            assertTrue(counts.values().stream().allMatch(count -> count == 0));
        }
    }

    private static String row(String type, String message) {
        return "1,CONNECTION,1,1,1,,2026-11-04 00:00:00+00," + type + ",INFO,\""
                + message.replace("\"", "\"\"") + "\"\n";
    }
}
