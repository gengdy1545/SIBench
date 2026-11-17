package org.sibench.generation;

import java.sql.Connection;
import java.sql.SQLException;

/** The engine identity reported by the running DuckDB host. */
public final class DuckDbDatabase {
    private DuckDbDatabase() {}

    public record Identity(String version, String sourceId) {}

    /** Records the actually loaded engine, not just the requested build directory. */
    public static Identity identity(Connection connection) throws SQLException {
        try (var statement = connection.createStatement();
                var rows = statement.executeQuery("PRAGMA version")) {
            if (!rows.next()) throw new SQLException("DuckDB did not report its build identity");
            return new Identity(rows.getString("library_version"), rows.getString("source_id"));
        }
    }
}
