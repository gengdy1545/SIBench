package org.sibench.generation;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/** In-process DuckDB connections and the loaded engine identity. */
public final class DuckDbDatabase {
    private DuckDbDatabase() {}

    public record Identity(String version, String sourceId) {}

    /** Reserves a fresh run directory; never opens an existing database for mutation. */
    public static Connection create(Path directory) throws IOException, SQLException {
        Path root = directory.toAbsolutePath().normalize();
        Files.createDirectory(root);
        // Leave a failed creation in place for diagnosis instead of silently reusing its state.
        var properties = new Properties();
        // Use the pinned engine's native format, including persistent transition-table triggers.
        properties.setProperty("storage_compatibility_version", "latest");
        return DriverManager.getConnection("jdbc:duckdb:" + file(root), properties);
    }

    public static Path file(Path directory) {
        return directory.resolve("data.duckdb");
    }

    /** Records the actually loaded engine, not just the requested Maven dependency. */
    public static Identity identity(Connection connection) throws SQLException {
        try (var statement = connection.createStatement();
                var rows = statement.executeQuery("PRAGMA version")) {
            if (!rows.next()) throw new SQLException("DuckDB did not report its build identity");
            return new Identity(rows.getString("library_version"), rows.getString("source_id"));
        }
    }
}
