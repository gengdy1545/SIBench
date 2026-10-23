package org.sibench.capture;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Prepares the database a native HyBench run reads. The verified SF1 load is never measured
 * directly: it is cloned, the upstream secondary indexes are created on the clone, and the row
 * capture triggers are installed on it, so a run always starts from a still, known state.
 *
 * <p>Nothing here generates data. The still database is produced by the native load and must
 * already exist, exactly as the YCSB and sysbench clients must already be built.
 */
final class HybenchNativePrepare {
    private static final String LIBRARY = "build/services/postgresql-capture/row_capture.so";

    private HybenchNativePrepare() {}

    static void prepare(Path root, CaptureConfig config) throws Exception {
        Path library = root.resolve(LIBRARY);
        if (!Files.isRegularFile(library))
            throw new IllegalStateException("Row capture library is not built: " + library);
        String cloneFrom = config.cloneFrom();
        String password = System.getenv().getOrDefault("PGPASSWORD", "");
        try (Connection admin =
                DriverManager.getConnection(config.adminJdbc(), config.user(), password)) {
            try (Statement statement = admin.createStatement()) {
                statement.execute(
                        "CREATE DATABASE " + config.database() + " TEMPLATE " + cloneFrom);
            }
        }
        try (Connection connection =
                        DriverManager.getConnection(config.jdbc(), config.user(), password);
                Statement statement = connection.createStatement()) {
            for (String sql : statements(config.source().resolve("conf/create_index_pg.sql")))
                statement.execute(sql);
            statement.execute(
                    "CREATE FUNCTION public.sibench_row_capture() RETURNS trigger AS '"
                            + library.toString().replace("'", "''")
                            + "', 'sibench_row_capture' LANGUAGE C");
            for (var table : org.sibench.schema.SourceSchema.hybench(config.source()).tables())
                statement.execute(
                        "CREATE TRIGGER sibench_row_capture AFTER INSERT OR UPDATE OR DELETE ON \""
                                + table.name()
                                + "\" FOR EACH ROW EXECUTE FUNCTION public.sibench_row_capture()");
        }
    }

    /** The upstream index script: one statement per non-empty line, comments and blanks skipped. */
    static List<String> statements(Path script) throws Exception {
        var result = new ArrayList<String>();
        for (String line : Files.readAllLines(script)) {
            String sql = line.trim();
            if (sql.isEmpty() || sql.startsWith("--")) continue;
            result.add(sql.endsWith(";") ? sql.substring(0, sql.length() - 1) : sql);
        }
        if (result.isEmpty()) throw new IllegalStateException("Index script has no statements: " + script);
        return result;
    }
}
