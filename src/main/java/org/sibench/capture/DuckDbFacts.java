package org.sibench.capture;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

/** Typed, connection-local capture facts; DuckDB's registered log types own the message grammar. */
public final class DuckDbFacts {
    private static final String HEADER = "context_id,scope,connection_id,transaction_id,query_id,thread_id,timestamp,type,log_level,message";
    private static final Map<String, String> TYPES = Map.of(
            "query", "SibenchQuery", "transaction", "SibenchTransaction", "mutation", "SibenchMutation",
            "index", "SibenchIndex", "index_rows", "SibenchIndexRows", "consumption", "SibenchConsumption");

    private DuckDbFacts() {}

    /** Call after source drain and logging shutdown. No benchmark rows or persistent tables are copied. */
    public static Map<String, Long> load(Connection database, Path capture) throws IOException, SQLException {
        if (!database.getAutoCommit()) throw new SQLException("Fact import requires an idle auto-commit connection");
        try (var statement = database.createStatement();
                var rows = statement.executeQuery("SELECT current_setting('enable_logging')")) {
            if (!rows.next() || rows.getBoolean(1)) throw new SQLException("Disable capture logging before importing facts");
        }
        try (var input = Files.newBufferedReader(capture)) {
            if (!HEADER.equals(input.readLine())) throw new IOException("Unexpected DuckDB capture CSV header: " + capture);
        }
        var counts = new LinkedHashMap<String, Long>();
        database.setAutoCommit(false);
        try {
            // The pinned serial CSV scanner silently drops an unterminated last field; the parallel scanner rejects it.
            try (var query = database.prepareStatement("CREATE TEMP TABLE sibench_raw AS SELECT ordinality AS raw_record, "
                    + "* EXCLUDE(ordinality) FROM read_csv(?, header=true, auto_detect=false, parallel=true, "
                    + "delim=',', quote='\"', escape='\"', strict_mode=true, null_padding=false, columns={"
                    + "'context_id':'UBIGINT','scope':'VARCHAR','connection_id':'UBIGINT',"
                    + "'transaction_id':'UBIGINT','query_id':'UBIGINT','thread_id':'UBIGINT',"
                    + "'timestamp':'TIMESTAMPTZ','type':'VARCHAR','log_level':'VARCHAR','message':'VARCHAR'}) WITH ORDINALITY")) {
                query.setString(1, capture.toRealPath().toString()); query.execute();
            }
            // File order identifies evidence; it is not a transaction serialization order.
            reject(database, "SELECT 1 FROM sibench_raw WHERE scope IS DISTINCT FROM 'CONNECTION' "
                    + "OR log_level IS DISTINCT FROM 'INFO' OR type IS NULL OR type NOT IN ("
                    + TYPES.values().stream().map(t -> "'" + t + "'").collect(java.util.stream.Collectors.joining(","))
                    + ") LIMIT 1", "Unexpected capture scope, level or log type");
            for (String name : TYPES.keySet().stream().sorted().toList()) {
                String table = table(name);
                execute(database, "CREATE TEMP TABLE " + table + " AS SELECT raw_record, context_id, connection_id, "
                        + "transaction_id, query_id, thread_id, timestamp, "
                        + "UNNEST(parse_duckdb_log_message('" + TYPES.get(name) + "', message)) "
                        + "FROM sibench_raw WHERE type='" + TYPES.get(name) + "'");
                // A valid STRUCT cast can still fill missing fields with NULL; reject incomplete facts.
                try (var statement = database.createStatement(); var rows = statement.executeQuery(
                        "SELECT count(*), count(COLUMNS(* EXCLUDE(thread_id))) FROM " + table)) {
                    if (!rows.next()) throw new SQLException("Missing fact count: " + name);
                    long count = rows.getLong(1);
                    for (int column = 2; column <= rows.getMetaData().getColumnCount(); column++) {
                        if (rows.getLong(column) != count)
                            throw new SQLException("Missing required capture field in " + name + ": "
                                    + rows.getMetaData().getColumnName(column));
                    }
                    counts.put(name, count);
                }
            }
            execute(database, "DROP TABLE sibench_raw");
            database.commit();
            return Map.copyOf(counts);
        } catch (SQLException | IOException | RuntimeException failure) {
            try { database.rollback(); } catch (SQLException rollback) { failure.addSuppressed(rollback); }
            throw failure;
        } finally {
            database.setAutoCommit(true);
        }
    }

    public static String table(String kind) {
        if (!TYPES.containsKey(kind)) throw new IllegalArgumentException("Unknown capture fact kind: " + kind);
        return "temp.main.sibench_" + kind;
    }

    private static void execute(Connection database, String sql) throws SQLException {
        try (var statement = database.createStatement()) { statement.execute(sql); }
    }

    private static void reject(Connection database, String sql, String message) throws SQLException {
        try (var statement = database.createStatement(); var rows = statement.executeQuery(sql)) {
            if (rows.next()) throw new SQLException(message);
        }
    }
}
