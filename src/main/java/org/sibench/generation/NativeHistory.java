package org.sibench.generation;

import java.sql.Connection;
import org.sibench.config.Engine;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/** Transactional before-images in the same native database as the benchmark's current rows. */
public final class NativeHistory {
    private static final String SCHEMA = "sibench_history";

    private NativeHistory() {}

    public static void initialize(Connection database, Engine engine) throws SQLException {
        requireTransaction(database);
        if (engine != Engine.DUCKDB && engine != Engine.POSTGRES) throw new IllegalArgumentException("SQL history requires a SQL engine");
        String createTable = engine == Engine.POSTGRES ? "CREATE UNLOGGED TABLE " : "CREATE TABLE ";
        execute(database, "CREATE SCHEMA " + SCHEMA);
        execute(database, createTable + SCHEMA + ".state AS SELECT false AS complete");
        execute(database, "CREATE SEQUENCE " + SCHEMA + ".row_ids START 1");
        execute(database, createTable + SCHEMA + ".tables(table_id INTEGER PRIMARY KEY, "
                + "schema_name VARCHAR NOT NULL, table_name VARCHAR NOT NULL, primary_key VARCHAR NOT NULL, "
                + "UNIQUE(schema_name, table_name))");
        execute(database, createTable + SCHEMA + ".commits(transaction_id " + (engine==Engine.DUCKDB?"UBIGINT":"BIGINT") + " PRIMARY KEY, "
                + "commit_version BIGINT UNIQUE NOT NULL CHECK(commit_version > 0))");
        if(engine==Engine.POSTGRES) {
            execute(database,"CREATE FUNCTION "+SCHEMA+".query_id() RETURNS BIGINT AS '$libdir/row_capture', "
                    +"'sibench_query_id' LANGUAGE C VOLATILE");
            execute(database,"CREATE FUNCTION "+SCHEMA+".row_change() RETURNS trigger AS '$libdir/row_capture', "
                    +"'sibench_row_capture' LANGUAGE C");
            execute(database,"CREATE FUNCTION "+SCHEMA+".row_identity() RETURNS trigger AS '$libdir/row_capture', "
                    +"'sibench_identity_capture' LANGUAGE C");
        }
    }

    /** Install after loading the baseline; only its identity/key mapping is copied. The caller commits the DDL. */
    public static void install(Connection database, Engine engine, int tableId, String schema, String table, String primaryKey)
            throws SQLException {
        requireTransaction(database);
        requireOpen(database);
        if (tableId < 1 || tableId > 65535) throw new IllegalArgumentException("Invalid table ID");
        String source = quote(schema) + "." + quote(table);
        List<String> columns = columns(database, source);
        if (columns.stream().anyMatch(c -> c.toLowerCase(java.util.Locale.ROOT).startsWith("_sib_")))
            throw new SQLException("Source columns must not use the reserved _sib_ prefix");
        if (engine==Engine.POSTGRES) {
            PostgresHistory.install(database,tableId,schema,table,primaryKey,columns);
            return;
        }
        try (var query = database.prepareStatement("SELECT count(*) FROM duckdb_constraints() "
                + "WHERE database_name=current_database() AND schema_name=? AND table_name=? "
                + "AND constraint_type='PRIMARY KEY' AND constraint_column_names=[?]")) {
            query.setString(1, schema);
            query.setString(2, table);
            query.setString(3, primaryKey);
            try (var rows = query.executeQuery()) {
                if (!rows.next() || rows.getLong(1) != 1)
                    throw new SQLException("History requires the declared single-column primary key: " + source);
            }
        }
        try (var insert = database.prepareStatement("INSERT INTO " + SCHEMA + ".tables VALUES(?,?,?,?)")) {
            insert.setInt(1, tableId);
            insert.setString(2, schema);
            insert.setString(3, table);
            insert.setString(4, primaryKey);
            insert.executeUpdate();
        }
        String history = history(tableId);
        execute(database, "CREATE TABLE " + history + " AS SELECT *, 0::UBIGINT AS _sib_tx, "
                + "0::UBIGINT AS _sib_query, ''::VARCHAR AS _sib_kind FROM " + source + " WHERE false");
        execute(database, "CREATE INDEX history_key_" + tableId + " ON " + history + "(" + quote(primaryKey) + ")");
        execute(database, "CREATE UNIQUE INDEX history_event_" + tableId + " ON " + history
                + "(_sib_tx, _sib_query, " + quote(primaryKey) + ")");
        execute(database, "CREATE TABLE " + identities(tableId) + " AS SELECT nextval('" + SCHEMA
                + ".row_ids') AS _sib_row_id, " + quote(primaryKey) + ", 0::UBIGINT AS _sib_tx, "
                + "0::UBIGINT AS _sib_query FROM " + source);
        execute(database, "CREATE UNIQUE INDEX identity_row_" + tableId + " ON " + identities(tableId) + "(_sib_row_id)");
        execute(database, "CREATE INDEX identity_key_" + tableId + " ON " + identities(tableId) + "(" + quote(primaryKey) + ")");

        String metadata = "current_transaction_id(), current_query_id(), ";
        String insertColumns = columns.stream().map(c -> c.equals(primaryKey) ? quote(c) : "NULL")
                .collect(Collectors.joining(", "));
        execute(database, "CREATE TRIGGER " + quote("sibench_insert_" + tableId)
                + " AFTER INSERT ON " + source + " REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT "
                + "INSERT INTO " + history + " SELECT " + insertColumns + ", " + metadata + writable("'I'") + " FROM new_rows");
        execute(database, "CREATE TRIGGER " + quote("sibench_identity_" + tableId)
                + " AFTER INSERT ON " + source + " REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT "
                + "INSERT INTO " + identities(tableId) + " SELECT nextval('" + SCHEMA + ".row_ids'), "
                + quote(primaryKey) + ", current_transaction_id(), current_query_id() FROM new_rows");
        execute(database, "CREATE TRIGGER " + quote("sibench_delete_" + tableId)
                + " AFTER DELETE ON " + source + " REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT "
                + "INSERT INTO " + history + " SELECT *, " + metadata + writable("'D'") + " FROM old_rows");
        // Comparing OLD/NEW key sets misses permutations; reject all key assignments for these fixed workloads.
        execute(database, "CREATE TRIGGER " + quote("sibench_primary_key_" + tableId)
                + " BEFORE UPDATE OF " + quote(primaryKey) + " ON " + source + " FOR EACH STATEMENT INSERT INTO "
                + SCHEMA + ".state VALUES(error('Primary key assignments are unsupported by row history'))");
        execute(database, "CREATE TRIGGER " + quote("sibench_update_" + tableId)
                + " AFTER UPDATE ON " + source + " REFERENCING OLD TABLE AS old_rows "
                + "FOR EACH STATEMENT INSERT INTO " + history + " SELECT old_rows.*, " + metadata
                + writable("'U'") + " FROM old_rows");
    }

    /** The capture compiler supplies the actual committed transaction's canonical version. */
    public static void recordCommit(Connection database, long transaction, long version) throws SQLException {
        if (transaction <= 0 || version <= 0) throw new IllegalArgumentException("Invalid commit identity");
        requireOpen(database);
        try (var insert = database.prepareStatement("INSERT INTO " + SCHEMA + ".commits VALUES(?,?)")) {
            insert.setLong(1, transaction);
            insert.setLong(2, version);
            insert.executeUpdate();
        }
    }

    /** Called after the generator has drained and reconciled its source completion counters. */
    public static void seal(Connection database) throws SQLException {
        requireTransaction(database);
        requireOpen(database);
        execute(database, "UPDATE " + SCHEMA + ".state SET complete=true");
    }

    /** Independent acceptance scan; normal generation establishes completeness at the capture boundary. */
    public static void verifyCommits(Connection database) throws SQLException {
        for (int id : tableIds(database)) {
            try (var query = database.createStatement(); var rows = query.executeQuery("SELECT count(*) FROM "
                    + history(id) + " h WHERE NOT EXISTS(SELECT 1 FROM " + SCHEMA + ".commits c WHERE h._sib_tx=c.transaction_id)")) {
                if (!rows.next() || rows.getLong(1) != 0)
                    throw new SQLException("Missing committed transaction facts for history table " + id);
            }
        }
    }

    /** A deleted/reinserted key gets a new RowID; an old identity can never resolve to its replacement. */
    public static Optional<Map<String, Object>> readRow(Connection database, int tableId, long rowId,
            long readVersion, long localTransaction, long precedingQuery) throws SQLException {
        checkReadPoint(readVersion, localTransaction, precedingQuery);
        requireSealed(database);
        String key;
        try (var query = database.prepareStatement("SELECT primary_key FROM " + SCHEMA + ".tables WHERE table_id=?")) {
            query.setInt(1, tableId);
            try (var rows = query.executeQuery()) {
                if (!rows.next()) throw new SQLException("Unknown history table: " + tableId);
                key = quote(rows.getString(1));
            }
        }
        Object primaryKey;
        try (var query = database.prepareStatement("SELECT " + key + " FROM " + identities(tableId) + " WHERE _sib_row_id=?")) {
            query.setLong(1, rowId);
            try (var rows = query.executeQuery()) {
                if (!rows.next()) return Optional.empty();
                primaryKey = rows.getObject(1);
            }
        }
        String sql = "SELECT i._sib_row_id FROM " + identities(tableId) + " i LEFT JOIN " + SCHEMA
                + ".commits c ON i._sib_tx=c.transaction_id WHERE " + key + "=? AND (i._sib_tx=0 "
                + "OR c.commit_version<=? OR (i._sib_tx=? AND i._sib_query<=?)) "
                + "ORDER BY coalesce(c.commit_version, 0) DESC, i._sib_query DESC LIMIT 1";
        try (var query = database.prepareStatement(sql)) {
            query.setObject(1, primaryKey);
            query.setLong(2, readVersion);
            query.setLong(3, localTransaction);
            query.setLong(4, precedingQuery);
            try (var rows = query.executeQuery()) {
                if (!rows.next() || rows.getLong(1) != rowId) return Optional.empty();
            }
        }
        return read(database, tableId, primaryKey, readVersion, localTransaction, precedingQuery);
    }

    static Optional<Map<String, Object>> read(Connection database, int tableId, Object primaryKey,
            long readVersion, long localTransaction, long precedingQuery) throws SQLException {
        checkReadPoint(readVersion, localTransaction, precedingQuery);
        requireSealed(database);
        String source, key;
        try (var query = database.prepareStatement("SELECT schema_name, table_name, primary_key FROM "
                + SCHEMA + ".tables WHERE table_id=?")) {
            query.setInt(1, tableId);
            try (var rows = query.executeQuery()) {
                if (!rows.next()) throw new SQLException("Unknown history table: " + tableId);
                source = quote(rows.getString(1)) + "." + quote(rows.getString(2));
                key = quote(rows.getString(3));
            }
        }
        List<String> names = columns(database, source);
        String fields = names.stream().map(NativeHistory::quote).collect(Collectors.joining(", "));
        // The first mutation after the requested state carries its before-image; an insert proves prior absence.
        String sql = "WITH events AS (SELECT h.*, c.commit_version FROM " + history(tableId) + " h JOIN "
                + SCHEMA + ".commits c ON h._sib_tx=c.transaction_id WHERE " + key + "=?), "
                + "visible AS (SELECT commit_version, _sib_query FROM events WHERE commit_version<=? "
                + "OR (_sib_tx=? AND _sib_query<=?) ORDER BY commit_version DESC, _sib_query DESC LIMIT 1) "
                + "SELECT " + fields + ", _sib_kind FROM events WHERE NOT EXISTS(SELECT 1 FROM visible v WHERE "
                + "events.commit_version<v.commit_version OR (events.commit_version=v.commit_version AND events._sib_query<=v._sib_query)) "
                + "ORDER BY commit_version, _sib_query LIMIT 1";
        try (var query = database.prepareStatement(sql)) {
            query.setObject(1, primaryKey);
            query.setLong(2, readVersion);
            query.setLong(3, localTransaction);
            query.setLong(4, precedingQuery);
            try (var rows = query.executeQuery()) {
                if (rows.next()) return rows.getString(names.size() + 1).equals("I")
                        ? Optional.empty() : Optional.of(row(rows, names));
            }
        }
        try (var query = database.prepareStatement("SELECT " + fields + " FROM " + source + " WHERE " + key + "=?")) {
            query.setObject(1, primaryKey);
            try (var rows = query.executeQuery()) {
                return rows.next() ? Optional.of(row(rows, names)) : Optional.empty();
            }
        }
    }

    private static Map<String, Object> row(ResultSet row, List<String> columns) throws SQLException {
        var result = new LinkedHashMap<String, Object>();
        for (int i = 0; i < columns.size(); i++) {
            Object value = row.getObject(i + 1);
            // Returned rows outlive the JDBC result and reader connection.
            result.put(columns.get(i), value instanceof java.sql.Blob ? row.getBytes(i + 1) : value);
        }
        return Collections.unmodifiableMap(result);
    }

    private static List<String> columns(Connection database, String table) throws SQLException {
        try (var query = database.createStatement(); var rows = query.executeQuery("SELECT * FROM " + table + " LIMIT 0")) {
            var columns = new ArrayList<String>();
            for (int i = 1; i <= rows.getMetaData().getColumnCount(); i++) columns.add(rows.getMetaData().getColumnName(i));
            return columns;
        }
    }

    private static List<Integer> tableIds(Connection database) throws SQLException {
        var ids = new ArrayList<Integer>();
        try (var query = database.createStatement(); var rows = query.executeQuery("SELECT table_id FROM " + SCHEMA + ".tables")) {
            while (rows.next()) ids.add(rows.getInt(1));
        }
        return ids;
    }

    private static String history(int id) { return SCHEMA + "." + quote("rows_" + id); }
    private static String identities(int id) { return SCHEMA + "." + quote("identities_" + id); }

    private static void checkReadPoint(long readVersion, long localTransaction, long precedingQuery) {
        if (readVersion < 0 || localTransaction < 0 || precedingQuery < 0
                || localTransaction == 0 && precedingQuery != 0)
            throw new IllegalArgumentException("Invalid history read point");
    }

    private static void requireSealed(Connection database) throws SQLException {
        try (var query = database.createStatement(); var rows = query.executeQuery("SELECT complete FROM " + SCHEMA + ".state")) {
            if (!rows.next() || !rows.getBoolean(1) || rows.next()) throw new SQLException("History is not sealed");
        }
    }

    private static String writable(String kind) {
        return "CASE WHEN (SELECT complete FROM " + SCHEMA + ".state) THEN error('Row history is sealed') ELSE "
                + kind + " END";
    }

    private static void requireOpen(Connection database) throws SQLException {
        try (var query = database.createStatement(); var rows = query.executeQuery("SELECT complete FROM " + SCHEMA + ".state")) {
            if (!rows.next() || rows.getBoolean(1) || rows.next()) throw new SQLException("History is not open");
        }
    }

    private static String quote(String name) {
        if (name == null || name.isEmpty() || name.indexOf('\0') >= 0) throw new IllegalArgumentException("Invalid identifier");
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }

    private static void requireTransaction(Connection database) throws SQLException {
        if (database.getAutoCommit()) throw new SQLException("History DDL requires an explicit transaction");
    }

    private static void execute(Connection database, String sql) throws SQLException {
        try (var statement = database.createStatement()) { statement.execute(sql); }
    }
}
