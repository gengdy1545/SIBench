package org.sibench.generation;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.sibench.config.Engine;

/**
 * RowID identities in the same native database as the benchmark's rows: the baseline mapping
 * and an identity per inserted row. Row contents of earlier versions are not retained.
 */
public final class NativeHistory {
    private static final String SCHEMA = "sibench_history";

    private NativeHistory() {}

    public static void initialize(Connection database, Engine engine) throws SQLException {
        requireTransaction(database);
        if (engine != Engine.DUCKDB && engine != Engine.POSTGRES) throw new IllegalArgumentException("SQL history requires a SQL engine");
        execute(database, "CREATE SCHEMA " + SCHEMA);
        execute(database, "CREATE SEQUENCE " + SCHEMA + ".row_ids START 1");
        if (engine == Engine.DUCKDB) {
            execute(database, "CREATE TABLE " + SCHEMA + ".tables(table_id INTEGER PRIMARY KEY, "
                    + "schema_name VARCHAR NOT NULL, table_name VARCHAR NOT NULL, primary_key VARCHAR NOT NULL, "
                    + "UNIQUE(schema_name, table_name))");
            return;
        }
        // PostgreSQL maps native commits to Trace versions in its projection; DuckDB binds per window.
        execute(database, "CREATE UNLOGGED TABLE " + SCHEMA + ".commits(transaction_id BIGINT PRIMARY KEY, "
                + "commit_version BIGINT UNIQUE NOT NULL CHECK(commit_version > 0))");
        execute(database,"CREATE FUNCTION "+SCHEMA+".query_id() RETURNS BIGINT AS '$libdir/row_capture', "
                +"'sibench_query_id' LANGUAGE C VOLATILE");
        execute(database,"CREATE FUNCTION "+SCHEMA+".row_change() RETURNS trigger AS '$libdir/row_capture', "
                +"'sibench_row_capture' LANGUAGE C");
        execute(database,"CREATE FUNCTION "+SCHEMA+".row_identity() RETURNS trigger AS '$libdir/row_capture', "
                +"'sibench_identity_capture' LANGUAGE C");
    }

    /** Install after loading the baseline; only its identity/key mapping is copied. The caller commits the DDL. */
    public static void install(Connection database, Engine engine, int tableId, String schema, String table, String primaryKey)
            throws SQLException {
        requireTransaction(database);
        if (tableId < 1 || tableId > 65535) throw new IllegalArgumentException("Invalid table ID");
        String source = quote(schema) + "." + quote(table);
        if (columns(database, source).stream().anyMatch(c -> c.toLowerCase(java.util.Locale.ROOT).startsWith("_sib_")))
            throw new SQLException("Source columns must not use the reserved _sib_ prefix");
        if (engine==Engine.POSTGRES) {
            PostgresHistory.install(database,tableId,schema,table,primaryKey);
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
                    throw new SQLException("RowID identities require the declared single-column primary key: " + source);
            }
        }
        try (var insert = database.prepareStatement("INSERT INTO " + SCHEMA + ".tables VALUES(?,?,?,?)")) {
            insert.setInt(1, tableId);
            insert.setString(2, schema);
            insert.setString(3, table);
            insert.setString(4, primaryKey);
            insert.executeUpdate();
        }
        String identities = identities(tableId);
        execute(database, "CREATE TABLE " + identities + " AS SELECT nextval('" + SCHEMA
                + ".row_ids') AS _sib_row_id, " + quote(primaryKey) + ", 0::UBIGINT AS _sib_tx, "
                + "0::UBIGINT AS _sib_query FROM " + source);
        execute(database, "CREATE UNIQUE INDEX identity_row_" + tableId + " ON " + identities + "(_sib_row_id)");
        execute(database, "CREATE INDEX identity_key_" + tableId + " ON " + identities + "(" + quote(primaryKey) + ")");
        execute(database, "CREATE TRIGGER " + quote("sibench_identity_" + tableId)
                + " AFTER INSERT ON " + source + " REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT "
                + "INSERT INTO " + identities + " SELECT nextval('" + SCHEMA + ".row_ids'), "
                + quote(primaryKey) + ", current_transaction_id(), current_query_id() FROM new_rows");
        // Comparing OLD/NEW key sets misses permutations; reject all key assignments for these fixed workloads.
        execute(database, "CREATE TRIGGER " + quote("sibench_primary_key_" + tableId)
                + " BEFORE UPDATE OF " + quote(primaryKey) + " ON " + source + " FOR EACH STATEMENT INSERT INTO "
                + SCHEMA + ".tables VALUES(error('Primary key assignments are unsupported by RowID identities'),NULL,NULL,NULL)");
    }

    private static List<String> columns(Connection database, String table) throws SQLException {
        try (var query = database.createStatement(); ResultSet rows = query.executeQuery("SELECT * FROM " + table + " LIMIT 0")) {
            var columns = new ArrayList<String>();
            for (int i = 1; i <= rows.getMetaData().getColumnCount(); i++) columns.add(rows.getMetaData().getColumnName(i));
            return columns;
        }
    }

    private static String identities(int id) { return SCHEMA + "." + quote("identities_" + id); }

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
