package org.sibench.capture;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/** Resolves observed keys to the RowIDs allocated by native history triggers, including aborted inserts. */
public final class DuckDbIdentities {
    private record Table(int id, String key, String type) {
        String identities() { return "sibench_history.\"identities_" + id + "\""; }
    }
    private DuckDbIdentities() {}

    /** Import and bind statements/transactions first, on the same database that owns the native history. */
    public static void bind(Connection database) throws SQLException {
        if (!database.getAutoCommit()) throw new SQLException("Identity binding requires an idle connection");
        var tables = new ArrayList<Table>();
        try (var query = database.createStatement(); var rows = query.executeQuery(
                "SELECT t.table_id,t.primary_key,c.data_type FROM sibench_history.tables t "
                + "LEFT JOIN duckdb_columns() c ON c.database_name=current_database() AND c.schema_name=t.schema_name "
                + "AND c.table_name=t.table_name AND c.column_name=t.primary_key ORDER BY t.table_id")) {
            while (rows.next()) {
                var table = new Table(rows.getInt(1), rows.getString(2), rows.getString(3));
                if (table.id()<1 || table.id()>65535 || table.type()==null
                        || !List.of("TINYINT","SMALLINT","INTEGER","BIGINT","VARCHAR").contains(table.type()))
                    throw new SQLException("Unsupported native identity table or primary key type: " + table);
                tables.add(table);
            }
        }
        if (tables.isEmpty()) throw new SQLException("Native history has no source tables");
        database.setAutoCommit(false);
        try {
            execute(database, "CREATE TEMP TABLE sibench_insert_identities(table_id INTEGER NOT NULL, "
                    + "primary_key_sql VARCHAR NOT NULL, row_id BIGINT PRIMARY KEY CHECK(row_id>0), "
                    + "transaction_id UBIGINT NOT NULL, query_id UBIGINT NOT NULL, version BIGINT NOT NULL, "
                    + "outcome VARCHAR NOT NULL, UNIQUE(table_id,primary_key_sql,transaction_id,query_id))");
            var baseline = new ArrayList<String>();
            boolean retained;
            try(var q=database.createStatement();var r=q.executeQuery("SELECT count(*) FROM duckdb_tables() WHERE database_name=current_database() AND schema_name='sibench_history' AND table_name='transactions'")) { r.next();retained=r.getLong(1)==1; }
            for (Table table : tables) {
                String key = keyExpression(quote(table.key()), table.type());
                baseline.add("SELECT " + table.id() + " AS table_id," + key + " AS primary_key_sql, "
                        + "_sib_row_id AS row_id, _sib_tx AS transaction_id, _sib_query AS query_id, "
                        + "0::BIGINT AS version,'commit' AS outcome FROM " + table.identities() + " WHERE _sib_tx=0");
                if(retained) baseline.add("SELECT "+table.id()+", "+key+",i._sib_row_id,i._sib_tx,i._sib_query,t.version,t.outcome FROM "
                        +table.identities()+" i JOIN sibench_history.transactions t ON i._sib_tx=t.transaction_id WHERE t.outcome='commit'");
                importTable(database, table, key);
            }
            for (Table table : tables) {
                reject(database, "SELECT 1 FROM temp.main.sibench_insert_identities i JOIN " + table.identities()
                        + " s ON i.row_id=s._sib_row_id WHERE i.outcome='rollback' LIMIT 1",
                        "Aborted RowID collides with persistent native identity");
            }
            execute(database, "CREATE TEMP VIEW sibench_identities AS " + String.join(" UNION ALL ", baseline)
                    + " UNION ALL SELECT * FROM temp.main.sibench_insert_identities");
            database.commit();
        } catch (SQLException | RuntimeException failure) {
            try { database.rollback(); } catch (SQLException rollback) { failure.addSuppressed(rollback); }
            throw failure;
        } finally { database.setAutoCommit(true); }
    }

    private static void importTable(Connection database, Table table, String key) throws SQLException {
        execute(database, "CREATE TEMP TABLE sibench_identity_events AS SELECT m.* FROM temp.main.sibench_mutation_statements m "
                + "WHERE schema_name='sibench_history' AND table_name='identities_" + table.id()
                + "' AND statement_outcome='completed'");
        String columns = "len(columns)=2 AND columns[1].name='_sib_row_id' AND columns[1].type='BIGINT' "
                + "AND columns[2].name=" + textKey(table.key()) + " AND columns[2].type=" + textKey(table.type());
        reject(database, "SELECT 1 FROM sibench_identity_events WHERE kind NOT IN ('append','merge') "
                + "OR event NOT IN ('begin','rows','complete') OR NOT coalesce(" + columns + ",false) "
                + "OR (event='complete' AND (len(rows)<>0 OR affected_rows<>requested_rows)) "
                + "OR (event<>'complete' AND affected_rows<>-1) OR (kind='append' AND event='rows') "
                + "OR (kind='merge' AND event='begin' AND len(rows)<>0) LIMIT 1",
                "Unexpected native identity mutation or columns");
        reject(database, "SELECT 1 FROM sibench_identity_events GROUP BY mutation_id HAVING "
                + "count(*) FILTER(WHERE event='begin')<>1 OR count(*) FILTER(WHERE event='complete')<>1 "
                + "OR min(connection_id)<>max(connection_id) OR min(transaction_id)<>max(transaction_id) "
                + "OR min(query_id)<>max(query_id) OR min(kind)<>max(kind) "
                + "OR min(requested_rows)<>max(requested_rows) OR max(requested_rows)=0 "
                + "OR max(affected_rows)<>max(requested_rows) OR sum(len(rows))<>max(affected_rows) LIMIT 1",
                "Incomplete or inconsistent native identity scope");
        execute(database, "CREATE TEMP TABLE sibench_identity_rows AS SELECT e.transaction_id,e.query_id, "
                + "t.version,t.outcome,unnest(e.rows) AS r FROM sibench_identity_events e "
                + "JOIN temp.main.sibench_transactions t USING(connection_id,transaction_id)");
        String validKey = table.type().equals("VARCHAR")
                ? "(NOT contains(r.values_sql[2],chr(0)) AND regexp_full_match(r.values_sql[2],'''([^'']|'''')*'''))"
                : "CAST(try_cast(r.values_sql[2] AS " + table.type() + ") AS VARCHAR)=r.values_sql[2]";
        reject(database, "SELECT 1 FROM sibench_identity_rows WHERE NOT coalesce(len(r.values_sql)=2 "
                + "AND try_cast(r.values_sql[1] AS BIGINT)>0 AND " + validKey + ",false) LIMIT 1",
                "Invalid native identity row");
        execute(database, "INSERT INTO temp.main.sibench_insert_identities SELECT " + table.id()
                + ",r.values_sql[2],CAST(r.values_sql[1] AS BIGINT),transaction_id,query_id,version,outcome "
                + "FROM sibench_identity_rows");
        // Reconcile committed identities against native state; aborted identities must remain absent there.
        reject(database, "SELECT 1 FROM (SELECT _sib_row_id AS row_id,_sib_tx AS transaction_id,"
                + "_sib_query AS query_id," + key + " AS primary_key_sql FROM " + table.identities()
                + " WHERE _sib_tx IN (SELECT transaction_id FROM temp.main.sibench_transactions)) stored FULL OUTER JOIN (SELECT * FROM temp.main.sibench_insert_identities "
                + "WHERE table_id=" + table.id() + " AND outcome='commit') captured USING(row_id) "
                + "WHERE stored.row_id IS NULL OR captured.row_id IS NULL "
                + "OR stored.transaction_id<>captured.transaction_id OR stored.query_id<>captured.query_id "
                + "OR stored.primary_key_sql<>captured.primary_key_sql LIMIT 1",
                "Captured committed identities disagree with native history");
        execute(database, "DROP TABLE sibench_identity_rows");
        execute(database, "DROP TABLE sibench_identity_events");
    }

    /** The source already observed this key. This lookup does not infer whether a row exists after a delete. */
    public static long observedRow(Connection database, int table, String primaryKeySql, long readVersion,
            long localVersion, long beforeQuery) throws SQLException {
        if (table<1 || primaryKeySql==null || readVersion<0 || localVersion<0 || beforeQuery<0
                || localVersion==0 && beforeQuery!=0 || localVersion>0 && localVersion<=readVersion)
            throw new IllegalArgumentException("Invalid observed-row read point");
        try (var query = database.prepareStatement("SELECT row_id FROM temp.main.sibench_identities "
                + "WHERE table_id=? AND primary_key_sql=? AND ((outcome='commit' AND version<=?) "
                + "OR (version=? AND query_id<?)) ORDER BY (version=? AND version>0) DESC,version DESC,query_id DESC LIMIT 1")) {
            query.setInt(1,table); query.setString(2,primaryKeySql); query.setLong(3,readVersion);
            query.setLong(4,localVersion); query.setLong(5,beforeQuery); query.setLong(6,localVersion);
            try (var rows = query.executeQuery()) {
                if (!rows.next()) throw new SQLException("No native identity for observed key in table " + table);
                long rowId = rows.getLong(1); rows.next(); return rowId;
            }
        }
    }

    /** Canonical VARCHAR token used by the pinned native Value::ToSQLString; never executed as SQL. */
    public static String textKey(String value) {
        if (value.indexOf('\0')>=0) throw new IllegalArgumentException("NUL is unsupported in index keys");
        return "'" + value.replace("'", "''") + "'";
    }

    private static String keyExpression(String column, String type) {
        if (!type.equals("VARCHAR")) return "CAST(" + column + " AS VARCHAR)";
        return "CASE WHEN contains(" + column + ",chr(0)) THEN error('NUL is unsupported in index keys') ELSE chr(39)||replace(" + column
                + ",chr(39),chr(39)||chr(39))||chr(39) END";
    }

    private static String quote(String name) { return "\"" + name.replace("\"", "\"\"") + "\""; }
    private static void execute(Connection database, String sql) throws SQLException {
        try (var statement = database.createStatement()) { statement.execute(sql); }
    }
    private static void reject(Connection database, String sql, String message) throws SQLException {
        try (var statement = database.createStatement(); var rows = statement.executeQuery(sql)) {
            if (rows.next()) throw new SQLException(message);
        }
    }
}
