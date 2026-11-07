package org.sibench.capture;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/** Canonical resolution slots and snapshot read points derived from native transaction facts. */
public final class DuckDbTransactions {
    private DuckDbTransactions() {}

    /** Retain one fully drained capture window before admitting another window on the same database. */
    public static void retain(Connection database, boolean first) throws SQLException {
        retain(database, first, false);
    }

    private static void retain(Connection database, boolean first, boolean seal) throws SQLException {
        if (!database.getAutoCommit()) throw new SQLException("Publication requires an idle connection");
        try (var query=database.createStatement();var row=query.executeQuery("SELECT current_setting('enable_logging')")) {
            if (!row.next() || row.getBoolean(1)) throw new SQLException("Stop capture before retaining read points");
        }
        database.setAutoCommit(false);
        try {
            if(first) {
                execute(database,"CREATE TABLE sibench_history.transactions(transaction_id UBIGINT PRIMARY KEY, "
                        +"version BIGINT UNIQUE NOT NULL CHECK(version>0), read_version BIGINT NOT NULL "
                        +"CHECK(read_version>=0 AND read_version<version), outcome VARCHAR NOT NULL CHECK(outcome IN ('commit','rollback')), "
                        +"commit_stamp UBIGINT NOT NULL)");
                execute(database,"CREATE TABLE sibench_history.read_points(connection_id UBIGINT,query_id UBIGINT,"
                        +"transaction_id UBIGINT,local_writes BOOLEAN,PRIMARY KEY(connection_id,query_id))");
            }
            execute(database,"INSERT INTO sibench_history.transactions SELECT transaction_id,version,read_version,outcome,commit_stamp "
                    +"FROM temp.main.sibench_transactions");
            execute(database,"INSERT INTO sibench_history.read_points SELECT p.connection_id,p.query_id,p.transaction_id, "
                    +"EXISTS(SELECT 1 FROM temp.main.sibench_mutation_statements m "
                    +"JOIN sibench_history.tables h USING(schema_name,table_name) WHERE m.transaction_id=p.transaction_id "
                    +"AND m.query_id<p.query_id AND m.statement_outcome='completed' AND m.event='complete' "
                    +"AND m.affected_rows>0) FROM temp.main.sibench_read_points p");
            execute(database,"INSERT INTO sibench_history.commits SELECT transaction_id,version "
                    +"FROM temp.main.sibench_transactions WHERE outcome='commit'");
            if (seal) finishHistory(database);
            database.commit();
        } catch(SQLException|RuntimeException failure) {
            try {database.rollback();} catch(SQLException rollback) {failure.addSuppressed(rollback);} throw failure;
        } finally {database.setAutoCommit(true);}
    }

    public static void finish(Connection database) throws SQLException {
        database.setAutoCommit(false);
        try {
            finishHistory(database);
            database.commit();
        } catch(SQLException|RuntimeException failure) {
            try {database.rollback();} catch(SQLException rollback) {failure.addSuppressed(rollback);} throw failure;
        } finally {database.setAutoCommit(true);}
    }

    private static void finishHistory(Connection database) throws SQLException {
            reject(database,"SELECT 1 FROM sibench_history.transactions HAVING coalesce(max(version),0)<>count(*)",
                    "Incomplete native resolution sequence");
            org.sibench.generation.NativeHistory.verifyCommits(database);
            org.sibench.generation.NativeHistory.seal(database);
    }

    public static void seal(Connection database) throws SQLException {
        retain(database,true,true);
    }

    /** Bind statements first. Empty statements without a native storage transaction need no replay transaction. */
    public static void bind(Connection database, String databaseName) throws SQLException {
        bind(database,databaseName,0);
    }

    public static void bind(Connection database, String databaseName, long prefix) throws SQLException {
        if(prefix<0)throw new IllegalArgumentException("Negative resolved prefix");
        if (!database.getAutoCommit()) throw new SQLException("Transaction binding requires an idle connection");
        if (databaseName == null || databaseName.isEmpty()) throw new IllegalArgumentException("Source database is required");
        database.setAutoCommit(false);
        try {
            execute(database, "CREATE TEMP VIEW sibench_source_transactions AS SELECT t.* FROM "
                    + DuckDbFacts.table("transaction") + " t SEMI JOIN temp.main.sibench_statements s "
                    + "USING(connection_id, transaction_id)");
            try (var query = database.prepareStatement("SELECT 1 FROM temp.main.sibench_source_transactions "
                    + "WHERE database_name<>? LIMIT 1")) {
                query.setString(1, databaseName);
                try (var rows = query.executeQuery()) {
                    if (rows.next()) throw new SQLException("Source transaction spans an unexpected database");
                }
            }
            reject(database, "SELECT 1 FROM temp.main.sibench_source_transactions WHERE "
                    + "event NOT IN ('begin','commit','rollback') OR engine_transaction=0 OR snapshot_start=0 "
                    + "OR (event='begin' AND (commit_stamp<>0 OR resolution_order<>0)) "
                    + "OR (event<>'begin' AND resolution_order=0) "
                    + "OR (event='commit' AND commit_stamp<=snapshot_start) "
                    + "OR (event='rollback' AND commit_stamp<>0) LIMIT 1", "Invalid native transaction event");
            execute(database, "CREATE TEMP TABLE sibench_transaction_pairs AS SELECT connection_id, transaction_id, "
                    + "min(engine_transaction) AS engine_transaction, min(snapshot_start) AS snapshot_start, "
                    + "max(commit_stamp) AS commit_stamp, max(resolution_order) AS resolution_order, "
                    + "any_value(event) FILTER(WHERE event<>'begin') AS outcome, "
                    + "count(*) FILTER(WHERE event='begin')=1 AND count(*) FILTER(WHERE event<>'begin')=1 "
                    + "AND min(engine_transaction)=max(engine_transaction) "
                    + "AND min(snapshot_start)=max(snapshot_start) AS paired "
                    + "FROM temp.main.sibench_source_transactions GROUP BY connection_id, transaction_id");
            reject(database, "SELECT 1 FROM temp.main.sibench_transaction_pairs WHERE NOT paired LIMIT 1",
                    "Missing, duplicate or inconsistent native transaction boundary");
            for (String column : List.of("transaction_id", "engine_transaction", "resolution_order")) {
                reject(database, "SELECT 1 FROM temp.main.sibench_transaction_pairs GROUP BY " + column
                        + " HAVING count(*)<>1 LIMIT 1", "Duplicate native transaction " + column);
            }
            reject(database, "SELECT 1 FROM (SELECT commit_stamp, lag(commit_stamp) OVER(ORDER BY resolution_order) "
                    + "AS previous FROM temp.main.sibench_transaction_pairs WHERE outcome='commit') "
                    + "WHERE commit_stamp<=previous LIMIT 1", "Commit stamps disagree with native resolution order");
            if (prefix > 0) {
                reject(database, "SELECT 1 FROM sibench_history.transactions HAVING max(version)<>" + prefix,
                        "Capture window disagrees with retained resolution prefix");
                reject(database, "SELECT 1 FROM temp.main.sibench_transaction_pairs WHERE snapshot_start<="
                        + "(SELECT max(commit_stamp) FROM sibench_history.transactions) LIMIT 1",
                        "Capture windows overlap native snapshots");
            }
            execute(database, "CREATE TEMP VIEW sibench_transaction_versions AS SELECT * EXCLUDE(paired), "
                    + "" + prefix + "+row_number() OVER(ORDER BY resolution_order) AS version FROM temp.main.sibench_transaction_pairs");
            // DuckDB sees commits strictly before its native snapshot start, plus its own local writes.
            String previous=prefix==0?"":" UNION ALL SELECT commit_stamp,version FROM sibench_history.transactions WHERE outcome='commit'";
            execute(database, "CREATE TEMP TABLE sibench_transactions AS SELECT t.*, coalesce(c.version,0)::BIGINT "
                    + "AS read_version FROM temp.main.sibench_transaction_versions t ASOF LEFT JOIN "
                    + "(SELECT commit_stamp, version FROM temp.main.sibench_transaction_versions WHERE outcome='commit'"+previous+") c "
                    + "ON t.snapshot_start>c.commit_stamp");
            reject(database, "SELECT 1 FROM temp.main.sibench_transactions WHERE read_version>=version LIMIT 1",
                    "Native snapshot includes its own or a later resolution");
            for (String kind : List.of("mutation", "index")) {
                reject(database, "SELECT 1 FROM " + DuckDbFacts.table(kind) + " f "
                        + "JOIN temp.main.sibench_statements s USING(connection_id, transaction_id, query_id) "
                        + "LEFT JOIN temp.main.sibench_transactions t USING(connection_id, transaction_id) "
                        + "WHERE t.transaction_id IS NULL LIMIT 1", "Missing native transaction for " + kind);
            }
            reject(database, "SELECT 1 FROM " + DuckDbFacts.table("index") + " f "
                    + "JOIN temp.main.sibench_statements s USING(connection_id, transaction_id, query_id) "
                    + "JOIN temp.main.sibench_transactions t USING(connection_id, transaction_id) "
                    + "WHERE f.snapshot_start<>t.snapshot_start LIMIT 1", "Index access disagrees with native snapshot");
            execute(database, "CREATE TEMP VIEW sibench_read_points AS SELECT s.*, t.engine_transaction, t.version, "
                    + "t.read_version, t.snapshot_start, t.outcome AS transaction_outcome "
                    + "FROM temp.main.sibench_statements s JOIN temp.main.sibench_transactions t "
                    + "USING(connection_id, transaction_id)");
            database.commit();
        } catch (SQLException | RuntimeException failure) {
            try { database.rollback(); } catch (SQLException rollback) { failure.addSuppressed(rollback); }
            throw failure;
        } finally { database.setAutoCommit(true); }
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
