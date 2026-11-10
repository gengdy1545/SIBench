package org.sibench.capture;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/** Joins collected facts to the complete statements admitted by native client receipts. */
public final class DuckDbStatements {
    public record Session(long connection, long afterQuery, long submissions) {
        public Session {
            if (connection < 0 || afterQuery < 0 || submissions < 0)
                throw new IllegalArgumentException("Negative native client receipt identity or count");
        }
    }

    private DuckDbStatements() {}

    /** Import facts first. This validates statement coverage, not index-access or transaction completeness. */
    public static void bind(Connection database, List<Session> sessions) throws SQLException {
        if (!database.getAutoCommit()) throw new SQLException("Statement binding requires an idle connection");
        if (sessions.isEmpty()) throw new IllegalArgumentException("Source client receipts are required");
        database.setAutoCommit(false);
        try {
            execute(database, "CREATE TEMP TABLE sibench_sessions(connection_id UBIGINT PRIMARY KEY, "
                    + "after_query UBIGINT NOT NULL, submissions UBIGINT NOT NULL)");
            try (var insert = database.prepareStatement("INSERT INTO temp.main.sibench_sessions VALUES(?,?,?)")) {
                for (Session session : sessions) {
                    insert.setLong(1, session.connection()); insert.setLong(2, session.afterQuery());
                    insert.setLong(3, session.submissions()); insert.executeUpdate();
                }
            }
            execute(database, "CREATE TEMP VIEW sibench_source_queries AS SELECT q.* FROM " + DuckDbFacts.table("query")
                    + " q JOIN temp.main.sibench_sessions s USING(connection_id) WHERE q.query_id>s.after_query");
            reject(database, "SELECT 1 FROM temp.main.sibench_source_queries WHERE event NOT IN ('begin','end') "
                    + "OR (event='begin' AND (outcome<>'running' OR submission_end)) "
                    + "OR (event='end' AND outcome NOT IN ('completed','failed','abandoned')) LIMIT 1",
                    "Invalid source statement event");
            execute(database, "CREATE TEMP TABLE sibench_statements AS SELECT connection_id, query_id, "
                    + "min(transaction_id) AS transaction_id, min(root_query) AS root_query, "
                    + "min(raw_record) FILTER(WHERE event='begin') AS begin_record, "
                    + "max(raw_record) FILTER(WHERE event='end') AS end_record, "
                    + "any_value(outcome) FILTER(WHERE event='end') AS outcome, "
                    + "any_value(submission_end) FILTER(WHERE event='end') AS submission_end, "
                    + "any_value(sql_text) FILTER(WHERE event='begin') AS sql_text, "
                    + "any_value(error) FILTER(WHERE event='end') AS error, "
                    + "count(*) FILTER(WHERE event='begin')=1 AND count(*) FILTER(WHERE event='end')=1 "
                    + "AND min(transaction_id)=max(transaction_id) AND min(root_query)=max(root_query) AS paired "
                    + "FROM temp.main.sibench_source_queries GROUP BY connection_id, query_id");
            reject(database, "SELECT 1 FROM temp.main.sibench_statements WHERE NOT paired "
                    + "OR begin_record>=end_record OR root_query>query_id LIMIT 1",
                    "Missing, duplicate or inconsistent source statement boundary");
            reject(database, "SELECT 1 FROM temp.main.sibench_sessions s LEFT JOIN (SELECT connection_id, "
                    + "count(DISTINCT root_query) AS roots, count(*) FILTER(WHERE submission_end) AS endings, "
                    + "count(DISTINCT root_query) FILTER(WHERE submission_end) AS ended_roots "
                    + "FROM temp.main.sibench_statements GROUP BY connection_id) q USING(connection_id) "
                    + "WHERE coalesce(q.roots,0)<>s.submissions OR coalesce(q.endings,0)<>s.submissions "
                    + "OR coalesce(q.ended_roots,0)<>s.submissions LIMIT 1", "Source submissions disagree with client receipts");
            for (String kind : List.of("mutation", "index", "index_rows", "consumption")) {
                reject(database, "SELECT 1 FROM " + DuckDbFacts.table(kind)
                        + " f JOIN temp.main.sibench_sessions c USING(connection_id) "
                        + "LEFT JOIN temp.main.sibench_statements s USING(connection_id, transaction_id, query_id) "
                        + "WHERE f.query_id>c.after_query AND s.query_id IS NULL LIMIT 1",
                        "Unmatched source statement for " + kind);
            }
            // A completed mutation scope inside a failed SQL statement remains diagnostic, even before Abort.
            execute(database, "CREATE TEMP VIEW sibench_mutation_statements AS SELECT m.*, s.outcome AS statement_outcome "
                    + "FROM " + DuckDbFacts.table("mutation") + " m JOIN temp.main.sibench_statements s "
                    + "USING(connection_id, transaction_id, query_id)");
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
