package org.sibench.capture;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.sibench.config.BenchmarkSelection.Mode;

/** Summaries of receipt-bound native facts, including facts that cannot become Trace operations. */
public final class DuckDbDiagnostics {
    private DuckDbDiagnostics() {}

    /** Run after statement binding and before projection, so unsupported accesses still have diagnostics. */
    public static Map<String,Object> summarize(Connection database, Mode mode) throws SQLException {
        String attempts = "SELECT i.*,s.outcome AS statement_outcome FROM " + DuckDbFacts.table("index")
                + " i JOIN temp.main.sibench_statements s USING(connection_id,transaction_id,query_id)"
                + " WHERE i.schema_name<>'sibench_history'";
        var report = new LinkedHashMap<String,Object>();
        report.put("mode",mode.name().toLowerCase(java.util.Locale.ROOT));
        report.put("scope","Receipt-bound storage SQL; excludes internal history-table accesses. "
                + "These counts are not API calls or projected Trace operations.");
        report.put("statements",rows(database,"WITH attempts AS ("+attempts+") SELECT outcome,"
                + "count(*) AS statements,count(*) FILTER(WHERE NOT EXISTS (SELECT 1 FROM attempts i "
                + "WHERE i.connection_id=s.connection_id AND i.transaction_id=s.transaction_id "
                + "AND i.query_id=s.query_id AND i.adopted)) AS without_adopted_art "
                + "FROM temp.main.sibench_statements s GROUP BY outcome ORDER BY outcome"));
        report.put("art_attempts",rows(database,"SELECT statement_outcome,adopted,count(*) AS attempts,"
                + "sum(scan_calls) AS probe_calls,sum(observed_candidates) AS sum_candidate_vector_maxima,"
                + "sum(probe_time_ns) AS probe_time_ns FROM ("+attempts+") "
                + "GROUP BY statement_outcome,adopted ORDER BY statement_outcome,adopted"));
        report.put("mutation_scopes",rows(database,"WITH scopes AS (SELECT connection_id,transaction_id,query_id,"
                + "mutation_id,statement_outcome,kind,count(*) FILTER(WHERE event='begin') AS begins,"
                + "count(*) FILTER(WHERE event='complete') AS completions,"
                + "count(*) FILTER(WHERE event='failed') AS failures,"
                + "coalesce(sum(requested_rows) FILTER(WHERE event='begin'),0) AS requested_rows,"
                + "coalesce(sum(affected_rows) FILTER(WHERE event='complete'),0) AS completed_scope_rows "
                + "FROM temp.main.sibench_mutation_statements WHERE schema_name<>'sibench_history' "
                + "GROUP BY connection_id,transaction_id,query_id,mutation_id,statement_outcome,kind) "
                + "SELECT statement_outcome,kind,count(*) AS scopes,sum(begins) AS begin_records,"
                + "sum(completions) AS complete_records,sum(failures) AS failed_records,"
                + "count(*) FILTER(WHERE completions=0) AS scopes_without_completion,"
                + "sum(requested_rows) AS requested_rows,sum(completed_scope_rows) AS completed_scope_rows "
                + "FROM scopes GROUP BY statement_outcome,kind ORDER BY statement_outcome,kind"));
        report.put("interpretation",List.of(
                "without_adopted_art does not imply no index maintenance or prove a column scan.",
                "Candidate vector maxima are not distinct rows or physical I/O; probe time excludes fetching and downstream SQL.",
                "Mutation scopes describe row-change instrumentation, not physical ART writes. Physical partial success is not measured.",
                "Requested rows are not successful writes. Completed internal scopes in failed or abandoned statements remain diagnostic; "
                        + "their counts do not establish a successful statement or a committed transaction."));
        return report;
    }

    private static List<Map<String,Object>> rows(Connection database,String sql) throws SQLException {
        var result=new ArrayList<Map<String,Object>>();
        try(var query=database.createStatement();var rows=query.executeQuery(sql)) {
            var metadata=rows.getMetaData();
            while(rows.next()) {
                var row=new LinkedHashMap<String,Object>();
                for(int column=1;column<=metadata.getColumnCount();column++)
                    row.put(metadata.getColumnLabel(column),rows.getObject(column));
                result.add(row);
            }
        }
        return result;
    }
}
