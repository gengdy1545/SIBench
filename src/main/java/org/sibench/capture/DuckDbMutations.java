package org.sibench.capture;

import java.sql.Connection;
import java.sql.SQLException;

/** Logical row changes derived from successful native mutation scopes and native identity births. */
public final class DuckDbMutations {
    private DuckDbMutations() {}

    /** Bind accesses/catalog and identities first. Failed statement internals remain raw diagnostics. */
    public static void bind(Connection database) throws SQLException {
        if(!database.getAutoCommit()) throw new SQLException("Mutation binding requires an idle connection");
        database.setAutoCommit(false);
        try {
            execute(database,"CREATE TEMP VIEW sibench_source_mutations AS SELECT m.*,h.table_id,h.primary_key,t.version,t.read_version "
                    +"FROM temp.main.sibench_mutation_statements m JOIN sibench_history.tables h USING(schema_name,table_name) "
                    +"JOIN temp.main.sibench_transactions t USING(connection_id,transaction_id)");
            // The pinned logger appends synchronously under its storage lock. Row fetch logs must precede write-begin logs.
            reject(database,"SELECT 1 FROM temp.main.sibench_accesses a JOIN temp.main.sibench_source_mutations m "
                    +"USING(connection_id,transaction_id,query_id) WHERE m.event='begin' AND a.raw_record>=m.raw_record LIMIT 1",
                    "Unsupported index probe after writes in the same SQL statement");
            reject(database,"SELECT 1 FROM temp.main.sibench_access_batches b JOIN temp.main.sibench_accesses a "
                    +"USING(access_id,connection_id,transaction_id,query_id) JOIN temp.main.sibench_source_mutations m "
                    +"USING(connection_id,transaction_id,query_id) WHERE len(b.physical_rows)>0 "
                    +"AND m.event='begin' AND b.raw_record>=m.raw_record LIMIT 1",
                    "Unsupported index read overlapping writes in the same SQL statement");
            // A delayed empty drain is safe only if writes cannot remove/change an old row this access has not read.
            reject(database,"SELECT 1 FROM temp.main.sibench_accesses a JOIN temp.main.sibench_source_mutations m "
                    +"USING(connection_id,transaction_id,query_id,table_id),unnest(m.rows) AS captured(r) "
                    +"WHERE m.event='begin' AND m.kind IN ('delete','update') AND NOT EXISTS(SELECT 1 FROM "
                    +"temp.main.sibench_access_keys k WHERE k.access_id=a.access_id AND k.key_sql="
                    +"r.values_sql[list_position(list_transform(m.columns,lambda c: c.name),m.primary_key)]) "
                    +"AND EXISTS(SELECT 1 FROM temp.main.sibench_access_batches b JOIN temp.main.sibench_source_mutations w "
                    +"USING(connection_id,transaction_id,query_id) WHERE b.access_id=a.access_id AND b.phase<>'closed' "
                    +"AND len(b.physical_rows)=0 AND w.event='begin' AND b.raw_record>=w.raw_record) LIMIT 1",
                    "Unsupported empty index drain overlapping changes to unread rows");
            execute(database,"CREATE TEMP VIEW sibench_completed_mutations AS SELECT * FROM temp.main.sibench_source_mutations "
                    +"WHERE statement_outcome='completed'");
            reject(database,"SELECT 1 FROM temp.main.sibench_completed_mutations WHERE kind NOT IN ('append','merge','update','delete') "
                    +"OR event NOT IN ('begin','rows','complete') OR requested_rows=0 "
                    +"OR (event='complete' AND affected_rows<>requested_rows) OR (event<>'complete' AND affected_rows<>-1) "
                    +"OR (event='rows' AND kind<>'merge') "
                    +"OR (kind='merge' AND event='begin' AND len(rows)<>0) "
                    +"OR (event='complete' AND kind<>'update' AND len(rows)<>0) LIMIT 1",
                    "Invalid or unsupported successful native mutation scope");
            reject(database,"SELECT 1 FROM temp.main.sibench_completed_mutations GROUP BY mutation_id HAVING "
                    +"count(*) FILTER(WHERE event='begin')<>1 OR count(*) FILTER(WHERE event='complete')<>1 "
                    +"OR min(connection_id)<>max(connection_id) OR min(transaction_id)<>max(transaction_id) "
                    +"OR min(query_id)<>max(query_id) OR min(table_id)<>max(table_id) OR min(kind)<>max(kind) "
                    +"OR min(requested_rows)<>max(requested_rows) OR count(DISTINCT columns)<>1 "
                    +"OR sum(len(rows))<>max(requested_rows)*CASE WHEN min(kind)='update' THEN 2 ELSE 1 END "
                    +"OR min(raw_record) FILTER(WHERE event='begin')>=max(raw_record) FILTER(WHERE event='complete') LIMIT 1",
                    "Incomplete or inconsistent successful native mutation scope");
            reject(database,"SELECT 1 FROM temp.main.sibench_completed_mutations m JOIN temp.main.sibench_index_catalog c USING(table_id) "
                    +"LEFT JOIN duckdb_columns() d ON d.database_name=current_database() AND d.schema_name=m.schema_name "
                    +"AND d.table_name=m.table_name AND d.column_name=c.column_name WHERE NOT coalesce("
                    +"list_contains(m.columns,{'name':c.column_name,'type':d.data_type}),false) LIMIT 1",
                    "Native mutation omits a canonical indexed column");
            execute(database,"CREATE TEMP TABLE sibench_change_images AS SELECT m.table_id,m.connection_id,m.transaction_id,"
                    +"m.query_id,m.version,m.read_version,m.raw_record,m.columns,r.values_sql,"
                    +"r.values_sql[list_position(list_transform(m.columns,lambda c: c.name),m.primary_key)] AS primary_key_sql,"
                    +"CASE WHEN m.kind IN ('delete','update') AND m.event='begin' THEN 'before' ELSE 'after' END AS image "
                    +"FROM temp.main.sibench_completed_mutations m,unnest(m.rows) AS captured(r)");
            reject(database,"SELECT 1 FROM temp.main.sibench_change_images WHERE primary_key_sql IS NULL "
                    +"OR len(values_sql)<>len(columns) OR list_unique(list_transform(columns,lambda c: c.name))<>len(columns) LIMIT 1",
                    "Invalid indexed native row image");
            reject(database,"SELECT 1 FROM temp.main.sibench_change_images GROUP BY table_id,transaction_id,query_id,primary_key_sql,image "
                    +"HAVING count(*)<>1 LIMIT 1","Unsupported repeated row changes within one SQL statement");
            execute(database,"CREATE TEMP TABLE sibench_row_changes AS SELECT coalesce(b.connection_id,a.connection_id) AS connection_id,"
                    +"coalesce(b.table_id,a.table_id) AS table_id,coalesce(b.transaction_id,a.transaction_id) AS transaction_id,"
                    +"coalesce(b.query_id,a.query_id) AS query_id,coalesce(b.version,a.version) AS version,"
                    +"coalesce(b.read_version,a.read_version) AS read_version,coalesce(b.primary_key_sql,a.primary_key_sql) AS primary_key_sql,"
                    +"least(b.raw_record,a.raw_record) AS source_event,coalesce(b.columns,a.columns) AS columns,"
                    +"b.values_sql AS before_values,a.values_sql AS after_values,"
                    +"CASE WHEN b.raw_record IS NULL THEN 'insert' WHEN a.raw_record IS NULL THEN 'delete' ELSE 'update' END AS kind "
                    +"FROM (SELECT * FROM temp.main.sibench_change_images WHERE image='before') b FULL OUTER JOIN "
                    +"(SELECT * FROM temp.main.sibench_change_images WHERE image='after') a "
                    +"USING(table_id,transaction_id,query_id,primary_key_sql)");
            reject(database,"SELECT 1 FROM temp.main.sibench_change_images GROUP BY table_id,transaction_id,query_id,primary_key_sql "
                    +"HAVING count(DISTINCT columns)<>1 LIMIT 1","Native indexed columns changed within one SQL statement");
            execute(database,"CREATE TEMP TABLE sibench_resolved_changes AS SELECT c.*,i.row_id FROM temp.main.sibench_row_changes c "
                    +"LEFT JOIN temp.main.sibench_identities i ON i.table_id=c.table_id AND i.primary_key_sql=c.primary_key_sql AND "
                    +"((c.kind='insert' AND i.version=c.version AND i.query_id=c.query_id) OR (c.kind<>'insert' AND "
                    +"((i.outcome='commit' AND i.version<=c.read_version) OR (i.version=c.version AND i.query_id<c.query_id)))) "
                    +"QUALIFY row_number() OVER(PARTITION BY c.table_id,c.transaction_id,c.query_id,c.primary_key_sql "
                    +"ORDER BY (i.version=c.version) DESC,i.version DESC,i.query_id DESC)=1");
            reject(database,"SELECT 1 FROM temp.main.sibench_resolved_changes WHERE row_id IS NULL LIMIT 1",
                    "Missing native birth or visible identity for row change");
            reject(database,"SELECT 1 FROM temp.main.sibench_resolved_changes c JOIN temp.main.sibench_insert_identities i "
                    +"ON c.table_id=i.table_id AND c.transaction_id=i.transaction_id AND c.query_id=i.query_id "
                    +"AND c.primary_key_sql=i.primary_key_sql WHERE c.kind<>'insert' LIMIT 1",
                    "Unsupported replacement identity within one SQL statement");
            execute(database,"CREATE TEMP TABLE sibench_changes AS SELECT *,row_number() OVER(ORDER BY version,query_id,source_event,"
                    +"table_id,primary_key_sql) AS change_id,row_number() OVER(PARTITION BY transaction_id ORDER BY query_id,"
                    +"source_event,table_id,primary_key_sql) AS mutation_ordinal FROM temp.main.sibench_resolved_changes");
            database.commit();
        } catch(SQLException|RuntimeException failure) {
            try{database.rollback();}catch(SQLException rollback){failure.addSuppressed(rollback);}throw failure;
        } finally {database.setAutoCommit(true);}
    }
    private static void execute(Connection database,String sql) throws SQLException {
        try(var query=database.createStatement()){query.execute(sql);}
    }
    private static void reject(Connection database,String sql,String message) throws SQLException {
        try(var query=database.createStatement();var rows=query.executeQuery(sql)){if(rows.next())throw new SQLException(message);}
    }
}
