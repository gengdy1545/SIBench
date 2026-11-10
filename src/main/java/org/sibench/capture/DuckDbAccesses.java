package org.sibench.capture;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/** Binds adopted ART accesses to the canonical catalog and their snapshot-visible native rows. */
public final class DuckDbAccesses {
    private DuckDbAccesses() {}

    /** Bind statements, transactions and identities first. Unrepresentable partial source output fails explicitly. */
    public static void bind(Connection database,List<IndexCatalog.Definition> catalog) throws SQLException {
        if(!database.getAutoCommit()) throw new SQLException("Access binding requires an idle connection");
        database.setAutoCommit(false);
        try {
            execute(database,"CREATE TEMP TABLE sibench_index_catalog(table_id INTEGER,index_id INTEGER,table_name VARCHAR,"
                    +"index_name VARCHAR,column_name VARCHAR,key_type VARCHAR,PRIMARY KEY(table_id,index_id),UNIQUE(table_id,index_name))");
            try(var insert=database.prepareStatement("INSERT INTO temp.main.sibench_index_catalog VALUES(?,?,?,?,?,?)")) {
                for(var index:catalog) {
                    if(index.tableId()<1||index.tableId()>65535||index.indexId()<0||index.indexId()>1024)
                        throw new SQLException("Invalid canonical index identity");
                    insert.setInt(1,index.tableId());insert.setInt(2,index.indexId());insert.setString(3,index.table());
                    insert.setString(4,index.name());insert.setString(5,index.column());insert.setString(6,index.type());insert.executeUpdate();
                }
            }
            reject(database,"SELECT 1 FROM temp.main.sibench_index_catalog c LEFT JOIN sibench_history.tables h USING(table_id,table_name) "
                    +"LEFT JOIN duckdb_columns() d ON d.database_name=current_database() AND d.schema_name=h.schema_name "
                    +"AND d.table_name=c.table_name AND d.column_name=c.column_name WHERE h.table_id IS NULL OR d.column_name IS NULL "
                    +"OR (c.index_id=0 AND c.column_name<>h.primary_key) OR NOT CASE c.key_type "
                    +"WHEN 'int' THEN d.data_type IN ('TINYINT','SMALLINT','INTEGER','BIGINT') "
                    +"WHEN 'timestamp' THEN d.data_type='TIMESTAMP' WHEN 'varchar' THEN d.data_type='VARCHAR' "
                    +"WHEN 'text' THEN d.data_type='VARCHAR' ELSE false END LIMIT 1","Canonical index catalog disagrees with native columns");
            reject(database,"SELECT 1 FROM sibench_history.tables h ANTI JOIN temp.main.sibench_index_catalog c "
                    +"ON h.table_id=c.table_id AND c.index_id=0 LIMIT 1","Missing canonical primary index");
            execute(database,"CREATE TEMP TABLE sibench_index_attempts AS SELECT i.*,s.outcome AS statement_outcome,t.version,"
                    +"t.read_version,t.outcome AS transaction_outcome FROM temp.main.sibench_index i "
                    +"JOIN temp.main.sibench_statements s USING(connection_id,transaction_id,query_id) "
                    +"JOIN temp.main.sibench_transactions t USING(connection_id,transaction_id) WHERE i.schema_name<>'sibench_history'");
            reject(database,"SELECT 1 FROM temp.main.sibench_index_attempts WHERE access_id=0 OR scan_calls=0 "
                    +"OR len(probes)<>scan_calls OR NOT coalesce(index_is_column AND len(index_columns)=1,false) LIMIT 1",
                    "Unsupported ART expression or invalid attempt metadata");
            reject(database,"SELECT 1 FROM temp.main.sibench_index_attempts GROUP BY access_id HAVING count(*)<>1 LIMIT 1",
                    "Duplicate ART access identity");
            reject(database,"SELECT 1 FROM temp.main.sibench_index_attempts,unnest(probes) AS probe(p) "
                    +"WHERE p.origin NOT IN ('base','deleted_rows','checkpoint_added') OR p.completed IS NULL "
                    +"OR len(p.predicates) NOT BETWEEN 1 AND 2 OR (adopted AND NOT p.completed) LIMIT 1",
                    "Invalid ART probe outcome or predicate scope");
            execute(database,"CREATE TEMP TABLE sibench_accesses AS SELECT a.*,h.table_id,c.index_id,c.key_type,"
                    +"c.column_name FROM temp.main.sibench_index_attempts a LEFT JOIN sibench_history.tables h "
                    +"ON h.schema_name=a.schema_name AND h.table_name=a.table_name "
                    +"LEFT JOIN temp.main.sibench_index_catalog c ON c.table_id=h.table_id "
                    +"AND ((a.index_primary AND c.index_id=0) OR (NOT a.index_primary AND c.index_id>0 AND c.index_name=a.index_name)) "
                    +"WHERE a.adopted");
            reject(database,"SELECT 1 FROM temp.main.sibench_accesses a LEFT JOIN duckdb_columns() d "
                    +"ON d.database_name=current_database() AND d.schema_name=a.schema_name AND d.table_name=a.table_name "
                    +"AND d.column_name=a.column_name WHERE index_id IS NULL OR NOT coalesce(index_columns[1].name=a.column_name "
                    +"AND index_columns[1].type=d.data_type,false) LIMIT 1","Adopted ART does not match canonical index");
            execute(database,"CREATE TEMP VIEW sibench_access_batches AS SELECT r.* FROM temp.main.sibench_index_rows r "
                    +"JOIN temp.main.sibench_accesses a USING(access_id,connection_id,transaction_id,query_id)");
            for(String kind:List.of("index_rows","consumption")) {
                reject(database,"SELECT 1 FROM temp.main.sibench_"+kind+" r JOIN temp.main.sibench_statements s "
                        +"USING(connection_id,transaction_id,query_id) LEFT JOIN temp.main.sibench_index i "
                        +"USING(access_id,connection_id,transaction_id,query_id) LEFT JOIN temp.main.sibench_accesses a "
                        +"USING(access_id,connection_id,transaction_id,query_id) WHERE a.access_id IS NULL "
                        +"AND (i.schema_name IS NULL OR i.schema_name<>'sibench_history') LIMIT 1",
                        "Unmatched adopted ART "+kind);
            }
            reject(database,"SELECT 1 FROM temp.main.sibench_access_batches WHERE phase NOT IN ('storage','local','closed') "
                    +"OR len(physical_rows)<>len(primary_keys_sql) OR len(candidate_rows)<>candidates_read "
                    +"OR (phase='closed' AND (batch_id<>0 OR len(physical_rows)<>0 OR candidates_read<>0 OR candidate_offset<>0)) "
                    +"OR (phase<>'closed' AND (batch_id=0 OR source_drained)) "
                    +"OR (phase='storage' AND (candidates_read=0 OR len(physical_rows)>candidates_read)) "
                    +"OR (phase='local' AND (candidates_read<>0 OR candidate_offset<>0)) LIMIT 1",
                    "Invalid ART row batch");
            reject(database,"SELECT 1 FROM temp.main.sibench_access_batches GROUP BY access_id,batch_id HAVING count(*)<>1 LIMIT 1",
                    "Duplicate ART row batch");
            reject(database,"SELECT 1 FROM (SELECT candidate_offset,coalesce(sum(candidates_read) OVER(PARTITION BY access_id "
                    +"ORDER BY candidate_offset ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) AS preceding "
                    +"FROM temp.main.sibench_access_batches WHERE phase='storage') WHERE candidate_offset<>preceding LIMIT 1",
                    "Incomplete ART candidate coverage");
            // A direct LIMIT can revisit the same input after shortening it; adjacency does not imply equal cardinality.
            reject(database,"SELECT 1 FROM temp.main.sibench_consumption c JOIN temp.main.sibench_accesses a "
                    +"USING(access_id,connection_id,transaction_id,query_id) LEFT JOIN temp.main.sibench_access_batches b "
                    +"USING(access_id,connection_id,transaction_id,query_id,batch_id) WHERE b.batch_id IS NULL OR b.phase='closed' "
                    +"OR c.consumed_prefix>c.input_rows OR (c.direct_from_scan AND c.input_rows>len(b.physical_rows)) LIMIT 1",
                    "Consumption disagrees with its ART source batch");
            reject(database,"SELECT 1 FROM temp.main.sibench_accesses a LEFT JOIN temp.main.sibench_access_batches r "
                    +"ON a.access_id=r.access_id AND r.phase='closed' WHERE r.access_id IS NULL LIMIT 1","Missing ART closing record");
            // A conflict can interrupt the final empty fetch after the sole primary-key match was already emitted.
            // shortcut: other partial output needs an exact representable range/order before it can become Search.
            reject(database,"SELECT 1 FROM temp.main.sibench_access_batches r JOIN temp.main.sibench_accesses a USING(access_id) "
                    +"WHERE r.phase='closed' AND NOT r.source_drained AND NOT coalesce(a.index_primary AND a.index_id=0 "
                    +"AND len(a.probes[1].predicates)=1 AND a.probes[1].predicates[1].comparison='EQUAL' "
                    +"AND NOT EXISTS (SELECT 1 FROM unnest(a.probes) p(probe) WHERE probe.predicates IS DISTINCT FROM a.probes[1].predicates) "
                    +"AND (SELECT sum(len(b.primary_keys_sql)) FROM temp.main.sibench_access_batches b WHERE b.access_id=a.access_id)=1 "
                    +"AND EXISTS (SELECT 1 FROM temp.main.sibench_access_batches b WHERE b.access_id=a.access_id "
                    +"AND b.primary_keys_sql[1]=a.probes[1].predicates[1].value_sql),false) LIMIT 1",
                    "Unsupported ART early stop: the source was not fully drained");
            reject(database,"SELECT 1 FROM temp.main.sibench_consumption c JOIN temp.main.sibench_accesses a "
                    +"USING(access_id,connection_id,transaction_id,query_id) WHERE c.stage NOT IN ('stop','limit') LIMIT 1",
                    "Unknown ART downstream diagnostic stage");
            reject(database,"SELECT 1 FROM (SELECT access_id,count(*) AS batches,max(batch_id) AS last_batch,"
                    +"sum(candidates_read) AS candidates FROM temp.main.sibench_access_batches GROUP BY access_id) r "
                    +"JOIN temp.main.sibench_accesses a USING(access_id) WHERE r.batches<>r.last_batch+1 "
                    +"OR r.candidates<>a.observed_candidates LIMIT 1","Incomplete ART batch coverage");
            execute(database,"CREATE TEMP TABLE sibench_access_keys AS SELECT r.access_id,r.batch_id,k.ordinality AS position,"
                    +"r.primary_key_type,k.key_sql FROM temp.main.sibench_access_batches r,"
                    +"unnest(r.primary_keys_sql) WITH ORDINALITY AS k(key_sql,ordinality)");
            reject(database,"SELECT 1 FROM temp.main.sibench_access_keys GROUP BY access_id,key_sql HAVING count(*)<>1 LIMIT 1",
                    "Duplicate visible primary key in ART access");
            // Verify source identity coverage without constructing per-query RowID answers.
            reject(database,"SELECT 1 FROM temp.main.sibench_access_keys k JOIN temp.main.sibench_accesses a USING(access_id) "
                    +"WHERE NOT EXISTS(SELECT 1 FROM temp.main.sibench_identities i WHERE i.table_id=a.table_id "
                    +"AND i.primary_key_sql=k.key_sql AND ((i.outcome='commit' AND i.version<=a.read_version) "
                    +"OR (i.version=a.version AND i.query_id<a.query_id))) LIMIT 1",
                    "Missing visible native identity for ART result");
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
