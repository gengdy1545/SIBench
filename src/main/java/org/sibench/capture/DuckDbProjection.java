package org.sibench.capture;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.sibench.trace.IndexKey;

/** Streams native capture operations as ROW_ID requests in canonical resolution and statement order. */
public final class DuckDbProjection {
    private DuckDbProjection() {}

    /**
     * Access and mutation binding must have completed. The consumer must not query this connection.
     * Mutation binding has checked every indexed column; only exported secondary keys are parsed.
     */
    public static long write(Connection database,boolean secondaryIndexes,IndexOperationTrace.Consumer consumer) throws SQLException,IOException {
        if(!database.getAutoCommit())throw new SQLException("Capture projection requires an idle connection");
        database.setAutoCommit(false);
        long[] count={0};IndexOperationTrace.Consumer output=operation->{consumer.accept(operation);count[0]++;};
        try {
            execute(database,"CREATE TEMP VIEW sibench_work_units AS SELECT row_number() OVER(ORDER BY version,query_id,"
                    +"CASE tag WHEN 'search' THEN 0 ELSE 1 END,source_event,identity) AS unit_id,* FROM ("
                    +"SELECT a.version,a.transaction_id,a.query_id,a.raw_record AS source_event,a.access_id AS identity,'search' AS tag,"
                    +"'' AS action,a.table_id,a.index_id,a.key_type,a.read_version,0::BIGINT AS row_id,"
                    +"(SELECT count(*) FROM temp.main.sibench_access_keys r WHERE r.access_id=a.access_id) AS expected_count,"
                    +"coalesce((SELECT max(c.mutation_ordinal) FROM temp.main.sibench_changes c "
                    +"WHERE c.transaction_id=a.transaction_id AND c.query_id<a.query_id),0) AS mutation_ordinal "
                    +"FROM temp.main.sibench_accesses a UNION ALL SELECT c.version,c.transaction_id,c.query_id,c.source_event,c.change_id,"
                    +"'write',c.kind,c.table_id,0,'',c.read_version,c.row_id,0,c.mutation_ordinal FROM temp.main.sibench_changes c)");
            execute(database,"CREATE TEMP VIEW sibench_unit_details AS SELECT u.unit_id,c.index_id,"
                    +"m.columns[list_position(list_transform(m.columns,lambda x: x.name),c.column_name)].type AS native_type,"
                    +"m.before_values[list_position(list_transform(m.columns,lambda x: x.name),c.column_name)] AS before_token,"
                    +"m.after_values[list_position(list_transform(m.columns,lambda x: x.name),c.column_name)] AS after_token,"
                    +"0 AS probe_position,c.index_id AS detail_position,'' AS origin,'' AS comparison,'' AS predicate_type,'' AS predicate_value "
                    +"FROM temp.main.sibench_work_units u JOIN temp.main.sibench_changes m ON u.tag='write' AND u.identity=m.change_id "
                    +"JOIN temp.main.sibench_index_catalog c ON c.table_id=m.table_id"+(secondaryIndexes?"":" AND c.index_id=0")+" UNION ALL "
                    +"SELECT u.unit_id,a.index_id,NULL,NULL,NULL,p.ordinality,d.ordinality,p.probe.origin,"
                    +"d.predicate.comparison,d.predicate.value_type,d.predicate.value_sql FROM temp.main.sibench_work_units u "
                    +"JOIN temp.main.sibench_accesses a ON u.tag='search' AND u.identity=a.access_id,"
                    +"unnest(a.probes) WITH ORDINALITY AS p(probe,ordinality),"
                    +"unnest(p.probe.predicates) WITH ORDINALITY AS d(predicate,ordinality)");
            try(var query=database.createStatement();var rows=query.executeQuery(
                    "SELECT 1 FROM temp.main.sibench_work_units u ANTI JOIN temp.main.sibench_unit_details d USING(unit_id) LIMIT 1")) {
                if(rows.next())throw new SQLException("Missing native operation details");
            }
            long transaction=0,version=0,writes=0;String outcome=null;Unit unit=null;
            try(var query=database.createStatement();var rows=query.executeQuery(
                    "SELECT t.version AS resolved_version,t.transaction_id AS resolved_transaction,t.outcome,u.*,"
                    +"d.index_id AS detail_index,d.native_type,d.before_token,d.after_token,d.probe_position,d.detail_position,"
                    +"d.origin,d.comparison,d.predicate_type,d.predicate_value FROM temp.main.sibench_transactions t "
                    +"LEFT JOIN temp.main.sibench_work_units u ON t.version=u.version "
                    +"LEFT JOIN temp.main.sibench_unit_details d USING(unit_id) ORDER BY t.version,u.unit_id,d.probe_position,d.detail_position")) {
                while(rows.next()) {
                    long nextVersion=rows.getLong("resolved_version"),nextUnit=rows.getLong("unit_id");
                    if(unit!=null&&nextUnit!=unit.id) {unit.emit(output);if(unit.tag.equals("write"))writes++;unit=null;}
                    if(nextVersion!=version) {
                        if(transaction!=0)finish(output,transaction,version,writes,outcome);
                        version=nextVersion;transaction=version;outcome=rows.getString("outcome");writes=0;
                        output.accept(IndexOperationTrace.transaction(0,IndexOperationTrace.TRANSACTION_BEGIN,transaction,version,0));
                    }
                    if(nextUnit==0)continue;
                    if(unit==null)unit=new Unit(rows);
                    unit.add(rows);
                }
            }
            if(unit!=null) {unit.emit(output);if(unit.tag.equals("write"))writes++;}
            if(transaction!=0)finish(output,transaction,version,writes,outcome);
            database.commit();return count[0];
        } catch(SQLException|IOException|RuntimeException failure) {
            try{database.rollback();}catch(SQLException rollback){failure.addSuppressed(rollback);}throw failure;
        } finally {database.setAutoCommit(true);}
    }

    private static final class Unit {
        final long id,source,transaction,version,position,readVersion,rowId,expected;
        final int table,index;final String tag,action,family;
        final Map<Integer,IndexKey> before=new LinkedHashMap<>(),after=new LinkedHashMap<>();
        final List<DuckDbPredicates.Probe> probes=new ArrayList<>();
        List<DuckDbPredicates.Predicate> predicates=new ArrayList<>();String origin;long probe;
        Unit(ResultSet row)throws SQLException {
            id=row.getLong("unit_id");source=row.getLong("source_event");transaction=row.getLong("version");
            version=row.getLong("version");position=row.getLong("mutation_ordinal");readVersion=row.getLong("read_version");
            rowId=row.getLong("row_id");expected=row.getLong("expected_count");table=row.getInt("table_id");index=row.getInt("index_id");
            tag=row.getString("tag");action=row.getString("action");family=row.getString("key_type");
        }
        void add(ResultSet row)throws SQLException,IOException {
            if(tag.equals("write")) {
                int target=row.getInt("detail_index");String type=row.getString("native_type");
                String old=row.getString("before_token"),next=row.getString("after_token");
                if(old!=null&&before.put(target,DuckDbKeys.parse(type,old))!=null)throw new IOException("Duplicate old indexed value");
                if(next!=null&&after.put(target,DuckDbKeys.parse(type,next))!=null)throw new IOException("Duplicate new indexed value");
            } else {
                long current=row.getLong("probe_position");
                if(probe!=current) {flushProbe();probe=current;origin=row.getString("origin");}
                predicates.add(new DuckDbPredicates.Predicate(row.getString("comparison"),row.getString("predicate_type"),row.getString("predicate_value")));
            }
        }
        void flushProbe() {if(!predicates.isEmpty()){probes.add(new DuckDbPredicates.Probe(origin,predicates));predicates=new ArrayList<>();}}
        void emit(IndexOperationTrace.Consumer output)throws IOException {
            if(tag.equals("write")) {
                IndexKey old=before.remove(0),next=after.remove(0),key=next==null?old:next;
                if(key==null||key.equals(IndexKey.NULL)||old!=null&&next!=null&&!old.equals(next))throw new IOException("Invalid primary row change");
                int kind=action.equals("delete")?IndexOperationTrace.LOGICAL_DELETE:IndexOperationTrace.UPSERT;
                int flags=action.equals("insert")?IndexOperationTrace.BUSINESS_INSERT:action.equals("update")?IndexOperationTrace.BUSINESS_UPDATE:0;
                var primary=IndexOperationTrace.write(source,kind,table,flags,key,version,0,0,rowId)
                        .withTransaction(transaction,position).withWorkUnitId(id);
                IndexMutation.emit(primary,before,after,output);
            } else {
                flushProbe();var ranges=DuckDbPredicates.ranges(family,probes);
                int flags=position>0?IndexOperationTrace.TRANSACTION_LOCAL_SEARCH:IndexOperationTrace.SNAPSHOT_SEARCH;
                if(ranges.size()>1)flags|=IndexOperationTrace.UNION_GROUP;
                for(int i=0;i<ranges.size();i++) {
                    var search=IndexOperationTrace.search(source,table,index,ranges.get(i),null,readVersion,expected,flags,
                            ranges.size()>1?i:0,ranges.size()>1?ranges.size():0).withTransaction(transaction,position).withWorkUnitId(id);
                    if(position>0)search=search.withVersion(version);
                    output.accept(search);
                }
            }
        }
    }
    private static void finish(IndexOperationTrace.Consumer output,long transaction,long version,long writes,String outcome)throws IOException {
        int kind=outcome.equals("rollback")?IndexOperationTrace.TRANSACTION_ABORT:IndexOperationTrace.TRANSACTION_COMMIT;
        output.accept(IndexOperationTrace.transaction(0,kind,transaction,version,writes));
    }
    private static void execute(Connection database,String sql)throws SQLException {try(var query=database.createStatement()){query.execute(sql);}}
}
