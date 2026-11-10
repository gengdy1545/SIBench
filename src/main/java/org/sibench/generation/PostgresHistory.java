package org.sibench.generation;

import java.sql.Connection;
import java.sql.SQLException;

/** PostgreSQL RowID identities used by the Trace; row contents of earlier versions are not retained. */
final class PostgresHistory {
    private PostgresHistory() {}

    static void install(Connection database,int id,String schema,String table,String key)
            throws SQLException {
        String source=quote(schema)+"."+quote(table),pk=quote(key);
        try(var query=database.prepareStatement("SELECT count(*) FROM pg_constraint c "
                +"JOIN pg_class t ON t.oid=c.conrelid JOIN pg_namespace n ON n.oid=t.relnamespace "
                +"JOIN pg_attribute a ON a.attrelid=t.oid AND a.attnum=c.conkey[1] "
                +"WHERE c.contype='p' AND cardinality(c.conkey)=1 AND n.nspname=? AND t.relname=? AND a.attname=?")) {
            query.setString(1,schema);query.setString(2,table);query.setString(3,key);
            try(var rows=query.executeQuery()) {
                if(!rows.next()||rows.getLong(1)!=1)throw new SQLException("RowID identities require a single-column primary key: "+source);
            }
        }
        String identities="sibench_history.identities_"+id;
        sql(database,"CREATE UNLOGGED TABLE "+identities+" AS SELECT nextval('sibench_history.row_ids') AS _sib_row_id,"
                +pk+",0::BIGINT AS _sib_tx,0::BIGINT AS _sib_query FROM "+source);
        sql(database,"CREATE UNIQUE INDEX identity_row_"+id+" ON "+identities+"(_sib_row_id)");
        sql(database,"CREATE INDEX identity_key_"+id+" ON "+identities+"("+pk+")");
        sql(database,"CREATE TRIGGER sibench_row_identity AFTER INSERT ON "+identities
                +" FOR EACH ROW EXECUTE FUNCTION sibench_history.row_identity('"+id+"')");
        // An insert allocates the new row's RowID; the projection resolves later writes by key.
        function(database,"insert_"+id,"INSERT INTO "+identities+" SELECT nextval('sibench_history.row_ids'),"+pk
                +",txid_current(),sibench_history.query_id() FROM new_rows; RETURN NULL;");
        trigger(database,"insert_"+id,"AFTER INSERT",source,"REFERENCING NEW TABLE AS new_rows");
        function(database,"primary_key_"+id,"RAISE EXCEPTION 'Primary key assignments are unsupported by RowID identities';");
        trigger(database,"primary_key_"+id,"BEFORE UPDATE OF "+pk,source,"");
        sql(database,"CREATE TRIGGER sibench_row_change AFTER INSERT OR UPDATE OR DELETE ON "+source
                +" FOR EACH ROW EXECUTE FUNCTION sibench_history.row_change()");
    }

    private static void function(Connection database,String name,String body)throws SQLException {
        sql(database,"CREATE FUNCTION sibench_history."+name+"() RETURNS trigger LANGUAGE plpgsql AS "
                +literal("BEGIN "+body+" END") );
    }
    private static void trigger(Connection database,String name,String event,String source,String transition)throws SQLException {
        sql(database,"CREATE TRIGGER "+quote("sibench_"+name)+" "+event+" ON "+source+" "+transition
                +" FOR EACH STATEMENT EXECUTE FUNCTION sibench_history."+name+"()");
    }
    private static String quote(String value){return "\""+value.replace("\"","\"\"")+"\"";}
    private static String literal(String value){return "'"+value.replace("'","''")+"'";}
    private static void sql(Connection database,String sql)throws SQLException {
        try(var statement=database.createStatement()){statement.execute(sql);}
    }
}
