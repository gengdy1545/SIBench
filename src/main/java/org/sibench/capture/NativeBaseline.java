package org.sibench.capture;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.List;
import org.sibench.config.Engine;
import org.sibench.trace.IndexKey;

/** Emits baseline indexes before the workload, directly from native rows and native identities. */
final class NativeBaseline {
    private NativeBaseline() {}
    /** An index value as its KV key; PostgreSQL passes its CHAR(n) rule (IndexCatalog.postgresKey). */
    interface Keys {IndexKey key(IndexCatalog.Definition definition,String value)throws IOException;}

    static Map<String,Long> write(Connection database,Engine engine,List<IndexCatalog.Definition> catalog,IndexOperationTrace.Consumer output)
            throws SQLException,IOException {
        return write(database,engine,catalog,IndexCatalog.Definition::encode,output);
    }

    static Map<String,Long> write(Connection database,Engine engine,List<IndexCatalog.Definition> catalog,Keys keys,
            IndexOperationTrace.Consumer output)throws SQLException,IOException {
        if(database.getAutoCommit())throw new SQLException("Native baseline streaming requires a caller-owned transaction");
        long count=0;var counts=new LinkedHashMap<String,Long>();
        for(var primary:catalog)if(primary.indexId()==0) {
            var indexes=new ArrayList<IndexCatalog.Definition>();indexes.add(primary);indexes.addAll(catalog.stream().filter(i->i.tableId()==primary.tableId()&&i.indexId()!=0)
                    .sorted(java.util.Comparator.comparingInt(IndexCatalog.Definition::indexId)).toList());
            String columns=String.join(",",indexes.stream().map(i->"r."+quote(i.column())).toList());
            String identities="sibench_history.identities_"+primary.tableId();
            String sql=indexes.size()==1
                    ?"SELECT _sib_row_id,"+quote(primary.column())+" FROM "+identities+" WHERE _sib_tx=0 ORDER BY "+quote(primary.column())
                    :"SELECT i._sib_row_id,"+columns+" FROM "+quote(primary.table())+" r JOIN "+identities
                            +" i USING("+quote(primary.column())+") WHERE i._sib_tx=0 ORDER BY r."+quote(primary.column());
            long rowsRead=0;
            try(var query=database.createStatement()) {
                if(engine==Engine.POSTGRES)query.setFetchSize(8192);
                try(var rows=query.executeQuery(sql)) {
                    while(rows.next()) {
                        IndexKey key=keys.key(primary,rows.getString(2));long rowId=rows.getLong(1);
                        if(key.equals(IndexKey.NULL)||rowId<=0)throw new IOException("Invalid Native baseline identity");
                        var secondary=new LinkedHashMap<Integer,IndexKey>();
                        for(int i=1;i<indexes.size();i++)secondary.put(indexes.get(i).indexId(),keys.key(indexes.get(i),rows.getString(i+2)));
                        IndexMutation.emit(IndexOperationTrace.write(0,IndexOperationTrace.INSERT,primary.tableId(),0,key,0,0,0,rowId)
                                .withWorkUnitId(1+count++/IndexOperationTrace.BLOCK_RECORDS),Map.of(),secondary,output);
                        rowsRead++;
                    }
                }
            }
            counts.put(primary.table(),rowsRead);
        }
        return counts;
    }
    private static String quote(String value){return "\""+value.replace("\"","\"\"")+"\"";}
}
