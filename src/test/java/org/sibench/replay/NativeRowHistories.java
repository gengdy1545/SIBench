package org.sibench.replay;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;
import org.sibench.capture.IndexCatalog;
import org.sibench.replay.SnapshotLookupValidation.History;
import org.sibench.replay.SnapshotLookupValidation.Row;
import org.sibench.trace.IndexKey;

/** Independent reverse reconstruction, retaining only one primary key's history at a time. */
final class NativeRowHistories {
    @FunctionalInterface interface Consumer {
        void accept(int table, IndexKey key, History history) throws IOException;
    }
    private record Position(long version,long query) implements Comparable<Position> {
        public int compareTo(Position other) {
            int order=Long.compare(version,other.version);
            return order==0?Long.compare(query,other.query):order;
        }
    }
    private static String quote(String name){return "\""+name.replace("\"","\"\"")+"\"";}

    static void read(Connection database,IndexCatalog catalog,Consumer consumer)throws SQLException,IOException {
        var seen=new HashSet<Integer>();
        try(var query=database.createStatement();var tables=query.executeQuery(
                "SELECT table_id,schema_name,table_name,primary_key FROM sibench_history.tables ORDER BY table_id")) {
            while(tables.next()) {
                int table=tables.getInt(1);seen.add(table);
                var indexes=catalog.definitions().stream().filter(i->i.tableId()==table)
                        .sorted(Comparator.comparingInt(IndexCatalog.Definition::indexId)).toList();
                if(indexes.isEmpty()||indexes.get(0).indexId()!=0)throw new IOException("Missing native primary index");
                String key=quote(tables.getString(4));
                String fields=indexes.stream().skip(1).map(i->","+quote(i.column())).collect(Collectors.joining());
                String empty=indexes.stream().skip(1).map(i->",NULL").collect(Collectors.joining());
                String sql="SELECT * FROM (SELECT "+key+" AS native_key,0 AS event,coalesce(c.commit_version,0) AS version,"
                        +"i._sib_query AS query_id,'' AS change,i._sib_row_id AS row_id,"
                        +"(i._sib_tx=0 OR c.commit_version IS NOT NULL) AS resolved"+empty
                        +" FROM sibench_history.identities_"+table+" i LEFT JOIN sibench_history.commits c ON i._sib_tx=c.transaction_id"
                        +" UNION ALL SELECT "+key+",1,0,0,'',0,true"+fields+" FROM "+quote(tables.getString(2))+"."+quote(tables.getString(3))
                        +" UNION ALL SELECT "+key+",2,c.commit_version,h._sib_query,h._sib_kind,0,c.commit_version IS NOT NULL"+fields
                        +" FROM sibench_history.rows_"+table+" h LEFT JOIN sibench_history.commits c ON h._sib_tx=c.transaction_id)"
                        +" ORDER BY native_key,event,version DESC,query_id DESC";
                try(var read=database.createStatement();var rows=read.executeQuery(sql)) {
                    IndexKey previous=null;var identities=new TreeMap<Position,Long>();History history=null;Row state=null;
                    boolean current=false;
                    while(rows.next()) {
                        IndexKey pk=indexes.get(0).encode(rows.getString(1));
                        if(!pk.equals(previous)) {
                            if(previous!=null)finish(table,previous,history,state,identities,consumer);
                            previous=pk;identities.clear();history=new History();state=null;current=false;
                        }
                        if(!rows.getBoolean(7))throw new IOException("Unresolved native identity or before-image");
                        int event=rows.getInt(2);long version=rows.getLong(3),statement=rows.getLong(4);
                        if(event==0) {
                            if(identities.put(new Position(version,statement),rows.getLong(6))!=null)
                                throw new IOException("Duplicate native row identity position");
                            continue;
                        }
                        if(identities.isEmpty())throw new IOException("Native row lacks identity");
                        var secondary=new LinkedHashMap<Integer,IndexKey>();
                        for(int i=1;i<indexes.size();i++)secondary.put(indexes.get(i).indexId(),indexes.get(i).encode(rows.getString(i+7)));
                        if(event==1) {
                            if(current)throw new IOException("Duplicate current primary key");
                            current=true;state=new Row(0,identities.lastEntry().getValue(),0,0,Map.copyOf(secondary));
                            continue;
                        }
                        if(version<=0)throw new IOException("Invalid native before-image version");
                        if(!history.commits.containsKey(version)) {
                            Row after=state==null?null:new Row(version,state.rowId(),0,0,state.indexes());
                            history.commits.put(version,after);history.remember(after);
                        }
                        String kind=rows.getString(5);
                        if(kind.equals("I")) {
                            if(state==null||!Objects.equals(identities.get(new Position(version,statement)),state.rowId()))
                                throw new IOException("Insert history disagrees with native identity");
                            state=null;
                        }else if(kind.equals("U")||kind.equals("D")) {
                            if(kind.equals("U")!=(state!=null))throw new IOException("Native before-image operation disagrees with row state");
                            var identity=identities.lowerEntry(new Position(version,statement));
                            if(identity==null)throw new IOException("Before-image lacks earlier row identity");
                            state=new Row(0,identity.getValue(),0,0,Map.copyOf(secondary));
                        }else throw new IOException("Unknown native before-image kind");
                    }
                    if(previous!=null)finish(table,previous,history,state,identities,consumer);
                }
            }
        }
        if(!seen.equals(catalog.definitions().stream().map(IndexCatalog.Definition::tableId).collect(Collectors.toSet())))
            throw new IOException("Native table inventory differs from source catalog");
    }

    private static void finish(int table,IndexKey key,History history,Row state,NavigableMap<Position,Long> identities,
            Consumer consumer)throws IOException {
        if(!Objects.equals(identities.get(new Position(0,0)),state==null?null:state.rowId()))
            throw new IOException("Reconstructed baseline differs from native identities");
        history.baseline=state;history.remember(state);consumer.accept(table,key,history);
    }
}
