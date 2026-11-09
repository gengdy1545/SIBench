package org.sibench.capture;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.sibench.state.SpillingMap;
import org.sibench.trace.IndexKey;

/** Ordered live transaction projection, retaining successful actions before either Commit or Abort. */
final class PostgresProjection implements PostgresAccessUnits.Listener,AutoCloseable {
    private final IndexCatalog catalog;
    private final IndexOperationTrace.Consumer output;
    private final WorkUnits.Output units;
    private final PostgresVersions versions;
    private final Path scratch;
    private final Map<Integer,PostgresAccessUnits.Detail> details=new HashMap<>();
    /** Backends with released read-only pieces whose transaction has not resolved yet. */
    private final java.util.Set<Integer> pieces=new java.util.HashSet<>();
    private final Map<Integer,PreparedStatement> identities=new HashMap<>(),insertions=new HashMap<>();
    private final Map<PostgresAccessUnits.Unit,Search> live=new IdentityHashMap<>();
    private final Map<PostgresAccessUnits.BitmapGroup,Search> groups=new IdentityHashMap<>();
    private final ArrayDeque<Object> pending=new ArrayDeque<>();
    private PostgresTransactions.Chunk chunk;
    private SpillingMap local,allocated;
    private long version,event,workUnits,currentUnit,unitEvent,readVersion,writes,statementWrites;
    private long commits,aborts,searches,mutations;

    private static final class Search {
        final long source,unit,readVersion,preceding;
        PostgresAccessProjection.Decision decision;
        Search(long source,long unit,long readVersion,long preceding) {
            this.source=source;this.unit=unit;this.readVersion=readVersion;this.preceding=preceding;
        }
    }
    private record Change(JsonObject row,long source,long unit,long ordinal) {}

    PostgresProjection(Connection database,IndexCatalog catalog,Path scratch,PostgresVersions versions,
            WorkUnits.Output units,IndexOperationTrace.Consumer output)throws SQLException {
        this.catalog=catalog;this.scratch=scratch;this.versions=versions;this.units=units;this.output=output;
        for(var primary:catalog.definitions())if(primary.indexId()==0) {
            String key=quote(primary.column()),cast=switch(primary.type()){case "int"->"bigint";case "timestamp"->"timestamp";default->"text";};
            String table="sibench_history.identities_"+primary.tableId();
            // Baseline identities need no commit lookup; recent mappings may still await their native batch.
            var identity=database.prepareStatement("SELECT i._sib_row_id,i._sib_tx,CASE WHEN i._sib_tx=0 THEN 0 ELSE "
                    +"(SELECT c.commit_version FROM sibench_history.commits c WHERE c.transaction_id=i._sib_tx) END "
                    +"FROM "+table+" i WHERE i."+key+"=?::"+cast+" ORDER BY i._sib_row_id DESC");
            identity.setFetchSize(64);identities.put(primary.tableId(),identity);
            insertions.put(primary.tableId(),database.prepareStatement("SELECT _sib_row_id FROM "+table
                    +" WHERE "+key+"=?::"+cast+" AND _sib_tx=? AND _sib_query=?"));
        }
    }

    /** Projects one chunk; returns whether it resolved a source transaction with requests. */
    boolean accept(PostgresTransactions.Chunk source)throws IOException,SQLException {
        chunk=source;writes=0;statementWrites=0;currentUnit=0;
        // A piece's searches are standalone snapshot reads; its transaction resolves with its outcome.
        if(source.piece)pieces.add(source.pid);
        boolean split=!source.piece&&pieces.remove(source.pid);
        version=split||!source.piece&&source.hasRequests?versions.begin(source):0;
        try(var rowIds=new SpillingMap(scratch.resolve("local-identities"),65536,4);
                var abortIds=new SpillingMap(scratch.resolve("aborted-identities"),65536,4)) {
            local=rowIds;allocated=abortIds;
            // Aborted allocations no longer exist in PG; only their identity facts need lookahead.
            if(!source.committed)source.read(e->{
                if(e.get("event").getAsString().equals("row_identity")&&e.get("req").getAsLong()<=source.lastCompletedRequest) {
                    int table=e.get("table_id").getAsInt();
                    allocated.put(identityKey(table,catalog.postgresKey(catalog.primary(table),text(e.get("primary_key"))),e.get("req").getAsLong()),e.get("row_id").getAsLong());
                }
            });
            var tracker=new PostgresAccessUnits(this);
            if(details.containsKey(source.pid))tracker.continueSession(details.get(source.pid));else tracker.beginSession();
            if(version>0)output.accept(IndexOperationTrace.transaction(0,IndexOperationTrace.TRANSACTION_BEGIN,version,version,0));
            source.read(e->{
                event++;
                if(e.get("event").getAsString().equals("executor_start")&&e.get("depth").getAsInt()==1)drain();
                tracker.accept(e);
                if(e.get("event").getAsString().equals("row_change")
                        &&(source.committed||e.get("req").getAsLong()<=source.lastCompletedRequest)) {
                    if(version==0||currentUnit==0||e.get("xid").getAsLong()!=source.transaction)
                        throw new IOException("PG mutation lacks its source transaction/request");
                    pending.add(new Change(e,unitEvent,currentUnit,++writes));statementWrites++;
                }
                drain();
                if(pending.size()>ProtoTrace.MAX_TRANSACTION_OPERATIONS)
                    throw new IOException("Unsettled PG operation prefix exceeds the buffer limit");
            });
            details.put(source.pid,tracker.detail());tracker.endSession();drain();
            if(!pending.isEmpty()||!live.isEmpty()||!groups.isEmpty())throw new IOException("PG transaction has unsettled accesses");
            if(version>0) {
                output.accept(IndexOperationTrace.transaction(0,source.committed?IndexOperationTrace.TRANSACTION_COMMIT:
                        IndexOperationTrace.TRANSACTION_ABORT,version,version,writes));
                versions.finish(source);if(source.committed)commits++;else aborts++;
            }
        }finally{local=null;allocated=null;chunk=null;}
        return version>0;
    }

    @Override public void requestStarted(PostgresAccessUnits.Request request,JsonObject event)throws IOException {
        startRequest(event);
    }
    private void startRequest(JsonObject event)throws IOException {
        if(version==0&&!chunk.piece)throw new IOException("PG request lacks a canonical transaction");
        currentUnit=++workUnits;unitEvent=this.event;statementWrites=0;
        try {
            readVersion=versions.readVersion(event.get("snapshot_csn").getAsLong());
            if(!chunk.piece&&readVersion>=version)throw new IOException("PG statement sees its own or a future Trace resolution");
        }catch(SQLException failure){throw new IOException("Cannot bind PG statement snapshot",failure);}
        units.lines().write("{\"work_unit_id\":"+currentUnit+",\"source_event\":"+unitEvent+"}\n");
    }

    @Override public void unitStarted(PostgresAccessUnits.Unit unit,JsonObject event)throws IOException {
        if(unit.source==PostgresAccessUnits.Source.PLANNER_PROBE)return;
        if(statementWrites>0)throw new IOException("PG index access interleaves same-statement writes");
        if(unit.snapshotCsn<=0||unit.snapshotSuboverflowed)throw new IOException("PG access has no exact snapshot");
        try{if(versions.readVersion(unit.snapshotCsn)!=readVersion)throw new IOException("PG scan and statement snapshots differ");}
        catch(SQLException failure){throw new IOException(failure);}
        // A group has one origin: the first unit it had, even when that unit later left it.
        if(unit.group!=null&&groups.containsKey(unit.group))return;
        var search=new Search(unitEvent,currentUnit,readVersion,writes);pending.add(search);
        if(unit.group==null)live.put(unit,search);else groups.put(unit.group,search);
    }
    @Override public void unitSettled(PostgresAccessUnits.Unit unit)throws IOException {
        if(unit.group!=null)return;
        var decision=PostgresAccessProjection.decide(unit,catalog,true);var search=live.remove(unit);
        if(decision.excluded()){if(search!=null)pending.remove(search);return;}
        settle(search,decision,unit.request);
    }
    @Override public void groupFormed(PostgresAccessUnits.BitmapGroup group)throws IOException {
        Search first=null;
        for(var unit:group.members) {
            var search=live.remove(unit);if(search==null)throw new IOException("PG bitmap member lacks an operation origin");
            if(first==null)first=search;else pending.remove(search);
        }
        groups.put(group,first);
    }
    @Override public void groupSettled(PostgresAccessUnits.BitmapGroup group)throws IOException {
        var decision=PostgresAccessProjection.decideGroup(group,catalog,true);var search=groups.remove(group);
        if(decision.excluded()){if(search!=null)pending.remove(search);return;}
        settle(search,decision,group.members.get(0).request);
    }
    private void settle(Search search,PostgresAccessProjection.Decision decision,PostgresAccessUnits.Request request)throws IOException {
        if(search==null||!decision.projected())throw new IOException("Unsupported PG index access: "+decision.reason()
                +" (plan "+request.planSignature()+"; SQL "+request.sql+")");
        search.decision=decision;
    }
    private void drain()throws IOException {
        while(!pending.isEmpty()) {
            Object item=pending.peekFirst();
            if(item instanceof Search search&&search.decision==null)return;
            pending.removeFirst();
            if(item instanceof Search search)search(search);else change((Change)item);
        }
    }
    private void search(Search search)throws IOException {
        var decision=search.decision;
        int group=switch(decision.mode()) {
            case INDEPENDENT->0;case UNION->IndexOperationTrace.UNION_GROUP;case INTERSECTION->IndexOperationTrace.INTERSECT_GROUP;
        };
        int size=group==0?0:decision.parts().size(),position=0;
        int flags=group|(search.preceding>0?IndexOperationTrace.TRANSACTION_LOCAL_SEARCH:IndexOperationTrace.SNAPSHOT_SEARCH);
        for(var part:decision.parts())output.accept(IndexOperationTrace.search(search.source,part.table(),part.index(),
                part.range(),part.pinned(),search.readVersion,size==0?part.count():decision.count(),flags,
                size==0?0:position++,size).withTransaction(version,search.preceding)
                .withVersion(search.preceding>0?version:0).withWorkUnitId(search.unit));
        searches++;
    }
    private void change(Change change)throws IOException {
        JsonObject row=change.row();int table=catalog.tableId(row.get("relation").getAsString());
        String action=row.get("operation").getAsString();
        JsonObject before=row.get("old").isJsonNull()?null:row.getAsJsonObject("old");
        JsonObject after=row.get("new").isJsonNull()?null:row.getAsJsonObject("new");
        if(!List.of("insert","update","delete").contains(action)||action.equals("insert")!=(before==null)
                ||action.equals("delete")!=(after==null))throw new IOException("Invalid PG row-change images");
        var primary=catalog.primary(table);
        IndexKey key=catalog.postgresKey(primary,text((after==null?before:after).get(primary.column())));
        if(key.equals(IndexKey.NULL)||before!=null&&!catalog.postgresKey(primary,text(before.get(primary.column()))).equals(key))
            throw new IOException("PG row change has a missing or changed primary key");
        byte[] identity=identityKey(table,key,0);long rowId;
        try {
            if(action.equals("insert")) {
                long request=row.get("req").getAsLong();
                if(chunk.committed) {
                    var statement=insertions.get(table);statement.setString(1,text(after.get(primary.column())));
                    statement.setLong(2,chunk.transaction);statement.setLong(3,request);
                    try(var rows=statement.executeQuery()) {
                        if(!rows.next())throw new IOException("PG insertion lacks its native RowID");rowId=rows.getLong(1);
                        if(rows.next())throw new IOException("PG insertion has ambiguous native RowIDs");
                    }
                }else rowId=allocated.get(identityKey(table,key,request));
            }else {
                rowId=local.get(identity);
                if(rowId==SpillingMap.ABSENT) {
                    var statement=identities.get(table);statement.setString(1,text(before.get(primary.column())));
                    try(var rows=statement.executeQuery()) {
                        while(rows.next()) {
                            long transaction=rows.getLong(2),committed=versions.commitVersion(transaction,rows.getLong(3));
                            if(transaction==0||committed>0&&committed<version){rowId=rows.getLong(1);break;}
                        }
                    }
                    if(rowId==SpillingMap.ABSENT)throw new IOException("PG mutation lacks its native RowID");
                }
            }
        }catch(SQLException failure){throw new IOException("Cannot resolve native PG row identity",failure);}
        if(rowId<=0)throw new IOException("Invalid native PG row identity");
        local.put(identity,action.equals("delete")?0:rowId);
        int kind=action.equals("delete")?IndexOperationTrace.LOGICAL_DELETE:IndexOperationTrace.UPSERT;
        int flags=action.equals("insert")?IndexOperationTrace.BUSINESS_INSERT:action.equals("update")?IndexOperationTrace.BUSINESS_UPDATE:0;
        IndexMutation.emit(IndexOperationTrace.write(change.source(),kind,table,flags,key,version,0,0,rowId)
                .withTransaction(version,change.ordinal()).withWorkUnitId(change.unit()),keys(table,before),keys(table,after),output);
        mutations++;
    }
    private Map<Integer,IndexKey> keys(int table,JsonObject row)throws IOException {
        if(row==null)return Map.of();var keys=new LinkedHashMap<Integer,IndexKey>();
        for(var index:catalog.secondary(table)) {
            if(!row.has(index.column()))throw new IOException("Missing indexed source column");
            keys.put(index.indexId(),catalog.postgresKey(index,text(row.get(index.column()))));
        }
        return keys;
    }
    private static byte[] identityKey(int table,IndexKey key,long query) {
        byte[] bytes=key.bytes();return ByteBuffer.allocate(12+bytes.length).putInt(table).putLong(query).put(bytes).array();
    }
    private static String text(JsonElement value){return value==null||value.isJsonNull()?null:value.getAsString();}
    private static String quote(String value){return "\""+value.replace("\"","\"\"")+"\"";}
    Map<String,Object> counts(){return Map.of("source_work_units",workUnits,"committed_transactions",commits,
            "aborted_transactions",aborts,"searched_accesses",searches,"logical_row_changes",mutations);}
    @Override public void close()throws SQLException {
        for(var statement:identities.values())statement.close();for(var statement:insertions.values())statement.close();
    }
}
