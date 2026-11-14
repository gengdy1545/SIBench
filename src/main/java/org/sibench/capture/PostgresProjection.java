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
import java.util.ArrayList;
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
    private final boolean secondaryIndexes;
    private final PostgresQueries coverage;
    private final Map<Integer,PostgresAccessUnits.Detail> details=new HashMap<>();
    private final Map<Integer,PreparedStatement> identities=new HashMap<>();
    private final Map<PostgresAccessUnits.Unit,Search> live=new IdentityHashMap<>();
    private final Map<PostgresAccessUnits.BitmapGroup,Search> groups=new IdentityHashMap<>();
    private final ArrayDeque<Object> pending=new ArrayDeque<>();
    private final ArrayList<CommandWrite> ownWrites=new ArrayList<>();
    private final ArrayDeque<Long> executors=new ArrayDeque<>();
    private final ArrayDeque<Integer> subtransactionDepths=new ArrayDeque<>();
    private PostgresTransactions.Chunk chunk;
    private SpillingMap local,allocated;
    private long version,event,workUnits,currentUnit,unitEvent,writes,allocations,insertions;
    private long commits,aborts,searches,mutations;

    private static final class Search {
        final long source,unit,readVersion,preceding,cid,sequence,execution;
        final PostgresAccessUnits.Request request;
        PostgresAccessProjection.Decision decision;
        Search(long source,long unit,long readVersion,long preceding,long cid,long sequence,long execution,PostgresAccessUnits.Request request) {
            this.source=source;this.unit=unit;this.readVersion=readVersion;this.preceding=preceding;
            this.cid=cid;this.sequence=sequence;this.execution=execution;this.request=request;
        }
    }
    private record CommandWrite(long cid,long rollback,long previous,IndexOperationTrace.Operation operation) {}
    /** An insert's allocation key names the native identity fact that must arrive before it is projected. */
    private record Change(JsonObject row,long source,long unit,long ordinal,byte[] allocation) {}

    PostgresProjection(Connection database,IndexCatalog catalog,Path scratch,PostgresVersions versions,
            WorkUnits.Output units,boolean secondaryIndexes,IndexOperationTrace.Consumer output,PostgresQueries coverage)throws SQLException {
        this.catalog=catalog;this.secondaryIndexes=secondaryIndexes;this.scratch=scratch;this.versions=versions;this.units=units;this.output=output;
        this.coverage=coverage;
        for(var primary:catalog.definitions())if(primary.indexId()==0) {
            String key=quote(primary.column()),cast=switch(primary.type()){case "int"->"bigint";case "timestamp"->"timestamp";default->"text";};
            String table="sibench_history.identities_"+primary.tableId();
            // Baseline identities need no commit lookup; recent mappings may still await their native batch.
            var identity=database.prepareStatement("SELECT i._sib_row_id,i._sib_tx,CASE WHEN i._sib_tx=0 THEN 0 ELSE "
                    +"(SELECT c.commit_version FROM sibench_history.commits c WHERE c.transaction_id=i._sib_tx) END "
                    +"FROM "+table+" i WHERE i."+key+"=?::"+cast+" ORDER BY i._sib_row_id DESC");
            identity.setFetchSize(64);identities.put(primary.tableId(),identity);
        }
    }

    /** Projects one chunk; returns whether it resolved a source transaction with requests. */
    boolean accept(PostgresTransactions.Chunk source)throws IOException,SQLException {
        Path localDirectory=scratch.resolve("local-identities"),allocationDirectory=scratch.resolve("aborted-identities");
        if(java.nio.file.Files.exists(localDirectory,java.nio.file.LinkOption.NOFOLLOW_LINKS)
                ||java.nio.file.Files.exists(allocationDirectory,java.nio.file.LinkOption.NOFOLLOW_LINKS))
            throw new IOException("PG transaction identity scratch already exists");
        chunk=source;writes=0;currentUnit=0;allocations=0;insertions=0;
        version=source.hasRequests?versions.begin(source):0;
        try(var rowIds=new SpillingMap(localDirectory,65536,4);
                var abortIds=new SpillingMap(allocationDirectory,65536,4)) {
            local=rowIds;allocated=abortIds;
            var tracker=new PostgresAccessUnits(this);
            if(details.containsKey(source.pid))tracker.continueSession(details.get(source.pid));else tracker.beginSession();
            if(version>0)output.accept(IndexOperationTrace.transaction(0,IndexOperationTrace.TRANSACTION_BEGIN,version,version,0));
            source.read(e->{
                event++;
                if(e.get("event").getAsString().equals("executor_start")&&e.get("depth").getAsInt()==1)drain();
                String type=e.get("event").getAsString();
                if(type.equals("executor_start"))executors.push(e.get("sequence").getAsLong());
                if(type.equals("subtransaction_begin"))subtransactionDepths.push(executors.size());
                tracker.accept(e);
                byte[] allocation=type.equals("row_change")?allocation(e):null;
                // Successful statements of an aborted transaction stay visible to its later reads.
                if(type.equals("row_identity"))allocate(e);
                if(type.equals("row_change")&&source.neededForRead(e)) {
                    if(version==0||currentUnit==0||e.get("xid").getAsLong()!=source.transaction)
                        throw new IOException("PG mutation lacks its source transaction/request");
                    if(source.completed(e.get("req").getAsLong())&&source.survived(e))writes++;
                    pending.add(new Change(e,unitEvent,currentUnit,writes,allocation));
                }
                if(type.equals("executor_end"))executors.pop();
                if(type.equals("subtransaction_commit")||type.equals("subtransaction_abort")) {
                    int depth=subtransactionDepths.pop();
                    if(type.equals("subtransaction_abort"))while(executors.size()>depth)executors.pop();
                }
                if(type.equals("transaction_abort"))executors.clear();
                drain();
                if(pending.size()>ProtoTrace.MAX_TRANSACTION_OPERATIONS)
                    throw new IOException("Unsettled PG operation prefix exceeds the buffer limit");
            });
            details.put(source.pid,tracker.detail());tracker.endSession();drain();
            if(pending.peekFirst() instanceof Change)throw new IOException("PG insertion lacks its native RowID");
            if(!pending.isEmpty()||!live.isEmpty()||!groups.isEmpty())throw new IOException("PG transaction has unsettled accesses");
            if(allocations!=insertions)throw new IOException("PG native RowID allocations do not match insertions");
            if(version>0) {
                output.accept(IndexOperationTrace.transaction(0,source.committed?IndexOperationTrace.TRANSACTION_COMMIT:
                        IndexOperationTrace.TRANSACTION_ABORT,version,version,writes));
                versions.finish(source);if(source.committed)commits++;else aborts++;
            }
            if(coverage!=null)coverage.endTransaction();
        }finally{ownWrites.clear();executors.clear();subtransactionDepths.clear();local=null;allocated=null;chunk=null;}
        for(Path directory:List.of(localDirectory,allocationDirectory))
            if(java.nio.file.Files.exists(directory))Generation.deleteTree(directory);
        return version>0;
    }

    @Override public void requestStarted(PostgresAccessUnits.Request request,JsonObject event)throws IOException {
        startRequest(event);
        if(coverage!=null)coverage.start(request,currentUnit);
    }
    @Override public void requestFinished(PostgresAccessUnits.Request request) {
        if(coverage!=null)coverage.finish(request,chunk.completed(request.ordinal));
    }
    private void startRequest(JsonObject event)throws IOException {
        if(version==0)throw new IOException("PG request lacks a canonical transaction");
        currentUnit=++workUnits;unitEvent=this.event;
        try {
            long readVersion=versions.readVersion(event.get("snapshot_csn").getAsLong());
            if(readVersion>=version)throw new IOException("PG statement sees its own or a future Trace resolution");
        }catch(SQLException failure){throw new IOException("Cannot bind PG statement snapshot",failure);}
        units.lines().write("{\"work_unit_id\":"+currentUnit+",\"source_event\":"+unitEvent+"}\n");
    }

    @Override public void unitStarted(PostgresAccessUnits.Unit unit,JsonObject event)throws IOException {
        if(unit.source==PostgresAccessUnits.Source.PLANNER_PROBE)return;
        if(unit.snapshotCsn<=0||unit.snapshotCid<0||unit.snapshotCid>=0xffffffffL||unit.snapshotSuboverflowed)
            throw new IOException("PG access has no exact snapshot");
        long readVersion;
        try{readVersion=versions.readVersion(unit.snapshotCsn);}
        catch(SQLException failure){throw new IOException(failure);}
        if(readVersion>=version)throw new IOException("PG access sees its own or a future Trace resolution");
        // A group has one origin: the first unit it had, even when that unit later left it.
        if(unit.group!=null&&groups.containsKey(unit.group)) {
            var first=groups.get(unit.group);
            if(first.readVersion!=readVersion||first.cid!=unit.snapshotCid)
                throw new IOException("PG bitmap inputs have different visibility");
            return;
        }
        if(executors.isEmpty())throw new IOException("PG access lacks an executor origin");
        var search=new Search(unitEvent,currentUnit,readVersion,writes,unit.snapshotCid,event.get("sequence").getAsLong(),executors.peek(),unit.request);pending.add(search);
        if(unit.group==null)live.put(unit,search);else groups.put(unit.group,search);
    }
    @Override public void unitSettled(PostgresAccessUnits.Unit unit)throws IOException {
        if(unit.source==PostgresAccessUnits.Source.PLANNER_PROBE)return;
        if(coverage!=null)coverage.index(unit);
        if(unit.group!=null)return;
        var search=live.remove(unit);
        if(search!=null&&!chunk.executionCompleted(search.execution)){pending.remove(search);return;}
        var decision=PostgresAccessProjection.decide(unit,catalog,true);
        if(decision.excluded()){if(search!=null)pending.remove(search);return;}
        settle(search,decision,unit.request);
    }
    @Override public void groupFormed(PostgresAccessUnits.BitmapGroup group)throws IOException {
        Search first=null;
        for(var unit:group.members) {
            var search=live.remove(unit);if(search==null)throw new IOException("PG bitmap member lacks an operation origin");
            if(first==null)first=search;else {
                if(first.readVersion!=search.readVersion||first.cid!=search.cid)
                    throw new IOException("PG bitmap inputs have different visibility");
                pending.remove(search);
            }
        }
        groups.put(group,first);
    }
    @Override public void groupSettled(PostgresAccessUnits.BitmapGroup group)throws IOException {
        var search=groups.remove(group);
        if(search!=null&&!chunk.executionCompleted(search.execution)){pending.remove(search);return;}
        var decision=PostgresAccessProjection.decideGroup(group,catalog,true);
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
            // The statement-level identity trigger reports an insert's RowID after its row change.
            if(item instanceof Change change&&change.allocation()!=null&&allocated.get(change.allocation())<=0)return;
            pending.removeFirst();
            if(item instanceof Search search)search(search);else change((Change)item);
        }
    }
    private void search(Search search)throws IOException {
        var decision=search.decision;
        if(coverage!=null)coverage.projected(search.request,decision.parts().size());
        int group=switch(decision.mode()) {
            case INDEPENDENT->0;case UNION->IndexOperationTrace.UNION_GROUP;case INTERSECTION->IndexOperationTrace.INTERSECT_GROUP;
        };
        int size=group==0?0:decision.parts().size(),position=0;
        int flags=group|IndexOperationTrace.SNAPSHOT_SEARCH;
        for(var part:decision.parts()) {
            var read=IndexOperationTrace.search(search.source,part.table(),part.index(),
                part.range(),part.pinned(),search.readVersion,size==0?part.count():decision.count(),flags,
                size==0?0:position++,size).withTransaction(version,search.preceding)
                .withWorkUnitId(search.unit);
            var visible=new java.util.TreeMap<com.google.protobuf.ByteString,IndexOperationTrace.Operation>(
                    com.google.protobuf.ByteString.unsignedLexicographicalComparator());
            // A scan hides its own command's writes and writes by later nested commands.
            for(var write:ownWrites)if(write.cid()<search.cid&&write.rollback()>search.sequence&&read.containsWrite(write.operation())) {
                var operation=write.operation();
                var key=new org.sibench.trace.IndexEntry(operation.table(),operation.index(),operation.indexedValue(),operation.key()).bytes();
                visible.put(key,IndexOperationTrace.privateWrite(operation));
            }
            output.accept(read.withLocalWrites(List.copyOf(visible.values())));
        }
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
            if(action.equals("insert")){rowId=allocated.get(change.allocation());insertions++;}
            else {
                long previous=local.get(identity);
                rowId=SpillingMap.ABSENT;
                while(previous!=SpillingMap.ABSENT) {
                    var write=ownWrites.get(Math.toIntExact(previous));
                    if(write.rollback()>row.get("sequence").getAsLong()) {
                        rowId=write.operation().kind()==IndexOperationTrace.LOGICAL_DELETE?0:write.operation().rowId();break;
                    }
                    previous=write.previous();
                }
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
        int kind=action.equals("delete")?IndexOperationTrace.LOGICAL_DELETE:IndexOperationTrace.UPSERT;
        int flags=action.equals("insert")?IndexOperationTrace.BUSINESS_INSERT:action.equals("update")?IndexOperationTrace.BUSINESS_UPDATE:0;
        if(!row.has("write_cid"))throw new IOException("PG mutation lacks its native write command");
        long cid=row.get("write_cid").getAsLong();
        if(cid<0||cid>=0xffffffffL)throw new IOException("Invalid PG write command");
        IndexMutation.emit(IndexOperationTrace.write(change.source(),kind,table,flags,key,version,0,0,rowId)
                .withTransaction(version,change.ordinal()).withWorkUnitId(change.unit()),keys(table,before),keys(table,after),operation->{
                    if(ownWrites.size()>=ProtoTrace.MAX_TRANSACTION_OPERATIONS)
                        throw new IOException("PG private write history exceeds the operation limit");
                    long previous=operation.index()==0?local.get(identity):SpillingMap.ABSENT;
                    ownWrites.add(new CommandWrite(cid,chunk.rollbackAt(row),previous,operation));
                    if(operation.index()==0)local.put(identity,ownWrites.size()-1);
                    IndexOperationTrace.validate(operation);
                    if(chunk.completed(row.get("req").getAsLong())&&chunk.survived(row))output.accept(operation);
                });
        mutations++;
    }
    private void allocate(JsonObject event)throws IOException {
        int table=event.get("table_id").getAsInt();
        byte[] key=allocationKey(table,catalog.postgresKey(catalog.primary(table),text(event.get("primary_key"))),event,false);
        long rowId=event.get("row_id").getAsLong();
        if(rowId<=0)throw new IOException("Invalid native PG RowID allocation");
        long expected=allocated.get(key);
        if(expected==SpillingMap.ABSENT) {
            if(chunk.completed(event.get("req").getAsLong()))throw new IOException("PG allocation lacks its insertion");
            return;
        }
        if(expected!=0)throw new IOException("PG insertion has ambiguous native RowIDs");
        allocated.put(key,rowId);allocations++;
    }
    private byte[] allocation(JsonObject row)throws IOException {
        if(!row.get("operation").getAsString().equals("insert")||row.get("new").isJsonNull())return null;
        int table=catalog.tableId(row.get("relation").getAsString());var primary=catalog.primary(table);
        byte[] key=allocationKey(table,catalog.postgresKey(primary,text(row.getAsJsonObject("new").get(primary.column()))),row,true);
        if(chunk.neededForRead(row))allocated.put(key,0);
        return key;
    }
    /** Every native index column must be captured; only exported secondary keys are built. */
    private Map<Integer,IndexKey> keys(int table,JsonObject row)throws IOException {
        if(row==null)return Map.of();var keys=new LinkedHashMap<Integer,IndexKey>();
        for(var index:catalog.secondary(table)) {
            if(!row.has(index.column()))throw new IOException("Missing indexed source column");
            if(secondaryIndexes)keys.put(index.indexId(),catalog.postgresKey(index,text(row.get(index.column()))));
        }
        return keys;
    }
    private static byte[] identityKey(int table,IndexKey key,long query) {
        byte[] bytes=key.bytes();return ByteBuffer.allocate(12+bytes.length).putInt(table).putLong(query).put(bytes).array();
    }
    private byte[] allocationKey(int table,IndexKey key,JsonObject row,boolean insertion)throws IOException {
        byte[] bytes=key.bytes();
        var counter=ByteBuffer.allocate(21+bytes.length).put((byte)(insertion?0:1)).putInt(table)
                .putLong(row.get("req").getAsLong()).putLong(row.get("subid").getAsLong()).put(bytes).array();
        long count=allocated.get(counter);count=count==SpillingMap.ABSENT?1:Math.addExact(count,1);
        allocated.put(counter,count);
        return ByteBuffer.allocate(29+bytes.length).put((byte)2).put(counter,1,counter.length-1).putLong(count).array();
    }
    private static String text(JsonElement value){return value==null||value.isJsonNull()?null:value.getAsString();}
    private static String quote(String value){return "\""+value.replace("\"","\"\"")+"\"";}
    Map<String,Object> counts(){return Map.of("source_work_units",workUnits,"committed_transactions",commits,
            "aborted_transactions",aborts,"searched_accesses",searches,"logical_row_changes",mutations);}
    @Override public void close()throws SQLException {
        for(var statement:identities.values())statement.close();
    }
}
