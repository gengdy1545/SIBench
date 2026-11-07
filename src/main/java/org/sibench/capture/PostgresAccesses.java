package org.sibench.capture;

import java.io.IOException;
import java.nio.file.Path;
import java.util.IdentityHashMap;

/** Adapts the validated PG scan/bitmap facts without guessing accesses from SQL text. */
final class PostgresAccesses {
    record Access(long process,long query,long sourceEvent,long snapshotCsn,
            PostgresAccessProjection.Decision projection) {}
    @FunctionalInterface interface Consumer {void accept(Access access)throws IOException;}
    private PostgresAccesses() {}

    static PostgresFacts.Receipt read(Path file,IndexCatalog catalog,boolean cCollation,Consumer consumer)throws IOException {
        var origins=new IdentityHashMap<PostgresAccessUnits.Unit,Long>();
        long[] process={0};
        var units=new PostgresAccessUnits(new PostgresAccessUnits.Listener() {
            @Override public void unitStarted(PostgresAccessUnits.Unit unit,com.google.gson.JsonObject event) {
                origins.put(unit,event.get("sequence").getAsLong());
            }
            @Override public void unitSettled(PostgresAccessUnits.Unit unit)throws IOException {
                var decision=PostgresAccessProjection.decide(unit,catalog,cCollation);
                if(decision.grouped())return;
                Long origin=origins.remove(unit);
                if(decision.excluded())return;
                emit(unit,origin,decision);
            }
            @Override public void groupSettled(PostgresAccessUnits.BitmapGroup group)throws IOException {
                var first=group.members.get(0);Long origin=null;
                for(var member:group.members) {
                    Long current=origins.remove(member);
                    if(current==null||member.request!=first.request||member.snapshotCsn!=first.snapshotCsn)
                        throw new IOException("PostgreSQL bitmap group crosses source read points");
                    origin=origin==null?current:Math.min(origin,current);
                }
                emit(first,origin,PostgresAccessProjection.decideGroup(group,catalog,cCollation));
            }
            private void emit(PostgresAccessUnits.Unit unit,Long origin,PostgresAccessProjection.Decision decision)throws IOException {
                if(!decision.projected())throw new IOException("Unsupported PostgreSQL index access: "+decision.reason());
                if(origin==null||unit.request==null||unit.snapshotCsn<=0||unit.snapshotSuboverflowed)
                    throw new IOException("PostgreSQL index access lacks an exact source read point");
                consumer.accept(new Access(process[0],unit.request.ordinal,origin,unit.snapshotCsn,decision));
            }
        });
        units.beginSession();
        var receipt=PostgresFacts.read(file,event->{process[0]=event.get("pid").getAsLong();units.accept(event);});
        units.endSession();
        if(!origins.isEmpty())throw new IOException("Unsettled PostgreSQL index access");
        return receipt;
    }
}
