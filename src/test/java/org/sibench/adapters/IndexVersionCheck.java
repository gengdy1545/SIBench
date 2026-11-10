package org.sibench.adapters;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.file.*;
import java.util.List;
import org.sibench.adapters.proto.*;
import org.sibench.trace.IndexKey;

/** Explicit backend acceptance with hand-derived historical results; outside timed Trace replay. */
public final class IndexVersionCheck {
    private static final ByteString KEY=ByteString.copyFrom(IndexKey.ofInt(7).bytes());

    public static void main(String[] args) throws Exception {
        var config=ServiceConfig.ofServer(Path.of(""),Path.of(args[0]));
        try(var backend=config.backend()) { check(backend); }
        Files.writeString(Path.of(args[1]),"{\"status\":\"passed\",\"scope\":\"hand-derived version, deletion and reinsert checks plus Flush; not full Trace result identities\"}\n",StandardOpenOption.CREATE_NEW);
    }

    static void check(IndexBackend backend) throws Exception {
        for(int version=0;version<=3;version++) {
            var write=Write.newBuilder().setKey(EntryKey.newBuilder().setTable(1).setPrimaryKey(KEY))
                    .setKind(version==0?Write.Kind.INSERT:version==2?Write.Kind.LOGICAL_DELETE:Write.Kind.UPSERT)
                    .setVersion(version);
            if(version!=2) write.setValue(Value.newBuilder().setRowId(70+version));
            backend.write(write.build());
        }
        for(int pass=0;pass<2;pass++) {
            for(int timestamp=0;timestamp<=5;timestamp++) {
                var actual=backend.search(Search.newBuilder().setTable(1).setReadVersion(timestamp).build());
                if(timestamp==2) { if(!actual.isEmpty())throw new IOException("Deletion resurrected an older value"); }
                else require(actual,timestamp<2?timestamp:3,timestamp<2?70+timestamp:73);
            }
            backend.flush();
        }
    }

    static void require(List<Result> actual,long version,long row) throws IOException {
        if(actual.size()!=1 || !actual.get(0).getPrimaryKey().equals(KEY)
                || actual.get(0).getVersion()!=version || actual.get(0).getValue().getRowId()!=row)
            throw new IOException("Historical key/version/RowID mismatch");
    }
}
