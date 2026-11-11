package org.sibench.adapters;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.file.*;
import java.util.List;
import org.sibench.adapters.proto.*;
import org.sibench.trace.IndexKey;
import org.sibench.trace.IndexEntry;

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
            var write=Mutation.newBuilder();
            if(version==2) write.setDelete(DeleteRequest.newBuilder().setKey(KEY).setTs(version));
            else write.setPut(PutRequest.newBuilder().setKey(KEY).setTs(version).setValue(IndexEntry.rowId(70+version)));
            backend.write(write.build());
        }
        for(int pass=0;pass<2;pass++) {
            for(int timestamp=0;timestamp<=5;timestamp++) {
                var actual=backend.scan(ScanRequest.newBuilder().setReadTs(timestamp).build());
                if(timestamp==2) { if(!actual.isEmpty())throw new IOException("Deletion resurrected an older value"); }
                else require(actual,timestamp<2?70+timestamp:73);
            }
            backend.flush();
        }
    }

    static void require(List<ByteString> actual,long row) throws IOException {
        if(actual.size()!=1 || !actual.get(0).equals(IndexEntry.rowId(row)))
            throw new IOException("Historical value mismatch");
    }
}
