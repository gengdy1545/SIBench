package org.sibench.analysis;

import static org.junit.Assert.*;
import com.google.protobuf.ByteString;
import java.util.List;
import org.junit.Test;
import org.sibench.adapters.proto.*;

public class TraceProfileCheckTest {
    @Test public void maximumTimestampIsNotMistakenForNoObservations() {
        var profile = new TraceProfileCheck.Profile();
        profile.operation(TraceOperation.newBuilder().setDelete(DeleteRequest.newBuilder().setTs(Long.MAX_VALUE)).build(), Long.MAX_VALUE);
        profile.operation(TraceOperation.newBuilder().setGet(GetRequest.newBuilder().setReadTs(Long.MAX_VALUE)).build(), Long.MAX_VALUE);
        assertEquals(List.of(Long.MAX_VALUE, Long.MAX_VALUE), profile.report().get("write_ts_range"));
        assertEquals(List.of(Long.MAX_VALUE, Long.MAX_VALUE), profile.report().get("read_ts_range"));
    }
    @Test public void separatesPrivateContextsAndReadPointsFromSharedVersions() {
        var profile = new TraceProfileCheck.Profile();
        var key = ByteString.copyFrom(new byte[]{0, 7, (byte) 255});
        var hidden = Mutation.newBuilder().setDelete(DeleteRequest.newBuilder().setKey(key).setTs(0)).build();
        profile.operation(TraceOperation.newBuilder().setPut(PutRequest.newBuilder().setKey(key).setTs(20)).build(), 20);
        profile.operation(TraceOperation.newBuilder().setPut(PutRequest.newBuilder().setKey(key).setTs(10)).build(), 10);
        profile.operation(TraceOperation.newBuilder().setGet(GetRequest.newBuilder().setKey(key).setReadTs(15).addLocalWrites(hidden)).build(), 20);
        profile.operation(TraceOperation.newBuilder().setGet(GetRequest.newBuilder().setKey(key).setReadTs(25)).build(), 20);
        var report = profile.report();
        assertEquals(List.of(10L, 20L), report.get("write_ts_range"));
        assertEquals(List.of(15L, 25L), report.get("read_ts_range"));
        assertEquals(Long.valueOf(2), profile.counts.get("put"));
        assertFalse(profile.counts.containsKey("delete"));
        assertEquals(Long.valueOf(1), profile.counts.get("private_delete_entries"));
        assertEquals(Long.valueOf(2), profile.counts.get("reads_with_read_ts_different_from_source_ts"));
        assertEquals(2, java.util.Arrays.stream(profile.getKeys).sum());
        assertEquals(2, java.util.Arrays.stream(profile.writeKeys).sum());
    }
}
