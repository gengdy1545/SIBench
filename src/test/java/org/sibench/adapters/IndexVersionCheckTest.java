package org.sibench.adapters;

import static org.junit.Assert.*;
import java.io.IOException;
import java.util.List;
import org.junit.Test;
import org.sibench.adapters.proto.*;

public class IndexVersionCheckTest {
    @Test public void equalCountsCannotCertifyKeysVersionsOrRowIds() throws Exception {
        var key=com.google.protobuf.ByteString.copyFrom(org.sibench.trace.IndexKey.ofInt(7).bytes());
        var result=Result.newBuilder().setKey(key).setTs(1).setValue(org.sibench.trace.IndexEntry.rowId(71)).build();
        var query=org.sibench.capture.IndexOperationTrace.search(1,1,0,
                org.sibench.capture.IndexOperationTrace.Range.equal(org.sibench.trace.IndexKey.ofInt(7)),1,1,0);
        IndexVersionCheck.require(List.of(result),1,71);
        for(var wrong:List.of(result.toBuilder().setTs(0).build(),
                result.toBuilder().setValue(org.sibench.trace.IndexEntry.rowId(70)).build(),
                result.toBuilder().setKey(com.google.protobuf.ByteString.copyFrom(
                        org.sibench.trace.IndexKey.ofInt(8).bytes())).build())) {
            var counts=new TraceRequests(ValueKind.ROW_ID);
            counts.check(query,Response.newBuilder().setStatus(Status.OK).addResults(wrong).build());
            assertEquals(1,counts.checkedResults());
            assertThrows(IOException.class,()->IndexVersionCheck.require(List.of(wrong),1,71));
        }
    }
}
