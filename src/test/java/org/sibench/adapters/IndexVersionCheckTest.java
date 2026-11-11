package org.sibench.adapters;

import com.google.protobuf.ByteString;
import static org.junit.Assert.*;
import java.io.IOException;
import java.util.List;
import org.junit.Test;
import org.sibench.adapters.proto.*;

public class IndexVersionCheckTest {
    @Test public void equalCountsCannotCertifyHistoricalValues() throws Exception {
        var result = org.sibench.trace.IndexEntry.rowId(71);
        var query=org.sibench.capture.IndexOperationTrace.search(1,1,0,
                org.sibench.capture.IndexOperationTrace.Range.equal(org.sibench.trace.IndexKey.ofInt(7)),1,1,0);
        IndexVersionCheck.require(List.of(result),71);
        // Previous-version value, another row's value, and a malformed RowID all have the right count.
        for(var wrong:List.of(org.sibench.trace.IndexEntry.rowId(70),
                org.sibench.trace.IndexEntry.rowId(999), ByteString.EMPTY)) {
            var counts=new TraceRequests(ValueKind.ROW_ID);
            counts.check(query,IndexResponse.ok(List.of(wrong)));
            assertEquals(1,counts.checkedResults());
            assertThrows(IOException.class,()->IndexVersionCheck.require(List.of(wrong),71));
        }
        assertThrows(IOException.class,()->IndexVersionCheck.require(List.of(),71));
        assertThrows(IOException.class,()->IndexVersionCheck.require(List.of(result,result),71));
    }
}
