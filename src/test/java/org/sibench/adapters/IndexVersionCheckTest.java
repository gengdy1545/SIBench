package org.sibench.adapters;

import static org.junit.Assert.*;
import java.io.IOException;
import java.util.List;
import org.junit.Test;
import org.sibench.adapters.proto.*;

public class IndexVersionCheckTest {
    @Test public void sameCountWithWrongVersionIsRejected() {
        var key=com.google.protobuf.ByteString.copyFrom(org.sibench.trace.IndexKey.ofInt(7).bytes());
        var result=Result.newBuilder().setPrimaryKey(key).setVersion(1).setValue(Value.newBuilder().setRowId(71)).build();
        assertThrows(IOException.class,()->IndexVersionCheck.require(List.of(result),0,70));
        assertThrows(IOException.class,()->IndexVersionCheck.require(List.of(result),0,71));
        assertThrows(IOException.class,()->IndexVersionCheck.require(List.of(result),1,70));
    }
}
