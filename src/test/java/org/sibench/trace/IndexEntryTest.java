package org.sibench.trace;

import static org.junit.Assert.*;
import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.util.*;
import org.junit.Test;
import org.sibench.state.ByteKeys;

public class IndexEntryTest {
    @Test public void escapedBytesPreserveUnsignedOrderAndPrefixIdentity() throws Exception {
        var keys = List.of(new byte[0], new byte[]{0}, new byte[]{0,0}, new byte[]{0,1},
                new byte[]{0,(byte)255}, new byte[]{1}, new byte[]{1,0}, new byte[]{(byte)255});
        for (int i=0;i<keys.size();i++) {
            var encoded=ByteKeys.encode(keys.get(i));
            var input=ByteBuffer.wrap(encoded);
            assertArrayEquals(keys.get(i),ByteKeys.decode(input)); assertFalse(input.hasRemaining());
            if(i>0) assertTrue(Arrays.compareUnsigned(ByteKeys.encode(keys.get(i-1)),encoded)<0);
        }
        assertArrayEquals(new byte[]{0,(byte)255,0,0},ByteKeys.encode(new byte[]{0}));
        for(var invalid:List.of(new byte[0],new byte[]{0},new byte[]{0,2},new byte[]{1}))
            assertThrows(java.io.IOException.class,()->ByteKeys.decode(ByteBuffer.wrap(invalid)));
    }

    @Test public void nonuniqueEntriesHaveDistinctKeysAndOnePrefixInterval() throws Exception {
        var lower=IndexEntry.bound(1,2,IndexKey.ofText("a"),false).toByteArray();
        var upper=IndexEntry.bound(1,2,IndexKey.ofText("a"),true).toByteArray();
        var first=new IndexEntry(1,2,IndexKey.ofText("a"),IndexKey.ofInt(1));
        var second=new IndexEntry(1,2,IndexKey.ofText("a"),IndexKey.ofInt(2));
        assertNotEquals(first.bytes(),second.bytes());
        for(var entry:List.of(first,second)) {
            assertEquals(entry,IndexEntry.decode(entry.bytes()));
            assertTrue(Arrays.compareUnsigned(entry.bytes().toByteArray(),lower)>=0);
            assertTrue(Arrays.compareUnsigned(entry.bytes().toByteArray(),upper)<0);
        }
        var next=new IndexEntry(1,2,IndexKey.ofText("aa"),IndexKey.ofInt(1));
        assertTrue(Arrays.compareUnsigned(next.bytes().toByteArray(),upper)>=0);
        assertEquals(new IndexEntry.Boundary(IndexKey.ofText("a"),true),IndexEntry.boundary(ByteString.copyFrom(upper),1,2,true));
    }
}
