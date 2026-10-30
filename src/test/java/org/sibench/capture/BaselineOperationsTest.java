package org.sibench.capture;

import static org.junit.Assert.*;
import org.junit.Test;

public class BaselineOperationsTest {
    @Test
    public void smallHeapCannotPrefetchSeveralLargeDecodedGroups() {
        var small = BaselineOperations.window(2L << 30, 480L << 20, 5, 1048576);
        assertEquals(1, small.groups());
        assertTrue(small.chunks() < BaselineOperations.CHUNKS_AHEAD);
        assertTrue(small.encoders() <= small.chunks());
        var large = BaselineOperations.window(16L << 30, 480L << 20, 5, 1048576);
        assertEquals(BaselineOperations.GROUPS_AHEAD, large.groups());
        assertEquals(BaselineOperations.CHUNKS_AHEAD, large.chunks());
        // Even a group larger than its scheduling budget must make progress with one permit.
        var oversized = BaselineOperations.window(256L << 20, 480L << 20, 5, 1048576);
        assertEquals(1, oversized.groups());
        assertTrue(oversized.chunks() >= 1);
    }
}
