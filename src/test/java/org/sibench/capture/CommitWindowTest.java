package org.sibench.capture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import java.io.IOException;

public class CommitWindowTest {
    private static CommitWindow window(int limit, int commits) {
        var window = new CommitWindow(limit);
        // Counters 10, 20, 30...; version n at counter 10n; XID 1000 + n.
        for (int n = 1; n <= commits; n++) window.add(10L * n, 1000 + n, n);
        return window;
    }

    @Test
    public void aSnapshotSeesTheLastWriterAtOrBeforeItsCounter() throws Exception {
        var window = window(100, 50);
        assertEquals(0, window.versionAt(0));
        assertEquals(0, window.versionAt(9));
        assertEquals(1, window.versionAt(10));
        assertEquals(1, window.versionAt(19));
        assertEquals(2, window.versionAt(20));
        assertEquals(50, window.versionAt(10_000));
        assertEquals(1005, window.xidAt(57));
        assertEquals(0, window.xidAt(5));
        assertEquals(150L, window.counterOf(1015).longValue());
        assertNull(window.counterOf(7));
        assertEquals(0, window.evicted());
        assertEquals(50, window.size());
    }

    @Test
    public void theWindowKeepsOnlyTheNewestCommitsAndAnswersTheSameInsideIt() throws Exception {
        var small = window(10, 1_000);
        var large = window(1_000, 1_000);
        assertEquals(10, small.size());
        assertEquals(990, small.evicted());
        for (long csn = 9_910; csn <= 10_500; csn += 3)
            assertEquals(large.versionAt(csn), small.versionAt(csn));
        for (long csn = 9_910; csn <= 10_500; csn += 7)
            assertEquals(large.xidAt(csn), small.xidAt(csn));
        // An evicted XID is no longer known; a retained one still is.
        assertNull(small.counterOf(1001));
        assertEquals(10_000L, small.counterOf(2_000).longValue());
    }

    @Test
    public void aSnapshotBehindTheWindowFailsInsteadOfSeeingNoCommit() throws Exception {
        var window = window(10, 1_000);
        // The oldest retained commit is counter 9_910; the evicted one before it was 9_900.
        assertEquals(991, window.versionAt(9_910));
        var between = assertThrows(IOException.class, () -> window.versionAt(9_905));
        assertTrue(between.getMessage(), between.getMessage().contains("behind the retained commit window"));
        assertThrows(IOException.class, () -> window.versionAt(0));
        assertThrows(IOException.class, () -> window.xidAt(100));
        // Without any eviction a snapshot before the first commit simply sees nothing.
        assertEquals(0, window(10, 5).versionAt(3));
    }

    @Test
    public void countersMustGrow() {
        var window = window(10, 3);
        assertThrows(IllegalStateException.class, () -> window.add(30, 5, 4));
        assertThrows(IllegalArgumentException.class, () -> new CommitWindow(0));
    }
}
