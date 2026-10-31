package org.sibench.capture;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * The most recent committed writers in completion-counter order: the counter, the version the
 * compiler gave each and its PostgreSQL XID. A search reads the version of the last commit whose
 * counter is at or before its snapshot's counter, so the entries stay sorted by counter and the
 * lookup is a binary search.
 *
 * <p>Only the newest {@code limit} commits are kept. A snapshot is never older than the longest
 * statement of the capture, so in practice it lies inside the window; when one reaches behind it
 * the lookup fails instead of answering "no commit yet".
 */
final class CommitWindow {
    private final int limit;
    private long[] csns, versions, xids;
    private int head, size;
    private long evicted, evictedThrough = -1;
    private final Map<Long, Long> counterByXid = new HashMap<>();

    CommitWindow(int limit) {
        if (limit < 1) throw new IllegalArgumentException("Commit window needs a positive size");
        this.limit = limit;
        int capacity = Math.min(limit, 1024);
        csns = new long[capacity];
        versions = new long[capacity];
        xids = new long[capacity];
    }

    int size() {
        return size;
    }

    long evicted() {
        return evicted;
    }

    int limit() {
        return limit;
    }

    private int at(int index) {
        return (head + index) % csns.length;
    }

    void add(long csn, long xid, long version) {
        if (size > 0 && csn <= csns[at(size - 1)] && xid != 0)
            throw new IllegalStateException("Commit counters must grow");
        if (size == limit) {
            int oldest = at(0);
            if (xids[oldest] != 0) counterByXid.remove(xids[oldest], csns[oldest]);
            evictedThrough = csns[oldest];
            evicted++;
            head = (head + 1) % csns.length;
            size--;
        }
        if (size == csns.length) grow();
        int slot = at(size);
        csns[slot] = csn;
        versions[slot] = version;
        xids[slot] = xid;
        size++;
        if (xid != 0) counterByXid.put(xid, csn);
    }

    private void grow() {
        int capacity = Math.min(limit, csns.length * 2);
        long[] newCsns = new long[capacity], newVersions = new long[capacity], newXids = new long[capacity];
        for (int i = 0; i < size; i++) {
            newCsns[i] = csns[at(i)];
            newVersions[i] = versions[at(i)];
            newXids[i] = xids[at(i)];
        }
        csns = newCsns;
        versions = newVersions;
        xids = newXids;
        head = 0;
    }

    /** Index of the last retained commit at or before the counter, or -1 when none is. */
    private int last(long csn) throws IOException {
        // The last commit at or before the counter may be one that was evicted.
        if (evicted > 0 && (size == 0 || csn < csns[at(0)]))
            throw new IOException(
                    "Snapshot counter "
                            + csn
                            + " reaches behind the retained commit window (oldest evicted counter "
                            + evictedThrough
                            + ", window "
                            + limit
                            + " commits)");
        int low = 0, high = size - 1, best = -1;
        while (low <= high) {
            int middle = (low + high) >>> 1;
            if (csns[at(middle)] <= csn) {
                best = middle;
                low = middle + 1;
            } else high = middle - 1;
        }
        return best;
    }

    /** The version of the last writer a snapshot with this counter sees; 0 before any. */
    long versionAt(long csn) throws IOException {
        int best = last(csn);
        return best < 0 ? 0 : versions[at(best)];
    }

    /** The XID of the last writer a snapshot with this counter sees; 0 when none or without XID. */
    long xidAt(long csn) throws IOException {
        int best = last(csn);
        return best < 0 ? 0 : xids[at(best)];
    }

    /** The counter at which the XID committed, or null when it is not in the window. */
    Long counterOf(long xid) {
        return counterByXid.get(xid);
    }

    @Override
    public String toString() {
        return "CommitWindow" + Arrays.toString(Arrays.copyOf(csns, Math.min(size, 4)));
    }
}
