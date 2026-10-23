package org.sibench.capture;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Merges the backend traces of one concurrent capture into a single event stream. A chunk is the
 * run of events ending at the commit or abort record, and that record carries PostgreSQL's
 * transaction completion counter, so chunks merge on the counter alone: no pid, xid magnitude,
 * file order or wall clock is consulted.
 *
 * <p>A transaction with an XID increments the counter under ProcArrayLock exactly when it stops
 * counting as running, and records that value: such writers have distinct positions, and a commit
 * is visible to a snapshot iff the snapshot's counter is at or after the commit's. A transaction
 * without an XID changes no snapshot and increments nothing; it records the counter it observed,
 * so it sorts after every writer at or below it. Ties are therefore a writer first, then readers,
 * which keep their session order. Within one backend a writer's position exceeds everything before
 * it, and a reader's is at least the previous one, so one buffered chunk per session suffices.
 */
final class SessionMerge implements AutoCloseable {
    /**
     * One transaction-sized run of events, keyed by the completion counter of its final record;
     * {@code writer} is a transaction that held an XID and so incremented the counter.
     */
    record Chunk(int session, long csn, boolean writer, List<String> lines) {}

    private static final String COMMIT = "{\"event\":\"transaction_commit\"";
    private static final String ABORT = "{\"event\":\"transaction_abort\"";

    private final List<Path> files;
    private final List<BufferedReader> readers = new ArrayList<>();
    private final PriorityQueue<Chunk> heads =
            new PriorityQueue<>(
                    (a, b) ->
                            a.csn() != b.csn()
                                    ? Long.compare(a.csn(), b.csn())
                                    : a.writer() != b.writer()
                                            ? a.writer() ? -1 : 1
                                            : Integer.compare(a.session(), b.session()));
    /** Counter positions writers took; two writers can never share one. */
    private final java.util.Set<Long> writerPositions = new java.util.HashSet<>();
    private final long[] lastCsn;

    private SessionMerge(List<Path> files) {
        this.files = files;
        this.lastCsn = new long[files.size()];
    }

    static SessionMerge open(List<Path> files) throws IOException {
        var merge = new SessionMerge(files);
        try {
            for (Path file : files) merge.readers.add(Files.newBufferedReader(file));
            for (int session = 0; session < files.size(); session++) merge.fill(session);
        } catch (IOException | RuntimeException error) {
            merge.close();
            throw error;
        }
        return merge;
    }

    boolean hasNext() {
        return !heads.isEmpty();
    }

    /** The chunk with the smallest completion counter; sessions break ties in manifest order. */
    Chunk next() throws IOException {
        Chunk chunk = heads.poll();
        if (chunk == null) throw new IOException("Merged session stream is exhausted");
        fill(chunk.session());
        return chunk;
    }

    private void fill(int session) throws IOException {
        Chunk chunk = read(session);
        if (chunk != null) heads.add(chunk);
    }

    private Chunk read(int session) throws IOException {
        var lines = new ArrayList<String>();
        String line;
        while ((line = readers.get(session).readLine()) != null) {
            lines.add(line);
            if (line.startsWith(COMMIT) || line.startsWith(ABORT)) {
                JsonObject event = JsonParser.parseString(line).getAsJsonObject();
                if (!event.has("csn"))
                    throw new IOException(
                            "Transaction record lacks a completion counter in "
                                    + files.get(session).getFileName()
                                    + "; recapture with the counter instrumentation");
                long csn = event.get("csn").getAsLong();
                boolean writer = event.get("xid").getAsLong() != 0;
                // A writer's increment follows everything before it in its backend.
                if (writer ? csn <= lastCsn[session] : csn < lastCsn[session])
                    throw new IOException(
                            "Completion counter does not grow in "
                                    + files.get(session).getFileName()
                                    + ": "
                                    + csn
                                    + " after "
                                    + lastCsn[session]);
                if (writer && !writerPositions.add(csn))
                    throw new IOException("Two writers share completion position " + csn);
                lastCsn[session] = csn;
                return new Chunk(session, csn, writer, lines);
            }
        }
        if (lines.isEmpty()) return null;
        throw new IOException(
                "Session trace ends without a commit or abort: "
                        + files.get(session).getFileName());
    }

    @Override
    public void close() {
        for (BufferedReader reader : readers)
            try {
                reader.close();
            } catch (IOException ignored) {
                // Closing after a failure must not mask the original error.
            }
    }
}
