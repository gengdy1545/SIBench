package org.sibench.capture;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.IOException;
import java.io.BufferedReader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Independent fixture ordering by completion counter, then writer before reader and session order.
 * Two sequential reads keep fixture transactions bounded without production parsing machinery.
 */
final class SessionMerge implements AutoCloseable {
    /**
     * One transaction-sized run of events, keyed by the completion counter of its final record;
     * {@code writer} is a transaction that held an XID and so incremented the counter, {@code
     * committed} whether that record is a commit. Both are known before the chunk's lines are read.
     */
    record Chunk(int session, long csn, boolean writer, boolean committed, long lineCount) {}

    private final List<Path> files;
    /** Per session: the scanner that finds chunk ends, and the reader that streams chunk events. */
    private final List<ServerTrace.Records> scanners = new ArrayList<>();
    private final List<BufferedReader> contents = new ArrayList<>();
    /** The chunk handed out by next() and the lines of it not yet streamed. */
    private Chunk current;
    private long remaining;
    private final PriorityQueue<Chunk> heads =
            new PriorityQueue<>(
                    (a, b) ->
                            a.csn() != b.csn()
                                    ? Long.compare(a.csn(), b.csn())
                                    : a.writer() != b.writer()
                                            ? a.writer() ? -1 : 1
                                            : Integer.compare(a.session(), b.session()));
    /** Writers emerge in counter order, so only their last position is needed for uniqueness. */
    private long lastWriterCsn;
    private final long[] lastCsn;

    private SessionMerge(List<Path> files) {
        this.files = files;
        this.lastCsn = new long[files.size()];
    }

    static SessionMerge open(List<Path> files) throws IOException {
        var merge = new SessionMerge(files);
        try {
            for (Path file : files) {
                merge.scanners.add(ServerTrace.Records.rowChanges(file));
                merge.contents.add(ServerTrace.open(file));
            }
            merge.fillAll();
        } catch (IOException | RuntimeException error) {
            merge.close();
            throw error;
        }
        return merge;
    }

    boolean hasNext() {
        return !heads.isEmpty();
    }

    /**
     * The chunk with the smallest completion counter; sessions break ties in manifest order. Its
     * events are streamed by {@link #event()}, all of them before the next chunk is taken.
     */
    Chunk next() throws IOException {
        if (remaining != 0) throw new IOException("Previous chunk was not read to its end");
        Chunk chunk = heads.poll();
        if (chunk == null) throw new IOException("Merged session stream is exhausted");
        if (chunk.writer()) {
            if (chunk.csn() == lastWriterCsn)
                throw new IOException("Two writers share completion position " + chunk.csn());
            lastWriterCsn = chunk.csn();
        }
        fill(chunk.session());
        current = chunk;
        remaining = chunk.lineCount();
        return chunk;
    }

    /** The next event of the current chunk, or null after its last one. */
    Event event() throws IOException {
        if (remaining == 0) return null;
        String line = contents.get(current.session()).readLine();
        if (line == null) throw new IOException("Session trace changed while it was merged");
        Event event = Event.parse(line);
        remaining--;
        return event;
    }

    private void fillAll() throws IOException {
        for (int session = 0; session < files.size(); session++) fill(session);
    }

    private void fill(int session) throws IOException {
        Chunk chunk = read(session);
        if (chunk != null) heads.add(chunk);
    }

    private Chunk read(int session) throws IOException {
        ServerTrace.Records scanner = scanners.get(session);
        long lines = 0;
        for (String line; (line = scanner.next()) != null; ) {
            lines += scanner.lines();
            if (!line.startsWith(ServerTrace.COMMIT) && !line.startsWith(ServerTrace.ABORT)) continue;
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
            lastCsn[session] = csn;
            return new Chunk(session, csn, writer, line.startsWith(ServerTrace.COMMIT), lines);
        }
        lines += scanner.lines();
        if (lines == 0) return null;
        throw new IOException(
                "Session trace ends without a commit or abort: "
                        + files.get(session).getFileName());
    }

    @Override
    public void close() {
        for (ServerTrace.Records scanner : scanners)
            try {
                scanner.close();
            } catch (IOException ignored) {
                // Closing after a failure must not mask the original error.
            }
        for (var reader : contents)
            try {
                reader.close();
            } catch (IOException ignored) {
                // Closing after a failure must not mask the original error.
            }
    }

}
