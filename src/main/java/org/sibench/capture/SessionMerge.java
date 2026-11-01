package org.sibench.capture;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.IOException;
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
 * it, and a reader's is at least the previous one, so one pending chunk per session suffices.
 *
 * <p>Memory stays bounded however large a transaction is (an analytical session can be one
 * multi-gigabyte chunk): each session has a scanner thread that finds its commit and abort records
 * and keeps each chunk's line count and counter (a bounded number of chunks ahead), and a second
 * sequential reader that streams a
 * chunk's events when the chunk is taken. Each file is read twice instead of buffered. The second
 * reader parses ahead on a pool shared by all sessions ({@link ParsedTrace}), within a bounded
 * window, so parsing runs in parallel with the compiler and with itself while events stay in order.
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
    /** Per session: the chunks its scanner thread found and the merge has not taken yet. */
    private final List<java.util.concurrent.BlockingQueue<Object>> ahead = new ArrayList<>();
    private final List<Thread> scannerThreads = new ArrayList<>();
    private final List<ParsedTrace> contents = new ArrayList<>();
    private final java.util.concurrent.ExecutorService parsers =
            java.util.concurrent.Executors.newFixedThreadPool(
                    parseThreads(),
                    task -> {
                        var thread = new Thread(task, "trace-parser");
                        thread.setDaemon(true);
                        return thread;
                    });
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

    /**
     * Threads that parse backend events ahead of the merge; the order of events never depends on
     * it, and memory is bounded per trace. Concurrency defaults to the available processors and
     * can be bounded explicitly for the capture environment.
     */
    static int parseThreads() {
        return Integer.getInteger("sibench.compile.parse_threads", Runtime.getRuntime().availableProcessors());
    }

    private SessionMerge(List<Path> files) {
        this.files = files;
        this.lastCsn = new long[files.size()];
    }

    static SessionMerge open(List<Path> files) throws IOException {
        var merge = new SessionMerge(files);
        try {
            for (Path file : files) {
                merge.scanners.add(new ServerTrace.Records(file));
                merge.contents.add(new ParsedTrace(file, merge.parsers));
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
        Event event = contents.get(current.session()).next();
        if (event == null) throw new IOException("Session trace changed while it was merged");
        remaining--;
        return event;
    }

    /** Chunks a session's scanner finds ahead of the merge; they are a few numbers each. */
    static final int CHUNKS_AHEAD = 4096;

    /** Marks the end of a session's chunks; never a chunk handed out. */
    private static final Object NO_MORE = new Object();

    /**
     * Starts every session's scanner: a thread that finds the session's chunks in order, ahead of
     * the merge and within a bounded queue, so the merge only takes them. A session whose first
     * transaction spans its whole trace is read to its end before its first chunk is known. The
     * queue orders chunks by counter, writer and session, so the order in which sessions find
     * them does not matter; a scanning failure surfaces where the merge takes that session's next
     * chunk, as it did when the merge scanned.
     */
    private void fillAll() throws IOException {
        for (int session = 0; session < files.size(); session++) {
            var queue = new java.util.concurrent.ArrayBlockingQueue<Object>(CHUNKS_AHEAD);
            ahead.add(queue);
            int own = session;
            var thread = new Thread(() -> {
                try {
                    while (true) {
                        Chunk chunk = read(own);
                        queue.put(chunk == null ? NO_MORE : chunk);
                        if (chunk == null) break;
                    }
                } catch (InterruptedException closed) {
                    Thread.currentThread().interrupt();
                } catch (IOException | RuntimeException failure) {
                    try {
                        queue.put(failure);
                    } catch (InterruptedException closed) {
                        Thread.currentThread().interrupt();
                    }
                }
            }, "trace-scanner-" + files.get(session).getFileName());
            thread.setDaemon(true);
            scannerThreads.add(thread);
            thread.start();
        }
        for (int session = 0; session < files.size(); session++) fill(session);
    }

    private void fill(int session) throws IOException {
        Object next;
        try {
            next = ahead.get(session).take();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while merging session traces", interrupted);
        }
        if (next instanceof IOException failure) throw failure;
        if (next instanceof RuntimeException failure) throw failure;
        if (next == NO_MORE) {
            // Later takes see the end again.
            ahead.get(session).add(NO_MORE);
            return;
        }
        heads.add((Chunk) next);
    }

    private Chunk read(int session) throws IOException {
        ServerTrace.Records scanner = scanners.get(session);
        String line = scanner.next();
        long lines = scanner.lines();
        if (line != null) {
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
        if (lines == 0) return null;
        throw new IOException(
                "Session trace ends without a commit or abort: "
                        + files.get(session).getFileName());
    }

    @Override
    public void close() {
        for (Thread thread : scannerThreads) thread.interrupt();
        for (Thread thread : scannerThreads)
            try {
                thread.join(10_000);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        for (ServerTrace.Records scanner : scanners)
            try {
                scanner.close();
            } catch (IOException ignored) {
                // Closing after a failure must not mask the original error.
            }
        for (ParsedTrace reader : contents) reader.close();
        parsers.shutdownNow();
    }

}
