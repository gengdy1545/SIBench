package org.sibench.capture;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Receives Retina tracer facts and pixels-sink transaction reports and runs the projection on one
 * thread. A barrier is sent to the tracer after reported commits; its acknowledgement follows every
 * fact appended before the sink's commit, so those transactions are then complete. The bounded
 * queue applies back pressure to Retina instead of buffering an unbounded backlog.
 */
final class PixelsCapture implements AutoCloseable {
    private static final int TRACE_MAGIC = 0x53495854, BARRIER = 10;
    private sealed interface Event permits FactEvent, Begin, Commit, Ack, Failed, Call {}
    private record FactEvent(PixelsTrace.Fact fact) implements Event {}
    private record Begin(long timestamp) implements Event {}
    private record Commit(long timestamp) implements Event {}
    private record Ack(long barrier) implements Event {}
    private record Failed(String message, Throwable cause) implements Event {}
    private record Call(Step step, CompletableFuture<Void> done, boolean barrier) implements Event {}
    interface Step { void run() throws Exception; }
    private record Barrier(long id, Set<Long> commits, List<CompletableFuture<Void>> waiters) {}

    private final PixelsTrace trace;
    private final ServerSocket traceListener, reportListener;
    private final String traceToken, reportToken;
    private final BlockingQueue<Event> events = new ArrayBlockingQueue<>(1 << 20);
    private final Set<Long> markers = java.util.concurrent.ConcurrentHashMap.newKeySet();
    private final List<CompletableFuture<Void>> syncWaiters = new ArrayList<>();
    private final Thread projector;
    private volatile Throwable failure;
    private volatile boolean closing, sinkStopping;
    private volatile Socket tracer;
    private volatile Socket sink;
    private volatile DataOutputStream barriers;
    private Barrier outstanding;
    private long barrierId;

    PixelsCapture(PixelsTrace trace, ServerSocket traceListener, String traceToken, ServerSocket reportListener, String reportToken) {
        this.trace = trace;
        this.traceListener = traceListener;
        this.traceToken = traceToken;
        this.reportListener = reportListener;
        this.reportToken = reportToken;
        projector = new Thread(this::project, "sibench-pixels-projection");
        projector.setDaemon(true);
        projector.start();
    }

    /**
     * Accepts the Retina tracer in the background: it connects when Retina first opens a primary
     * index, which is during LOAD. Barriers wait for the connection; a missing one fails the run.
     */
    void acceptTracer(Duration timeout) {
        reader("sibench-pixels-facts", () -> {
            traceListener.setSoTimeout((int) timeout.toMillis());
            var socket = traceListener.accept();
            var input = new DataInputStream(new java.io.BufferedInputStream(socket.getInputStream(), 1 << 20));
            if (input.readInt() != TRACE_MAGIC || !same(input.readUTF(), traceToken)) throw new IOException("Unauthenticated Pixels tracer");
            tracer = socket;
            barriers = new DataOutputStream(socket.getOutputStream());
            while (true) {
                int kind;
                try {
                    kind = input.readByte();
                } catch (EOFException end) {
                    if (!closing) put(new Failed("Pixels tracer connection closed", null));
                    return;
                }
                long table = input.readLong(), index = input.readLong(), timestamp = input.readLong();
                byte[] key = input.readNBytes(input.readInt());
                long rowId = input.readLong(), fileId = input.readLong();
                int rgId = input.readInt(), offset = input.readInt();
                long previous = input.readLong();
                put(kind == BARRIER ? new Ack(rowId)
                        : new FactEvent(new PixelsTrace.Fact(kind, table, index, timestamp, key, rowId, fileId, rgId, offset, previous)));
            }
        });
    }

    /**
     * Accepts the patched sink's report connection in the background; it connects at its first
     * BEGIN. B/C lines carry Pixels timestamps, F a failure.
     */
    void acceptSink(Duration timeout) {
        reader("sibench-pixels-sink", () -> {
            reportListener.setSoTimeout((int) timeout.toMillis());
            sink = reportListener.accept();
            var lines = new BufferedReader(new InputStreamReader(sink.getInputStream(), StandardCharsets.UTF_8));
            String hello = lines.readLine();
            if (hello == null || !hello.startsWith("T ") || !same(hello.substring(2), reportToken)) throw new IOException("Unauthenticated pixels-sink");
            for (String line; (line = lines.readLine()) != null; ) {
                if (line.length() < 3 || line.charAt(1) != ' ') throw new IOException("Malformed sink report: " + line);
                switch (line.charAt(0)) {
                    case 'B' -> put(new Begin(Long.parseLong(line.substring(2))));
                    case 'C' -> put(new Commit(Long.parseLong(line.substring(2))));
                    case 'F' -> put(new Failed("pixels-sink: " + line.substring(2), null));
                    default -> throw new IOException("Malformed sink report: " + line);
                }
            }
            if (!closing && !sinkStopping) put(new Failed("pixels-sink report connection closed", null));
        });
    }

    /** Runs a projection step after every fact appended before now has arrived. */
    void synced(Step step, Duration timeout) throws Exception {
        call(step, true, timeout);
    }

    /** Waits until the marker transaction {@code id}, and so every transaction before it, is exported. */
    void awaitMarker(long id, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!markers.contains(id)) {
            check();
            if (System.nanoTime() > deadline) throw new IOException("Pixels window marker " + id + " not applied in time");
            Thread.sleep(10);
        }
    }

    /** The generator stops the sink after its last marker; the report connection may then close. */
    void sinkStopping() { sinkStopping = true; }

    void check() throws IOException {
        if (failure != null) throw new IOException("Pixels capture failed", failure);
    }

    /** Runs a step on the projection thread, after a fresh barrier when {@code barrier} is set. */
    void call(Step step, boolean barrier, Duration timeout) throws Exception {
        check();
        var done = new CompletableFuture<Void>();
        put(new Call(step, done, barrier));
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            try {
                done.get(100, TimeUnit.MILLISECONDS);
                return;
            } catch (java.util.concurrent.ExecutionException error) {
                check();
                throw new IOException("Pixels projection step failed", error.getCause());
            } catch (java.util.concurrent.TimeoutException pending) {
                // A failed projection never runs the step; report its cause instead of waiting.
                check();
                if (System.nanoTime() > deadline) throw new IOException("Pixels projection step timed out", pending);
            }
        }
    }

    private void project() {
        try {
            while (true) {
                Event event = events.poll(10, TimeUnit.MILLISECONDS);
                if (event instanceof FactEvent fact) trace.fact(fact.fact());
                else if (event instanceof Begin begin) trace.begin(begin.timestamp());
                else if (event instanceof Commit commit) trace.commit(commit.timestamp());
                else if (event instanceof Failed failed) throw new IOException(failed.message(), failed.cause());
                else if (event instanceof Call call) {
                    if (call.barrier()) {
                        // The step runs when a barrier sent after this call is acknowledged.
                        var covered = new CompletableFuture<Void>();
                        covered.whenComplete((ignored, error) -> {
                            if (error == null) run(call);
                            else call.done().completeExceptionally(error);
                        });
                        syncWaiters.add(covered);
                    } else run(call);
                } else if (event instanceof Ack ack) {
                    if (outstanding == null || ack.barrier() != outstanding.id()) throw new IOException("Unexpected tracer barrier");
                    trace.covered(outstanding.commits());
                    markers.addAll(trace.markers());
                    var waiters = outstanding.waiters();
                    outstanding = null;
                    waiters.forEach(waiter -> waiter.complete(null));
                }
                if (outstanding == null && barriers != null && (trace.hasUncoveredCommits() || !syncWaiters.isEmpty())) {
                    outstanding = new Barrier(++barrierId, trace.committedSnapshot(), List.copyOf(syncWaiters));
                    syncWaiters.clear();
                    barriers.writeLong(outstanding.id());
                    barriers.flush();
                }
            }
        } catch (Throwable error) {
            // Interruption by close() ends the projection; it is not a failure.
            if (failure == null && !closing) failure = error;
            syncWaiters.forEach(waiter -> waiter.completeExceptionally(error));
            if (outstanding != null) outstanding.waiters().forEach(waiter -> waiter.completeExceptionally(error));
            for (Event left; (left = events.poll()) != null; )
                if (left instanceof Call call) call.done().completeExceptionally(error);
        }
    }

    private static void run(Call call) {
        try {
            call.step().run();
            call.done().complete(null);
        } catch (Throwable error) {
            call.done().completeExceptionally(error);
        }
    }

    private void put(Event event) throws InterruptedException {
        while (!events.offer(event, 100, TimeUnit.MILLISECONDS))
            if (failure != null) return;
    }

    private void reader(String name, Step body) {
        var thread = new Thread(() -> {
            try {
                body.run();
            } catch (Throwable error) {
                if (!closing) {
                    try {
                        put(new Failed(name + " failed", error));
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }, name);
        thread.setDaemon(true);
        thread.start();
    }

    private static boolean same(String actual, String expected) {
        return MessageDigest.isEqual(actual.getBytes(StandardCharsets.UTF_8), expected.getBytes(StandardCharsets.UTF_8));
    }

    /** Stops accepting events; facts after this point are not expected. */
    @Override
    public void close() throws IOException {
        closing = true;
        projector.interrupt();
        IOException first = null;
        for (var closeable : new java.io.Closeable[] {tracer, sink, traceListener, reportListener}) {
            try {
                if (closeable != null) closeable.close();
            } catch (IOException error) {
                if (first == null) first = error;
            }
        }
        if (first != null) throw first;
    }
}
