package org.sibench.capture;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Function;

/** Parallel JSON decoding with bounded lookahead and caller-thread, source-ordered delivery. */
final class EventDecoder implements AutoCloseable {
    static final int BATCH_BYTES = 1 << 20;
    static final int BATCH_EVENTS = 2048;
    private final int ahead;
    private final ExecutorService workers;
    private final Function<String, Event> parse;

    EventDecoder() {
        this(Math.max(1, Math.min(4, Runtime.getRuntime().availableProcessors() - 1)), Event::parse);
    }

    EventDecoder(int threads, Function<String, Event> parse) {
        if (threads < 1) throw new IllegalArgumentException("Positive decoder thread count required");
        ahead = Math.multiplyExact(threads, 2);
        this.parse = parse;
        workers = Executors.newFixedThreadPool(threads, task -> {
            var thread = new Thread(task, "capture-decode");
            thread.setDaemon(true);
            return thread;
        });
    }

    void read(BufferedReader input, CaptureTransactions.Events consumer) throws IOException {
        var pending = new ArrayDeque<Future<List<Event>>>();
        try {
            String line = input.readLine();
            while (line != null) {
                // A single oversized event retains the existing acceptance limit, without multiplying it by lookahead.
                if (cost(line) > BATCH_BYTES) {
                    while (!pending.isEmpty()) deliver(pending, consumer);
                    consumer.accept(parse.apply(line));
                    line = input.readLine();
                    continue;
                }
                var lines = new ArrayList<String>();
                long bytes = 0;
                while (line != null && lines.size() < BATCH_EVENTS && cost(line) <= BATCH_BYTES - bytes) {
                    bytes += cost(line);
                    lines.add(line);
                    line = input.readLine();
                }
                if (line == null && pending.isEmpty()) {
                    // Short spilled transactions do not need a worker handoff.
                    for (String value : lines) consumer.accept(parse.apply(value));
                } else {
                    pending.add(workers.submit(() -> decode(lines)));
                    if (pending.size() == ahead) deliver(pending, consumer);
                }
            }
            while (!pending.isEmpty()) deliver(pending, consumer);
        } finally {
            for (var future : pending) future.cancel(true);
        }
    }

    private static long cost(String line) {
        // Bound UTF-16 text plus per-line overhead; parsed-object expansion is not a JVM heap cap.
        return 2L * line.length() + 64;
    }

    private List<Event> decode(List<String> lines) {
        var events = new ArrayList<Event>(lines.size());
        for (int i = 0; i < lines.size(); i++) {
            if (Thread.currentThread().isInterrupted()) throw new CancellationException("Event decoding interrupted");
            events.add(parse.apply(lines.set(i, null)));
        }
        return events;
    }

    private static void deliver(ArrayDeque<Future<List<Event>>> pending, CaptureTransactions.Events consumer)
            throws IOException {
        try {
            var events = pending.getFirst().get();
            pending.removeFirst();
            for (Event event : events) consumer.accept(event);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while decoding capture events", interrupted);
        } catch (ExecutionException failed) {
            Throwable cause = failed.getCause();
            if (cause instanceof RuntimeException runtime) throw runtime;
            if (cause instanceof Error error) throw error;
            throw new IOException("Capture event decoding failed", cause);
        }
    }

    @Override public void close() throws IOException {
        workers.shutdownNow();
        try {
            if (!workers.awaitTermination(5, TimeUnit.SECONDS))
                throw new IOException("Capture event decoders did not stop");
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while closing capture event decoders", interrupted);
        }
    }
}
