package org.sibench.capture;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * One backend trace's events in file order, parsed ahead of the reader. A thread per trace reads
 * lines in batches and hands each batch to a pool shared by all traces, so a long chunk of one
 * session is parsed by several threads while the compiler consumes it; events still come out one
 * at a time and in order. At most {@link #WINDOW} batches per trace are read ahead, which bounds
 * the memory however large a chunk is. A line that does not parse fails where the compiler reaches
 * it, after every event before it, exactly as parsing in place would.
 */
final class ParsedTrace implements SessionMerge.Items {
    static final int BATCH = 512, WINDOW = 4;
    /** Parsed lines up to the first that failed, and that line's failure. */
    private record Batch(List<Event> events, RuntimeException failure) {}

    /** Marks the end of the trace; never a parsed batch. */
    private static final Future<Batch> END = CompletableFuture.completedFuture(new Batch(List.of(), null));

    private final BlockingQueue<Future<Batch>> ahead = new ArrayBlockingQueue<>(WINDOW + 1);
    private final Thread reader;
    private Batch batch = new Batch(List.of(), null);
    private int index;
    private boolean ended;

    ParsedTrace(Path file, ExecutorService parsers) {
        reader =
                new Thread(
                        () -> {
                            try (BufferedReader lines = ServerTrace.open(file)) {
                                while (true) {
                                    var text = new ArrayList<String>(BATCH);
                                    String line;
                                    while (text.size() < BATCH && (line = lines.readLine()) != null) text.add(line);
                                    if (!text.isEmpty()) ahead.put(parsers.submit(() -> parse(text)));
                                    if (text.size() < BATCH) break;
                                }
                                ahead.put(END);
                            } catch (InterruptedException closed) {
                                Thread.currentThread().interrupt();
                            } catch (IOException | RuntimeException error) {
                                try {
                                    ahead.put(CompletableFuture.failedFuture(error));
                                } catch (InterruptedException closed) {
                                    Thread.currentThread().interrupt();
                                }
                            }
                        },
                        "trace-" + file.getFileName());
        reader.setDaemon(true);
        reader.start();
    }

    private static Batch parse(List<String> lines) {
        var events = new ArrayList<Event>(lines.size());
        for (String line : lines)
            try {
                events.add(Event.parse(line));
            } catch (RuntimeException failure) {
                return new Batch(events, failure);
            }
        return new Batch(events, null);
    }

    /** The next event, or null after the last one. */
    @Override
    public Event next() throws IOException {
        while (index == batch.events().size()) {
            if (batch.failure() != null) throw batch.failure();
            if (ended) return null;
            Future<Batch> next;
            try {
                next = ahead.take();
                if (next == END) {
                    ended = true;
                    return null;
                }
                batch = next.get();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while reading a backend trace", interrupted);
            } catch (ExecutionException failed) {
                Throwable cause = failed.getCause();
                if (cause instanceof IOException io) throw io;
                if (cause instanceof UncheckedIOException io) throw io.getCause();
                if (cause instanceof RuntimeException runtime) throw runtime;
                if (cause instanceof Error error) throw error;
                throw new IOException(cause);
            }
            index = 0;
        }
        return batch.events().get(index++);
    }

    @Override
    public void close() {
        reader.interrupt();
    }
}
