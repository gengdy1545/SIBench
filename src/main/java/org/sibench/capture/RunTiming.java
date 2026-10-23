package org.sibench.capture;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Observability for one pipeline run: the wall clock of every phase, how many raw events and bytes
 * each phase consumed, and the facts two runs should be compared by. Every stage of a capture
 * appends to one file, so a run ends with a single timing artifact instead of numbers scattered
 * across logs.
 *
 * <p>Timing never decides an outcome: a run that fails still fails, and writing the file is
 * best-effort so that an unwritable path cannot invalidate a capture.
 */
public final class RunTiming implements AutoCloseable {
    /** Reports a rate to a stream now and then, so a long pass is not a silent one. */
    public static final class Progress {
        private final PrintStream out;
        private final long intervalNanos;
        private long due;
        private long events;

        Progress(PrintStream out, long intervalNanos) {
            this.out = out;
            this.intervalNanos = intervalNanos;
            this.due = System.nanoTime() + intervalNanos;
        }

        /** Prints "phase: n events (x/s)" when the interval has elapsed. */
        void advance(String phase, long events, long total) {
            this.events += events;
            long now = System.nanoTime();
            if (now < due) return;
            double seconds = (now - (due - intervalNanos)) / 1e9;
            out.printf(
                    "  %s: %,d events (%,.0f/s)%n",
                    phase, total, seconds > 0 ? this.events / seconds : 0.0);
            this.events = 0;
            this.due = now + intervalNanos;
        }
    }

    private static final RunTiming NONE = new RunTiming(null);
    private static volatile RunTiming current = NONE;

    private final Path path;
    private final Progress progress;
    private final List<Map<String, Object>> phases = new ArrayList<>();
    private final Map<String, Object> notes = new LinkedHashMap<>();
    private String phase;
    private long startedNanos, events;

    private RunTiming(Path path) {
        this.path = path;
        this.progress = path == null ? null : new Progress(System.err, 10_000_000_000L);
    }

    /** The instance a stage installed, or a no-op one when no timing path was given. */
    public static RunTiming current() {
        return current;
    }

    /** Opens the timing file of one stage and installs it as the current instance. */
    public static RunTiming open(Path path) {
        if (path == null) return NONE;
        var timing = new RunTiming(path);
        current = timing;
        return timing;
    }

    /** Closes the open phase and opens the next one. */
    public RunTiming phase(String name) {
        finish();
        phase = name;
        startedNanos = System.nanoTime();
        events = 0;
        return this;
    }

    /** Counts one raw event against the open phase and feeds the progress report. */
    public void advance(long count) {
        events += count;
        if (progress != null && phase != null) progress.advance(phase, count, events);
    }

    /** A fact that makes two runs comparable: rows, files, bytes, session count. */
    public RunTiming note(String key, Object value) {
        notes.put(key, value);
        return this;
    }

    private void finish() {
        if (phase == null) return;
        var record = new LinkedHashMap<String, Object>();
        record.put("phase", phase);
        record.put("millis", (System.nanoTime() - startedNanos) / 1_000_000L);
        record.put("events", events);
        phases.add(record);
        phase = null;
    }

    /** Merges into an existing file, so every stage of one run lands in the same artifact. */
    @Override
    public void close() {
        finish();
        current = NONE;
        if (path == null || phases.isEmpty()) return;
        try {
            var gson = new GsonBuilder().serializeNulls().setPrettyPrinting().create();
            JsonObject report = new JsonObject();
            if (Files.exists(path)) {
                try {
                    report = JsonParser.parseString(Files.readString(path)).getAsJsonObject();
                } catch (RuntimeException ignored) {
                    // A damaged or foreign timing file is replaced, not fatal.
                }
            }
            JsonArray all =
                    report.has("phases") && report.get("phases").isJsonArray()
                            ? report.getAsJsonArray("phases")
                            : new JsonArray();
            for (var record : phases) all.add(gson.toJsonTree(record));
            report.add("phases", all);
            for (var entry : notes.entrySet()) report.add(entry.getKey(), gson.toJsonTree(entry.getValue()));
            Path partial = path.resolveSibling(path.getFileName() + ".partial");
            Files.writeString(partial, gson.toJson(report), StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING);
            Files.move(partial, path, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException | RuntimeException ignored) {
            // Timing is evidence, never a reason to fail a run.
        }
    }
}
