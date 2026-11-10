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
import java.time.Duration;
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
 * <p>A run may also register output directories. A low-frequency sampler records their peak
 * logical bytes, and the final sample is kept with the process CPU, peak RSS and thread count.
 * Sampling is deliberately simple and bounded; it is evidence about this process, not a
 * monitoring system.
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

    private record DirectoryMeasure(Long bytes, Long peakBytes, Long files) {}

    private static final RunTiming NONE = new RunTiming(null);
    private static volatile RunTiming current = NONE;

    private final Path path;
    private final Progress progress;
    private final List<Map<String, Object>> phases = new ArrayList<>();
    private final Map<String, Object> notes = new LinkedHashMap<>();
    private final Map<Path, Long> measured = new LinkedHashMap<>();
    private final Map<Path, Long> peaks = new LinkedHashMap<>();
    private final long diskIntervalMillis =
            Long.getLong("sibench.timing.disk_interval_ms", 5_000L);
    private Thread sampler;
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

    /**
     * Measures an output directory until this timing artifact closes. The first call starts a
     * daemon sampler; the interval is {@code sibench.timing.disk_interval_ms} (5 s by default).
     */
    public synchronized RunTiming measure(Path directory) {
        if (path == null || directory == null) return this;
        Path absolute = directory.toAbsolutePath().normalize();
        if (measured.putIfAbsent(absolute, 0L) != null) return this;
        if (sampler == null) {
            sampler =
                    new Thread(
                            () -> {
                                while (!Thread.currentThread().isInterrupted()) {
                                    try {
                                        Thread.sleep(diskIntervalMillis);
                                    } catch (InterruptedException stopped) {
                                        Thread.currentThread().interrupt();
                                        return;
                                    }
                                    sampleDirectories();
                                }
                            },
                            "sibench-disk-sampler");
            sampler.setDaemon(true);
            sampler.start();
        }
        return this;
    }

    private synchronized void sampleDirectories() {
        for (Path directory : measured.keySet()) {
            Long bytes = measureDirectory(directory).bytes();
            if (bytes != null) peaks.merge(directory, bytes, Math::max);
        }
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

    /** Stops the directory sampler before the final sample and report are written. */
    private void stopSampler() {
        Thread current;
        synchronized (this) {
            current = sampler;
            sampler = null;
        }
        if (current == null) return;
        current.interrupt();
        try {
            current.join(2_000);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private synchronized Map<Path, DirectoryMeasure> directoryMeasures() {
        var result = new LinkedHashMap<Path, DirectoryMeasure>();
        for (var entry : measured.entrySet()) {
            var sample = measureDirectory(entry.getKey());
            if (sample.bytes() != null) peaks.merge(entry.getKey(), sample.bytes(), Math::max);
            result.put(entry.getKey(), new DirectoryMeasure(sample.bytes(), peaks.get(entry.getKey()), sample.files()));
        }
        return result;
    }

    private static DirectoryMeasure measureDirectory(Path directory) {
        var unknown = new DirectoryMeasure(null, null, null);
        try {
            if (!Files.readAttributes(directory, java.nio.file.attribute.BasicFileAttributes.class).isDirectory()) return unknown;
        } catch (java.nio.file.NoSuchFileException removed) {
            return new DirectoryMeasure(0L, 0L, 0L);
        } catch (IOException | RuntimeException unavailable) { return unknown; }
        long bytes = 0, files = 0;
        try (var paths = Files.walk(directory)) {
            var iterator = paths.iterator();
            while (iterator.hasNext()) {
                try {
                    var attributes = Files.readAttributes(iterator.next(), java.nio.file.attribute.BasicFileAttributes.class);
                    if (attributes.isRegularFile()) { bytes += attributes.size(); files++; }
                } catch (java.nio.file.NoSuchFileException removed) {
                    // Concurrent cleanup is expected; other read failures make this sample unknown.
                }
            }
            return new DirectoryMeasure(bytes, bytes, files);
        } catch (IOException | RuntimeException ignored) {
            return unknown;
        }
    }

    /** Process and filesystem facts at close, so a run's resource envelope is not guessed. */
    private static Map<String, Object> process() {
        return process(Path.of("/proc/self/status"), ProcessHandle.current().info().totalCpuDuration());
    }

    static Map<String, Object> process(Path status, java.util.Optional<Duration> cpu) {
        var report = new LinkedHashMap<String, Object>();
        Long peakRss = null, rss = null, threads = null;
        if (Files.isRegularFile(status)) {
            try {
                for (String line : Files.readAllLines(status)) {
                    if (line.startsWith("VmHWM:")) peakRss = kilobytes(line) * 1024;
                    else if (line.startsWith("VmRSS:")) rss = kilobytes(line) * 1024;
                    else if (line.startsWith("Threads:")) threads = number(line);
                }
            } catch (IOException | RuntimeException ignored) {
                // /proc is best-effort and absent on some platforms.
            }
        }
        var runtime = Runtime.getRuntime();
        report.put("cpu_seconds", cpu.map(value -> value.toNanos() / 1e9).orElse(null));
        report.put("peak_rss_bytes", peakRss);
        report.put("rss_bytes", rss);
        report.put("threads", threads);
        report.put("heap_used_bytes", runtime.totalMemory() - runtime.freeMemory());
        report.put("heap_max_bytes", runtime.maxMemory());
        report.put("available_processors", runtime.availableProcessors());
        try {
            var store = Files.getFileStore(Path.of(".").toAbsolutePath());
            report.put("filesystem_total_bytes", store.getTotalSpace());
            report.put("filesystem_free_bytes", store.getUsableSpace());
        } catch (IOException | RuntimeException ignored) {
            report.put("filesystem_total_bytes", null);
            report.put("filesystem_free_bytes", null);
        }
        return report;
    }

    private static long kilobytes(String line) {
        String[] fields = line.trim().split("\\s+");
        if (fields.length < 2) throw new IllegalArgumentException("Missing process metric");
        return Long.parseLong(fields[1]);
    }

    private static long number(String line) {
        String[] fields = line.trim().split("\\s+");
        if (fields.length < 2) throw new IllegalArgumentException("Missing process metric");
        return Long.parseLong(fields[1]);
    }

    /** Merges into an existing file, so every stage of one run lands in the same artifact. */
    @Override
    public void close() {
        stopSampler();
        finish();
        current = NONE;
        if (path == null) return;
        var directories = directoryMeasures();
        if (phases.isEmpty() && notes.isEmpty() && directories.isEmpty()) return;
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
            report.add("process", gson.toJsonTree(process()));
            var disk = new JsonObject();
            for (var entry : directories.entrySet()) {
                var item = new JsonObject();
                item.addProperty("bytes", entry.getValue().bytes());
                item.addProperty("peak_bytes", entry.getValue().peakBytes());
                item.addProperty("files", entry.getValue().files());
                disk.add(entry.getKey().toString(), item);
            }
            report.add("directories", disk);
            Path partial = path.resolveSibling(path.getFileName() + ".partial");
            Files.writeString(partial, gson.toJson(report), StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING);
            Files.move(partial, path, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException | RuntimeException ignored) {
            // Timing is evidence, never a reason to fail a run.
        }
    }
}
