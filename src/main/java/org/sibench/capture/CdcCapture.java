package org.sibench.capture;

import static org.sibench.config.ConfigValues.*;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/** Source-writer completion reports; no Index Trace or target visibility queries are involved. */
final class CdcCapture implements AutoCloseable {
    private final Process process;
    private final Socket socket;
    private final BufferedWriter control;
    private final Set<String> markers = new HashSet<>(), completed = new HashSet<>();
    private final Thread reader;
    private IOException failure;
    private boolean ended, finished;
    private long transactions, changes, markerChanges;

    static CdcCapture start(Path build, Map<String, Object> settings, Path root, Path schema, int port) throws Exception {
        var sink = PixelsSink.start(build, settings, root, PixelsSink.properties(object(settings, "sink_properties"), root, port),
                List.of(), Map.of(), "org.sibench.pixels.ProtoCapture", schema.toString(), root.getParent().resolve("cdc").toString());
        return new CdcCapture(sink.process(), sink.socket(), sink.reports());
    }

    private CdcCapture(Process process, Socket socket, BufferedReader input) throws IOException {
        this.process = process;
        this.socket = socket;
        this.control = new BufferedWriter(new OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8));
        reader = new Thread(() -> {
            try {
                for (String line; (line = input.readLine()) != null;) {
                    synchronized (this) {
                        if (line.equals("E")) { ended = true; notifyAll(); return; }
                        if (line.startsWith("F ")) throw new IOException("CDC capture failed: " + line.substring(2));
                        String[] fields = line.split(" ");
                        if (fields.length != 3 || !fields[0].equals("P") || !fields[1].matches("[0-9]+"))
                            throw new IOException("Malformed CDC completion report: " + line);
                        long count = Long.parseLong(fields[2]);
                        if (count < 0) throw new IOException("Negative CDC change count");
                        transactions = Math.addExact(transactions, 1);
                        changes = Math.addExact(changes, count);
                        if (markers.contains(fields[1])) {
                            if (count != 1 || !completed.add(fields[1])) throw new IOException("Invalid or repeated CDC marker");
                            markerChanges++;
                        }
                        notifyAll();
                    }
                }
                throw new IOException("CDC report connection closed before normal writer completion");
            } catch (Exception error) {
                synchronized (this) {
                    if (failure == null) failure = error instanceof IOException io ? io : new IOException(error);
                    notifyAll();
                }
            }
        }, "sibench-cdc-reports");
        reader.setDaemon(true);
        reader.start();
    }

    Process process() { return process; }

    synchronized void expectMarker(String id) throws IOException {
        check();
        if (!markers.add(id)) throw new IOException("Repeated source marker ID");
    }

    synchronized void awaitMarker(String id, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!completed.contains(id)) {
            check();
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) throw new IOException("CDC marker did not reach the file writer: " + id);
            wait(Math.min(200, Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining))));
        }
        check();
    }

    synchronized void check() throws IOException {
        if (failure != null) throw failure;
        if (!process.isAlive() && !ended) throw new IOException("CDC capture exited with " + process.exitValue());
    }

    synchronized CdcInput.Counts counts() {
        return new CdcInput.Counts(transactions, changes, changes - markerChanges, markerChanges);
    }

    void finish() throws Exception {
        check();
        control.write("close\n");
        control.flush();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (!process.waitFor(200, TimeUnit.MILLISECONDS)) {
            synchronized (this) { if (failure != null) throw failure; }
            if (System.nanoTime() > deadline) throw new IOException("CDC sink shutdown deadline exceeded");
        }
        reader.join(5_000);
        synchronized (this) {
            if (failure != null) throw failure;
            if (!ended || process.exitValue() != 0 || !completed.equals(markers))
                throw new IOException("CDC sink did not close normally with every marker complete");
            finished = true;
        }
    }

    @Override public void close() throws IOException {
        try {
            if (!finished && process.isAlive()) process.destroyForcibly();
            socket.close();
            control.close();
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                try {
                    if (!process.waitFor(10, TimeUnit.SECONDS)) throw new IOException("CDC sink did not terminate");
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted stopping CDC sink", interrupted);
                }
            }
        }
    }
}
