package org.sibench.capture;

import static org.sibench.config.ConfigValues.*;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
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
        java.nio.file.Files.createDirectory(root);
        Path properties = java.nio.file.Files.writeString(root.resolve("sink.properties"),
                PixelsGeneration.sinkProperties(object(settings, "sink_properties"), root, port, "proto"));
        Path logging = PixelsGeneration.sinkLogging(root);
        String token = UUID.randomUUID().toString();
        Process process = null;
        try (var listener = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            listener.setSoTimeout(30_000);
            var command = List.of(Path.of(System.getProperty("java.home"), "bin/java").toString(),
                    "-Xmx" + string(object(settings, "memory"), "sink_heap"), "-Dlog4j2.configurationFile=" + logging,
                    "-Dsibench.report.endpoint=127.0.0.1:" + listener.getLocalPort(), "-Dsibench.report.token=" + token,
                    "-cp", build.resolve("jars/sibench-pixels.jar") + ":" + build.resolve("jars/pixels-sink.jar"),
                    "org.sibench.pixels.ProtoCapture", properties.toString(), schema.toString(), root.getParent().resolve("cdc").toString());
            process = new ProcessBuilder(command).directory(root.toFile()).redirectErrorStream(true)
                    .redirectOutput(root.resolve("sink.log").toFile()).start();
            Socket socket = listener.accept();
            try {
                socket.setSoTimeout(30_000);
                var input = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                if (!("T " + token).equals(input.readLine()) || !"R".equals(input.readLine()))
                    throw new IOException("Invalid CDC capture handshake; see " + root.resolve("sink.log"));
                socket.setSoTimeout(0);
                return new CdcCapture(process, socket, input);
            } catch (Exception | Error failure) {
                socket.close();
                throw failure;
            }
        } catch (Exception | Error failure) {
            if (process != null) process.destroyForcibly();
            throw failure;
        }
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
