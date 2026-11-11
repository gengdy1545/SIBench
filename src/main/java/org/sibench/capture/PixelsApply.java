package org.sibench.capture;

import static org.sibench.config.ConfigValues.*;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** Finite file sink controller. Successful native commits, independently of tracing, finish a run. */
final class PixelsApply implements AutoCloseable {
    private final Process process;
    private final Socket socket;
    private final DataOutputStream frames;
    private final PixelsCapture capture;
    private final Path freshnessReport;
    private final Map<Long, Long> pending = new HashMap<>();
    private final Thread reader;
    private Throwable failure;
    private long lastBegin = -1, transactions, changes;
    private boolean ended, finished;

    static PixelsApply start(Path build, Map<String, Object> settings, Path root, Path pixelsConfig,
            PixelsCapture capture, Map<String, Object> freshness, Map<String, Object> trino) throws Exception {
        Files.createDirectory(root);
        String properties = PixelsGeneration.sinkProperties(object(settings, "sink_properties"), root, 1, "retina");
        if (bool(freshness, "enabled")) {
            var server = URI.create(string(trino, "server"));
            if (!server.getScheme().equals("http")) throw new IOException("Sink freshness currently requires an HTTP Trino JDBC endpoint");
            properties += "\nsink.monitor.enable=true\nsink.monitor.report.enable=true\nsink.monitor.report.interval=100\n"
                    + "sink.monitor.report.file=" + root.resolve("metrics.csv") + "\n"
                    + "sink.monitor.freshness.level=embed\nsink.monitor.freshness.embed.warmup=0\n"
                    + "sink.monitor.freshness.embed.static=true\nsink.monitor.freshness.embed.tablelist=" + string(freshness, "table") + "\n"
                    + "sink.monitor.freshness.verbose=true\nsink.monitor.freshness.file=" + root.resolve("freshness.csv") + "\n"
                    + "sink.query.url=jdbc:trino://" + server.getRawAuthority() + "/" + string(trino, "catalog") + "/" + string(trino, "schema") + "\n"
                    + "sink.query.user=" + string(trino, "user") + "\nsink.query.password=\nsink.query.parallel=1\n";
        }
        Path config = Files.writeString(root.resolve("sink.properties"), properties);
        Path logging = PixelsGeneration.sinkLogging(root);
        String token = UUID.randomUUID().toString();
        Process process = null;
        try (var listener = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            listener.setSoTimeout(30_000);
            var command = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin/java").toString(),
                    "-Xmx" + string(object(settings, "memory"), "sink_heap"), "-Dlog4j2.configurationFile=" + logging,
                    "-Dsibench.report.endpoint=127.0.0.1:" + listener.getLocalPort(), "-Dsibench.report.token=" + token,
                    "-cp", build.resolve("jars/sibench-pixels.jar") + ":" + build.resolve("jars/pixels-sink.jar"),
                    "org.sibench.pixels.ProtoApply", config.toString()));
            if (bool(freshness, "enabled")) command.addAll(1, List.of("-Dsibench.freshness.table=" + string(freshness, "table"),
                    "-Dsibench.freshness.column=" + string(freshness, "column"),
                    "-Duser.timezone=UTC",
                    "-Dsibench.freshness.sample_ms=" + positive(freshness, "sample_seconds") * 1000));
            var builder = new ProcessBuilder(command).directory(root.toFile()).redirectErrorStream(true)
                    .redirectOutput(root.resolve("sink.log").toFile());
            builder.environment().put("PIXELS_CONFIG", pixelsConfig.toString());
            process = builder.start();
            Socket socket = listener.accept();
            try {
                socket.setSoTimeout(30_000);
                var lines = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                if (!("T " + token).equals(lines.readLine()) || !"R".equals(lines.readLine()))
                    throw new IOException("Invalid file sink handshake; see " + root);
                socket.setSoTimeout(0);
                return new PixelsApply(process, socket, lines, capture, bool(freshness, "enabled") ? root.resolve("freshness.csv") : null);
            } catch (Exception | Error error) { socket.close(); throw error; }
        } catch (Exception | Error error) {
            if (process != null) process.destroyForcibly();
            throw error;
        }
    }

    private PixelsApply(Process process, Socket socket, BufferedReader lines, PixelsCapture capture, Path freshnessReport) {
        this.process = process;
        this.socket = socket;
        this.capture = capture;
        this.freshnessReport = freshnessReport;
        frames = new DataOutputStream(new BufferedOutputStream(process.getOutputStream(), 1 << 20));
        reader = new Thread(() -> {
            try {
                for (String line; (line = lines.readLine()) != null;) {
                    synchronized (this) {
                        report(line);
                        notifyAll();
                        if (ended) return;
                    }
                }
                throw new IOException("File sink report closed before normal drain");
            } catch (Throwable error) {
                synchronized (this) { failure = error; notifyAll(); }
            }
        }, "sibench-file-sink-reports");
        reader.setDaemon(true);
        reader.start();
    }

    private void report(String line) throws Exception {
        if (line.startsWith("F ")) throw new IOException("File sink: " + line.substring(2));
        if (line.equals("E")) {
            if (!pending.isEmpty()) throw new IOException("File sink closed with pending transactions");
            ended = true;
            return;
        }
        String[] fields = line.split(" ");
        switch (fields[0]) {
            case "B" -> {
                long timestamp = Long.parseLong(fields[1]);
                if (fields.length != 2 || timestamp <= lastBegin) throw new IOException("Unordered file sink BEGIN");
                lastBegin = timestamp;
                pending.put(timestamp, -1L);
                if (capture != null) capture.begin(timestamp);
            }
            case "P" -> {
                long count = Long.parseLong(fields[2]);
                if (fields.length != 3 || !fields[1].matches("[0-9]+") || count < 0
                        || !Objects.equals(pending.get(lastBegin), -1L)) throw new IOException("Invalid file sink END");
                pending.put(lastBegin, count);
            }
            case "C" -> {
                long timestamp = Long.parseLong(fields[1]);
                Long count = pending.remove(timestamp);
                if (fields.length != 2 || count == null || count < 0) throw new IOException("Unexpected file sink commit");
                transactions = Math.addExact(transactions, 1);
                changes = Math.addExact(changes, count);
                if (capture != null) capture.commit(timestamp);
            }
            default -> throw new IOException("Malformed file sink report: " + line);
        }
    }

    synchronized void check() throws IOException {
        if (failure != null) throw new IOException("File sink failed", failure);
        if (!process.isAlive() && !ended) throw new IOException("File sink exited with " + process.exitValue());
        if (capture != null) capture.check();
    }

    void frame(int table, byte[] bytes) throws Exception {
        check();
        frames.writeInt(table);
        frames.writeInt(bytes.length);
        frames.write(bytes);
    }

    void finish(CdcInput.Counts expected, Duration timeout) throws Exception {
        frames.writeInt(-2);
        frames.writeInt(0);
        frames.flush();
        long deadline = System.nanoTime() + timeout.toNanos();
        synchronized (this) {
            while (transactions < expected.transactions()) {
                check();
                if (System.nanoTime() >= deadline) throw new IOException("File sink commit drain timed out");
                wait(100);
            }
            check();
            if (transactions != expected.transactions() || changes != expected.changes() || !pending.isEmpty())
                throw new IOException("Applied transaction/change counts differ from the selected input prefix");
        }
        frames.writeInt(-3);
        frames.flush();
        if (!process.waitFor(Math.max(1, TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())), TimeUnit.MILLISECONDS))
            throw new IOException("File sink normal close timed out");
        reader.join(5_000);
        synchronized (this) {
            check();
            if (!ended || process.exitValue() != 0) throw new IOException("File sink did not close normally");
            if (freshnessReport != null && (!Files.isRegularFile(freshnessReport) || Files.size(freshnessReport) == 0))
                throw new IOException("Sink freshness did not publish any samples");
            finished = true;
        }
    }

    @Override public void close() throws IOException {
        if (!finished) process.destroyForcibly();
        try { socket.close(); frames.close(); }
        finally {
            if (process.isAlive()) process.destroyForcibly();
            try { if (!process.waitFor(10, TimeUnit.SECONDS)) throw new IOException("File sink did not terminate"); }
            catch (InterruptedException error) { Thread.currentThread().interrupt(); throw new IOException(error); }
        }
    }
}
