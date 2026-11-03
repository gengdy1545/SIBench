package org.sibench.capture;

import com.google.gson.*;
import org.sibench.config.ConfigValues;

import java.io.*;
import java.net.*;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

/** Receives source facts during generation; only unresolved transactions may use scratch files. */
final class DirectCapture implements AutoCloseable {
    private static volatile DirectCapture active;
    final CaptureProfile profile;
    final CaptureTransactions transactions;
    final GenerationBudget budget;
    private final boolean jdbc, bindings;
    private final boolean full;
    private final ServerSocketChannel listener;
    private final ExecutorService readers = Executors.newCachedThreadPool();
    private final Map<Path, Stream> streams = new LinkedHashMap<>();
    private final Map<Integer, RequestCaptureAudit.State> audits = new HashMap<>();
    private final CompletableFuture<Map<String, Object>> compilation;
    private final CompletableFuture<Void> prepared = new CompletableFuture<>();
    private final CompletableFuture<Void> drained = new CompletableFuture<>();
    private final CompletableFuture<Void> publication = new CompletableFuture<>();
    private final Path endpoint;
    private volatile Throwable failure;
    private volatile boolean stopping;
    private volatile Connection watermarkConnection;

    private final class Stream {
        final Path path;
        final MessageDigest digest = org.sibench.config.Digests.digest();
        final CompletableFuture<Void> ended = new CompletableFuture<>();
        final SocketChannel socket;
        final boolean server;
        final ServerCaptureAudit.State audit = new ServerCaptureAudit.State();
        JsonObject header;
        int pid;
        long events, bytes, requests;
        long completedSequence, completedCsn, acknowledgedSequence;
        String sha256;
        Map<String, Object> serverReport;
        Map<String, Object> pairedReport;
        CaptureTransactions.Chunk chunk;
        final OutputStream raw;
        Stream(Path path, SocketChannel socket) throws IOException {
            this.path = path; this.socket = socket;
            server = ServerTrace.isServerTrace(path.getFileName().toString());
            raw = full ? new com.github.luben.zstd.ZstdOutputStream(new BufferedOutputStream(
                    Files.newOutputStream(path, StandardOpenOption.CREATE_NEW), 1 << 16), 1) : null;
        }
    }

    DirectCapture(CaptureProfile profile, Path endpoint, Connection connection, IndexTraceCompiler.Checks checks)
            throws Exception {
        this.profile = profile;
        full = TraceOutput.from(profile.layout().document()) == TraceOutput.FULL;
        jdbc = !profile.layout().benchmark().equals("sysbench");
        bindings = profile.layout().benchmark().equals("ycsb");
        this.endpoint = endpoint;
        var settings = org.sibench.config.SibenchConfig.read(profile.artifacts().resolve("native/sibench.yaml"));
        var generation = ConfigValues.object(settings, "generation");
        budget = new GenerationBudget(generation, budgetPath(profile.artifacts()));
        if (budget.enabled() && !budgetPath(profile.artifacts()).toString().equals(System.getenv("SIBENCH_BUDGET_STOP")))
            throw new IOException("Generation targets require the native admission-stop environment");
        transactions = new CaptureTransactions(profile.artifacts().resolve("transactions"),
                ConfigValues.positive(generation, "pending_transaction_bytes"),
                Math.toIntExact(ConfigValues.positive(generation, "pending_transactions")));
        listener = ServerSocketChannel.open(StandardProtocolFamily.UNIX);
        listener.bind(UnixDomainSocketAddress.of(endpoint));
        watermarkConnection = connection;
        readers.submit(this::accept);
        compilation = CompletableFuture.supplyAsync(() -> {
            try {
                return IndexTraceCompiler.compileDirect(profile, this, checks);
            } catch (Throwable error) { fail(error); throw new CompletionException(error); }
        });
    }

    static synchronized DirectCapture begin(CaptureProfile profile, String jdbc, String user) throws Exception {
        if (active != null) throw new IOException("A direct capture is already active");
        Path endpoint = Path.of(Objects.requireNonNull(System.getenv("SIBENCH_CAPTURE_SOCKET"),
                "Generation requires SIBENCH_CAPTURE_SOCKET"));
        Connection connection = DriverManager.getConnection(jdbc, user, System.getenv().getOrDefault("PGPASSWORD", ""));
        DirectCapture capture = null;
        try {
            capture = new DirectCapture(profile, endpoint, connection, new IndexTraceCompiler.Checks(
                    profile.artifacts().resolve("index-counters-before.json"),
                    profile.artifacts().resolve("index-counters-after.json")));
            active = capture;
            capture.awaitReady();
            return capture;
        } catch (Exception | Error error) {
            try { if (capture != null) capture.close(); else connection.close(); }
            catch (Exception close) { error.addSuppressed(close); }
            throw error;
        }
    }

    static DirectCapture current(Path directory) {
        var capture = active;
        return capture != null && capture.profile.raw().toAbsolutePath().normalize()
                .equals(directory.toAbsolutePath().normalize()) ? capture : null;
    }

    static Path budgetPath(Path artifacts) {
        return artifacts.resolve("native/budget-stop").toAbsolutePath().normalize();
    }

    static boolean budgetStopped(Path directory) {
        var capture = current(directory);
        return capture != null && capture.budget.requested();
    }

    private void accept() {
        try {
            while (!stopping) {
                var socket = listener.accept();
                readers.submit(() -> receive(socket));
            }
        } catch (Throwable error) { if (!stopping) fail(error); }
    }

    private void receive(SocketChannel socket) {
        Stream stream = null;
        try (socket; var input = new BufferedReader(new InputStreamReader(
                Channels.newInputStream(socket), StandardCharsets.UTF_8), 1 << 16)) {
            String name = input.readLine();
            if (name == null || !name.matches("(?:pg-[0-9]+|jdbc-[0-9]+-[0-9]+)\\.jsonl\\.zst"))
                throw new IOException("Invalid source stream identity");
            Path path = profile.raw().resolve(name).toAbsolutePath().normalize();
            stream = new Stream(path, socket);
            synchronized (this) {
                if (streams.putIfAbsent(path, stream) != null) throw new IOException("Repeated source stream: " + name);
                notifyAll();
            }
            for (String line; (line = input.readLine()) != null; ) {
                var event = JsonParser.parseString(line).getAsJsonObject();
                String type = event.get("event").getAsString();
                if (type.equals("capture_end")) {
                    if (stream.chunk != null) throw new IOException("Source stream ends inside a transaction");
                    if (!stream.server) audit(stream.pid).endClient();
                    if (stream.raw != null) stream.raw.close();
                    stream.sha256 = HexFormat.of().formatHex(stream.digest.digest());
                    if (stream.server) {
                        transactions.endStream(stream.pid);
                        socketAck(stream.socket, 'E');
                    }
                    stream.ended.complete(null);
                    retireAudit(stream.pid);
                    return;
                }
                byte[] bytes = (line + "\n").getBytes(StandardCharsets.UTF_8);
                if (stream.raw != null) stream.raw.write(bytes);
                stream.digest.update(bytes);
                stream.bytes = Math.addExact(stream.bytes, bytes.length);
                stream.events++;
                if (stream.server) server(stream, line, event, bytes.length);
                else client(stream, event);
            }
            throw new EOFException("Source stream lost its completion marker: " + name);
        } catch (Throwable error) {
            if (stream != null) {
                if (stream.raw != null) try { stream.raw.close(); } catch (IOException close) { error.addSuppressed(close); }
                stream.ended.completeExceptionally(error);
            }
            fail(error);
        }
    }

    private synchronized RequestCaptureAudit.State audit(int pid) {
        return audits.computeIfAbsent(pid, ignored -> new RequestCaptureAudit.State(bindings));
    }

    private void server(Stream stream, String line, JsonObject event, int encodedBytes) throws IOException {
        if (stream.header == null) {
            ServerTrace.detail(event);
            stream.header = event;
            stream.pid = event.get("pid").getAsInt();
            if (stream.pid != ServerTrace.pid(stream.path.getFileName().toString()))
                throw new IOException("Source PID differs from stream identity");
        }
        if (jdbc) audit(stream.pid).server(event);
        else stream.audit.accept(event);
        if (stream.chunk == null) stream.chunk = transactions.begin(stream.pid);
        stream.chunk.append(line, event, encodedBytes);
        String type = event.get("event").getAsString();
        if (type.equals("transaction_commit") || type.equals("transaction_abort")) {
            long sequence = event.get("capture_sequence").getAsLong();
            long csn = event.get("csn").getAsLong();
            synchronized (stream) {
                if (sequence != stream.completedSequence + 1 || csn < stream.completedCsn)
                    throw new IOException("Source transaction sequence moved backwards or skipped");
                transactions.complete(stream.chunk, csn,
                        event.get("xid").getAsLong() != 0, type.equals("transaction_commit"));
                stream.chunk = null;
                stream.completedCsn = csn;
                stream.completedSequence = sequence;
            }
        }
    }

    private static void socketAck(SocketChannel socket, char acknowledgement) throws IOException {
        var buffer = java.nio.ByteBuffer.wrap(new byte[] {(byte) acknowledgement});
        while (buffer.hasRemaining()) socket.write(buffer);
    }

    private void client(Stream stream, JsonObject event) throws IOException {
        if (stream.header == null) {
            if (!event.get("event").getAsString().equals("connection"))
                throw new IOException("JDBC stream lacks connection identity");
            stream.header = event;
            stream.pid = event.get("pid").getAsInt();
        }
        if (event.get("event").getAsString().equals("request_start")) stream.requests++;
        audit(stream.pid).client(event);
    }

    synchronized List<Path> paths(boolean server) throws IOException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
        while (!server && jdbc && streams.values().stream().filter(s -> s.server).count()
                > streams.values().stream().filter(s -> !s.server).count()) {
            check();
            if (System.nanoTime() > deadline) throw new IOException("Missing JDBC source streams");
            try { wait(20); }
            catch (InterruptedException error) { Thread.currentThread().interrupt(); throw new IOException(error); }
        }
        return streams.values().stream().filter(s -> s.server == server).map(s -> s.path).sorted().toList();
    }

    private Stream stream(Path path) throws IOException {
        synchronized (this) {
            check();
            Stream stream = streams.get(path.toAbsolutePath().normalize());
            if (stream == null) throw new IOException("Missing source stream: " + path);
            return stream;
        }
    }

    private Stream ended(Path path) throws IOException {
        Stream stream = stream(path);
        try { stream.ended.get(120, TimeUnit.SECONDS); }
        catch (Exception error) { throw new IOException("Source stream did not complete: " + path, error); }
        check();
        return stream;
    }

    int connectionPid(Path path) throws IOException { return ended(path).pid; }
    long requestCount(Path path) throws IOException { return ended(path).requests; }

    Map<String, Object> serverAudit(Path path) throws IOException {
        Stream stream = ended(path);
        synchronized (stream) {
            if (stream.serverReport == null) stream.serverReport = stream.audit.finish();
            return stream.serverReport;
        }
    }

    Map<String, Object> sessionAudit(Path client, Path server) throws IOException {
        Stream source = ended(server);
        synchronized (source) {
            if (source.pairedReport != null) return new LinkedHashMap<>(source.pairedReport);
            var result = new LinkedHashMap<String, Object>();
            result.put("pg_file", server.getFileName().toString());
            result.put("transport", "direct_events");
            if (full) result.put("pg_sha256", FileDigests.sha256(server));
            result.put("pg_events_sha256", source.sha256);
            result.put("source_events", source.events);
            result.put("source_bytes", source.bytes);
            if (client != null) {
                Stream connection = ended(client);
                if (connection.pid != source.pid) throw new IOException("Client/server source identities differ");
                result.put("jdbc_file", client.getFileName().toString());
                result.put("jdbc_events_sha256", connection.sha256);
                if (full) result.put("jdbc_sha256", FileDigests.sha256(client));
                result.put("audit", audit(source.pid).finish());
            } else result.put("audit", serverAudit(server));
            source.pairedReport = new LinkedHashMap<>(result);
            return result;
        }
    }

    /** Closed source pairs retain their certificate, not their per-request audit state. */
    private void retireAudit(int pid) throws IOException {
        Path server = null, client = null;
        synchronized (this) {
            for (Stream stream : streams.values()) {
                if (!stream.ended.isDone() || stream.ended.isCompletedExceptionally() || stream.pid != pid) continue;
                if (stream.server) server = stream.path; else client = stream.path;
            }
        }
        if (server == null || jdbc && client == null) return;
        sessionAudit(client, server);
        synchronized (this) { audits.remove(pid); }
    }

    private long watermark() throws SQLException {
        List<Stream> snapshot;
        synchronized (this) { snapshot = List.copyOf(streams.values()); }
        long safe = -1;
        for (Stream stream : snapshot) {
            if (!stream.server) continue;
            long sequence, csn;
            synchronized (stream) {
                sequence = stream.completedSequence;
                csn = stream.completedCsn;
            }
            if (sequence <= stream.acknowledgedSequence) continue;
            safe = watermarkQuery("SELECT public.sibench_capture_watermark(" + stream.pid + "," + sequence + "," + csn + ")");
            stream.acknowledgedSequence = sequence;
        }
        return safe < 0 ? watermarkQuery("SELECT public.sibench_capture_watermark()") : safe;
    }

    private long watermarkQuery(String sql) throws SQLException {
        try (var statement = watermarkConnection.createStatement(); var result = statement.executeQuery(sql)) {
            if (!result.next()) throw new SQLException("Missing source completion watermark");
            return result.getLong(1);
        }
    }

    CaptureTransactions.Chunk next() throws Exception { return transactions.next(this::watermark); }

    void ready() { prepared.complete(null); }
    void awaitReady() throws Exception { prepared.get(); }

    List<Map<String, Object>> sourceReports() throws Exception {
        drained.complete(null);
        publication.get();
        var reports = new ArrayList<Map<String, Object>>();
        for (Path path : paths(true)) {
            var stream = ended(path);
            reports.add(Map.of("source_stream", path.getFileName().toString(), "pid", stream.pid,
                    "sha256", stream.sha256, "source_events", stream.events));
        }
        return reports;
    }

    void endSources() throws Exception {
        for (Path path : paths(true)) ended(path);
        var clients = new HashMap<Integer, Path>();
        for (Path path : paths(false)) {
            int pid = ended(path).pid;
            if (clients.put(pid, path) != null) throw new IOException("Several JDBC streams for one backend");
        }
        for (Path path : paths(true)) {
            Path client = clients.remove(ended(path).pid);
            if (jdbc && client == null) throw new IOException("Server stream has no JDBC source");
            sessionAudit(client, path);
        }
        if (!clients.isEmpty()) throw new IOException("JDBC source has no server stream");
        check();
        transactions.finish();
        drained.get();
        RunTiming.current().note("pending_source_transactions", transactions.metrics());
        watermarkConnection.close();
        watermarkConnection = null;
    }

    Map<String, Object> finish() throws Exception {
        publication.complete(null);
        try { return compilation.get(); }
        catch (ExecutionException error) {
            if (error.getCause() instanceof Exception cause) throw cause;
            throw new IOException(error.getCause());
        }
    }

    synchronized void check() throws IOException {
        if (failure != null) throw new IOException("Direct capture failed", failure);
    }

    private void fail(Throwable error) {
        List<Stream> snapshot;
        synchronized (this) {
            if (failure != null) return;
            failure = error;
            snapshot = List.copyOf(streams.values());
        }
        CaptureRun.recordFailure(error, Map.of("stage", "direct_generation"));
        transactions.fail(error);
        prepared.completeExceptionally(error);
        drained.completeExceptionally(error);
        publication.completeExceptionally(error);
        for (Stream stream : snapshot) {
            try { stream.socket.close(); }
            catch (IOException close) { error.addSuppressed(close); }
        }
    }

    @Override public void close() throws Exception {
        stopping = true;
        listener.close();
        boolean successful = compilation.isDone() && !compilation.isCompletedExceptionally() && !compilation.isCancelled();
        if (!successful) fail(new IOException("Generation closed before successful publication"));
        List<Stream> snapshot;
        synchronized (this) { snapshot = List.copyOf(streams.values()); }
        for (Stream stream : snapshot) stream.socket.close();
        readers.shutdownNow();
        readers.awaitTermination(5, TimeUnit.SECONDS);
        if (watermarkConnection != null) watermarkConnection.close();
        if (successful) transactions.close();
        else transactions.retainFailure();
        Files.deleteIfExists(endpoint);
        active = null;
    }
}
