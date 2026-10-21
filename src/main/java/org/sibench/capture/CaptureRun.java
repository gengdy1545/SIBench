package org.sibench.capture;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.sibench.generation.BaselineGenerator;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.sql.SQLException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/** A capture is usable only after the same-execution client/server audit succeeds. */
final class CaptureRun implements AutoCloseable {
    private static volatile CaptureRun active;
    private static final String MANIFEST = "capture-run.json";
    private final Path directory;
    private final Map<String, Object> manifest = new LinkedHashMap<>();
    private volatile Throwable failure;
    private Path jdbc;
    private int backendPid;
    private final Map<Path, Integer> sessions = new LinkedHashMap<>();
    private final Map<String, Object> phases = new LinkedHashMap<>();
    private volatile String phase;
    private final java.util.Set<Path> closed = new java.util.HashSet<>();
    private final boolean nativeHtap;

    private CaptureRun(Path directory, boolean nativeHtap) throws IOException {
        this.nativeHtap = nativeHtap;
        this.directory = directory.toAbsolutePath().normalize();
        Files.createDirectories(this.directory);
        try (var paths = Files.list(this.directory)) {
            if (paths.anyMatch(p -> p.getFileName().toString().startsWith("jdbc-")))
                throw new IOException(
                        "Capture directory already contains a runtime; use a fresh run");
        }
        manifest.put("schema_version", 1);
        manifest.put("native_mode", nativeHtap ? "runhtap" : "tp_subset");
        manifest.put("status", "running");
        manifest.put("started_at", Instant.now().toString());
        Files.writeString(this.directory.resolve(MANIFEST), json(), StandardOpenOption.CREATE_NEW);
    }

    static synchronized CaptureRun begin(Path directory) throws IOException {
        return begin(directory, false);
    }

    static synchronized CaptureRun begin(Path directory, boolean nativeHtap) throws IOException {
        if (active != null) throw new IOException("A capture is already active");
        CaptureRun run = new CaptureRun(directory, nativeHtap);
        active = run;
        return run;
    }

    static synchronized void session(Path jdbc, int pid) throws IOException {
        CaptureRun run = active;
        if (run == null) return;
        synchronized (run) {
            if (run.jdbc != null && !run.nativeHtap)
                throw new IOException("Only one capture connection is supported");
            if (Files.exists(run.directory.resolve("pg-" + pid + ".jsonl"))
                    || run.sessions.containsValue(pid) || run.sessions.containsKey(jdbc))
                throw new IOException("Backend trace already exists; restart in a fresh run directory");
            run.jdbc = jdbc;
            run.backendPid = pid;
            run.sessions.put(jdbc, pid);
        }
    }

    static void recordFailure(Throwable error, Map<String, ?> context) {
        CaptureRun run = active;
        if (run != null) run.fail(error, context);
    }

    static void checkFailure() throws Exception {
        CaptureRun run = active;
        if (run != null && run.failure != null) {
            if (run.failure instanceof Exception error) throw error;
            if (run.failure instanceof Error error) throw error;
            throw new IllegalStateException("Capture failed", run.failure);
        }
    }

    synchronized void fail(Throwable error, Map<String, ?> context) {
        if (failure != null) return;
        failure = error;
        manifest.put("status", "failed");
        manifest.put("failed_at", Instant.now().toString());
        Map<String, Object> details = new LinkedHashMap<>(context);
        details.put("type", error.getClass().getName());
        details.put("message", error.getMessage());
        if (error instanceof SQLException sql) {
            details.put("sqlstate", sql.getSQLState());
            details.put("error_code", sql.getErrorCode());
        }
        StringWriter stack = new StringWriter();
        error.printStackTrace(new PrintWriter(stack));
        details.put("stacktrace", stack.toString());
        manifest.put("failure", details);
        try {
            publish();
        } catch (IOException logging) {
            error.addSuppressed(logging);
        }
    }

    void complete(long[] counts) throws Exception {
        if (nativeHtap) {
            completeHtap();
            return;
        }
        try {
            checkFailure();
            if (jdbc == null) throw new IOException("Capture has no JDBC connection");
            Path pg = directory.resolve("pg-" + backendPid + ".jsonl");
            manifest.put("audit", RequestCaptureAudit.audit(jdbc, pg));
            manifest.put("jdbc_file", jdbc.getFileName().toString());
            manifest.put("jdbc_sha256", BaselineGenerator.sha256(jdbc));
            manifest.put("pg_file", pg.getFileName().toString());
            manifest.put("pg_sha256", BaselineGenerator.sha256(pg));
            manifest.put("transaction_counts", counts);
            manifest.put("completed_at", Instant.now().toString());
            manifest.put("status", "complete");
            publish();
        } catch (Exception | Error error) {
            fail(error, Map.of("operation", "complete_capture"));
            throw error;
        }
    }

    synchronized void phase(String name, Object summary) throws IOException {
        phase = name;
        if (summary != null) phases.put(name, summary);
        manifest.put("phases", phases);
        publish();
    }

    synchronized void schemaPreflight(Map<String, Object> report) throws IOException {
        manifest.put("schema_preflight", report);
        publish();
    }

    static String phase() {
        CaptureRun run = active;
        return run == null ? null : run.phase;
    }

    static void sessionClosed(Path path) {
        CaptureRun run = active;
        if (run == null) return;
        synchronized (run) {
            run.closed.add(path);
            run.notifyAll();
        }
    }

    synchronized void drainSessions() throws Exception {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.MINUTES.toNanos(2);
        while (closed.size() != sessions.size()) {
            checkFailure();
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) throw new IOException("Native workers did not close their connections");
            wait(Math.min(1000, Math.max(1, remaining / 1_000_000)));
        }
    }

    private void completeHtap() throws Exception {
        try {
            checkFailure();
            drainSessions();
            if (sessions.isEmpty()) throw new IOException("Capture has no JDBC connections");
            var records = new java.util.ArrayList<Map<String, Object>>();
            var inventory = new java.util.HashSet<String>();
            for (var session : sessions.entrySet()) {
                Path client = session.getKey();
                Path server = directory.resolve("pg-" + session.getValue() + ".jsonl");
                inventory.add(client.getFileName().toString());
                inventory.add(server.getFileName().toString());
                records.add(Map.of(
                        "jdbc_file", client.getFileName().toString(),
                        "jdbc_sha256", BaselineGenerator.sha256(client),
                        "pg_file", server.getFileName().toString(),
                        "pg_sha256", BaselineGenerator.sha256(server),
                        "audit", RequestCaptureAudit.audit(client, server, true)));
            }
            verifyInventory(directory, inventory);
            manifest.put("sessions", records);
            manifest.put("completed_at", Instant.now().toString());
            manifest.put("status", "complete");
            publish();
        } catch (Exception | Error error) {
            fail(error, Map.of("operation", "complete_capture"));
            throw error;
        }
    }

    static void verifyInput(Path raw) throws Exception {
        raw = raw.toAbsolutePath().normalize();
        Path directory = Files.isDirectory(raw) ? raw : raw.getParent();
        Path manifestPath = directory.resolve(MANIFEST);
        if (Files.exists(manifestPath)) {
            JsonObject doc =
                    JsonParser.parseString(Files.readString(manifestPath)).getAsJsonObject();
            if (!doc.get("status").getAsString().equals("complete"))
                throw new IOException("Capture is failed or incomplete; fix the cause and rerun");
            if (doc.has("sessions")) {
                var inventory = new java.util.HashSet<String>();
                for (var item : doc.getAsJsonArray("sessions")) {
                    var session = item.getAsJsonObject();
                    if (!inventory.add(session.get("pg_file").getAsString())
                            || !inventory.add(session.get("jdbc_file").getAsString()))
                        throw new IOException("Duplicate capture session files");
                }
                if (inventory.isEmpty()) throw new IOException("Empty HTAP capture");
                verifyInventory(directory, inventory);
                for (var item : doc.getAsJsonArray("sessions")) {
                    var session = item.getAsJsonObject();
                    Path server = checkedPath(directory, session.get("pg_file").getAsString());
                    Path client = checkedPath(directory, session.get("jdbc_file").getAsString());
                    if (!BaselineGenerator.sha256(server).equals(session.get("pg_sha256").getAsString())
                            || !BaselineGenerator.sha256(client).equals(session.get("jdbc_sha256").getAsString()))
                        throw new IOException("HTAP capture changed after completion");
                    RequestCaptureAudit.audit(client, server, true);
                }
                String rawName = raw.getFileName().toString();
                if (!raw.equals(directory) && !doc.getAsJsonArray("sessions").asList().stream()
                        .anyMatch(e -> e.getAsJsonObject().get("pg_file").getAsString()
                                .equals(rawName)))
                    throw new IOException("Trace is not part of completed HTAP capture");
                return;
            }
            if (!raw.getFileName().toString().equals(doc.get("pg_file").getAsString())
                    || !BaselineGenerator.sha256(raw).equals(doc.get("pg_sha256").getAsString()))
                throw new IOException("PG trace does not match the completed capture");
            String jdbcName = doc.get("jdbc_file").getAsString();
            if (!Path.of(jdbcName).getFileName().toString().equals(jdbcName))
                throw new IOException("Invalid JDBC trace path");
            Path jdbc = directory.resolve(jdbcName);
            if (!BaselineGenerator.sha256(jdbc).equals(doc.get("jdbc_sha256").getAsString()))
                throw new IOException("JDBC trace changed after capture completion");
            RequestCaptureAudit.audit(jdbc, raw);
            return;
        }
        // Existing evidence predates run manifests; require its actual paired JDBC trace.
        int pid;
        try (var input = Files.newBufferedReader(raw)) {
            String line = input.readLine();
            if (line == null) throw new IOException("Empty PG trace");
            pid = JsonParser.parseString(line).getAsJsonObject().get("pid").getAsInt();
        }
        Path match = null;
        try (var paths = Files.list(directory)) {
            for (Path candidate :
                    paths.filter(p -> p.getFileName().toString().startsWith("jdbc-"))
                            .sorted()
                            .toList()) {
                try (var input = Files.newBufferedReader(candidate)) {
                    String line = input.readLine();
                    if (line == null) continue;
                    JsonObject connection = JsonParser.parseString(line).getAsJsonObject();
                    if (connection.get("event").getAsString().equals("connection")
                            && connection.get("pid").getAsInt() == pid) {
                        if (match != null) throw new IOException("Ambiguous JDBC capture");
                        match = candidate;
                    }
                }
            }
        }
        if (match == null) throw new IOException("Paired JDBC trace required for legacy capture");
        RequestCaptureAudit.audit(match, raw);
    }

    private static Path checkedPath(Path directory, String name) throws IOException {
        if (!Path.of(name).getFileName().toString().equals(name))
            throw new IOException("Invalid capture trace path");
        return directory.resolve(name);
    }

    private static void verifyInventory(Path directory, java.util.Set<String> expected)
            throws IOException {
        try (var paths = Files.list(directory)) {
            var actual = paths.map(p -> p.getFileName().toString())
                    .filter(n -> n.startsWith("pg-") || n.startsWith("jdbc-"))
                    .collect(java.util.stream.Collectors.toSet());
            if (!actual.equals(expected)) throw new IOException("Unregistered or missing capture traces");
        }
    }

    private String json() {
        return new GsonBuilder().serializeNulls().setPrettyPrinting().create().toJson(manifest);
    }

    private void publish() throws IOException {
        Path pending = directory.resolve(MANIFEST + ".partial");
        Files.writeString(pending, json());
        Files.move(
                pending,
                directory.resolve(MANIFEST),
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING);
    }

    @Override
    public void close() {
        if (failure == null && !manifest.get("status").equals("complete"))
            fail(new IOException("Capture ended without completion"), Map.of());
        active = null;
    }
}
