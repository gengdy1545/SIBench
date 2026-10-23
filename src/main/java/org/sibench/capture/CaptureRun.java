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
import java.util.List;
import java.util.Map;
import java.util.Set;

/** A capture is usable only after the same-execution client/server audit succeeds. */
final class CaptureRun implements AutoCloseable {
    private static volatile CaptureRun active;
    private static final String MANIFEST = "capture-run.json";
    private final Path directory;
    private final Map<String, Object> manifest = new LinkedHashMap<>();
    private volatile Throwable failure;
    private final Map<Path, Integer> sessions = new LinkedHashMap<>();
    private final Map<String, Object> phases = new LinkedHashMap<>();
    private volatile String phase;
    private final java.util.Set<Path> closed = new java.util.HashSet<>();

    private CaptureRun(Path directory, String mode) throws IOException {
        this.directory = directory.toAbsolutePath().normalize();
        Files.createDirectories(this.directory);
        try (var paths = Files.list(this.directory)) {
            if (paths.anyMatch(p -> p.getFileName().toString().startsWith("jdbc-")))
                throw new IOException(
                        "Capture directory already contains a runtime; use a fresh run");
        }
        manifest.put("schema_version", 1);
        manifest.put("native_mode", mode);
        manifest.put("status", "running");
        manifest.put("started_at", Instant.now().toString());
        Files.writeString(this.directory.resolve(MANIFEST), json(), StandardOpenOption.CREATE_NEW);
    }

    /** A run whose sessions are JDBC connections of this process (HyBench runHTAP). */
    static synchronized CaptureRun begin(Path directory) throws IOException {
        if (active != null) throw new IOException("A capture is already active");
        CaptureRun run = new CaptureRun(directory, "runhtap");
        active = run;
        return run;
    }

    /**
     * A run whose native client is a separate process (YCSB, sysbench). The client's sessions are
     * discovered in the directory once it has exited; see {@link #completeExternal}.
     */
    static synchronized CaptureRun beginExternal(Path directory, String mode) throws IOException {
        if (active != null) throw new IOException("A capture is already active");
        CaptureRun run = new CaptureRun(directory, mode);
        active = run;
        return run;
    }

    static synchronized void session(Path jdbc, int pid) throws IOException {
        CaptureRun run = active;
        if (run == null) return;
        synchronized (run) {
            if (Files.exists(run.directory.resolve("pg-" + pid + ".jsonl"))
                    || run.sessions.containsValue(pid) || run.sessions.containsKey(jdbc))
                throw new IOException("Backend trace already exists; restart in a fresh run directory");
            run.sessions.put(jdbc, pid);
        }
    }

    static void recordFailure(Throwable error, Map<String, ?> context) {
        CaptureRun run = active;
        if (run != null) run.fail(error, context);
        else recordExternalFailure(error, context);
    }

    /**
     * A native client runs in its own JVM, where no CaptureRun is active; its failures are left as
     * files in the capture directory for the controlling run to find.
     */
    private static void recordExternalFailure(Throwable error, Map<String, ?> context) {
        String directory = System.getProperty("sibench.capture.directory");
        if (directory == null) return;
        Map<String, Object> details = new LinkedHashMap<>(context);
        details.put("type", error.getClass().getName());
        details.put("message", error.getMessage());
        try {
            Files.writeString(
                    Path.of(directory)
                            .resolve("capture-failure-" + ProcessHandle.current().pid() + ".json"),
                    new GsonBuilder().serializeNulls().setPrettyPrinting().create().toJson(details),
                    StandardOpenOption.CREATE_NEW);
        } catch (IOException ignored) {
            // The client already fails; the missing marker is detected by its absent sessions.
        }
    }

    static void checkFailure() throws Exception {
        CaptureRun run = active;
        if (run != null && run.failure != null) {
            if (run.failure instanceof Exception error) throw error;
            if (run.failure instanceof Error error) throw error;
            throw new IllegalStateException("Capture failed", run.failure);
        }
    }

    /** Nothing, or the identity the recording driver stated; never the text "null". */
    private static String text(Object value) {
        return value == null ? null : String.valueOf(value);
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
            // Enough identity for a later classification to consume: the SQLSTATE, the driver error
            // code and the request / transaction the recording driver was executing. The policy
            // decides only what kind of failure this is; it does NOT relax the rule below, which
            // keeps every JDBC exception fatal until the taxonomy has real-run evidence
            // (plan.md: 完成分类和实跑验证前，当前 JDBC 异常使采集失败的策略不放宽).
            var classification =
                    SourceFailurePolicy.classify(
                            SourceFailurePolicy.evidence(
                                    sql,
                                    text(details.get("request")),
                                    text(details.get("transaction"))));
            details.put("source_failure_category", classification.category().name());
            details.put("source_failure_basis", classification.basis());
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

    /**
     * Pairs every server session with the JDBC trace of the same backend, when the client had one,
     * and audits each session once. {@code bindings} requires runtime keys to be client-bound.
     */
    void completeExternal(boolean bindings, Map<String, Object> client) throws Exception {
        try {
            checkFailure();
            try (var failures = Files.list(directory)) {
                var names =
                        failures.map(p -> p.getFileName().toString())
                                .filter(n -> n.startsWith("capture-failure-"))
                                .sorted()
                                .toList();
                if (!names.isEmpty())
                    throw new IOException("Native client reported capture failures: " + names);
            }
            var clients = new java.util.TreeMap<Integer, Path>();
            var servers = new java.util.TreeMap<Integer, Path>();
            try (var paths = Files.list(directory)) {
                for (Path path : paths.sorted().toList()) {
                    String name = path.getFileName().toString();
                    if (name.startsWith("jdbc-") && name.endsWith(".jsonl")) {
                        int pid = connectionPid(path);
                        if (clients.put(pid, path) != null)
                            throw new IOException("Several JDBC traces for backend " + pid);
                    } else if (name.startsWith("pg-") && name.endsWith(".jsonl")) {
                        servers.put(
                                Integer.parseInt(name.substring(3, name.length() - ".jsonl".length())),
                                path);
                    }
                }
            }
            if (servers.isEmpty()) throw new IOException("Native client produced no server trace");
            // Each workload's client process exited and its backends drained before the next one
            // started, so the client's workload order is the session order.
            var order = new java.util.ArrayList<Integer>();
            var workloads = new java.util.HashMap<Integer, Object>();
            for (Object item : (java.util.List<?>) client.get("workloads")) {
                var workload = (Map<?, ?>) item;
                for (Object session : (java.util.List<?>) workload.get("sessions")) {
                    String name = (String) ((Map<?, ?>) session).get("pg_file");
                    int pid = Integer.parseInt(name.substring(3, name.length() - ".jsonl".length()));
                    order.add(pid);
                    workloads.put(
                            pid, workload.containsKey("workload") ? workload.get("workload") : workload.get("script"));
                }
            }
            if (!new java.util.HashSet<>(order).equals(servers.keySet()) || order.size() != servers.size())
                throw new IOException("Server traces differ from the sessions the native client ran");
            var records = new java.util.ArrayList<Map<String, Object>>();
            for (int pid : order) {
                Path server = servers.get(pid), jdbcTrace = clients.remove(pid);
                var record = new LinkedHashMap<String, Object>();
                if (jdbcTrace != null) {
                    record.put("jdbc_file", jdbcTrace.getFileName().toString());
                    record.put("jdbc_sha256", BaselineGenerator.sha256(jdbcTrace));
                }
                record.put("workload", workloads.get(pid));
                record.put("pg_file", server.getFileName().toString());
                record.put("pg_sha256", BaselineGenerator.sha256(server));
                record.put("bindings", bindings);
                record.put(
                        "audit",
                        jdbcTrace != null
                                ? RequestCaptureAudit.audit(jdbcTrace, server, bindings)
                                : ServerCaptureAudit.audit(server));
                records.add(record);
            }
            if (!clients.isEmpty())
                throw new IOException("JDBC traces without a server trace: " + clients.values());
            var inventory = new java.util.HashSet<String>();
            for (var record : records) {
                inventory.add((String) record.get("pg_file"));
                if (record.containsKey("jdbc_file")) inventory.add((String) record.get("jdbc_file"));
            }
            verifyInventory(directory, inventory);
            manifest.put("session_order", "sequential");
            manifest.put(
                    "session_order_evidence",
                    "one native client process per workload; each exited and no capture backend"
                            + " remained before the next started");
            manifest.put("sessions", records);
            manifest.put("client", client);
            manifest.put("completed_at", Instant.now().toString());
            manifest.put("status", "complete");
            publish();
        } catch (Exception | Error error) {
            fail(error, Map.of("operation", "complete_capture"));
            throw error;
        }
    }

    static int connectionPid(Path jdbc) throws IOException {
        try (var input = Files.newBufferedReader(jdbc)) {
            String line = input.readLine();
            if (line == null) throw new IOException("Empty JDBC trace: " + jdbc.getFileName());
            JsonObject first = JsonParser.parseString(line).getAsJsonObject();
            if (!first.get("event").getAsString().equals("connection"))
                throw new IOException("JDBC trace does not start with its connection");
            return first.get("pid").getAsInt();
        }
    }

    synchronized void phase(String name, Object summary) throws IOException {
        phase = name;
        if (summary != null) phases.put(name, summary);
        manifest.put("phases", phases);
        publish();
    }

    /**
     * Access templates are PostgreSQL query identifiers, emitted only when the server computes
     * them; checking before the run avoids a capture that cannot be accounted for afterwards.
     */
    static void requireQueryIdentity(String jdbc, String user) throws Exception {
        try (var connection =
                        java.sql.DriverManager.getConnection(
                                jdbc, user, System.getenv().getOrDefault("PGPASSWORD", ""));
                var statement = connection.createStatement();
                var rows = statement.executeQuery("SHOW compute_query_id")) {
            rows.next();
            if (!rows.getString(1).equals("on"))
                throw new IOException(
                        "Capture server must run with compute_query_id = on, found "
                                + rows.getString(1));
        }
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

    /** Audits every JDBC/PG session pair of the run once its workers have closed them. */
    void complete() throws Exception {
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
                // HTAP joins take keys from rows, so scan keys need not be client-bound.
                records.add(session(client, server, false));
            }
            verifyInventory(directory, inventory);
            manifest.put("session_order", "concurrent");
            manifest.put("sessions", records);
            manifest.put("completed_at", Instant.now().toString());
            manifest.put("status", "complete");
            publish();
        } catch (Exception | Error error) {
            fail(error, Map.of("operation", "complete_capture"));
            throw error;
        }
    }

    /** One audited JDBC/PG session pair as the run manifest records it. */
    private static Map<String, Object> session(Path client, Path server, boolean bindings)
            throws Exception {
        var record = new LinkedHashMap<String, Object>();
        record.put("jdbc_file", client.getFileName().toString());
        record.put("jdbc_sha256", BaselineGenerator.sha256(client));
        record.put("pg_file", server.getFileName().toString());
        record.put("pg_sha256", BaselineGenerator.sha256(server));
        record.put("bindings", bindings);
        record.put("audit", RequestCaptureAudit.audit(client, server, bindings));
        return record;
    }

    /** Re-checks a completed run: every registered session file, its digest and its audit. */
    static void verifyInput(Path raw) throws Exception {
        Path input = raw.toAbsolutePath().normalize();
        Path directory = Files.isDirectory(input) ? input : input.getParent();
        Path manifestPath = directory.resolve(MANIFEST);
        if (!Files.exists(manifestPath))
            throw new IOException("Capture has no run manifest: " + manifestPath);
        JsonObject doc =
                JsonParser.parseString(Files.readString(manifestPath)).getAsJsonObject();
        if (!doc.get("status").getAsString().equals("complete"))
            throw new IOException("Capture is failed or incomplete; fix the cause and rerun");
        if (!doc.has("sessions") || !doc.has("session_order"))
            throw new IOException("Capture manifest lacks its sessions; recapture");
        var inventory = new java.util.HashSet<String>();
        for (var item : doc.getAsJsonArray("sessions")) {
            var session = item.getAsJsonObject();
            if (!inventory.add(session.get("pg_file").getAsString())
                    || session.has("jdbc_file")
                            && !inventory.add(session.get("jdbc_file").getAsString()))
                throw new IOException("Duplicate capture session files");
        }
        if (inventory.isEmpty()) throw new IOException("Capture has no sessions");
        verifyInventory(directory, inventory);
        for (var item : doc.getAsJsonArray("sessions")) {
            var session = item.getAsJsonObject();
            Path server = checkedPath(directory, session.get("pg_file").getAsString());
            if (!BaselineGenerator.sha256(server).equals(session.get("pg_sha256").getAsString()))
                throw new IOException("Capture changed after completion");
            if (!session.has("jdbc_file")) {
                ServerCaptureAudit.audit(server);
                continue;
            }
            Path client = checkedPath(directory, session.get("jdbc_file").getAsString());
            if (!BaselineGenerator.sha256(client).equals(session.get("jdbc_sha256").getAsString()))
                throw new IOException("Capture changed after completion");
            RequestCaptureAudit.audit(client, server, session.get("bindings").getAsBoolean());
        }
        if (!input.equals(directory)
                && doc.getAsJsonArray("sessions").asList().stream()
                        .noneMatch(
                                e ->
                                        e.getAsJsonObject()
                                                .get("pg_file")
                                                .getAsString()
                                                .equals(input.getFileName().toString())))
            throw new IOException("Trace is not part of the completed capture");
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
