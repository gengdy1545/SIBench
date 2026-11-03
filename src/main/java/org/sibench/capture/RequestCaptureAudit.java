package org.sibench.capture;

import com.google.gson.*;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;

/** Correlate JDBC and server records from the same execution, without repeating queries. */
public final class RequestCaptureAudit {
    private RequestCaptureAudit() {}

    /**
     * {@code bindings} additionally requires every runtime equality key to be a value bound by
     * the client: true for point-access workloads, false for joins whose keys come from rows.
     */
    static Map<String, Object> audit(Path jdbc, Path pg, boolean bindings) throws Exception {
        var audit = new State(bindings);
        try (var input = ServerTrace.open(jdbc)) {
            for (String line; (line = input.readLine()) != null; )
                audit.client(JsonParser.parseString(line).getAsJsonObject());
        }
        audit.endClient();
        try (var input = ServerTrace.open(pg)) {
            for (String line; (line = input.readLine()) != null; )
                audit.server(JsonParser.parseString(line).getAsJsonObject());
        }
        return audit.finish();
    }

    /** Event-fed audit shared by retained files and direct generation. */
    static final class State {
        private final boolean bindings;
        private Map<String, Object> certificate;
        private final Map<String, Request> requests = new LinkedHashMap<>();
        private final ArrayDeque<JsonObject> waiting = new ArrayDeque<>();
        private static final int MAX_PENDING_REQUESTS = 65_536;
        private static final int MAX_PENDING_ROWS = 1_048_576;
        private String closeApplication, latestApplication, serverApplication, applicationPrefix;
        private long lastRequest, requestCount, checkedRows, noop;
        private int executorReturns, executorCleanup, pendingRows, peakRequests;
        private final List<String> withoutExecutorEnd = new ArrayList<>();
        private final Map<String, Long> abortedCommits = new LinkedHashMap<>();
        private boolean clientTransactionOpen, serverTransactionOpen, serverHeader, clientComplete;
        private long clientCommits, clientRollbacks, commits, aborts, indexEvents, unmatched;
        private int backendPid, serverPid, connections, closes;

        State(boolean bindings) { this.bindings = bindings; }

        synchronized void client(JsonObject e) throws IOException {
            String event = e.get("event").getAsString();
            if (closes != 0) throw new IOException("Client events after connection close");
            if (event.equals("connection")) {
                if (++connections != 1)
                    throw new IOException("Only one JDBC connection supported");
                backendPid = e.get("pid").getAsInt();
                return;
            }
            String app =
                    e.has("application") && !e.get("application").isJsonNull()
                            ? e.get("application").getAsString()
                            : null;
            if (connections != 1 || app == null && !event.equals("close"))
                throw new IOException("Client event without connection or request identity");
            switch (event) {
                case "request_start" -> {
                    var parameters = e.getAsJsonObject("parameters");
                    List<String> bound = !bindings || parameters == null ? List.of()
                            : parameters.entrySet().stream()
                                    .filter(p -> !p.getValue().isJsonNull())
                                    .map(p -> p.getValue().getAsString()).toList();
                    long ordinal = e.has("request") ? e.get("request").getAsLong() : 0;
                    if (e.has("request")) {
                        int separator = app.lastIndexOf(':');
                        if (separator < 0 || ordinal <= lastRequest
                                || !app.substring(separator + 1).equals(Long.toString(ordinal)))
                            throw new IOException("Invalid request sequence");
                        String prefix = app.substring(0, separator);
                        if (applicationPrefix != null && !applicationPrefix.equals(prefix))
                            throw new IOException("Request connection identity changed");
                        applicationPrefix = prefix;
                        lastRequest = ordinal;
                    }
                    String previous = latestApplication;
                    if (requests.put(app, new Request(e.get("sql").getAsString(), bound, ordinal)) != null)
                        throw new IOException("Duplicate request identity");
                    latestApplication = app;
                    requestCount++;
                    peakRequests = Math.max(peakRequests, requests.size());
                    if (requests.size() > MAX_PENDING_REQUESTS)
                        throw new IOException("Pending request audit budget reached");
                    retire(previous);
                    clientTransactionOpen = true;
                }
                case "request_return" -> {
                    if (!requests.containsKey(app))
                        throw new IOException("Return without request start");
                    Request request = requests.get(app);
                    if (request.returned) throw new IOException("Duplicate request return");
                    request.returned = true;
                }
                case "consumed_row" -> {
                    if (!requests.containsKey(app) || !requests.get(app).returned)
                        throw new IOException("Consumed row before request return");
                    Request request = requests.get(app);
                    if (request.rowsComplete) throw new IOException("Consumed row after result completion");
                    reserveRow();
                    if (request.consumed == null) request.consumed = new Consumed();
                    request.consumed.add(e);
                }
                case "request_end" -> {
                    Request request = requests.get(app);
                    if (request == null || !request.returned || request.rowsComplete)
                        throw new IOException("Invalid request result completion");
                    completeRows(app, request);
                }
                case "commit" -> {
                    if (!requests.containsKey(app) || !requests.get(app).returned)
                        throw new IOException("Commit without completed request");
                    clientCommits++;
                    requests.get(app).clientCommits++;
                    clientTransactionOpen = false;
                }
                case "rollback" -> {
                    if (!requests.containsKey(app) || !requests.get(app).returned)
                        throw new IOException("Rollback without completed request");
                    clientRollbacks++;
                    requests.get(app).clientRollbacks++;
                    clientTransactionOpen = false;
                }
                case "autocommit" -> {
                    // An autocommit connection ends every request with an implicit commit; the
                    // server also commits the label statement that names the request.
                    if (!requests.containsKey(app) || !requests.get(app).returned)
                        throw new IOException("Autocommit without completed request");
                    long implicit = e.get("implicit_commits").getAsLong();
                    clientCommits += implicit;
                    requests.get(app).clientCommits += implicit;
                    clientTransactionOpen = false;
                }
                case "close" -> {
                    closes++;
                    closeApplication = app;
                }
                default ->
                        throw new IOException(
                                "Event outside successful single-session scope: " + event);
            }
            drain();
            retire(app);
        }

        synchronized void server(JsonObject e) throws IOException {
            if (!serverHeader) {
                ServerTrace.detail(e);
                serverPid = e.get("pid").getAsInt();
                serverHeader = true;
                return;
            }
            if (e.get("event").getAsString().equals("capture_start"))
                throw new IOException("Repeated capture_start");
            if (waiting.size() >= MAX_PENDING_ROWS)
                throw new IOException("Pending server association audit budget reached");
            waiting.add(e);
            drain();
        }

        private void drain() throws IOException {
            while (!waiting.isEmpty()) {
                var e = waiting.peek();
                if (!requests.containsKey(e.get("application").getAsString())) return;
                waiting.remove();
                serverEvent(e);
            }
        }

        private void serverEvent(JsonObject e) throws IOException {
            String event = e.get("event").getAsString();
            if (e.get("pid").getAsInt() != backendPid)
                throw new IOException("Backend PID differs from JDBC connection");
            String app = e.get("application").getAsString();
            if (!Objects.equals(app, serverApplication)) {
                Request previous = requests.get(serverApplication);
                if (previous != null) previous.serverPassed = true;
                retire(serverApplication);
                serverApplication = app;
            }
            Request request = requests.get(app);
            if (request == null) {
                throw new IOException("Server request has no JDBC identity");
            }
            if (event.equals("executor_start")) {
                serverTransactionOpen = true;
                if (request.started)
                    throw new IOException("Multiple executions for one request");
                request.started = true;
                String sql = request.sql;
                StringBuilder expanded = new StringBuilder();
                int bind = 0;
                // Known HyBench templates have no question marks in literals.
                for (int i = 0; i < sql.length(); i++) {
                    if (sql.charAt(i) == '?') expanded.append('$').append(++bind);
                    else expanded.append(sql.charAt(i));
                }
                if (!normalize(expanded.toString())
                        .equals(normalize(e.get("sql").getAsString())))
                    throw new IOException("Client/server SQL mismatch");
            } else if (event.equals("executor_end")) {
                if (!request.started) throw new IOException("Executor end without start");
                if (request.finished) throw new IOException("Duplicate executor completion");
                request.finished = true;
            } else if (event.equals("executor_run_end")) {
                if (!request.started)
                    throw new IOException("Execution return without start");
                request.executed = true;
                if (!e.has("row_hashes") || !e.has("returned")
                        || e.get("returned").getAsLong() != e.getAsJsonArray("row_hashes").size())
                    throw new IOException("Executor return lacks its complete result hashes");
                if (e.get("depth").getAsInt() <= 1) {
                    for (JsonElement hash : e.getAsJsonArray("row_hashes")) {
                        if (request.rowsComplete) {
                            if (request.consumed != null) request.consumed.checkHash(app, hash.getAsString());
                        } else {
                            reserveRow();
                            if (request.serverHashes == null) request.serverHashes = new ArrayList<>();
                            request.serverHashes.add(hash.getAsString());
                        }
                    }
                }
            } else if (event.equals("result_row")) {
                if (request.rowsComplete) {
                    if (request.consumed != null) request.consumed.checkValues(app, e.getAsJsonArray("values"));
                } else {
                    reserveRow();
                    if (request.serverValues == null) request.serverValues = new ArrayList<>();
                    request.serverValues.add(e.getAsJsonArray("values"));
                }
            } else if (event.equals("transaction_commit")) {
                commits++;
                request.serverCommits++;
                serverTransactionOpen = false;
            } else if (event.equals("row_change")
                    || event.equals("index_insert_attempt")) {
                request.wrote = true;
            } else if (event.equals("transaction_abort")) {
                aborts++;
                request.aborts++;
                serverTransactionOpen = false;
            }
            if (event.equals("scan_rescan") && bindings && !isPlannerProbe(e)) {
                for (JsonElement item : e.getAsJsonArray("keys")) {
                    JsonObject key = item.getAsJsonObject();
                    if (key.get("value").isJsonNull()) continue;
                    if (!request.bound.contains(key.get("value").getAsString()))
                        throw new IOException(
                                "Runtime search key is not a bound template value");
                }
            }
            if (event.startsWith("scan_")
                    || event.startsWith("bitmap_")
                    || event.equals("index_insert_attempt")) indexEvents++;
            retire(app);
        }

        private void reserveRow() throws IOException {
            if (++pendingRows > MAX_PENDING_ROWS) throw new IOException("Pending result audit budget reached");
        }

        private void completeRows(String app, Request request) throws IOException {
            if (request.rowsComplete) return;
            request.rowsComplete = true;
            if (request.serverHashes != null) {
                if (request.consumed != null)
                    for (String hash : request.serverHashes) request.consumed.checkHash(app, hash);
                pendingRows -= request.serverHashes.size();
                request.serverHashes = null;
            }
            if (request.serverValues != null) {
                if (request.consumed != null)
                    for (JsonArray values : request.serverValues) request.consumed.checkValues(app, values);
                pendingRows -= request.serverValues.size();
                request.serverValues = null;
            }
        }

        private void retire(String app) throws IOException {
            Request request = requests.get(app);
            // ExecutorEnd precedes scan cleanup; both streams must move past a sealed request.
            if (request == null || request.ordinal == 0 || Objects.equals(app, latestApplication)
                    || !request.rowsComplete || !request.returned || !request.started
                    || !request.executed || !request.finished || !request.serverPassed
                    || request.clientCommits != request.serverCommits || request.aborts != request.clientRollbacks)
                return;
            summarize(app, request, 0);
            requests.remove(app);
            if (request.consumed != null) pendingRows -= request.consumed.size;
        }

        synchronized void endClient() throws IOException {
            if (clientComplete) return;
            clientComplete = true;
            for (var entry : requests.entrySet()) completeRows(entry.getKey(), entry.getValue());
        }

        private void summarize(String app, Request request, long closeAbort) throws IOException {
            if (request.started && !request.finished) withoutExecutorEnd.add(app);
            if (!request.started || !request.executed)
                throw new IOException("Incomplete client/server event association");
            executorReturns++;
            if (request.finished) executorCleanup++;
            long missingCommits = request.clientCommits - request.serverCommits;
            if (missingCommits > 0) {
                if (request.wrote || request.aborts == 0)
                    throw new IOException("Client commit has no server commit, and the request neither changed rows"
                            + " nor ended in a server abort: " + app);
                abortedCommits.put(app, missingCommits);
                noop += missingCommits;
            } else if (missingCommits < 0) throw new IOException("Commit request identities differ");
            if (request.aborts - request.clientRollbacks != closeAbort)
                throw new IOException("Server abort is not associated with rollback or an open transaction at close");
            if (request.consumed != null) {
                if (request.consumed.hashesChecked < request.consumed.size)
                    throw new IOException("Client consumed uncaptured row: " + app);
                checkedRows += request.consumed.size;
            }
        }

        synchronized Map<String, Object> finish() throws IOException {
            if (certificate != null) return new LinkedHashMap<>(certificate);
            endClient();
        if (connections != 1 || closes != 1 || requests.isEmpty())
            throw new IOException("JDBC capture has no complete runtime");
        if (requests.values().stream().anyMatch(request -> !request.returned))
            throw new IOException("Missing client request completion");

            drain();
            if (!serverHeader || serverPid != backendPid || !waiting.isEmpty())
                throw new IOException("Incomplete client/server event association");
        if (clientTransactionOpen && closeApplication != null && !requests.containsKey(closeApplication))
            throw new IOException("Close has no matching open request");
        for (var entry : requests.entrySet())
            summarize(entry.getKey(), entry.getValue(),
                    clientTransactionOpen && Objects.equals(entry.getKey(), closeApplication) ? 1 : 0);
        withoutExecutorEnd.sort(null);
        if (unmatched != 0 || commits + noop != clientCommits || serverTransactionOpen
                || aborts < clientRollbacks || aborts > clientRollbacks + 1)
            throw new IOException("Incomplete client/server event association");
        var report = new LinkedHashMap<String, Object>();
        // The two tolerances, named so a run's report shows them instead of hiding them.
        report.put("requests_without_executor_end", withoutExecutorEnd);
        report.put("client_commits_ended_by_server_abort", abortedCommits);
        report.putAll(
                Map.of(
                        "status",
                        "passed",
                        "requests",
                        requestCount,
                        "commits",
                        commits,
                        "matched_index_events",
                        indexEvents,
                        "client_consumed_rows_checked",
                        checkedRows,
                        "unmatched_server_events",
                        unmatched,
                        "scope",
                        "one JDBC session; same-execution SQL, results, commits and rollback/close"
                                + (bindings ? "; runtime scan keys are client-bound values" : "")));
        report.put("peak_pending_requests", peakRequests);
        report.put("explicit_rollbacks", clientRollbacks);
        report.put("server_aborts", aborts);
        report.put("close_aborts", aborts - clientRollbacks);
        report.put("executor_returns", executorReturns);
        report.put("executor_cleanup_events", executorCleanup);
        certificate = new LinkedHashMap<>(report);
        return report;
    }
    }

    /** One state per request; late result consumption can refer to an earlier request. */
    private static final class Request {
        final String sql;
        final List<String> bound;
        final long ordinal;
        boolean returned, started, finished, executed, wrote, rowsComplete, serverPassed;
        long clientCommits, clientRollbacks, serverCommits, aborts;
        Consumed consumed;
        List<String> serverHashes;
        List<JsonArray> serverValues;

        Request(String sql, List<String> bound, long ordinal) {
            this.ordinal = ordinal;
            this.sql = sql;
            this.bound = bound;
        }
    }

    /** Exact 64-bit row hashes; summary captures retain no per-row JSON or server-side copies. */
    private static final class Consumed {
        private long[] hashes = new long[4];
        private int size;
        private long hashesChecked, valuesChecked;
        private Map<Integer, JsonObject> detailed;

        void add(JsonObject client) throws IOException {
            long hash = hashBits(clientHash(client));
            if (size == hashes.length) hashes = Arrays.copyOf(hashes, Math.multiplyExact(size, 2));
            hashes[size] = hash;
            if (client.has("column_types")) {
                if (detailed == null) detailed = new HashMap<>();
                detailed.put(size, client);
            }
            size++;
        }

        void checkHash(String request, String hash) throws IOException {
            if (hashesChecked < size && hashes[(int) hashesChecked] != hashBits(hash))
                throw new IOException("Client result differs from same-execution server result: "
                        + request + " row=" + (hashesChecked + 1) + " server_hash=" + hash);
            hashesChecked++;
        }

        void checkValues(String request, JsonArray server) throws IOException {
            JsonObject client = detailed == null || valuesChecked >= size
                    ? null : detailed.remove((int) valuesChecked);
            valuesChecked++;
            if (client == null) return;
            for (int j = 0; j < server.size(); j++) {
                JsonElement expected = server.get(j), actual = client.get(Integer.toString(j + 1));
                if (!Objects.equals(expected, actual)
                        && !equalResult(expected, actual,
                                client.getAsJsonObject("column_types")
                                        .get(Integer.toString(j + 1)).getAsInt()))
                    throw new IOException("Client result differs from same-execution server result: "
                            + request + " column=" + (j + 1)
                            + " server=" + expected + " client=" + actual);
            }
        }

        private static long hashBits(String hash) throws IOException {
            if (hash.length() != 16) throw new IOException("Invalid recorded row hash: " + hash);
            for (int i = 0; i < hash.length(); i++) {
                char c = hash.charAt(i);
                if (!(c >= '0' && c <= '9' || c >= 'a' && c <= 'f'))
                    throw new IOException("Invalid recorded row hash: " + hash);
            }
            return Long.parseUnsignedLong(hash, 16);
        }
    }

    /**
     * The client's hash of a consumed row. A row that also kept its text must hash to what it
     * recorded.
     */
    static String clientHash(JsonObject client) throws IOException {
        if (!client.has("row_hash")) throw new IOException("Consumed row without recorded hash");
        String recorded = client.get("row_hash").getAsString();
        if (!client.has("column_types")) {
            if (recorded == null) throw new IOException("Consumed row without values or hash");
            return recorded;
        }
        JsonObject types = client.getAsJsonObject("column_types");
        var values = new ArrayList<String>();
        var jdbcTypes = new ArrayList<Integer>();
        for (int j = 1; j <= types.size(); j++) {
            JsonElement value = client.get(Integer.toString(j));
            values.add(value == null || value.isJsonNull() ? null : value.getAsString());
            jdbcTypes.add(types.get(Integer.toString(j)).getAsInt());
        }
        String computed = ResultRowHash.of(values, jdbcTypes);
        if (recorded != null && !recorded.equals(computed))
            throw new IOException("Consumed row hash differs from its values");
        return computed;
    }

    /** Planner endpoint probes carry no bound value; they are not search predicates. */
    static boolean isPlannerProbe(JsonObject scan) {
        JsonArray keys = scan.getAsJsonArray("keys");
        if (keys.size() != 1) return false;
        JsonObject key = keys.get(0).getAsJsonObject();
        return key.has("strategy")
                && key.has("flags")
                && key.get("strategy").getAsInt() == 0
                && (key.get("flags").getAsInt() & 0x80) != 0;
    }

    private static String normalize(String sql) {
        return sql.strip().replaceAll(";+$", "").replaceAll("\\s+", " ").toLowerCase(Locale.ROOT);
    }

    static boolean equalResult(JsonElement expected, JsonElement actual, int jdbcType) {
        if (expected == null || actual == null || expected.isJsonNull() || actual.isJsonNull())
            return Objects.equals(expected, actual);
        String a = expected.getAsString(), b = actual.getAsString();
        return switch (jdbcType) {
            case java.sql.Types.REAL ->
                    Float.floatToIntBits(Float.parseFloat(a))
                            == Float.floatToIntBits(Float.parseFloat(b));
            case java.sql.Types.FLOAT, java.sql.Types.DOUBLE ->
                    Double.doubleToLongBits(Double.parseDouble(a))
                            == Double.doubleToLongBits(Double.parseDouble(b));
            case java.sql.Types.NUMERIC, java.sql.Types.DECIMAL ->
                    new java.math.BigDecimal(a).compareTo(new java.math.BigDecimal(b)) == 0;
            case java.sql.Types.TIMESTAMP ->
                    java.time.LocalDateTime.parse(a.replace(' ', 'T'))
                            .equals(java.time.LocalDateTime.parse(b.replace(' ', 'T')));
            case java.sql.Types.CHAR -> a.stripTrailing().equals(b.stripTrailing());
            default -> a.equals(b);
        };
    }
}
