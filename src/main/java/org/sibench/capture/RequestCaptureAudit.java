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
        Map<String, JsonObject> requests = new LinkedHashMap<>();
        Set<String> returned = new HashSet<>();
        Map<String, List<JsonObject>> consumed = new HashMap<>();
        Map<String, Long> clientCommitRequests = new HashMap<>(),
                rollbackRequests = new HashMap<>();
        Map<String, Long> serverCommitRequests = new HashMap<>(), abortRequests = new HashMap<>();
        String closeApplication = null;
        boolean clientTransactionOpen = false;
        long clientCommits = 0, clientRollbacks = 0;
        int backendPid = 0, connections = 0, closes = 0;
        try (var input = ServerTrace.open(jdbc)) {
            String line;
            while ((line = input.readLine()) != null) {
                JsonObject e = JsonParser.parseString(line).getAsJsonObject();
                String event = e.get("event").getAsString();
                if (closes != 0) throw new IOException("Client events after connection close");
                if (event.equals("connection")) {
                    if (++connections != 1)
                        throw new IOException("Only one JDBC connection supported");
                    backendPid = e.get("pid").getAsInt();
                    continue;
                }
                String app =
                        e.has("application") && !e.get("application").isJsonNull()
                                ? e.get("application").getAsString()
                                : null;
                if (connections != 1 || app == null && !event.equals("close"))
                    throw new IOException("Client event without connection or request identity");
                switch (event) {
                    case "request_start" -> {
                        if (requests.put(app, e) != null)
                            throw new IOException("Duplicate request identity");
                        clientTransactionOpen = true;
                    }
                    case "request_return" -> {
                        if (!requests.containsKey(app))
                            throw new IOException("Return without request start");
                        if (!returned.add(app)) throw new IOException("Duplicate request return");
                    }
                    case "consumed_row" -> {
                        if (!returned.contains(app))
                            throw new IOException("Consumed row before request return");
                        consumed.computeIfAbsent(app, k -> new ArrayList<>()).add(e);
                    }
                    case "commit" -> {
                        if (!returned.contains(app))
                            throw new IOException("Commit without completed request");
                        clientCommits++;
                        clientCommitRequests.merge(app, 1L, Long::sum);
                        clientTransactionOpen = false;
                    }
                    case "rollback" -> {
                        if (!returned.contains(app))
                            throw new IOException("Rollback without completed request");
                        clientRollbacks++;
                        rollbackRequests.merge(app, 1L, Long::sum);
                        clientTransactionOpen = false;
                    }
                    case "autocommit" -> {
                        // An autocommit connection ends every request with an implicit commit; the
                        // server also commits the label statement that names the request.
                        if (!returned.contains(app))
                            throw new IOException("Autocommit without completed request");
                        long implicit = e.get("implicit_commits").getAsLong();
                        clientCommits += implicit;
                        clientCommitRequests.merge(app, implicit, Long::sum);
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
            }
        }
        if (connections != 1 || closes != 1 || requests.isEmpty())
            throw new IOException("JDBC capture has no complete runtime");
        if (!requests.keySet().equals(returned))
            throw new IOException("Missing client request completion");
        Set<String> started = new HashSet<>(),
                finished = new HashSet<>(),
                executed = new HashSet<>(),
                writeRequests = new HashSet<>();
        Map<String, List<JsonArray>> serverResults = new HashMap<>();
        // Row hashes from each top-level return, in result order.
        Map<String, List<String>> serverHashes = new HashMap<>();
        long commits = 0, aborts = 0, indexEvents = 0, unmatched = 0;
        boolean serverTransactionOpen = false;
        try (var input = ServerTrace.open(pg)) {
            String line;
            while ((line = input.readLine()) != null) {
                JsonObject e = JsonParser.parseString(line).getAsJsonObject();
                String event = e.get("event").getAsString();
                if (event.equals("capture_start")) continue;
                if (e.get("pid").getAsInt() != backendPid)
                    throw new IOException("Backend PID differs from JDBC connection");
                String app = e.get("application").getAsString();
                JsonObject request = requests.get(app);
                if (request == null) {
                    unmatched++;
                    continue;
                }
                if (event.equals("executor_start")) {
                    serverTransactionOpen = true;
                    if (!started.add(app))
                        throw new IOException("Multiple executions for one request");
                    String sql = request.get("sql").getAsString();
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
                    if (!started.contains(app)) throw new IOException("Executor end without start");
                    if (!finished.add(app)) throw new IOException("Duplicate executor completion");
                } else if (event.equals("executor_run_end")) {
                    if (!started.contains(app))
                        throw new IOException("Execution return without start");
                    executed.add(app);
                    if (e.has("row_hashes") && e.get("depth").getAsInt() <= 1) {
                        var hashes = serverHashes.computeIfAbsent(app, k -> new ArrayList<>());
                        for (JsonElement hash : e.getAsJsonArray("row_hashes"))
                            hashes.add(hash.getAsString());
                    }
                } else if (event.equals("result_row")) {
                    serverResults
                            .computeIfAbsent(app, k -> new ArrayList<>())
                            .add(e.getAsJsonArray("values"));
                } else if (event.equals("transaction_commit")) {
                    commits++;
                    serverCommitRequests.merge(app, 1L, Long::sum);
                    serverTransactionOpen = false;
                } else if (event.equals("row_change")
                        || event.equals("index_insert_attempt")) {
                    writeRequests.add(app);
                } else if (event.equals("transaction_abort")) {
                    aborts++;
                    abortRequests.merge(app, 1L, Long::sum);
                    serverTransactionOpen = false;
                }
                if (event.equals("scan_rescan") && bindings && !isPlannerProbe(e)) {
                    List<String> bound =
                            request.getAsJsonObject("parameters").entrySet().stream()
                                    .filter(p -> !p.getValue().isJsonNull())
                                    .map(p -> p.getValue().getAsString())
                                    .toList();
                    for (JsonElement item : e.getAsJsonArray("keys")) {
                        JsonObject key = item.getAsJsonObject();
                        if (key.get("value").isJsonNull()) continue;
                        if (!bound.contains(key.get("value").getAsString()))
                            throw new IOException(
                                    "Runtime search key is not a bound template value");
                    }
                }
                if (event.startsWith("scan_")
                        || event.startsWith("bitmap_")
                        || event.equals("index_insert_attempt")) indexEvents++;
            }
        }
        // A request that is still running when a connection closes has no ExecutorEnd: the
        // executor cleanup event is missing, while the execution itself returned. Reported, not
        // tolerated away, and it never excuses a request whose execution did not return.
        var withoutExecutorEnd = new ArrayList<String>(started);
        withoutExecutorEnd.removeAll(finished);
        withoutExecutorEnd.sort(null);
        // A read-only transaction the client commits can end at the server as an abort instead:
        // PostgreSQL never assigned it an XID, so nothing is written and the abort discards
        // nothing. Anything else - a request that changed rows, or one whose transaction ended
        // neither in a commit nor in an abort - is a lost server commit and fails the audit.
        var abortedCommits = new LinkedHashMap<String, Long>();
        for (var entry : clientCommitRequests.entrySet()) {
            long server = serverCommitRequests.getOrDefault(entry.getKey(), 0L);
            if (server >= entry.getValue()) continue;
            if (writeRequests.contains(entry.getKey())
                    || abortRequests.getOrDefault(entry.getKey(), 0L) == 0)
                throw new IOException(
                        "Client commit has no server commit, and the request neither changed rows"
                                + " nor ended in a server abort: "
                                + entry.getKey());
            abortedCommits.put(entry.getKey(), entry.getValue() - server);
        }
        long noop = abortedCommits.values().stream().mapToLong(Long::longValue).sum();
        if (unmatched != 0
                || !started.equals(requests.keySet())
                || !executed.equals(requests.keySet())
                || commits + noop != clientCommits
                || serverTransactionOpen
                || aborts < clientRollbacks
                || aborts > clientRollbacks + 1)
            throw new IOException("Incomplete client/server event association");
        var expectedCommits = new LinkedHashMap<>(clientCommitRequests);
        for (var entry : abortedCommits.entrySet())
            expectedCommits.merge(entry.getKey(), -entry.getValue(), Long::sum);
        expectedCommits.values().removeIf(v -> v == 0);
        if (!expectedCommits.equals(serverCommitRequests))
            throw new IOException("Commit request identities differ");
        Map<String, Long> remainingAborts = new HashMap<>(abortRequests);
        for (var rollback : rollbackRequests.entrySet()) {
            long remaining =
                    remainingAborts.getOrDefault(rollback.getKey(), 0L) - rollback.getValue();
            if (remaining < 0) throw new IOException("Rollback has no matching server abort");
            if (remaining == 0) remainingAborts.remove(rollback.getKey());
            else remainingAborts.put(rollback.getKey(), remaining);
        }
        Map<String, Long> expectedCloseAbort =
                clientTransactionOpen && closeApplication != null
                        ? Map.of(closeApplication, 1L)
                        : Map.of();
        if (!remainingAborts.equals(expectedCloseAbort))
            throw new IOException(
                    "Server abort is not associated with an open transaction at close");
        long checkedRows = 0;
        for (var request : consumed.entrySet()) {
            List<JsonArray> server = serverResults.getOrDefault(request.getKey(), List.of());
            List<String> hashes = serverHashes.getOrDefault(request.getKey(), List.of());
            int consumedRows = request.getValue().size();
            if (consumedRows > Math.max(server.size(), hashes.size()))
                throw new IOException("Client consumed uncaptured row: " + request.getKey());
            for (int i = 0; i < consumedRows; i++) {
                JsonObject client = request.getValue().get(i);
                boolean hashed = i < hashes.size();
                if (hashed && !hashes.get(i).equals(clientHash(client)))
                    throw new IOException(
                            "Client result differs from same-execution server result: "
                                    + request.getKey()
                                    + " row="
                                    + (i + 1)
                                    + " server_hash="
                                    + hashes.get(i));
                boolean valued = i < server.size() && client.has("column_types");
                if (!hashed && !valued)
                    throw new IOException("Client row has nothing to compare: " + request.getKey());
                if (!valued) {
                    checkedRows++;
                    continue;
                }
                for (int j = 0; j < server.get(i).size(); j++) {
                    JsonElement expected = server.get(i).get(j),
                            actual = client.get(Integer.toString(j + 1));
                    boolean same = Objects.equals(expected, actual);
                    if (!same)
                        same =
                                equalResult(
                                        expected,
                                        actual,
                                        client.getAsJsonObject("column_types")
                                                .get(Integer.toString(j + 1))
                                                .getAsInt());
                    if (!same)
                        throw new IOException(
                                "Client result differs from same-execution server result: "
                                        + request.getKey()
                                        + " column="
                                        + (j + 1)
                                        + " server="
                                        + expected
                                        + " client="
                                        + actual);
                }
                checkedRows++;
            }
        }
        var report = new LinkedHashMap<String, Object>();
        // The two tolerances, named so a run's report shows them instead of hiding them.
        report.put("requests_without_executor_end", withoutExecutorEnd);
        report.put("client_commits_ended_by_server_abort", abortedCommits);
        report.putAll(
                Map.of(
                        "status",
                        "passed",
                        "requests",
                        requests.size(),
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
        report.put("explicit_rollbacks", clientRollbacks);
        report.put("server_aborts", aborts);
        report.put("close_aborts", aborts - clientRollbacks);
        report.put("executor_returns", executed.size());
        report.put("executor_cleanup_events", finished.size());
        return report;
    }

    /**
     * The client's hash of a consumed row. A row that also kept its text must hash to what it
     * recorded; a row from before hashes existed is hashed from its text.
     */
    static String clientHash(JsonObject client) throws IOException {
        String recorded = client.has("row_hash") ? client.get("row_hash").getAsString() : null;
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
