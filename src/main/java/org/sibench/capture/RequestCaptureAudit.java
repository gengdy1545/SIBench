package org.sibench.capture;

import com.google.gson.*;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;

/** Correlate JDBC and server records from the same execution, without repeating queries. */
public final class RequestCaptureAudit {
    private RequestCaptureAudit() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 3)
            throw new IllegalArgumentException("RequestCaptureAudit JDBC_JSONL PG_JSONL REPORT");
        Files.writeString(
                Path.of(args[2]),
                new GsonBuilder()
                        .setPrettyPrinting()
                        .create()
                        .toJson(audit(Path.of(args[0]), Path.of(args[1]))),
                StandardOpenOption.CREATE_NEW);
    }

    static Map<String, Object> audit(Path jdbc, Path pg) throws Exception {
        return audit(jdbc, pg, false);
    }

    static Map<String, Object> audit(Path jdbc, Path pg, boolean htap) throws Exception {
        return audit(jdbc, pg, htap, !htap);
    }

    /**
     * {@code bindings} additionally requires every runtime equality key to be a value bound by
     * the client: true for point-access workloads, false for joins whose keys come from rows.
     */
    static Map<String, Object> audit(Path jdbc, Path pg, boolean htap, boolean bindings)
            throws Exception {
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
        try (var input = Files.newBufferedReader(jdbc)) {
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
                executed = new HashSet<>();
        Map<String, List<JsonArray>> serverResults = new HashMap<>();
        long commits = 0, aborts = 0, indexEvents = 0, unmatched = 0;
        boolean serverTransactionOpen = false;
        try (var input = Files.newBufferedReader(pg)) {
            String line;
            while ((line = input.readLine()) != null) {
                JsonObject e = JsonParser.parseString(line).getAsJsonObject();
                if (e.get("pid").getAsInt() != backendPid)
                    throw new IOException("Backend PID differs from JDBC connection");
                String app = e.get("application").getAsString(),
                        event = e.get("event").getAsString();
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
                } else if (event.equals("result_row")) {
                    serverResults
                            .computeIfAbsent(app, k -> new ArrayList<>())
                            .add(e.getAsJsonArray("values"));
                } else if (event.equals("transaction_commit")) {
                    commits++;
                    serverCommitRequests.merge(app, 1L, Long::sum);
                    serverTransactionOpen = false;
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
        if (unmatched != 0
                || !started.equals(requests.keySet())
                || !(htap ? executed : finished).equals(requests.keySet())
                || commits != clientCommits
                || serverTransactionOpen
                || aborts < clientRollbacks
                || aborts > clientRollbacks + 1)
            throw new IOException("Incomplete client/server event association");
        if (!clientCommitRequests.equals(serverCommitRequests))
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
            if (request.getValue().size() > server.size())
                throw new IOException("Client consumed uncaptured row: " + request.getKey());
            for (int i = 0; i < request.getValue().size(); i++) {
                JsonObject client = request.getValue().get(i);
                for (int j = 0; j < server.get(i).size(); j++) {
                    JsonElement expected = server.get(i).get(j),
                            actual = client.get(Integer.toString(j + 1));
                    boolean same = Objects.equals(expected, actual);
                    if (!same && htap && client.has("column_types"))
                        same =
                                equalResult(
                                        expected,
                                        actual,
                                        client.getAsJsonObject("column_types")
                                                .get(Integer.toString(j + 1))
                                                .getAsInt());
                    String sql = normalize(requests.get(request.getKey()).get("sql").getAsString());
                    if (!same
                            && j == 0
                            && (sql.startsWith("select balance from savingaccount")
                                    || htap
                                            && (sql.startsWith(
                                                            "select balance from checkingaccount")
                                                    || sql.startsWith("select loan_balance from")))
                            && !expected.isJsonNull()
                            && actual != null
                            && !actual.isJsonNull()) {
                        same =
                                Float.floatToIntBits(Float.parseFloat(expected.getAsString()))
                                        == Float.floatToIntBits(
                                                Float.parseFloat(actual.getAsString()));
                    }
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
                        htap
                                ? "native HTAP session; same-execution SQL, results, commits and"
                                      + " rollback/close"
                                : "single-session TP3/TP5/TP9 known templates; same-execution SQL"
                                      + " identity, runtime scan bindings, consumed results and"
                                      + " rollback/close"));
        report.put("explicit_rollbacks", clientRollbacks);
        report.put("server_aborts", aborts);
        report.put("close_aborts", aborts - clientRollbacks);
        if (htap) {
            report.put("executor_returns", executed.size());
            report.put("executor_cleanup_events", finished.size());
        }
        return report;
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
