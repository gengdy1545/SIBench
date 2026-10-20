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
        Map<String, JsonObject> requests = new LinkedHashMap<>();
        Set<String> returned = new HashSet<>();
        Map<String, List<JsonObject>> consumed = new HashMap<>();
        long clientCommits = 0;
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
                    }
                    case "close" -> closes++;
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
        Set<String> started = new HashSet<>(), finished = new HashSet<>();
        Map<String, List<JsonArray>> serverResults = new HashMap<>();
        long commits = 0, indexEvents = 0, unmatched = 0;
        try (var input = Files.newBufferedReader(pg)) {
            String line;
            while ((line = input.readLine()) != null) {
                JsonObject e = JsonParser.parseString(line).getAsJsonObject();
                if (e.get("pid").getAsInt() != backendPid)
                    throw new IOException("Backend PID differs from JDBC connection");
                String app = e.get("application").getAsString(),
                        event = e.get("event").getAsString();
                if (event.equals("transaction_abort"))
                    throw new IOException("Abort is outside successful capture scope");
                JsonObject request = requests.get(app);
                if (request == null) {
                    unmatched++;
                    continue;
                }
                if (event.equals("executor_start")) {
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
                } else if (event.equals("result_row")) {
                    serverResults
                            .computeIfAbsent(app, k -> new ArrayList<>())
                            .add(e.getAsJsonArray("values"));
                } else if (event.equals("transaction_commit")) commits++;
                if (event.equals("scan_rescan")) {
                    List<String> bindings =
                            request.getAsJsonObject("parameters").entrySet().stream()
                                    .filter(p -> !p.getValue().isJsonNull())
                                    .map(p -> p.getValue().getAsString())
                                    .toList();
                    for (JsonElement item : e.getAsJsonArray("keys")) {
                        JsonObject key = item.getAsJsonObject();
                        if (!bindings.contains(key.get("value").getAsString()))
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
                || !finished.equals(requests.keySet())
                || commits != clientCommits)
            throw new IOException("Incomplete client/server event association");
        long checkedRows = 0;
        for (var request : consumed.entrySet()) {
            List<JsonArray> server = serverResults.getOrDefault(request.getKey(), List.of());
            if (request.getValue().size() > server.size())
                throw new IOException("Client consumed uncaptured row");
            for (int i = 0; i < request.getValue().size(); i++) {
                JsonObject client = request.getValue().get(i);
                for (int j = 0; j < server.get(i).size(); j++) {
                    JsonElement expected = server.get(i).get(j),
                            actual = client.get(Integer.toString(j + 1));
                    boolean same = Objects.equals(expected, actual);
                    String sql = normalize(requests.get(request.getKey()).get("sql").getAsString());
                    if (!same
                            && j == 0
                            && sql.startsWith("select balance from savingaccount")
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
        return Map.of(
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
                "single-session TP3/TP5/TP9 known templates; same-execution SQL identity, runtime"
                        + " scan bindings and consumed results");
    }

    private static String normalize(String sql) {
        return sql.strip().replaceAll(";+$", "").replaceAll("\\s+", " ").toLowerCase(Locale.ROOT);
    }
}
