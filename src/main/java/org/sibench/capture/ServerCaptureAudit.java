package org.sibench.capture;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/**
 * Checks one backend trace of a native client that has no JDBC layer (sysbench). The server's own
 * request ordinals are the only identity, so executions, results and transaction boundaries must
 * be self-consistent; the client's totals are compared by the caller that knows them.
 */
public final class ServerCaptureAudit {
    private ServerCaptureAudit() {}

    public static Map<String, Object> audit(Path server) throws IOException {
        long requests = 0, commits = 0, setupCommits = 0, aborts = 0, indexEvents = 0;
        long resultRows = 0, nested = 0;
        long[] zeroRowModifications = {0};
        long lastRequest = 0, returned = 0, processed = -1, failed = 0;
        boolean running = false, ranToEnd = false, executorEnded = true, transaction = false;
        String sql = "";
        int pid = 0;
        var statements = new TreeMap<String, Long>();
        try (var input = Files.newBufferedReader(server)) {
            String line;
            while ((line = input.readLine()) != null) {
                JsonObject e = JsonParser.parseString(line).getAsJsonObject();
                if (!e.has("req") || !e.has("depth"))
                    throw new IOException("Server-only capture requires request ordinals");
                int eventPid = e.get("pid").getAsInt(), depth = e.get("depth").getAsInt();
                if (pid == 0) pid = eventPid;
                else if (pid != eventPid) throw new IOException("Server trace spans several backends");
                String type = e.get("event").getAsString();
                switch (type) {
                    case "executor_start" -> {
                        transaction = true;
                        if (depth > 1) {
                            if (!running) throw new IOException("Nested executor outside a request");
                            nested++;
                            break;
                        }
                        if (running) {
                            if (!ranToEnd || !executorEnded)
                                throw new IOException("Overlapping executions: request " + lastRequest);
                            checkResult(sql, returned, processed, lastRequest, zeroRowModifications);
                        }
                        if (e.get("req").getAsLong() != lastRequest + 1)
                            throw new IOException(
                                    "Request ordinals are not consecutive at " + e.get("req"));
                        lastRequest++;
                        requests++;
                        running = true;
                        ranToEnd = false;
                        executorEnded = false;
                        returned = 0;
                        processed = -1;
                        sql = e.has("sql") ? e.get("sql").getAsString() : "";
                        statements.merge(firstWord(sql), 1L, Long::sum);
                    }
                    case "executor_run_end" -> {
                        if (!running) throw new IOException("Execution return without a request");
                        if (depth <= 1) {
                            ranToEnd = true;
                            processed = e.get("processed").getAsLong();
                        }
                    }
                    case "executor_end" -> {
                        if (!running) throw new IOException("Executor end without a request");
                        if (depth <= 1) executorEnded = true;
                    }
                    case "result_row" -> {
                        if (!running) throw new IOException("Result row without a request");
                        if (depth <= 1) {
                            returned++;
                            resultRows++;
                        }
                    }
                    case "transaction_commit" -> {
                        if (running && !ranToEnd)
                            throw new IOException("Commit while request " + lastRequest + " has not returned");
                        // Statements prepared at connect commit protocol-level transactions that
                        // run nothing; they precede the first execution and are not workload.
                        if (requests == 0) setupCommits++;
                        else commits++;
                        transaction = false;
                    }
                    case "transaction_abort" -> {
                        aborts++;
                        transaction = false;
                        if (running && !ranToEnd) failed++;
                        if (running && ranToEnd) checkResult(sql, returned, processed, lastRequest, zeroRowModifications);
                        running = false;
                    }
                    case "scan_rescan", "scan_next", "heap_visibility", "bitmap_candidate",
                            "bitmap_end", "scan_end", "plan_scan", "index_insert_attempt" ->
                            indexEvents++;
                    case "row_change", "index_build_start", "index_build_end" -> {}
                    default -> throw new IOException("Unclassified raw event: " + type);
                }
            }
        }
        if (requests == 0) throw new IOException("Server trace has no executions");
        if (transaction)
            throw new IOException("Server trace ends inside an unfinished transaction");
        if (running) {
            if (!ranToEnd) failed++;
            else checkResult(sql, returned, processed, lastRequest, zeroRowModifications);
        }
        if (failed != 0)
            throw new IOException("Server trace contains " + failed + " request(s) that never returned");
        var report = new LinkedHashMap<String, Object>();
        report.put("status", "passed");
        report.put("requests", requests);
        report.put("commits", commits);
        report.put("setup_commits_before_first_execution", setupCommits);
        report.put("aborts", aborts);
        report.put("nested_executions", nested);
        report.put("modifications_affecting_no_row", zeroRowModifications[0]);
        report.put("statements", statements);
        report.put("result_rows", resultRows);
        report.put("index_events", indexEvents);
        report.put(
                "scope",
                "native client without JDBC layer; server request ordinals, executor returns,"
                        + " result rows and transaction boundaries are mutually consistent; client"
                        + " parameters and consumed rows are not observed");
        return report;
    }

    private static void checkResult(
            String sql, long returned, long processed, long request, long[] zeroRowModifications)
            throws IOException {
        String word = firstWord(sql);
        boolean select = word.equals("select");
        if (processed == 0 && List.of("insert", "update", "delete").contains(word))
            zeroRowModifications[0]++;
        if (processed >= 0 && (returned > 0 || select) && returned != processed)
            throw new IOException(
                    "Result rows differ from executor return for request "
                            + request
                            + ": "
                            + returned
                            + " != "
                            + processed);
    }

    private static String firstWord(String sql) {
        String text = sql.strip().toLowerCase(Locale.ROOT);
        int end = 0;
        while (end < text.length() && Character.isLetter(text.charAt(end))) end++;
        return end == 0 ? "other" : text.substring(0, end);
    }
}
