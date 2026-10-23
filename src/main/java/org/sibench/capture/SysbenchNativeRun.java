package org.sibench.capture;

import static org.sibench.config.ConfigValues.*;

import org.sibench.schema.SourceSchema;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Runs the pinned sysbench binary and Lua scripts unchanged. The client is a libpq process with no
 * JDBC layer, so the server's request ordinals identify its statements; PGAPPNAME switches on
 * capture for its connections only.
 */
final class SysbenchNativeRun {
    private static final Pattern COUNT = Pattern.compile("^\\s*([a-z ]+?):\\s+(\\d+)(?:\\s.*)?$");

    private SysbenchNativeRun() {}

    static Path binary(NativeCaptureConfig config) throws IOException {
        Path build = config.root().resolve(string(config.nativeConfig(), "build_directory"));
        NativeBuildCheck.require(
                config.source(), build, List.of("src/lua", "src/drivers/pgsql", "src/sb_lua.c"));
        Path binary = build.resolve(string(config.nativeConfig(), "binary"));
        if (!Files.isExecutable(binary)) throw new IOException("sysbench is not built: " + binary);
        return binary;
    }

    /** The unchanged pinned scripts find their shared module through the Lua search path. */
    static Map<String, String> environment(NativeCaptureConfig config, boolean capture) {
        var environment = new LinkedHashMap<String, String>();
        environment.put("LUA_PATH", config.source().resolve("src/lua") + "/?.lua;;");
        if (capture) environment.put("PGAPPNAME", "sibench:sysbench");
        return environment;
    }

    static List<String> command(
            NativeCaptureConfig config, String script, long events, String action) throws IOException {
        // Only prepare may use several threads: it creates and fills different tables in parallel.
        long threads = action.equals("prepare") ? config.prepareThreads() : positive(config.nativeConfig(), "threads");
        var nativeConfig = config.nativeConfig();
        Path lua = config.source().resolve("src/lua").resolve(script + ".lua").normalize();
        if (!lua.startsWith(config.source().resolve("src/lua")) || !Files.isRegularFile(lua))
            throw new IOException("Unknown sysbench script: " + script);
        var command = new ArrayList<String>();
        command.add(binary(config).toString());
        command.add(lua.toString());
        command.addAll(
                List.of(
                        "--db-driver=pgsql",
                        "--pgsql-host=127.0.0.1",
                        "--pgsql-port=" + config.port(),
                        "--pgsql-user=" + config.user(),
                        "--pgsql-db=" + config.database(),
                        "--tables=" + positive(nativeConfig, "tables"),
                        "--table-size=" + positive(nativeConfig, "table-size"),
                        "--threads=" + threads,
                        "--rand-type=" + string(nativeConfig, "rand-type"),
                        "--rand-seed=" + positive(nativeConfig, "rand-seed"),
                        "--auto-inc=" + (bool(nativeConfig, "auto-inc") ? "on" : "off"),
                        "--create-secondary=" + (bool(nativeConfig, "create-secondary") ? "on" : "off"),
                        // The switch every pinned script guards begin()/commit() with, stated by
                        // the run so the control-statement count is never inferred from the source.
                        "--skip-trx=" + (skipTrx(nativeConfig) ? "on" : "off"),
                        "--report-interval=0"));
        if (action.equals("run")) command.addAll(List.of("--events=" + events, "--time=0"));
        command.add(action);
        return command;
    }

    /** Upstream default is off; an explicit value keeps the derived control count grounded. */
    private static boolean skipTrx(Map<String, Object> nativeConfig) {
        return nativeConfig.containsKey("skip-trx") && bool(nativeConfig, "skip-trx");
    }

    static void prepare(NativeCaptureConfig config, Path logs) throws Exception {
        String first = object(config.nativeConfig(), "workloads").keySet().iterator().next();
        var result =
                NativeProcess.run(
                        "prepare",
                        command(config, first, 0, "prepare"),
                        environment(config, false),
                        config.root(),
                        logs,
                        config.timeout());
        if (result.exit() != 0) throw new IOException("sysbench prepare failed");
    }

    static Map<String, Object> run(NativeCaptureConfig config, Path logs) throws Exception {
        var reports = new ArrayList<Map<String, Object>>();
        long requests = 0, transactions = 0;
        var schema = SourceSchema.read(config.root(), config.document());
        for (var entry : object(config.nativeConfig(), "workloads").entrySet()) {
            String script = entry.getKey();
            long events = positive(object(object(config.nativeConfig(), "workloads"), script), "events");
            var before = serverTraces(config);
            var command = command(config, script, events, "run");
            var result =
                    NativeProcess.run(
                            script,
                            command,
                            environment(config, true),
                            config.root(),
                            logs,
                            config.timeout());
            NativeProcess.drain(config.adminJdbc(), config.user());
            if (result.exit() != 0) throw new IOException("sysbench exited with " + result.exit() + ": " + script);
            var counts = parse(result.stdout());
            for (String field : List.of("read", "write", "other", "transactions"))
                if (!counts.containsKey(field))
                    throw new IOException(script + ": sysbench output lacks '" + field + "'");
            var failures = new SourceFailurePolicy.Tally();
            classifyIgnoredErrors(failures, script, counts);
            classifyReconnects(failures, script, counts);
            if (failures.invalidatesCapture())
                throw new IOException(
                        script
                                + ": sysbench reported a failure no pinned source behaviour explains"
                                + failures.report().get("occurrences"));
            if (counts.get("transactions") != events)
                throw new IOException(script + ": sysbench completed " + counts.get("transactions") + " of " + events + " events");
            var added = new ArrayList<>(serverTraces(config));
            added.removeAll(before);
            long serverRequests = 0, serverCommits = 0, setupCommits = 0, zeroRows = 0;
            var sessions = new ArrayList<Map<String, Object>>();
            for (Path trace : added) {
                var audit = ServerCaptureAudit.audit(trace);
                serverRequests += (Long) audit.get("requests");
                serverCommits += (Long) audit.get("commits");
                setupCommits += (Long) audit.get("setup_commits_before_first_execution");
                zeroRows += (Long) audit.get("modifications_affecting_no_row");
                sessions.add(Map.of("pg_file", trace.getFileName().toString(), "requests", audit.get("requests"), "commits", audit.get("commits")));
            }
            // The pgsql driver counts a modification as "write" only when it changed a row and as
            // "other" otherwise; the prepared BEGIN and COMMIT are "other" too but are utility
            // statements that never reach the executor. Both parts are checked independently:
            // control statements from the pinned script and the run's own --skip-trx, zero-row
            // modifications from the server.
            Path lua = config.source().resolve("src/lua").resolve(script + ".lua");
            var control = SysbenchControlStatements.derive(
                    lua, SysbenchControlStatements.skipTrx(command), events);
            long clientStatements = counts.get("read") + counts.get("write") + counts.get("other");
            long residual =
                    SysbenchControlStatements.requireAgreement(
                            script, clientStatements, serverRequests, control);
            long expected = clientStatements - control.controlStatements();
            if (counts.get("other") - control.controlStatements() != zeroRows)
                throw new IOException(
                        script
                                + ": sysbench counts "
                                + (counts.get("other") - control.controlStatements())
                                + " statements as neither read nor write but the server saw "
                                + zeroRows
                                + " modifications that changed no row");
            if (added.size() != 1 || serverRequests != expected || serverCommits != counts.get("transactions"))
                throw new IOException(
                        script
                                + ": sysbench issued "
                                + expected
                                + " statements in "
                                + counts.get("transactions")
                                + " transactions; the server trace has "
                                + serverRequests
                                + " executions and "
                                + serverCommits
                                + " commits in "
                                + added.size()
                                + " sessions");
            var report = new LinkedHashMap<String, Object>();
            report.put("script", script);
            report.put("events", events);
            report.put("reads", counts.get("read"));
            report.put("writes", counts.get("write"));
            report.put("other", counts.get("other"));
            report.putAll(control.describe());
            report.put("transaction_control_statements", control.controlStatements());
            report.put("server_observed_control_residual", residual);
            report.put("setup_commits_before_first_execution", setupCommits);
            report.put("modifications_affecting_no_row", zeroRows);
            report.put("transactions", counts.get("transactions"));
            report.put("server_executions", serverRequests);
            report.put("server_commits", serverCommits);
            report.put("sessions", sessions);
            report.put("source_failures", failures.report());
            report.put("change_kinds", ChangeKinds.countTraces(schema, added).describe());
            reports.add(report);
            requests += serverRequests;
            transactions += serverCommits;
        }
        var summary = new LinkedHashMap<String, Object>();
        summary.put("client", "sysbench, pinned commit, Lua scripts unchanged, single thread");
        summary.put("workloads", reports);
        summary.put("server_executions", requests);
        summary.put("server_commits", transactions);
        return summary;
    }

    static Map<String, Long> parse(String output) {
        var result = new LinkedHashMap<String, Long>();
        boolean statistics = false;
        for (String line : output.split("\n")) {
            if (line.startsWith("SQL statistics")) statistics = true;
            else if (line.startsWith("General statistics")) statistics = false;
            if (!statistics) continue;
            var matcher = COUNT.matcher(line);
            if (matcher.matches()) result.putIfAbsent(matcher.group(1).strip(), Long.parseLong(matcher.group(2)));
        }
        return result;
    }

    /**
     * "ignored errors" counts statements the pinned pgsql driver decided to ignore: it rolls them
     * back and continues, and every other SQLSTATE ends the process before any report is printed.
     * The summary carries no SQLSTATE, so the driver status is the evidence.
     */
    static void classifyIgnoredErrors(
            SourceFailurePolicy.Tally failures, String script, Map<String, Long> counts) {
        long ignored = counts.getOrDefault("ignored errors", 0L);
        for (long attempt = 1; attempt <= ignored; attempt++)
            failures.record(
                    new SourceFailurePolicy.Evidence(
                            null,
                            null,
                            null,
                            SourceFailurePolicy.DRIVER_IGNORED_ERROR,
                            true,
                            false,
                            script,
                            null,
                            Math.toIntExact(attempt),
                            Math.toIntExact(ignored)));
    }

    /** A reconnect is the source retrying its own connection; sysbench counts each one. */
    static void classifyReconnects(
            SourceFailurePolicy.Tally failures, String script, Map<String, Long> counts) {
        long reconnects = counts.getOrDefault("reconnects", 0L);
        for (long attempt = 1; attempt <= reconnects; attempt++)
            failures.record(
                    new SourceFailurePolicy.Evidence(
                            null,
                            null,
                            null,
                            null,
                            false,
                            true,
                            script,
                            null,
                            Math.toIntExact(attempt),
                            Math.toIntExact(reconnects)));
    }

    private static List<Path> serverTraces(NativeCaptureConfig config) throws IOException {
        if (!Files.isDirectory(config.raw())) return List.of();
        try (var paths = Files.list(config.raw())) {
            return paths.filter(p -> p.getFileName().toString().startsWith("pg-")).sorted().toList();
        }
    }
}
