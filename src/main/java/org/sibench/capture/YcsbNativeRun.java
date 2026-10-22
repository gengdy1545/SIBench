package org.sibench.capture;

import static org.sibench.config.ConfigValues.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Runs the pinned YCSB client unchanged, with its JDBC binding connected through the recording
 * driver. The client is a separate JVM: Client.main always ends in System.exit.
 */
final class YcsbNativeRun {
    private static final Pattern MEASUREMENT = Pattern.compile("^\\[([A-Z][A-Z-]*)\\], ([^,]+), (.+)$");
    private static final List<String> OPERATIONS = List.of("READ", "UPDATE", "INSERT", "SCAN", "DELETE");

    private YcsbNativeRun() {}

    static Path libraries(NativeCaptureConfig config) throws IOException {
        Path build = config.root().resolve(string(config.nativeConfig(), "build_directory"));
        Path libraries = build.resolve(string(config.nativeConfig(), "client_libraries"));
        NativeBuildCheck.require(
                config.source(), build, List.of("core/src/main/java", "jdbc/src/main/java"));
        if (!Files.isDirectory(libraries)) throw new IOException("YCSB client libraries are not built");
        return libraries;
    }

    /** The properties of a native client invocation; transaction runs add the capture driver. */
    static List<String> arguments(
            NativeCaptureConfig config,
            String workload,
            boolean load,
            long recordCount,
            long operationCount,
            String driverUrl)
            throws IOException {
        return arguments(config, workload, load, recordCount, operationCount, driverUrl, Map.of());
    }

    /** {@code overrides} are native client properties documented for the workload. */
    static List<String> arguments(
            NativeCaptureConfig config,
            String workload,
            boolean load,
            long recordCount,
            long operationCount,
            String driverUrl,
            Map<String, Object> overrides)
            throws IOException {
        var nativeConfig = config.nativeConfig();
        Path file = config.source().resolve("workloads").resolve(workload).normalize();
        if (!file.startsWith(config.source().resolve("workloads")) || !Files.isRegularFile(file))
            throw new IOException("Unknown YCSB workload: " + workload);
        var arguments = new ArrayList<String>();
        arguments.addAll(List.of(load ? "-load" : "-t", "-db", "site.ycsb.db.JdbcDBClient"));
        arguments.addAll(List.of("-P", file.toString(), "-threads", Long.toString(load ? config.loadThreads() : 1)));
        // The driver class decides the client's SQL dialect; keep the PostgreSQL one.
        var properties =
                new LinkedHashMap<String, String>(
                        Map.of(
                                "db.driver", "org.postgresql.Driver",
                                "db.url", driverUrl,
                                "db.user", config.user(),
                                "db.passwd", System.getenv().getOrDefault("PGPASSWORD", ""),
                                "table", string(nativeConfig, "table"),
                                "recordcount", Long.toString(recordCount),
                                "fieldcount", Long.toString(positive(nativeConfig, "fieldcount")),
                                "fieldlength", Long.toString(positive(nativeConfig, "fieldlength")),
                                "fieldlengthdistribution", string(nativeConfig, "fieldlengthdistribution"),
                                "insertorder", string(nativeConfig, "insertorder")));
        properties.put("zeropadding", Long.toString(positive(nativeConfig, "zeropadding")));
        properties.put("insertstart", Long.toString(integer(nativeConfig, "insertstart")));
        if (load) properties.put("insertcount", Long.toString(recordCount));
        else properties.put("operationcount", Long.toString(operationCount));
        for (var override : overrides.entrySet()) {
            if (properties.containsKey(override.getKey()))
                throw new IOException("Workload property conflicts with the capture: " + override.getKey());
            properties.put(override.getKey(), String.valueOf(override.getValue()));
        }
        properties.forEach((key, value) -> arguments.addAll(List.of("-p", key + "=" + value)));
        return arguments;
    }

    /** Creates the table with the upstream creator, then loads it with the unmodified client. */
    static void load(NativeCaptureConfig config, Path logs) throws Exception {
        Path libraries = libraries(config);
        var nativeConfig = config.nativeConfig();
        Path self = NativeProcess.self();
        var create =
                NativeProcess.java(
                        libraries,
                        self,
                        "site.ycsb.db.JdbcDBCreateTable",
                        "-p", "db.driver=org.postgresql.Driver",
                        "-p", "db.url=" + config.jdbc(),
                        "-p", "db.user=" + config.user(),
                        "-p", "db.passwd=" + System.getenv().getOrDefault("PGPASSWORD", ""),
                        "-n", string(nativeConfig, "table"),
                        "-f", Long.toString(positive(nativeConfig, "fieldcount")));
        require(NativeProcess.run("create-table", create, Map.of(), config.root(), logs, Duration.ofMinutes(5)).exit() == 0,
                "YCSB table creation failed");
        String firstWorkload = object(nativeConfig, "workloads").keySet().iterator().next();
        long records = positive(nativeConfig, "recordcount");
        var command =
                NativeProcess.java(
                        libraries,
                        self,
                        concat(
                                List.of("site.ycsb.Client"),
                                arguments(config, firstWorkload, true, records, 0, config.jdbc())));
        var result = NativeProcess.run("load", command, Map.of(), config.root(), logs, config.timeout());
        require(result.exit() == 0, "YCSB load failed");
        var counts = parse(result.stdout());
        require(
                counts.getOrDefault("INSERT", Map.of()).getOrDefault("Return=OK", 0L) == records,
                "YCSB load did not insert every record");
    }

    static Map<String, Object> run(NativeCaptureConfig config, Path logs) throws Exception {
        Path libraries = libraries(config);
        var nativeConfig = config.nativeConfig();
        Path self = NativeProcess.self();
        long records = positive(nativeConfig, "recordcount"), requests = 0;
        var reports = new ArrayList<Map<String, Object>>();
        for (var entry : object(nativeConfig, "workloads").entrySet()) {
            String workload = entry.getKey();
            var entryConfig = object(object(nativeConfig, "workloads"), workload);
            long operations = positive(entryConfig, "operationcount");
            Map<String, Object> overrides =
                    entryConfig.containsKey("properties") ? object(entryConfig, "properties") : Map.of();
            var before = jdbcTraces(config);
            var command =
                    NativeProcess.java(
                            libraries,
                            self,
                            concat(
                                    List.of(
                                            "-Dsibench.capture.directory=" + config.raw().toAbsolutePath(),
                                            "-Djdbc.drivers=org.sibench.capture.CaptureDriver",
                                            "site.ycsb.Client"),
                                    arguments(
                                            config,
                                            workload,
                                            false,
                                            records,
                                            operations,
                                            config.jdbc().replace("jdbc:postgresql:", "jdbc:sibench:"),
                                            overrides)));
            var result =
                    NativeProcess.run(workload, command, Map.of(), config.root(), logs, config.timeout());
            NativeProcess.drain(config.adminJdbc(), config.user());
            CaptureRun.checkFailure();
            require(result.exit() == 0, "YCSB client exited with " + result.exit() + ": " + workload);
            var measured = parse(result.stdout());
            long executed = 0, inserted = 0;
            var returns = new LinkedHashMap<String, Object>();
            for (String operation : OPERATIONS) {
                var counts = measured.get(operation);
                if (counts == null) continue;
                for (var count : counts.entrySet()) {
                    if (!count.getKey().startsWith("Return=")) continue;
                    returns.put(operation + "." + count.getKey().substring("Return=".length()), count.getValue());
                    if (!count.getKey().equals("Return=OK"))
                        throw new IOException(
                                workload + ": " + operation + " returned " + count.getKey() + " x" + count.getValue());
                }
                executed += counts.getOrDefault("Operations", 0L);
                if (operation.equals("INSERT")) inserted = counts.getOrDefault("Return=OK", 0L);
            }
            var added = new ArrayList<>(jdbcTraces(config));
            added.removeAll(before);
            long clientRequests = 0;
            for (Path trace : added) clientRequests += countRequests(trace);
            if (added.size() != 1 || clientRequests != executed)
                throw new IOException(
                        workload
                                + ": client reported "
                                + executed
                                + " statement operations but the JDBC trace has "
                                + clientRequests
                                + " requests in "
                                + added.size()
                                + " sessions");
            var report = new LinkedHashMap<String, Object>();
            report.put("workload", workload);
            report.put(
                    "sessions",
                    List.of(Map.of("pg_file", "pg-" + CaptureRun.connectionPid(added.get(0)) + ".jsonl")));
            report.put("recordcount_at_start", records);
            report.put("operationcount", operations);
            report.put("statement_operations", executed);
            report.put("jdbc_requests", clientRequests);
            report.put("returns", returns);
            if (!overrides.isEmpty()) report.put("native_property_overrides", overrides);
            var composite = measured.get("READ-MODIFY-WRITE");
            if (composite != null) report.put("read_modify_write", composite.getOrDefault("Operations", 0L));
            reports.add(report);
            requests += clientRequests;
            // Later workloads continue on the grown table: inserted keys are existing records.
            records += inserted;
        }
        var summary = new LinkedHashMap<String, Object>();
        summary.put("client", "YCSB JdbcDBClient, pinned commit, single thread");
        summary.put("workloads", reports);
        summary.put("jdbc_requests", requests);
        summary.put("final_recordcount", records);
        return summary;
    }

    static Map<String, Map<String, Long>> parse(String output) {
        var result = new LinkedHashMap<String, Map<String, Long>>();
        for (String line : output.split("\n")) {
            var matcher = MEASUREMENT.matcher(line.strip());
            if (!matcher.matches()) continue;
            String category = matcher.group(1), metric = matcher.group(2), value = matcher.group(3);
            try {
                result.computeIfAbsent(category, ignored -> new LinkedHashMap<>())
                        .put(metric, Long.parseLong(value.strip()));
            } catch (NumberFormatException ignored) {
                // Latencies and throughput are not counts.
            }
        }
        return result;
    }

    private static List<Path> jdbcTraces(NativeCaptureConfig config) throws IOException {
        if (!Files.isDirectory(config.raw())) return List.of();
        try (var paths = Files.list(config.raw())) {
            return paths.filter(p -> p.getFileName().toString().startsWith("jdbc-")).sorted().toList();
        }
    }

    private static long countRequests(Path trace) throws IOException {
        try (var lines = Files.lines(trace)) {
            return lines.filter(line -> line.contains("\"event\":\"request_start\"")).count();
        }
    }

    private static List<String> concat(List<String> first, List<String> second) {
        var result = new ArrayList<>(first);
        result.addAll(second);
        return result;
    }

    private static void require(boolean condition, String message) throws IOException {
        if (!condition) throw new IOException(message);
    }
}
