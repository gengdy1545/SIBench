package org.sibench.capture;

import static org.sibench.config.ConfigValues.*;

import org.sibench.schema.SourceSchema;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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
        NativeBuildCheck.verify(config.root(), config.benchmark(), config.source(), build,
                string(object(config.document(), "source"), "commit"));
        Path libraries = NativeBuildCheck.ycsbLibraries(build);
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
        arguments.addAll(List.of("-P", file.toString(), "-threads", Long.toString(load ? config.loadThreads() : config.threads())));
        // The driver class decides the client's SQL dialect; keep the PostgreSQL one.
        var properties =
                new LinkedHashMap<String, String>(
                        Map.of(
                                "db.driver", "org.postgresql.Driver",
                                "db.url", driverUrl,
                                "db.user", config.user(),
                                "db.passwd", "",
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
                        "-p", "db.passwd=",
                        "-n", string(nativeConfig, "table"),
                        "-f", Long.toString(positive(nativeConfig, "fieldcount")));
        require(NativeProcess.run("create-table", create, Map.of(), config.root(), logs, Duration.ofMinutes(5)).exit() == 0,
                "YCSB table creation failed");
        String firstWorkload = object(nativeConfig, "workloads").keySet().iterator().next();
        long records = positive(nativeConfig, "recordcount");
        // The JDBC binding's documented batched load (README: db.batchsize, jdbc.batchupdateapi,
        // reWriteBatchedInserts). The load is not captured; the workload runs unbatched.
        long batch = nativeConfig.containsKey("load_batch_size") ? positive(nativeConfig, "load_batch_size") : 0;
        String url = config.jdbc() + (batch > 0 ? (config.jdbc().contains("?") ? "&" : "?") + "reWriteBatchedInserts=true" : "");
        Map<String, Object> batching =
                batch > 0 ? Map.of("db.batchsize", batch, "jdbc.batchupdateapi", true) : Map.of();
        var command =
                NativeProcess.java(
                        libraries,
                        self,
                        concat(
                                List.of("site.ycsb.Client"),
                                arguments(config, firstWorkload, true, records, 0, url, batching)));
        var result = NativeProcess.run("load", command, Map.of(), config.root(), logs, config.timeout());
        require(result.exit() == 0, "YCSB load failed");
        var counts = parse(result.stdout());
        var inserts = counts.getOrDefault("INSERT", Map.of());
        // A batched insert reports BATCHED_OK until its batch is sent; every other code is a failure.
        long loaded = inserts.getOrDefault("Return=OK", 0L) + inserts.getOrDefault("Return=BATCHED_OK", 0L);
        require(
                loaded == records && inserts.keySet().stream().allMatch(code -> code.equals("Return=OK") || code.equals("Return=BATCHED_OK") || !code.startsWith("Return=")),
                "YCSB load did not insert every record");
        // The last batch is sent at client cleanup, after its inserts reported BATCHED_OK.
        try (var connection =
                        java.sql.DriverManager.getConnection(
                                config.jdbc(), config.user(), System.getenv().getOrDefault("PGPASSWORD", ""));
                var statement = connection.createStatement();
                var count = statement.executeQuery(
                        "SELECT count(*) FROM \"" + string(nativeConfig, "table").replace("\"", "\"\"") + "\"")) {
            require(count.next() && count.getLong(1) == records, "YCSB load left a different row count in PG");
        }
    }

    static Map<String, Object> run(NativeCaptureConfig config, Path logs) throws Exception {
        Path libraries = libraries(config);
        var nativeConfig = config.nativeConfig();
        Path self = NativeProcess.self();
        var schema = SourceSchema.read(config.root(), config.document());
        long records = positive(nativeConfig, "recordcount"), requests = 0;
        var reports = new ArrayList<Map<String, Object>>();
        for (var entry : object(nativeConfig, "workloads").entrySet()) {
            String workload = entry.getKey();
            var entryConfig = object(object(nativeConfig, "workloads"), workload);
            long operations = positive(entryConfig, "operationcount");
            Map<String, Object> overrides =
                    entryConfig.containsKey("properties") ? object(entryConfig, "properties") : Map.of();
            var before = jdbcTraces(config);
            var serversBefore = serverTraces(config);
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
                    String status = count.getKey().substring("Return=".length());
                    returns.put(operation + "." + status, count.getValue());
                }
                executed += counts.getOrDefault("Operations", 0L);
                if (operation.equals("INSERT")) inserted = counts.getOrDefault("Return=OK", 0L);
            }
            var failures = classifyReturns(workload, measured);
            if (failures.invalidatesCapture())
                throw new IOException(
                        workload
                                + ": YCSB reported a return status no pinned source behaviour explains: "
                                + failures.report().get("occurrences"));
            var added = new ArrayList<>(jdbcTraces(config));
            added.removeAll(before);
            long clientRequests = 0;
            for (Path trace : added) clientRequests += countRequests(trace);
            // One JDBC connection per client thread.
            if (added.size() != config.threads() || clientRequests != executed)
                throw new IOException(
                        workload
                                + ": client reported "
                                + executed
                                + " statement operations but the JDBC trace has "
                                + clientRequests
                                + " requests in "
                                + added.size()
                                + " sessions");
            var addedServers = new ArrayList<>(serverTraces(config));
            addedServers.removeAll(serversBefore);
            var declaration = declaration(config, workload);
            var observed = observed(measured);
            requireDeclared(workload, declaration, observed);
            if (observed.get("total") != operations)
                throw new IOException(
                        workload
                                + ": the run asked for "
                                + operations
                                + " operations but the client measured "
                                + observed.get("total"));
            var report = new LinkedHashMap<String, Object>();
            report.put("workload", workload);
            var sessions = new ArrayList<Map<String, Object>>();
            for (Path trace : added)
                sessions.add(Map.of("pg_file", ServerTrace.locate(config.raw(), CaptureRun.connectionPid(trace)).getFileName().toString()));
            report.put("sessions", sessions);
            report.put("recordcount_at_start", records);
            report.put("operationcount", operations);
            report.put("statement_operations", executed);
            report.put("jdbc_requests", clientRequests);
            report.put("returns", returns);
            // Counts per category in the returns map the workload report already builds.
            @SuppressWarnings("unchecked")
            var categoryCounts = (Map<String, Object>) failures.report().get("counts");
            for (var category : categoryCounts.entrySet())
                returns.put("source_failures." + category.getKey(), category.getValue());
            report.put("declared", declaration);
            report.put("observed", observed);
            report.put("read_modify_write", observed.get("read_modify_write"));
            if (!overrides.isEmpty()) report.put("native_property_overrides", overrides);
            report.put("source_failures", failures.report());
            report.put("change_kinds", ChangeKinds.countTraces(schema, addedServers).describe());
            reports.add(report);
            requests += clientRequests;
            // Later workloads continue on the grown table: inserted keys are existing records.
            records += inserted;
        }
        var summary = new LinkedHashMap<String, Object>();
        summary.put("client", "YCSB JdbcDBClient, pinned commit, " + config.threads() + " thread(s)");
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

    /**
     * Records every return status the client did not report as OK. The client summary carries no
     * SQLSTATE, so a status the pinned client does not define as an ordinary outcome stays
     * unclassified and ends the capture.
     */
    static SourceFailurePolicy.Tally classifyReturns(
            String workload, Map<String, Map<String, Long>> measured) {
        var failures = new SourceFailurePolicy.Tally();
        for (String operation : OPERATIONS) {
            var counts = measured.get(operation);
            if (counts == null) continue;
            for (var count : counts.entrySet()) {
                if (!count.getKey().startsWith("Return=")) continue;
                String status = count.getKey().substring("Return=".length());
                if (status.equals("OK")) continue;
                long total = count.getValue();
                for (long attempt = 1; attempt <= total; attempt++)
                    failures.record(
                            new SourceFailurePolicy.Evidence(
                                    null,
                                    null,
                                    status,
                                    null,
                                    false,
                                    false,
                                    workload + ":" + operation,
                                    null,
                                    Math.toIntExact(attempt),
                                    Math.toIntExact(total)));
            }
        }
        return failures;
    }

    /**
     * What the pinned workload file declares, so the artefact says which proportions, field updates
     * and scan bound the client was asked for. Defaults are the ones {@code CoreWorkload} uses.
     */
    static Map<String, Object> declaration(NativeCaptureConfig config, String workload)
            throws IOException {
        return declaration(
                config.source().resolve("workloads"),
                positive(config.nativeConfig(), "fieldcount"),
                workload);
    }

    static Map<String, Object> declaration(Path workloads, long fields, String workload)
            throws IOException {
        Path directory = workloads.toAbsolutePath().normalize();
        Path file = directory.resolve(workload).normalize();
        if (!file.startsWith(directory) || !Files.isRegularFile(file))
            throw new IOException("Unknown YCSB workload: " + workload);
        var properties = new Properties();
        try (var input = Files.newBufferedReader(file)) {
            properties.load(input);
        }
        var proportions = new LinkedHashMap<String, Double>();
        for (String operation : List.of("read", "update", "insert", "scan", "read_modify_write"))
            proportions.put(
                    operation,
                    Double.parseDouble(
                            properties.getProperty(
                                    operation.equals("read_modify_write")
                                            ? "readmodifywriteproportion"
                                            : operation + "proportion",
                                    operation.equals("read") ? "0.95" : operation.equals("update") ? "0.05" : "0.0")));
        boolean writeAllFields = Boolean.parseBoolean(properties.getProperty("writeallfields", "false"));
        boolean readAllFields = Boolean.parseBoolean(properties.getProperty("readallfields", "true"));
        var result = new LinkedHashMap<String, Object>();
        result.put("workload_file", directory.relativize(file).toString());
        result.put("workload_class", properties.getProperty("workload", "site.ycsb.workloads.CoreWorkload"));
        result.put("proportions", proportions);
        result.put("request_distribution", properties.getProperty("requestdistribution", "uniform"));
        result.put("max_scan_length", Long.parseLong(properties.getProperty("maxscanlength", "1000")));
        result.put("scan_length_distribution", properties.getProperty("scanlengthdistribution", "uniform"));
        result.put("fields_per_record", fields);
        result.put("read_all_fields", readAllFields);
        result.put("fields_written_per_update", writeAllFields ? fields : 1L);
        // CoreWorkload.doTransactionReadModifyWrite issues a separate read and update with no
        // surrounding transaction, so a concurrent writer may sit between them.
        result.put("read_modify_write_atomic", false);
        return result;
    }

    /**
     * The operations the client chose, as the pinned measurement names them. {@code DBWrapper}
     * measures every db.read / db.update, so a READ-MODIFY-WRITE appears once under its own name
     * and again under READ and UPDATE.
     */
    static Map<String, Long> observed(Map<String, Map<String, Long>> measured) {
        long modify = measured.getOrDefault("READ-MODIFY-WRITE", Map.of()).getOrDefault("Operations", 0L);
        long read = measured.getOrDefault("READ", Map.of()).getOrDefault("Operations", 0L) - modify;
        long update = measured.getOrDefault("UPDATE", Map.of()).getOrDefault("Operations", 0L) - modify;
        long insert = measured.getOrDefault("INSERT", Map.of()).getOrDefault("Operations", 0L);
        long scan = measured.getOrDefault("SCAN", Map.of()).getOrDefault("Operations", 0L);
        var result = new LinkedHashMap<String, Long>();
        result.put("read", read);
        result.put("update", update);
        result.put("insert", insert);
        result.put("scan", scan);
        result.put("read_modify_write", modify);
        result.put("total", read + update + insert + scan + modify);
        return result;
    }

    /**
     * The pinned {@code DiscreteGenerator} samples proportions, so observed counts are compared
     * against the declared ones with a sampling tolerance; a declared zero must stay zero.
     */
    static void requireDeclared(
            String workload, Map<String, Object> declaration, Map<String, Long> observed)
            throws IOException {
        @SuppressWarnings("unchecked")
        var proportions = (Map<String, Double>) declaration.get("proportions");
        long total = observed.get("total");
        if (total == 0) throw new IOException(workload + ": the client performed no operation");
        for (var entry : proportions.entrySet()) {
            double declared = entry.getValue();
            double actual = observed.get(entry.getKey()) / (double) total;
            if (declared == 0.0) {
                if (observed.get(entry.getKey()) != 0L)
                    throw new IOException(
                            workload
                                    + ": declares no "
                                    + entry.getKey()
                                    + " but the client performed "
                                    + observed.get(entry.getKey()));
                continue;
            }
            double tolerance = Math.max(0.02, 8 * Math.sqrt(declared * (1 - declared) / total));
            if (Math.abs(actual - declared) > tolerance)
                throw new IOException(
                        workload
                                + ": declares a "
                                + entry.getKey()
                                + " proportion of "
                                + declared
                                + " but the client performed "
                                + observed.get(entry.getKey())
                                + " of "
                                + total
                                + " operations ("
                                + actual
                                + ")");
        }
    }

    private static List<Path> jdbcTraces(NativeCaptureConfig config) throws IOException {
        if (!Files.isDirectory(config.raw())) return List.of();
        try (var paths = Files.list(config.raw())) {
            return paths.filter(p -> p.getFileName().toString().startsWith("jdbc-")).sorted().toList();
        }
    }

    private static List<Path> serverTraces(NativeCaptureConfig config) throws IOException {
        if (!Files.isDirectory(config.raw())) return List.of();
        try (var paths = Files.list(config.raw())) {
            return paths.filter(p -> p.getFileName().toString().startsWith("pg-")).sorted().toList();
        }
    }

    private static long countRequests(Path trace) throws IOException {
        try (var lines = ServerTrace.open(trace).lines()) {
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
