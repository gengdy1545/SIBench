package org.sibench.capture;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;

import org.postgresql.PGConnection;
import org.sibench.generation.BaselineGenerator;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.regex.Pattern;

/**
 * Loads this run's HyBench database through native generation, upstream DDL and COPY.
 * Generated file identities and loaded counts remain in the run's load manifest; loaded CSVs
 * are removed while the two files consumed by the native workload remain.
 */
final class HybenchNativeLoad {
    /** One upstream load statement: {@code \copy table from 'Data_1x/file.csv' CSV DELIMITER ','}. */
    private static final Pattern COPY =
            Pattern.compile("\\\\copy\\s+(\\w+)\\s+from\\s+'Data_1x/([\\w.]+)'\\s+CSV\\s+DELIMITER\\s+','\\s*;?", Pattern.CASE_INSENSITIVE);

    /**
     * The generated files the workload clients read when they start ({@code Client.init}). Upstream
     * only logs a missing one and leaves its id list null, so every AP client then fails on its
     * first query; the load stage checks them before anything is exported or run.
     */
    static final java.util.List<String> RUNTIME_INPUTS = java.util.List.of("Related_transfer_bids", "Related_checking_bids");

    private HybenchNativeLoad() {}

    static Path manifest(CaptureConfig config) {
        return NativeLoad.manifest(config.artifacts());
    }

    /** Loads the database in the run's freshly initialized cluster. */
    static void build(CaptureConfig config) throws Exception {
        NativeBuildCheck.verify(config.root(), "hybench", config.source(),
                config.root().resolve(NativeBuildCheck.HYBENCH_DIRECTORY), config.sourceCommit());
        String sf = config.scaleFactor();
        Path data = config.workDir().resolve("Data_" + sf);
        Path manifest = manifest(config);
        String password = System.getenv().getOrDefault("PGPASSWORD", "");
        if (Files.exists(manifest)) throw new IllegalStateException("Native load already recorded: " + manifest);
        Path library = NativeLoad.library(config.root());
        var report = new LinkedHashMap<String, Object>();
        report.put("sf", sf);
        report.put("database", config.database());
        report.put("table_persistence", "unlogged");
        report.put("source_commit", config.sourceCommit());
        if (!Files.isDirectory(data)) {
            // The generator writes Data_<sf> relative to the working directory.
            if (!Path.of("").toAbsolutePath().equals(config.workDir().toAbsolutePath()))
                throw new IllegalStateException("Run the load stage from the work directory " + config.workDir());
            long started = System.nanoTime();
            var properties = config.nativeProperties();
            Class.forName("com.hybench.ConfigLoader").getField("prop").set(null, properties);
            Object generator =
                    Class.forName("com.hybench.load.DataGenerator_RiskControlling")
                            .getConstructor(String.class)
                            .newInstance(sf);
            generator.getClass().getMethod("dataGenerator").invoke(generator);
            report.put("generation_seconds", (System.nanoTime() - started) / 1e9);
            report.put("generated", true);
        } else report.put("generated", false);
        var files = new LinkedHashMap<String, Object>();
        try (var list = Files.list(data)) {
            for (Path file : list.sorted().toList()) files.put(file.getFileName().toString(), BaselineGenerator.sha256(file));
        }
        report.put("data_sha256", files);
        try (Connection admin = DriverManager.getConnection(config.adminJdbc(), config.user(), password);
                Statement statement = admin.createStatement()) {
            statement.execute("CREATE DATABASE " + config.database());
        }
        var counts = new LinkedHashMap<String, Long>();
        long started = System.nanoTime();
        String jdbc = config.jdbc();
        try (Connection connection = DriverManager.getConnection(jdbc, config.user(), password);
                Statement statement = connection.createStatement()) {
            NativeLoad.unloggedTables(connection);
            for (String sql : statements(config.source().resolve("conf/ddl_pg.sql"))) statement.execute(sql);
            var copy = connection.unwrap(PGConnection.class).getCopyAPI();
            for (String line : Files.readAllLines(config.source().resolve("conf/load_data_pg.sql"))) {
                if (line.isBlank()) continue;
                String[] match = copyStatement(line);
                if (match == null) throw new IllegalStateException("Unexpected upstream load statement: " + line);
                // The upstream script names the SF1 directory; the statement is otherwise unchanged.
                try (Reader input = Files.newBufferedReader(data.resolve(match[1]))) {
                    counts.put(match[0].toLowerCase(java.util.Locale.ROOT),
                            copy.copyIn("COPY " + match[0] + " FROM STDIN CSV DELIMITER ','", input));
                }
            }
        }
        report.put("load_seconds", (System.nanoTime() - started) / 1e9);
        report.put("loaded_rows", counts);
        // Install the upstream secondary indexes, row-capture triggers and statistics before export.
        started = System.nanoTime();
        try (Connection connection = DriverManager.getConnection(jdbc, config.user(), password);
                Statement statement = connection.createStatement()) {
            for (String sql : indexStatements(config.source().resolve("conf/create_index_pg.sql")))
                statement.execute(sql);
        }
        NativeLoad.captureReady(jdbc, config.user(), library, org.sibench.schema.SourceSchema.hybench(config.source()));
        report.put("capture_ready_seconds", (System.nanoTime() - started) / 1e9);
        report.put("total_rows", counts.values().stream().mapToLong(Long::longValue).sum());
        Files.writeString(manifest, new GsonBuilder().setPrettyPrinting().create().toJson(report), StandardOpenOption.CREATE_NEW);
        requireRuntimeInputs(data, manifest);
        dropLoadInputs(data);
        System.out.println("HyBench " + sf + " loaded database " + config.database() + ": " + report.get("total_rows") + " rows");
    }

    /**
     * The client's runtime inputs exist in the work directory's {@code Data_<sf>} and, when the
     * load has a manifest, are the very files generated with that load.
     */
    static void requireRuntimeInputs(Path data, Path manifest) throws Exception {
        var recorded = Files.exists(manifest)
                ? JsonParser.parseString(Files.readString(manifest)).getAsJsonObject().getAsJsonObject("data_sha256")
                : null;
        for (String name : RUNTIME_INPUTS) {
            Path file = data.resolve(name);
            if (!Files.isRegularFile(file))
                throw new IllegalStateException(
                        "HyBench runtime input missing: " + file + "; the work directory must hold the Data_ directory"
                                + " its database was generated with");
            if (recorded == null) continue;
            if (!recorded.has(name) || !BaselineGenerator.sha256(file).equals(recorded.get(name).getAsString()))
                throw new IllegalStateException("HyBench runtime input differs from the load manifest: " + file);
        }
    }

    /** Deletes the loaded CSV files of {@code Data_<sf>}, keeping the client's runtime inputs. */
    static void dropLoadInputs(Path data) throws IOException {
        long freed = 0;
        try (var list = Files.list(data)) {
            for (Path file : list.sorted().toList()) {
                if (RUNTIME_INPUTS.contains(file.getFileName().toString()) || !Files.isRegularFile(file)) continue;
                freed += Files.size(file);
                Files.delete(file);
            }
        }
        if (freed > 0) System.out.println("Deleted loaded CSV files of " + data + ": " + freed / (1 << 20) + " MiB");
    }

    /** The table and file of one upstream load statement, or null when it is not one. */
    static String[] copyStatement(String line) {
        var match = COPY.matcher(line.strip());
        return match.matches() ? new String[] {match.group(1), match.group(2)} : null;
    }

    /** The upstream DDL: statements separated by semicolons, comments skipped. */
    static java.util.List<String> statements(Path script) throws Exception {
        var result = new java.util.ArrayList<String>();
        var current = new StringBuilder();
        for (String line : Files.readAllLines(script)) {
            String trimmed = line.strip();
            if (trimmed.startsWith("--")) continue;
            current.append(line).append('\n');
            if (trimmed.endsWith(";")) {
                String sql = current.toString().strip();
                result.add(sql.substring(0, sql.length() - 1));
                current.setLength(0);
            }
        }
        if (!current.toString().isBlank()) result.add(current.toString().strip());
        return result;
    }
    /** The upstream index script: one statement per non-empty line, comments and blanks skipped. */
    static java.util.List<String> indexStatements(Path script) throws Exception {
        var result = new java.util.ArrayList<String>();
        for (String line : Files.readAllLines(script)) {
            String sql = line.trim();
            if (sql.isEmpty() || sql.startsWith("--")) continue;
            result.add(sql.endsWith(";") ? sql.substring(0, sql.length() - 1) : sql);
        }
        if (result.isEmpty()) throw new IllegalStateException("Index script has no statements: " + script);
        return result;
    }
}
