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
 * Builds the still database a HyBench capture clones ({@code postgres.clone_from}) the way the
 * upstream README does: native data generation in the work directory, the upstream DDL, and the
 * upstream {@code \copy} load.
 *
 * <p>The upstream generator draws from unseeded random sources, so one generation is the fact
 * every capture of that scale shares: an existing {@code Data_<sf>} is never regenerated, and the
 * base manifest records each file's SHA256 and the rows PostgreSQL loaded from it. The still
 * database then receives what every capture needs before it runs (upstream secondary indexes,
 * row-capture triggers, statistics) and is never written again; captures run on reflink copies
 * of its stopped cluster ({@link StillBase}).
 *
 * <p>Once the base database holds the rows, the generated CSV files serve nothing: only the
 * runtime inputs stay ({@link #RUNTIME_INPUTS}), the rest is deleted (at SF1000 they are the size
 * of the data). A lost base database is generated again, as a new fact, rather than reloaded.
 */
final class HybenchNativeBase {
    /** One upstream load statement: {@code \copy table from 'Data_1x/file.csv' CSV DELIMITER ','}. */
    private static final Pattern COPY =
            Pattern.compile("\\\\copy\\s+(\\w+)\\s+from\\s+'Data_1x/([\\w.]+)'\\s+CSV\\s+DELIMITER\\s+','\\s*;?", Pattern.CASE_INSENSITIVE);

    /**
     * The generated files the workload clients read when they start ({@code Client.init}). Upstream
     * only logs a missing one and leaves its id list null, so every AP client then fails on its
     * first query; the base stage checks them before anything is exported or run.
     */
    static final java.util.List<String> RUNTIME_INPUTS = java.util.List.of("Related_transfer_bids", "Related_checking_bids");

    private HybenchNativeBase() {}

    static Path manifest(CaptureConfig config) {
        return StillBase.manifest(config.root(), config.cloneFrom());
    }

    /** Builds the still database in its own, freshly initialized cluster (see {@link StillBase}). */
    static void build(CaptureConfig config) throws Exception {
        NativeBuildCheck.verify(config.root(), "hybench", config.source(),
                config.root().resolve(NativeBuildCheck.HYBENCH_DIRECTORY), config.sourceCommit());
        String sf = config.scaleFactor();
        Path data = config.workDir().resolve("Data_" + sf);
        Path manifest = manifest(config);
        String password = System.getenv().getOrDefault("PGPASSWORD", "");
        if (Files.exists(manifest)) throw new IllegalStateException("Still database already built: " + manifest);
        Path library = StillBase.library(config.root());
        var report = new LinkedHashMap<String, Object>();
        report.put("sf", sf);
        report.put("database", config.cloneFrom());
        report.put("source_commit", config.sourceCommit());
        if (!Files.isDirectory(data)) {
            // The generator writes Data_<sf> relative to the working directory.
            if (!Path.of("").toAbsolutePath().equals(config.workDir().toAbsolutePath()))
                throw new IllegalStateException("Run the base stage from the work directory " + config.workDir());
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
            statement.execute("CREATE DATABASE " + config.cloneFrom());
        }
        var counts = new LinkedHashMap<String, Long>();
        long started = System.nanoTime();
        String base = config.adminJdbc().substring(0, config.adminJdbc().lastIndexOf('/') + 1) + config.cloneFrom();
        try (Connection connection = DriverManager.getConnection(base, config.user(), password);
                Statement statement = connection.createStatement()) {
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
        // Everything a capture needs before it runs is part of the still database, done once:
        // the upstream secondary indexes, the row-capture triggers and the statistics.
        started = System.nanoTime();
        try (Connection connection = DriverManager.getConnection(base, config.user(), password);
                Statement statement = connection.createStatement()) {
            for (String sql : HybenchNativePrepare.statements(config.source().resolve("conf/create_index_pg.sql")))
                statement.execute(sql);
        }
        StillBase.captureReady(base, config.user(), library, org.sibench.schema.SourceSchema.hybench(config.source()));
        report.put("capture_ready_seconds", (System.nanoTime() - started) / 1e9);
        report.put("total_rows", counts.values().stream().mapToLong(Long::longValue).sum());
        Files.writeString(manifest, new GsonBuilder().setPrettyPrinting().create().toJson(report), StandardOpenOption.CREATE_NEW);
        requireRuntimeInputs(data, manifest);
        dropLoadInputs(data);
        System.out.println("HyBench " + sf + " base " + config.cloneFrom() + ": " + report.get("total_rows") + " rows");
    }

    /**
     * The client's runtime inputs exist in the work directory's {@code Data_<sf>} and, when the
     * base has a manifest, are the very files generated with that base.
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
                                + " its base was generated with");
            if (recorded == null) continue;
            if (!recorded.has(name) || !BaselineGenerator.sha256(file).equals(recorded.get(name).getAsString()))
                throw new IllegalStateException("HyBench runtime input differs from the base manifest: " + file);
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
}
