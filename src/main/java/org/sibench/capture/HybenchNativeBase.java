package org.sibench.capture;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;

import org.postgresql.PGConnection;
import org.sibench.generation.BaselineGenerator;

import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.regex.Pattern;

/**
 * Builds the still database a HyBench capture clones ({@code postgres.clone_from}) the way the
 * upstream README does: native data generation in the work directory, the upstream DDL, and the
 * upstream {@code \copy} load. Its secondary indexes, sequences and statistics come later, on each
 * clone ({@link HybenchNativePrepare}).
 *
 * <p>The upstream generator draws from unseeded random sources, so one generation is the fact
 * every capture of that scale shares: an existing {@code Data_<sf>} is never regenerated, and the
 * base manifest records each file's SHA256 and the rows PostgreSQL loaded from it. An existing
 * base database is reused only when its row counts still match that manifest.
 */
final class HybenchNativeBase {
    /** One upstream load statement: {@code \copy table from 'Data_1x/file.csv' CSV DELIMITER ','}. */
    private static final Pattern COPY =
            Pattern.compile("\\\\copy\\s+(\\w+)\\s+from\\s+'Data_1x/([\\w.]+)'\\s+CSV\\s+DELIMITER\\s+','\\s*;?", Pattern.CASE_INSENSITIVE);

    private HybenchNativeBase() {}

    static Path manifest(CaptureConfig config) {
        return config.workDir().resolve("base-" + config.cloneFrom() + ".json");
    }

    static void build(CaptureConfig config) throws Exception {
        String sf = config.scaleFactor();
        Path data = config.workDir().resolve("Data_" + sf);
        Path manifest = manifest(config);
        String password = System.getenv().getOrDefault("PGPASSWORD", "");
        boolean exists;
        try (Connection admin = DriverManager.getConnection(config.adminJdbc(), config.user(), password);
                Statement statement = admin.createStatement();
                ResultSet rows = statement.executeQuery(
                        "SELECT 1 FROM pg_database WHERE datname = '" + config.cloneFrom() + "'")) {
            exists = rows.next();
        }
        if (exists) {
            verify(config, manifest, password);
            return;
        }
        if (Files.exists(manifest)) throw new IllegalStateException("Base manifest without its database: " + manifest);
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
        report.put("total_rows", counts.values().stream().mapToLong(Long::longValue).sum());
        Files.writeString(manifest, new GsonBuilder().setPrettyPrinting().create().toJson(report), StandardOpenOption.CREATE_NEW);
        System.out.println("HyBench " + sf + " base " + config.cloneFrom() + ": " + report.get("total_rows") + " rows");
    }

    /** A reused base must still hold exactly the rows its manifest says were loaded. */
    private static void verify(CaptureConfig config, Path manifest, String password) throws Exception {
        if (!Files.exists(manifest)) {
            // The SF1 base predates base manifests; it was verified when its first capture ran.
            System.out.println("Reusing base " + config.cloneFrom() + " without a base manifest");
            return;
        }
        var expected = JsonParser.parseString(Files.readString(manifest)).getAsJsonObject().getAsJsonObject("loaded_rows");
        String base = config.adminJdbc().substring(0, config.adminJdbc().lastIndexOf('/') + 1) + config.cloneFrom();
        try (Connection connection = DriverManager.getConnection(base, config.user(), password);
                Statement statement = connection.createStatement()) {
            for (var table : expected.entrySet())
                try (ResultSet rows = statement.executeQuery("SELECT count(*) FROM \"" + table.getKey() + "\"")) {
                    rows.next();
                    if (rows.getLong(1) != table.getValue().getAsLong())
                        throw new IllegalStateException("Base " + config.cloneFrom() + " changed: " + table.getKey());
                }
        }
        System.out.println("Reusing base " + config.cloneFrom() + " (" + expected.size() + " tables match " + manifest.getFileName() + ")");
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
