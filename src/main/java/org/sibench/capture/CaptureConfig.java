package org.sibench.capture;

import static org.sibench.config.ConfigValues.*;

import org.sibench.generation.GenerationConfig;

import java.nio.file.*;
import java.util.*;

/**
 * Configuration contract for native HyBench runHTAP capture. Transaction ratios are the upstream
 * ones: the profile sets client counts and phase durations, never workload weights.
 */
public record CaptureConfig(
        Path root,
        Path source,
        Path artifacts,
        Path raw,
        String jdbc,
        String user,
        int seconds,
        int clients,
        GenerationConfig layout,
        Map<String, Object> document) {
    public static CaptureConfig read(Path root, Path path) throws Exception {
        root = root.toRealPath();
        var doc = parse(Files.readString(root.resolve(path)));
        require(
                integer(doc, "schema_version") == 1
                        && string(doc, "purpose").equals("native_capture_poc"),
                "Expected capture config v1");
        require(string(doc, "benchmark").equals("hybench"), "Only HyBench capture is implemented");
        var source = object(doc, "source");
        org.sibench.schema.SourceSchema.read(root, doc);
        Path directory = root.resolve(string(source, "path")).toRealPath();
        var nativeConfig = object(doc, "native");
        require(string(nativeConfig, "sf").matches("[1-9][0-9]*x"), "sf must be a native HyBench scale such as 1x or 10x");
        int clients = Math.toIntExact(positive(nativeConfig, "clients"));
        int seconds =
                Math.toIntExact(Math.multiplyExact(positive(nativeConfig, "duration_minutes"), 60));
        for (String field : List.of("ap_clients", "xp_transaction_clients", "xp_query_clients"))
            require(positive(nativeConfig, field) <= 256, "Invalid client count: " + field);
        require(clients <= 256, "Invalid TP client count");
        if (nativeConfig.containsKey("phase_duration_minutes")) {
            var durations = object(nativeConfig, "phase_duration_minutes");
            for (String phase : durations.keySet()) {
                require(Set.of("ap", "tp", "xp").contains(phase), "Unknown phase: " + phase);
                require(positive(durations, phase) <= 1440, "Invalid phase duration");
            }
        }
        var pg = object(doc, "postgres");
        require(string(pg, "version").equals("14.24"), "Capture patch requires PG 14.24");
        require(string(pg, "isolation").equals("read_committed"), "Only read_committed supported");
        require(
                integer(pg, "max_parallel_workers_per_gather") == 0,
                "Parallel PG execution is not captured");
        int port = Math.toIntExact(positive(pg, "port"));
        require(port <= 65535, "Invalid PG port");
        String database = string(pg, "database");
        require(database.matches("[a-z][a-z0-9_]*"), "Invalid database identifier");
        String user = string(pg, "user");
        require(user.equals("postgres"), "PoC requires dedicated postgres role");
        var capture = object(doc, "capture");
        require(
                string(capture, "mode").equals("instrumented_native_execution"),
                "Invalid capture mode");
        require(
                string(capture, "application_prefix").equals("sibench:"),
                "Prefix must match compiled capture patch");
        require(!bool(capture, "allow_event_loss"), "Lossy capture is unsupported");
        require(
                string(capture, "statement_results").equals("server_rows_and_consumed_rows"),
                "Capture includes actual server result rows");
        Path artifacts = child(root, string(object(doc, "artifacts"), "directory"));
        Path raw = child(root, string(capture, "raw_directory"));
        require(raw.startsWith(artifacts), "Raw directory must be under artifacts");
        var data = object(doc, "data_layout");
        String compression = string(data, "compression");
        require(compression.equals("zstd"), "PoC currently requires Zstd");
        int level = Math.toIntExact(integer(data, "compression_level"));
        require(level >= 1 && level <= 22, "Invalid Zstd level");
        long target = positive(data, "target_file_size_bytes"),
                maxRows = positive(data, "row_group_max_rows"),
                buffer = positive(data, "row_group_buffer_bytes");
        int page = Math.toIntExact(positive(data, "page_size_bytes"));
        require(buffer <= target && page <= buffer, "Inconsistent file/group/page budgets");
        GenerationConfig layout =
                new GenerationConfig(
                        root,
                        doc,
                        "hybench",
                        0,
                        target,
                        GenerationConfig.maxOvershootPercent(data),
                        maxRows,
                        buffer,
                        page,
                        page,
                        true,
                        org.apache.parquet.hadoop.metadata.CompressionCodecName.ZSTD,
                        level,
                        1,
                        0,
                        artifacts.resolve("baseline"),
                        21474836480L);
        return new CaptureConfig(
                root,
                directory,
                artifacts,
                raw,
                "jdbc:postgresql://127.0.0.1:" + port + "/" + database,
                user,
                seconds,
                clients,
                layout,
                doc);
    }

    private static Path child(Path root, String path) throws Exception {
        Path relative = Path.of(path);
        require(!relative.isAbsolute(), "Output must be repository-relative");
        Path result = root.resolve(relative).normalize();
        require(
                result.startsWith(root.resolve("results/poc")),
                "PoC output must be under results/poc");
        Path parent = result;
        while (!Files.exists(parent)) parent = parent.getParent();
        require(parent.toRealPath().startsWith(root), "Symlink escape in output path");
        return result;
    }

    public Properties nativeProperties() {
        Properties p = new Properties();
        try (var input = Files.newInputStream(source.resolve("conf/pg.props"))) {
            p.load(input);
        } catch (java.io.IOException error) {
            throw new java.io.UncheckedIOException(error);
        }
        p.setProperty("sf", scaleFactor());
        p.setProperty("db", "postgreSQL");
        p.setProperty("tpclient", Integer.toString(clients));
        p.setProperty("username", user);
        p.setProperty("password", "");
        p.setProperty("url", jdbc.replace("jdbc:postgresql:", "jdbc:sibench:"));
        p.setProperty("classname", CaptureDriver.class.getName());
        p.setProperty("url_ap", p.getProperty("url"));
        p.setProperty("username_ap", user);
        p.setProperty("password_ap", "");
        p.setProperty("classname_ap", CaptureDriver.class.getName());
        var nativeConfig = object(document, "native");
        p.setProperty("apclient", Long.toString(positive(nativeConfig, "ap_clients")));
        p.setProperty("xtpclient", Long.toString(positive(nativeConfig, "xp_transaction_clients")));
        p.setProperty("xapclient", Long.toString(positive(nativeConfig, "xp_query_clients")));
        for (String name : List.of("apRunMins", "tpRunMins", "xpRunMins"))
            p.setProperty(name, Integer.toString(seconds / 60));
        if (nativeConfig.containsKey("phase_duration_minutes")) {
            var durations = object(nativeConfig, "phase_duration_minutes");
            for (String phase : durations.keySet())
                p.setProperty(phase + "RunMins", Long.toString(positive(durations, phase)));
        }
        return p;
    }

    /** The database a native run reads; the capture database is cloned from a verified one. */
    public String database() {
        return string(object(document, "postgres"), "database");
    }

    /** The still database the capture database is cloned from. */
    public String cloneFrom() {
        return string(object(document, "postgres"), "clone_from");
    }

    /** HyBench's scale factor as its configuration names it, e.g. {@code 10x}. */
    public String scaleFactor() {
        return string(object(document, "native"), "sf");
    }

    public String sourceCommit() {
        return string(object(document, "source"), "commit");
    }

    /** The directory HyBench resolves its native {@code Data_<sf>} input from. */
    public Path workDir() {
        var nativeConfig = object(document, "native");
        return nativeConfig.containsKey("work_dir")
                ? root.resolve(string(nativeConfig, "work_dir"))
                : artifacts.resolve("native");
    }

    public String adminJdbc() {
        int port = Math.toIntExact(positive(object(document, "postgres"), "port"));
        return "jdbc:postgresql://127.0.0.1:" + port + "/postgres";
    }
}
