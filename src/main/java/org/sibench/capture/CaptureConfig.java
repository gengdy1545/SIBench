package org.sibench.capture;

import static org.sibench.config.ConfigValues.*;

import org.sibench.generation.GenerationConfig;

import java.nio.file.*;
import java.util.*;

/** Configuration contract for native SF1 capture and the explicitly selected TP diagnostic. */
public record CaptureConfig(
        Path root,
        Path source,
        Path artifacts,
        Path raw,
        String jdbc,
        String user,
        int seconds,
        int clients,
        Map<Integer, Integer> percentages,
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
        Path directory = root.resolve(string(source, "path")).toRealPath();
        require(directory.startsWith(root.resolve("benchmark")), "Source must be inside benchmark");
        require(
                runGit(directory, "rev-parse", "HEAD").equals(string(source, "commit")),
                "Source commit mismatch");
        require(
                runGit(directory, "status", "--porcelain=v1", "--untracked-files=all").isEmpty(),
                "Source worktree must be clean");
        var nativeConfig = object(doc, "native");
        require(string(nativeConfig, "sf").equals("1x"), "Capture currently requires native SF1");
        int clients = Math.toIntExact(positive(nativeConfig, "clients"));
        String mode = nativeConfig.containsKey("mode") ? string(nativeConfig, "mode") : "runhtap";
        require(Set.of("runhtap", "tp_subset").contains(mode), "Unsupported native mode: " + mode);
        require(mode.equals("runhtap") || clients == 1,
                "Only single-session TP subset normalization is implemented");
        int seconds =
                Math.toIntExact(Math.multiplyExact(positive(nativeConfig, "duration_minutes"), 60));
        Map<Integer, Integer> percentages = new LinkedHashMap<>();
        int total = 0;
        Map<String, Object> configuredPercentages = nativeConfig.containsKey("transaction_percentages")
                ? object(nativeConfig, "transaction_percentages") : Map.of();
        for (var entry : configuredPercentages.entrySet()) {
            require(
                    mode.equals("runhtap") ? entry.getKey().matches("tp([1-9]|1[0-8])")
                            : Set.of("tp3", "tp5", "tp9").contains(entry.getKey()),
                    "Unsupported transaction: " + entry.getKey());
            int percent =
                    Math.toIntExact(
                            integer(
                                    object(nativeConfig, "transaction_percentages"),
                                    entry.getKey()));
            require(percent >= 0 && percent <= 100, "Invalid transaction percentage");
            total += percent;
            percentages.put(Integer.parseInt(entry.getKey().substring(2)), percent);
        }
        require(mode.equals("runhtap") ? configuredPercentages.isEmpty() || total == 100 : total == 100,
                "Transaction percentages must sum to 100");
        if (mode.equals("runhtap")) {
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
                Map.copyOf(percentages),
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

    private static String runGit(Path source, String... args) throws Exception {
        List<String> command = new ArrayList<>(List.of("git", "-C", source.toString()));
        command.addAll(List.of(args));
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        String value =
                new String(
                                process.getInputStream().readAllBytes(),
                                java.nio.charset.StandardCharsets.UTF_8)
                        .trim();
        if (process.waitFor() != 0) throw new IllegalArgumentException("git failed: " + value);
        return value;
    }

    public Properties nativeProperties() {
        Properties p = new Properties();
        if (mode().equals("runhtap")) {
            try (var input = Files.newInputStream(source.resolve("conf/pg.props"))) {
                p.load(input);
            } catch (java.io.IOException error) {
                throw new java.io.UncheckedIOException(error);
            }
        }
        p.setProperty("sf", "1x");
        p.setProperty("db", "postgreSQL");
        p.setProperty("tpclient", Integer.toString(clients));
        if (mode().equals("tp_subset")) {
            p.setProperty("contention_num", "100");
            p.setProperty("risk_rate", "0.1");
        }
        p.setProperty("username", user);
        p.setProperty("password", "");
        p.setProperty("url", jdbc.replace("jdbc:postgresql:", "jdbc:sibench:"));
        p.setProperty("classname", CaptureDriver.class.getName());
        if (mode().equals("runhtap")) {
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
        }
        if (!percentages.isEmpty() || mode().equals("tp_subset"))
            for (int i = 1; i <= 18; i++)
                p.setProperty("tp" + i + "_percent", Integer.toString(percentages.getOrDefault(i, 0)));
        return p;
    }

    public String mode() {
        var nativeConfig = object(document, "native");
        return nativeConfig.containsKey("mode") ? string(nativeConfig, "mode") : "runhtap";
    }
}
