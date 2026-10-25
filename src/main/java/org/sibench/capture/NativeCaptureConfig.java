package org.sibench.capture;

import static org.sibench.config.ConfigValues.*;

import org.sibench.generation.GenerationConfig;
import org.sibench.schema.SourceSchema;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

/** Configuration of a native capture whose client is a separate process (YCSB, sysbench). */
public record NativeCaptureConfig(
        Path root,
        String benchmark,
        Path artifacts,
        Path raw,
        String database,
        int port,
        String user,
        GenerationConfig layout,
        Map<String, Object> document,
        Map<String, Object> nativeConfig) {
    public static NativeCaptureConfig read(Path root, Path path) throws Exception {
        root = root.toRealPath();
        var doc = parse(Files.readString(root.resolve(path)));
        require(
                integer(doc, "schema_version") == 1
                        && string(doc, "purpose").equals("native_capture_poc"),
                "Expected capture config v1");
        String benchmark = string(doc, "benchmark");
        require(Set.of("ycsb", "sysbench").contains(benchmark), "Not a native client capture: " + benchmark);
        // Verifies the pinned source commit and a clean worktree.
        SourceSchema.read(root, doc);
        var nativeConfig = object(doc, "native");
        // Each client thread is one backend session; several overlap and are merged by PG's
        // transaction completion counter.
        require(positive(nativeConfig, "threads") <= 256, "threads must be at most 256");
        // sysbench decides BEGIN / COMMIT at run time through --skip-trx; the run must state it.
        if (nativeConfig.containsKey("skip-trx"))
            require(
                    nativeConfig.get("skip-trx") instanceof Boolean,
                    "skip-trx must be a boolean: the control-statement count is derived from it");
        // Loading is not captured, so it may use more threads than the captured run.
        for (String key : new String[] {"load_threads", "prepare_threads"})
            if (nativeConfig.containsKey(key))
                require(positive(nativeConfig, key) <= 64, key + " must be at most 64");
        if (nativeConfig.containsKey("timeout_minutes"))
            require(positive(nativeConfig, "timeout_minutes") <= 1440, "timeout_minutes must be at most 1440");
        require(!object(nativeConfig, "workloads").isEmpty(), "At least one workload is required");
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
        Path artifacts = child(root, string(object(doc, "artifacts"), "directory"));
        Path raw = child(root, string(capture, "raw_directory"));
        require(raw.startsWith(artifacts), "Raw directory must be under artifacts");
        var data = object(doc, "data_layout");
        require(string(data, "compression").equals("zstd"), "PoC currently requires Zstd");
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
                        benchmark,
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
        return new NativeCaptureConfig(
                root, benchmark, artifacts, raw, database, port, user, layout, doc, nativeConfig);
    }

    /** Upper bound for each native client process (load, prepare or one workload). */
    public java.time.Duration timeout() {
        return java.time.Duration.ofMinutes(
                nativeConfig.containsKey("timeout_minutes") ? positive(nativeConfig, "timeout_minutes") : 30);
    }

    /** Client threads of a captured run: one backend session each. */
    public long threads() {
        return positive(nativeConfig, "threads");
    }

    /** Client threads used to load YCSB, which is not captured. */
    public long loadThreads() {
        return nativeConfig.containsKey("load_threads") ? positive(nativeConfig, "load_threads") : 1;
    }

    /** sysbench prepare parallelises across tables; it is not captured. */
    public long prepareThreads() {
        return nativeConfig.containsKey("prepare_threads") ? positive(nativeConfig, "prepare_threads") : 1;
    }

    public String jdbc() {
        return "jdbc:postgresql://127.0.0.1:" + port + "/" + database;
    }

    public String adminJdbc() {
        return "jdbc:postgresql://127.0.0.1:" + port + "/postgres";
    }

    public Path source() {
        return root.resolve(string(object(document, "source"), "path"));
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
}
