package org.sibench.workloads;

import org.sibench.generation.GenerationConfig;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Inspect configuration and source identity without generating data. */
public final class ScaleInspector {
    public static final List<String> DEFAULT_PROFILES =
            List.of(
                    "config/benchmark/hybench.yaml",
                    "config/benchmark/ycsb.yaml",
                    "config/benchmark/sysbench.yaml");

    // Reviewed against the pinned HyBench resource and generation loops.
    private static final Map<String, Long> HYBENCH_SF1000_ROWS =
            Map.of(
                    "customer",
                    300000000L,
                    "company",
                    2000000L,
                    "savingaccount",
                    302000000L,
                    "checkingaccount",
                    302000000L,
                    "transfer",
                    6000000000L,
                    "checking",
                    600000000L,
                    "loanapps",
                    600000000L,
                    "loantrans",
                    600000000L);

    private ScaleInspector() {}

    public static Map<String, Object> inspect(Path root, Path profile)
            throws IOException, InterruptedException {
        root = root.toRealPath();
        Path path = inside(root, profile);
        byte[] bytes = Files.readAllBytes(path);
        Map<String, Object> config = parse(new String(bytes, StandardCharsets.UTF_8));
        require(integer(config, "schema_version") == 1, "Unsupported profile schema");
        if (string(config, "purpose").equals("native_capture_poc")) {
            var schema = org.sibench.schema.SourceSchema.read(root, config);
            var nativeConfig = object(config, "native");
            long rows;
            switch (schema.benchmark()) {
                case "hybench" -> {
                    Path source = inside(root, Path.of(string(object(config, "source"), "path")));
                    var parameters = new com.moandjiezana.toml.Toml()
                            .read(source.resolve("src/main/resources/parameters.toml").toFile())
                            .getTable(string(nativeConfig, "sf"));
                    require(parameters != null, "Unknown HyBench scale");
                    rows = Math.multiplyExact(3, Math.addExact(parameters.getLong("customer_number"),
                            parameters.getLong("company_number")));
                    for (String table : List.of("transfer", "checking", "loanapps", "loantrans"))
                        rows = Math.addExact(rows, parameters.getLong(table + "_number"));
                }
                case "ycsb" -> rows = positive(nativeConfig, "insertcount");
                case "sysbench" -> rows = Math.multiplyExact(positive(nativeConfig, "tables"),
                        positive(nativeConfig, "table-size"));
                default -> throw new IllegalArgumentException("Unknown benchmark");
            }
            return new LinkedHashMap<>(Map.of(
                    "benchmark", schema.benchmark(), "baseline_rows", rows,
                    "generation_mode", "native", "generation_verified", false,
                    "capacity_calibrated", false, "source_commit", string(object(config, "source"), "commit"),
                    "profile", root.relativize(path).toString(), "profile_sha256", sha256(bytes)));
        }
        Map<String, Object> report = validate(config);
        Map<String, Object> source = object(config, "source");
        Path sourcePath = inside(root, Path.of(string(source, "path")));
        Path definition = inside(sourcePath, Path.of(string(source, "definition")));
        if (!sha256(Files.readAllBytes(definition)).equals(string(source, "definition_sha256"))) {
            throw new IllegalArgumentException("Source definition changed: " + definition);
        }
        Process process =
                new ProcessBuilder("git", "-C", sourcePath.toString(), "rev-parse", "HEAD")
                        .redirectErrorStream(true)
                        .start();
        String commit =
                new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
        if (process.waitFor() != 0) {
            throw new IOException("Unable to read source commit: " + commit);
        }
        if (!commit.equals(string(source, "commit"))) {
            throw new IllegalArgumentException("Source commit changed: " + sourcePath);
        }
        Process status =
                new ProcessBuilder(
                                "git",
                                "-C",
                                sourcePath.toString(),
                                "status",
                                "--porcelain=v1",
                                "--untracked-files=all")
                        .redirectErrorStream(true)
                        .start();
        String changes =
                new String(status.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
        if (status.waitFor() != 0 || !changes.isEmpty()) {
            throw new IllegalArgumentException(
                    "Benchmark source must be clean: " + sourcePath + "\n" + changes);
        }
        GenerationConfig.from(root, config);
        report.put("profile", root.relativize(path).toString());
        report.put("profile_sha256", sha256(bytes));
        report.put("source_commit", commit);
        return report;
    }

    static Map<String, Object> parse(String text) {
        LoaderOptions options = new LoaderOptions();
        options.setAllowDuplicateKeys(false);
        Object loaded = new Yaml(new SafeConstructor(options)).load(text);
        return asObject(loaded, "profile");
    }

    static Map<String, Object> validate(Map<String, Object> config) {
        require(
                integer(config, "schema_version") == 1
                        && string(config, "purpose").equals("baseline_generation"),
                "Unsupported profile schema or purpose");
        Map<String, Object> scale = object(config, "scale");
        require(
                string(scale, "basis").equals("native_parameters"),
                "Expected native_parameters scale basis");
        String benchmark = string(config, "benchmark");
        Map<String, Object> nativeConfig = object(config, "native");
        long rows = positive(scale, "total_rows");
        long tableCount;
        Map<String, Object> estimates = new LinkedHashMap<>();
        switch (benchmark) {
            case "hybench" -> {
                require(
                        string(nativeConfig, "sf").equals("1000x"),
                        "HyBench profile requires sf=1000x");
                Map<String, Object> tables = object(scale, "table_rows");
                require(
                        tables.keySet().equals(HYBENCH_SF1000_ROWS.keySet()),
                        "Incorrect HyBench table set");
                long sum = 0;
                for (Map.Entry<String, Long> expected : HYBENCH_SF1000_ROWS.entrySet()) {
                    long count = positive(tables, expected.getKey());
                    require(
                            count == expected.getValue(),
                            "Incorrect HyBench row count: " + expected.getKey());
                    sum = Math.addExact(sum, count);
                }
                require(sum == rows, "HyBench table counts do not sum to total_rows");
                tableCount = tables.size();
            }
            case "ycsb" -> {
                require(
                        positive(nativeConfig, "recordcount") == rows
                                && positive(nativeConfig, "insertcount") == rows,
                        "YCSB loading counts must match total_rows");
                require(
                        integer(nativeConfig, "insertstart") == 0,
                        "Baseline requires insertstart=0");
                require(
                        string(nativeConfig, "fieldlengthdistribution").equals("constant"),
                        "Field payload calculation requires constant length");
                long payload =
                        Math.multiplyExact(
                                rows,
                                Math.multiplyExact(
                                        positive(nativeConfig, "fieldcount"),
                                        positive(nativeConfig, "fieldlength")));
                estimates.put("field_payload_bytes_excluding_keys", payload);
                tableCount = 1;
            }
            case "sysbench" -> {
                tableCount = positive(nativeConfig, "tables");
                long perTable = positive(nativeConfig, "table-size");
                require(
                        Math.multiplyExact(tableCount, perTable) == rows,
                        "sysbench tables * table-size must match total_rows");
                require(
                        perTable <= Integer.MAX_VALUE,
                        "sysbench baseline IDs exceed native signed int32");
                long width = 119 + 59;
                estimates.put(
                        "generated_ascii_text_bytes_excluding_id_and_k",
                        Math.multiplyExact(rows, width));
            }
            default -> throw new IllegalArgumentException("Unknown benchmark: " + benchmark);
        }
        Map<String, Object> parquet = object(object(config, "data_layout"), "parquet");
        positive(parquet, "target_file_size_bytes");
        positive(parquet, "row_group_max_rows");
        long seed = integer(object(config, "generation"), "seed");
        if (benchmark.equals("sysbench")) {
            require(
                    integer(nativeConfig, "rand-seed") == seed,
                    "sysbench seed differs from requested seed");
        }
        Map<String, Object> report = new LinkedHashMap<>();
        report.put("benchmark", benchmark);
        report.put("basis", scale.get("basis"));
        report.put("baseline_rows", rows);
        report.put("table_count", tableCount);
        report.put("native", nativeConfig);
        report.put("uncompressed_content_estimates", estimates);
        report.put("estimated_parquet_bytes", null);
        report.put("parquet", parquet);
        report.put("generation_seed", seed);
        report.put("generation_implemented", true);
        report.put("generation_verified", false);
        report.put("capacity_calibrated", false);
        report.put(
                "blockers",
                List.of(
                        "Parquet generation has not been executed or end-to-end verified.",
                        "Representative full-scale-key samples are required for capacity"
                                + " calibration."));
        return report;
    }

    private static Path inside(Path root, Path path) throws IOException {
        Path resolved = root.resolve(path).toRealPath();
        require(resolved.startsWith(root), "Path is outside root: " + path);
        return resolved;
    }

    private static String sha256(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException(error);
        }
    }

    static Map<String, Object> object(Map<String, Object> parent, String key) {
        return asObject(parent.get(key), key);
    }

    private static Map<String, Object> asObject(Object value, String name) {
        require(value instanceof Map<?, ?>, name + " must be a mapping");
        Map<String, Object> result = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
            require(entry.getKey() instanceof String, name + " must have string keys");
            result.put((String) entry.getKey(), entry.getValue());
        }
        return result;
    }

    private static String string(Map<String, Object> map, String key) {
        Object value = map.get(key);
        require(value instanceof String, key + " must be a string");
        return (String) value;
    }

    private static long integer(Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (value instanceof BigInteger big) {
            return big.longValueExact();
        }
        require(
                value instanceof Integer || value instanceof Long,
                key + " must be an integer in signed int64 range");
        return ((Number) value).longValue();
    }

    private static long positive(Map<String, Object> map, String key) {
        long value = integer(map, key);
        require(value > 0, key + " must be positive");
        return value;
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
