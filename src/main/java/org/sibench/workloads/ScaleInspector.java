package org.sibench.workloads;

import static org.sibench.config.ConfigValues.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Inspect configuration and source identity without generating data. */
public final class ScaleInspector {
    public static final List<String> DEFAULT_PROFILES =
            List.of(
                    "config/benchmark/hybench.yaml",
                    "config/benchmark/ycsb.yaml",
                    "config/benchmark/sysbench.yaml");

    private ScaleInspector() {}

    /** Native loader counts, from the configured source parameters rather than observed output. */
    public static Map<String, Long> nativeRows(Path root, Map<String, Object> config) {
        var nativeConfig = object(config, "native");
        var rows = new LinkedHashMap<String, Long>();
        switch (string(config, "benchmark")) {
            case "hybench" -> {
                Path source = root.resolve(string(object(config, "source"), "path"));
                var parameters = new com.moandjiezana.toml.Toml()
                        .read(source.resolve("src/main/resources/parameters.toml").toFile())
                        .getTable(string(nativeConfig, "sf"));
                require(parameters != null, "Unknown HyBench scale");
                rows.put("customer", parameters.getLong("customer_number"));
                rows.put("company", parameters.getLong("company_number"));
                long accounts = Math.addExact(rows.get("customer"), rows.get("company"));
                rows.put("savingaccount", accounts);
                rows.put("checkingaccount", accounts);
                for (String table : List.of("transfer", "checking", "loanapps", "loantrans"))
                    rows.put(table, parameters.getLong(table + "_number"));
            }
            case "ycsb" -> rows.put(string(nativeConfig, "table"), positive(nativeConfig, "recordcount"));
            case "sysbench" -> {
                long count = positive(nativeConfig, "tables"), perTable = positive(nativeConfig, "table-size");
                for (long table = 1; table <= count; table++) rows.put("sbtest" + table, perTable);
            }
            default -> throw new IllegalArgumentException("Unknown benchmark");
        }
        return rows;
    }

    public static Map<String, Object> inspect(Path root, Path profile)
            throws IOException, InterruptedException {
        root = root.toRealPath();
        Path path = inside(root, profile);
        byte[] bytes = Files.readAllBytes(path);
        Map<String, Object> config = parse(new String(bytes, StandardCharsets.UTF_8));
        require(integer(config, "schema_version") == 1, "Unsupported profile schema");
        require(string(config, "purpose").equals("native_capture"), "Expected native_capture purpose");
        var schema = org.sibench.schema.SourceSchema.read(root, config);
        long rows = 0;
        for (long count : nativeRows(root, config).values()) rows = Math.addExact(rows, count);
        return new LinkedHashMap<>(Map.of(
                "benchmark", schema.benchmark(), "baseline_rows", rows,
                "generation_mode", "native", "generation_verified", false,
                "capacity_calibrated", false, "source_commit", string(object(config, "source"), "commit"),
                "profile", root.relativize(path).toString(), "profile_sha256", sha256(bytes)));
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

}
