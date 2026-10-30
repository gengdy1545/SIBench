package org.sibench.generation;

import static org.sibench.config.ConfigValues.*;

import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

/** Fully resolved baseline parameters; no data is produced during validation. */
public record GenerationConfig(
        Path root,
        Map<String, Object> document,
        String benchmark,
        long seed,
        long targetBytes,
        int maxOvershootPercent,
        long maxRows,
        long groupBytes,
        int pageBytes,
        int dictionaryBytes,
        boolean dictionary,
        CompressionCodecName codec,
        int codecLevel,
        int buckets,
        long baselineTimestamp,
        Path output,
        long minFreeBytes) {
    public static final String VERSION = "sibench-baseline-v1";
    public static final String FILE_SIZE_POLICY = "compressed_bytes_v1";

    public GenerationConfig {
        require(targetBytes > 0, "target_file_size_bytes must be positive");
        require(maxOvershootPercent >= 1 && maxOvershootPercent <= 100,
                "file_size_max_overshoot_percent must be between 1 and 100");
        require(targetBytes <= Long.MAX_VALUE - overshootBytes(targetBytes, maxOvershootPercent),
                "target_file_size_bytes is too large for the overshoot allowance");
    }

    public static int maxOvershootPercent(Map<String, Object> parquet) {
        return parquet.containsKey("file_size_max_overshoot_percent")
                ? Math.toIntExact(integer(parquet, "file_size_max_overshoot_percent")) : 5;
    }

    private static long overshootBytes(long target, int percent) {
        return Math.addExact(Math.multiplyExact(target / 100, percent), target % 100 * percent / 100);
    }

    public long maxFileBytes() {
        return Math.addExact(targetBytes, overshootBytes(targetBytes, maxOvershootPercent));
    }

    public static GenerationConfig from(Path root, Map<String, Object> document) {
        require(integer(document, "schema_version") == 1, "Unsupported schema_version");
        require(
                string(document, "purpose").equals("baseline_generation"),
                "Expected baseline_generation purpose");
        String benchmark = string(document, "benchmark");
        require(Set.of("hybench", "ycsb", "sysbench").contains(benchmark), "Unknown benchmark");
        Map<String, Object> generation = object(document, "generation");
        require(string(generation, "adapter").equals(VERSION), "Unsupported generation adapter");
        require(
                string(generation, "mode").equals("deterministic_derived"),
                "Expected deterministic_derived mode");
        long seed = integer(generation, "seed");
        long timestamp = integer(generation, "baseline_timestamp");
        require(timestamp >= 0, "baseline_timestamp must be nonnegative");
        long minFree = positive(generation, "min_free_disk_bytes");
        Path relative = Path.of(string(generation, "output_directory"));
        require(!relative.isAbsolute(), "output_directory must be repository-relative");
        Path normalizedRoot = root.toAbsolutePath().normalize();
        Path output = normalizedRoot.resolve(relative).normalize();
        require(
                output.startsWith(normalizedRoot.resolve("results/datasets"))
                        && !output.equals(normalizedRoot.resolve("results/datasets")),
                "output_directory must be below results/datasets");
        Map<String, Object> layout = object(document, "data_layout");
        require(
                string(layout, "partitioning").equals("none"),
                "Only unpartitioned tables are implemented");
        require(
                string(layout, "bucket_function").equals("ordinal_mod"),
                "Unsupported bucket_function");
        int buckets = Math.toIntExact(positive(layout, "buckets"));
        require(buckets <= 4096, "buckets exceeds 4096");
        Map<String, Object> p = object(layout, "parquet");
        long target = positive(p, "target_file_size_bytes");
        long rows = positive(p, "row_group_max_rows");
        long group = positive(p, "row_group_buffer_bytes");
        require(group <= Integer.MAX_VALUE, "row_group_buffer_bytes exceeds supported budget");
        int page = Math.toIntExact(positive(p, "page_size_bytes"));
        int dict = Math.toIntExact(positive(p, "dictionary_page_size_bytes"));
        require(
                page <= group && dict <= group,
                "page and dictionary budgets must not exceed group budget");
        require(group <= target, "row_group_buffer_bytes must not exceed target_file_size_bytes");
        String compression = string(p, "compression");
        require(Set.of("zstd", "snappy", "none").contains(compression), "Unsupported compression");
        int level = Math.toIntExact(integer(p, "compression_level"));
        require(
                compression.equals("zstd") ? level >= 1 && level <= 22 : level == 0,
                "Zstd level must be 1..22; other codecs require level 0");
        var codec =
                switch (compression) {
                    case "zstd" -> CompressionCodecName.ZSTD;
                    case "snappy" -> CompressionCodecName.SNAPPY;
                    default -> CompressionCodecName.UNCOMPRESSED;
                };
        return new GenerationConfig(
                normalizedRoot,
                document,
                benchmark,
                seed,
                target,
                maxOvershootPercent(p),
                rows,
                group,
                page,
                dict,
                bool(p, "dictionary_enabled"),
                codec,
                level,
                buckets,
                timestamp,
                output,
                minFree);
    }
}
