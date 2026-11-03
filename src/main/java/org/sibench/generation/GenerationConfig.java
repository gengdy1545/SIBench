package org.sibench.generation;

import static org.sibench.config.ConfigValues.*;

import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.nio.file.Path;
import java.util.Map;

/** Fully resolved baseline parameters; no data is produced during validation. */
public record GenerationConfig(
        Path root,
        Map<String, Object> document,
        String benchmark,
        long targetBytes,
        int maxOvershootPercent,
        long maxRows,
        long groupBytes,
        int pageBytes,
        int dictionaryBytes,
        boolean dictionary,
        CompressionCodecName codec,
        int codecLevel,
        Path output,
        long minFreeBytes) {
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
}
