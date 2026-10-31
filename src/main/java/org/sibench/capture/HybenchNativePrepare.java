package org.sibench.capture;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Prepares the database a native HyBench run reads: this capture's reflink clone of the still
 * database ({@link StillBase}), which already carries the upstream secondary indexes, the row
 * capture triggers and statistics, is renamed to the profile's database, so a run always starts
 * from a still, known state. Nothing here generates or loads data.
 */
final class HybenchNativePrepare {
    private HybenchNativePrepare() {}

    /** This capture's clone of the still database becomes the profile's database. */
    static void prepare(Path root, CaptureConfig config) throws Exception {
        StillBase.library(root);
        StillBase.attach(root, config.adminJdbc(), config.user(), config.cloneFrom(), config.database(), null);
        HybenchNativeBase.requireRuntimeInputs(
                config.workDir().resolve("Data_" + config.scaleFactor()), HybenchNativeBase.manifest(config));
    }

    /** The upstream index script: one statement per non-empty line, comments and blanks skipped. */
    static List<String> statements(Path script) throws Exception {
        var result = new ArrayList<String>();
        for (String line : Files.readAllLines(script)) {
            String sql = line.trim();
            if (sql.isEmpty() || sql.startsWith("--")) continue;
            result.add(sql.endsWith(";") ? sql.substring(0, sql.length() - 1) : sql);
        }
        if (result.isEmpty()) throw new IllegalStateException("Index script has no statements: " + script);
        return result;
    }
}
