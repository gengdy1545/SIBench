package org.sibench.generation;

import static org.sibench.config.ConfigValues.*;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.apache.parquet.Version;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.sibench.workloads.ScaleInspector;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class BaselineGenerator {
    private static final Gson JSON =
            new GsonBuilder().serializeNulls().disableHtmlEscaping().create();

    private BaselineGenerator() {}

    public record Prepared(
            GenerationConfig config, List<TableData> tables, Map<String, Object> source) {}

    public static Prepared prepare(Path root, Path profile)
            throws IOException, InterruptedException {
        Map<String, Object> source = ScaleInspector.inspect(root, profile);
        root = root.toRealPath();
        GenerationConfig config =
                GenerationConfig.from(root, parse(Files.readString(root.resolve(profile))));
        List<TableData> tables = SourceTables.describe(config);
        require(
                TableData.totalRows(tables)
                        == positive(object(config.document(), "scale"), "total_rows"),
                "Adapter table counts differ from scale.total_rows");
        if (config.benchmark().equals("hybench")) {
            var declared = object(object(config.document(), "scale"), "table_rows");
            require(declared.size() == tables.size(), "Unexpected HyBench table declaration");
            for (var table : tables)
                require(
                        positive(declared, table.name()) == table.rows(),
                        "HyBench count mismatch: " + table.name());
        }
        return new Prepared(config, tables, source);
    }

    public static Map<String, Object> describe(Prepared prepared) throws IOException {
        var c = prepared.config();
        Map<String, Object> report = new LinkedHashMap<>();
        report.put("status", "configuration_valid_not_generated");
        report.put("adapter", GenerationConfig.VERSION);
        report.put("mode", "deterministic_derived");
        report.put("source", prepared.source());
        report.put("output_directory", c.output().toString());
        report.put("output_exists", Files.exists(c.output()));
        report.put("workspace_free_bytes", Files.getFileStore(c.root()).getUsableSpace());
        report.put("capacity_calibrated", false);
        List<Map<String, Object>> tables = new ArrayList<>();
        for (var t : prepared.tables())
            tables.add(
                    Map.of(
                            "name",
                            t.name(),
                            "rows",
                            t.rows(),
                            "primary_key",
                            t.primaryKey(),
                            "row_id_start",
                            t.rowIdStart(),
                            "schema",
                            t.schema().toString()));
        report.put("tables", tables);
        return report;
    }

    public static Map<String, Object> generate(Prepared prepared) throws IOException {
        GenerationConfig config = prepared.config();
        Path output = config.output();
        require(!Files.exists(output), "Output directory already exists: " + output);
        Path parent = output.getParent();
        // Reject symlink escapes before any dataset directory or file is created.
        Path existing = parent;
        while (!Files.exists(existing)) existing = existing.getParent();
        require(
                existing.toRealPath().startsWith(config.root()),
                "Output parent escapes repository");
        checkSpace(config, existing);
        Files.createDirectories(parent);
        Files.createDirectory(output);
        Files.writeString(
                output.resolve("INCOMPLETE"),
                "Baseline generation has not committed.\n",
                StandardOpenOption.CREATE_NEW);
        Files.writeString(
                output.resolve("resolved-config.json"),
                JSON.toJson(config.document()),
                StandardOpenOption.CREATE_NEW);
        long fileId = 1, totalRows = 0;
        Path fileList = output.resolve("files.jsonl");
        try (BufferedWriter manifest =
                Files.newBufferedWriter(fileList, StandardOpenOption.CREATE_NEW)) {
            for (TableData table : prepared.tables()) {
                for (int bucket = 0; bucket < config.buckets(); bucket++) {
                    long index = bucket;
                    while (index < table.rows()) {
                        checkSpace(config, output);
                        Path directory =
                                output.resolve(table.name())
                                        .resolve("partition=all")
                                        .resolve("bucket=" + bucket);
                        Files.createDirectories(directory);
                        String name = String.format(java.util.Locale.ROOT, "%020d.parquet", fileId);
                        Path temporary = directory.resolve(name + ".partial");
                        Path published = directory.resolve(name);
                        long firstOrdinal = index, count = 0;
                        Map<String, Object> sizing;
                        ParquetMetadata footer;
                        try (ParquetDataFile writer =
                                new ParquetDataFile(
                                        temporary,
                                        table,
                                        config,
                                        Map.of(
                                                "sibench.file_id",
                                                Long.toString(fileId),
                                                "sibench.adapter",
                                                GenerationConfig.VERSION))) {
                            while (index < table.rows()) {
                                if (count % 4096 == 0) checkSpace(config, output);
                                boolean full = writer.write(table.row().apply(index));
                                count++;
                                index = Math.addExact(index, config.buckets());
                                if (full) break;
                            }
                            footer = writer.finish();
                            sizing = writer.sizing();
                        }
                        long footerRows =
                                footer.getBlocks().stream().mapToLong(b -> b.getRowCount()).sum();
                        if (footerRows != count)
                            throw new IOException("Parquet row count mismatch");
                        Files.move(temporary, published, StandardCopyOption.ATOMIC_MOVE);
                        Map<String, Object> entry = new LinkedHashMap<>();
                        entry.put("file_id", fileId);
                        entry.put("path", output.relativize(published).toString());
                        entry.put("table", table.name());
                        entry.put("partition", "all");
                        entry.put("bucket", bucket);
                        entry.put("rows", count);
                        entry.put("size_bytes", Files.size(published));
                        entry.put("file_size", sizing);
                        entry.put("sha256", sha256(published));
                        entry.put("first_source_ordinal", firstOrdinal);
                        entry.put("source_ordinal_stride", config.buckets());
                        entry.put("row_id_start", table.rowIdStart());
                        entry.put("baseline_timestamp", config.baselineTimestamp());
                        entry.put("index_operation", "INSERT");
                        entry.put("primary_key_column", table.primaryKey());
                        List<Map<String, Object>> groups = new ArrayList<>();
                        for (var block : footer.getBlocks()) {
                            if (block.getRowCount() <= 0
                                    || block.getRowCount() > config.maxRows()) {
                                throw new IOException(
                                        "Parquet row group violates configured row limit");
                            }
                            groups.add(
                                    Map.of(
                                            "rows",
                                            block.getRowCount(),
                                            "uncompressed_encoded_bytes",
                                            block.getTotalByteSize(),
                                            "compressed_bytes",
                                            block.getCompressedSize()));
                        }
                        entry.put("row_groups", groups);
                        manifest.write(JSON.toJson(entry));
                        manifest.newLine();
                        manifest.flush();
                        totalRows = Math.addExact(totalRows, count);
                        fileId++;
                    }
                }
            }
        }
        require(
                totalRows == TableData.totalRows(prepared.tables()),
                "Generated row count mismatch");
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("status", "complete");
        summary.put("adapter", GenerationConfig.VERSION);
        summary.put("rows", totalRows);
        summary.put("files", fileId - 1);
        summary.put("files_manifest", "files.jsonl");
        summary.put("files_manifest_sha256", sha256(fileList));
        summary.put("resolved_config_sha256", sha256(output.resolve("resolved-config.json")));
        summary.put("source", prepared.source());
        summary.put("parquet_version", Version.FULL_VERSION);
        summary.put("tables", describe(prepared).get("tables"));
        summary.put("row_position", "zero_based_file_ordinal");
        summary.put("dataset_identity", sha256(output.resolve("resolved-config.json")));
        summary.put("row_id_scope", "dataset");
        summary.put(
                "file_size_policy",
                GenerationConfig.FILE_SIZE_POLICY);
        summary.put("java_version", System.getProperty("java.version"));
        summary.put("java_vendor", System.getProperty("java.vendor"));
        summary.put("jvm_arguments", ManagementFactory.getRuntimeMXBean().getInputArguments());
        summary.put("core_code", coreFingerprint());
        Path temporary = output.resolve("manifest.json.partial");
        Files.writeString(temporary, JSON.toJson(summary), StandardOpenOption.CREATE_NEW);
        Files.move(temporary, output.resolve("manifest.json"), StandardCopyOption.ATOMIC_MOVE);
        Files.delete(output.resolve("INCOMPLETE"));
        return summary;
    }

    private static Map<String, Object> coreFingerprint() throws IOException {
        try {
            Path location =
                    Path.of(
                            BaselineGenerator.class
                                    .getProtectionDomain()
                                    .getCodeSource()
                                    .getLocation()
                                    .toURI());
            return Map.of(
                    "location",
                    location.toString(),
                    "sha256",
                    Files.isRegularFile(location) ? sha256(location) : "unpackaged-classes");
        } catch (java.net.URISyntaxException error) {
            throw new IOException(error);
        }
    }

    private static void checkSpace(GenerationConfig config, Path path) throws IOException {
        if (Files.getFileStore(path).getUsableSpace() < config.minFreeBytes())
            throw new IOException("Free disk reserve reached; generation stopped");
    }

    public static String sha256(Path path) throws IOException {
        return org.sibench.config.Digests.sha256(path);
    }
}
