package org.sibench.capture;

import com.google.gson.*;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.sibench.generation.BaselineGenerator;
import org.sibench.schema.SourceSchema;

import java.io.IOException;
import java.nio.file.*;
import java.security.MessageDigest;
import java.sql.*;
import java.util.*;

/** Independently compare every baseline file with an unchanged source database. */
public final class PgBaselineVerify {
    private PgBaselineVerify() {}

    static void verify(String jdbc, String user, SourceSchema schema, Path directory)
            throws Exception {
        try (Connection pg =
                DriverManager.getConnection(
                        jdbc, user, System.getenv().getOrDefault("PGPASSWORD", ""))) {
            verify(pg, schema, directory);
        }
    }

    /**
     * The Parquet field of each table column, in table order. The file's own columns (all but the
     * SIBench identity fields) must be exactly the table's.
     */
    private static int[] fields(SourceSchema.Table table, org.apache.parquet.schema.GroupType type)
            throws IOException {
        var own = new java.util.HashSet<String>();
        for (var field : type.getFields())
            if (!field.getName().startsWith("_sib_")) own.add(field.getName());
        var names = new java.util.HashSet<String>();
        for (var column : table.columns()) names.add(column.name());
        if (!own.equals(names)) throw new IOException("Incomplete row image for " + table.name());
        int[] fields = new int[table.columns().size()];
        for (int i = 0; i < fields.length; i++) fields[i] = type.getFieldIndex(table.columns().get(i).name());
        return fields;
    }

    static void verify(Connection pg, SourceSchema schema, Path directory) throws Exception {
        if (Files.exists(directory.resolve("verified-manifest.json")))
            throw new IOException("Baseline verification already exists");
        List<JsonObject> files =
                Files.readAllLines(directory.resolve("files.jsonl")).stream()
                        .map(line -> JsonParser.parseString(line).getAsJsonObject())
                        .toList();
        List<Map<String, Object>> tables = new ArrayList<>();
        Set<String> expectedTables =
                schema.tables().stream()
                        .map(SourceSchema.Table::name)
                        .collect(java.util.stream.Collectors.toSet());
        for (JsonObject file : files)
            if (!expectedTables.contains(file.get("table").getAsString()))
                throw new IOException("Unknown baseline manifest table");
        long total = 0;
        long rowId = 1;
        if (!pg.getAutoCommit())
            throw new IOException("Baseline verification requires a fresh autocommit connection");
        try {
            pg.setAutoCommit(false);
            pg.setReadOnly(true);
            pg.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            for (var table : schema.tables()) {
                MessageDigest digest = MessageDigest.getInstance("SHA-256");
                long count = 0;
                try (Statement statement = pg.createStatement()) {
                    statement.setFetchSize(8192);
                    try (ResultSet expected =
                            statement.executeQuery(
                                    "SELECT * FROM "
                                            + "\""
                                            + table.name()
                                            + "\""
                                            + " ORDER BY "
                                            + "\""
                                            + SourceRow.primary(table).name()
                                            + "\""
                                            + (SourceRow.primary(table).type().equals("integer")
                                                    ? ""
                                                    : " COLLATE \"C\""))) {
                        for (JsonObject file : files) {
                            if (!file.get("table").getAsString().equals(table.name())) continue;
                            Path path = directory.resolve(file.get("path").getAsString());
                            long fileCount = 0;
                            var columns = table.columns();
                            int[] fields = null;
                            try (ParquetReader<Group> reader =
                                    ParquetReader.builder(
                                                    new GroupReadSupport(),
                                                    new org.apache.hadoop.fs.Path(path.toUri()))
                                            .build()) {
                                Group actual;
                                while ((actual = reader.read()) != null) {
                                    if (!expected.next())
                                        throw new IllegalStateException(
                                                "Extra parquet row: " + table.name());
                                    // One schema per file: its columns must be the table's, once.
                                    if (fields == null) fields = fields(table, actual.getType());
                                    for (int i = 0; i < columns.size(); i++) {
                                        var column = columns.get(i);
                                        String source = expected.getString(column.name());
                                        String value =
                                                actual.getFieldRepetitionCount(fields[i]) == 0
                                                        ? null
                                                        : actual.getValueToString(fields[i], 0);
                                        if ((source == null || value == null) && !column.nullable())
                                            throw new IOException(
                                                    "Null required column: "
                                                            + table.name()
                                                            + "."
                                                            + column.name());
                                        if (!java.util.Objects.equals(
                                                SourceRow.normalize(column, source),
                                                SourceRow.normalize(column, value)))
                                            throw new IOException(
                                                    "Baseline row mismatch: "
                                                            + table.name()
                                                            + "/"
                                                            + count);
                                    }
                                    if (actual.getLong("_sib_row_id", 0) != rowId++
                                            || actual.getLong("_sib_timestamp", 0) != 0)
                                        throw new IllegalStateException(
                                                "Baseline identity mismatch");
                                    byte[] data =
                                            actual.toString()
                                                    .getBytes(
                                                            java.nio.charset.StandardCharsets
                                                                    .UTF_8);
                                    digest.update(
                                            java.nio.ByteBuffer.allocate(4)
                                                    .putInt(data.length)
                                                    .array());
                                    digest.update(data);
                                    count++;
                                    fileCount++;
                                }
                            }
                            if (fileCount != file.get("rows").getAsLong())
                                throw new IllegalStateException("File count mismatch");
                            file.addProperty("size_bytes", Files.size(path));
                            file.addProperty("sha256", BaselineGenerator.sha256(path));
                        }
                        if (expected.next())
                            throw new IllegalStateException(
                                    "Missing Parquet rows: " + table.name());
                    }
                }
                tables.add(
                        Map.of(
                                "table",
                                table.name(),
                                "rows",
                                count,
                                "row_digest",
                                HexFormat.of().formatHex(digest.digest()),
                                "verified",
                                true));
                total += count;
                System.out.println("Verified " + table.name() + " rows=" + count);
            }
            pg.commit();
        } catch (Exception error) {
            try {
                pg.rollback();
            } catch (SQLException rollback) {
                error.addSuppressed(rollback);
            }
            throw error;
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("status", "verified_complete");
        result.put("rows", total);
        result.put("tables", tables);
        result.put("files", files);
        result.put("source_schema", schema.tables());
        result.put("source_schema_sha256", schema.fingerprints());
        result.put(
                "verification",
                "all Parquet values and row identities compared with the unchanged source PG");
        result.put(
                "files_manifest_sha256",
                BaselineGenerator.sha256(directory.resolve("files.jsonl")));
        result.put("locators_sha256", BaselineGenerator.sha256(directory.resolve("locators.tsv")));
        Path partial = directory.resolve("verified-manifest.json.partial");
        Files.writeString(
                partial,
                new GsonBuilder().setPrettyPrinting().create().toJson(result),
                StandardOpenOption.CREATE_NEW);
        Files.move(
                partial,
                directory.resolve("verified-manifest.json"),
                StandardCopyOption.ATOMIC_MOVE);
    }

    static void verifyFiles(Path baseline) throws Exception {
        Path verified = baseline.resolve("verified-manifest.json");
        if (!Files.exists(verified))
            throw new IOException("Independent verified baseline manifest required");
        JsonObject verification =
                JsonParser.parseString(Files.readString(verified)).getAsJsonObject();
        if (!verification.get("status").getAsString().equals("verified_complete"))
            throw new IOException("Baseline is not verified");
        if (!FileDigests.sha256(baseline.resolve("files.jsonl"))
                .equals(verification.get("files_manifest_sha256").getAsString()))
            throw new IOException("Baseline file manifest changed");
        if (!FileDigests.sha256(baseline.resolve("locators.tsv"))
                .equals(verification.get("locators_sha256").getAsString()))
            throw new IOException("Baseline locators changed");
        for (JsonElement item : verification.getAsJsonArray("files")) {
            JsonObject file = item.getAsJsonObject();
            Path data = baseline.resolve(file.get("path").getAsString()).normalize();
            if (!data.startsWith(baseline.normalize())
                    || !FileDigests.sha256(data).equals(file.get("sha256").getAsString()))
                throw new IOException("Baseline data changed");
        }
    }
}
