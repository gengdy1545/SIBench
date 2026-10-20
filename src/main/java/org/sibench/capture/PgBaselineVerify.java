package org.sibench.capture;

import com.google.gson.*;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.sibench.generation.BaselineGenerator;

import java.nio.file.*;
import java.security.MessageDigest;
import java.sql.*;
import java.util.*;

/** Independently compare every baseline file with an unchanged source database. */
public final class PgBaselineVerify {
    private PgBaselineVerify() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2)
            throw new IllegalArgumentException("PgBaselineVerify JDBC_URL BASELINE_DIR");
        Path directory = Path.of(args[1]);
        List<JsonObject> files =
                Files.readAllLines(directory.resolve("files.jsonl")).stream()
                        .map(line -> JsonParser.parseString(line).getAsJsonObject())
                        .toList();
        List<Map<String, Object>> tables = new ArrayList<>();
        long rowId = 1, total = 0;
        try (Connection pg = DriverManager.getConnection(args[0], "postgres", "")) {
            pg.setAutoCommit(false);
            pg.setReadOnly(true);
            pg.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            for (var table : PgBaselineExport.TABLES.entrySet()) {
                MessageDigest digest = MessageDigest.getInstance("SHA-256");
                long count = 0;
                try (Statement statement = pg.createStatement()) {
                    statement.setFetchSize(8192);
                    try (ResultSet expected =
                            statement.executeQuery(
                                    "SELECT * FROM "
                                            + table.getKey()
                                            + " ORDER BY "
                                            + table.getValue())) {
                        for (JsonObject file : files) {
                            if (!file.get("table").getAsString().equals(table.getKey())) continue;
                            Path path = directory.resolve(file.get("path").getAsString());
                            long fileCount = 0;
                            try (ParquetReader<Group> reader =
                                    ParquetReader.builder(
                                                    new GroupReadSupport(),
                                                    new org.apache.hadoop.fs.Path(path.toUri()))
                                            .build()) {
                                Group actual;
                                while ((actual = reader.read()) != null) {
                                    if (!expected.next())
                                        throw new IllegalStateException(
                                                "Extra parquet row: " + table.getKey());
                                    for (int i = 1;
                                            i <= expected.getMetaData().getColumnCount();
                                            i++) {
                                        String name = expected.getMetaData().getColumnName(i);
                                        String value = expected.getString(i);
                                        boolean present = actual.getFieldRepetitionCount(name) > 0;
                                        if ((value != null) != present)
                                            throw new IllegalStateException(
                                                    "Null mismatch: " + name);
                                        if (value == null) continue;
                                        String normalized =
                                                switch (actual.getType()
                                                        .getType(name)
                                                        .asPrimitiveType()
                                                        .getPrimitiveTypeName()) {
                                                    case INT64 ->
                                                            Long.toString(expected.getLong(i));
                                                    case DOUBLE ->
                                                            Double.toString(expected.getDouble(i));
                                                    default -> value;
                                                };
                                        if (!normalized.equals(
                                                actual.getValueToString(
                                                        actual.getType().getFieldIndex(name), 0)))
                                            throw new IllegalStateException(
                                                    "Value mismatch: "
                                                            + table.getKey()
                                                            + "/"
                                                            + name
                                                            + " row="
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
                                    "Missing Parquet rows: " + table.getKey());
                    }
                }
                tables.add(
                        Map.of(
                                "table",
                                table.getKey(),
                                "rows",
                                count,
                                "row_digest",
                                HexFormat.of().formatHex(digest.digest()),
                                "verified",
                                true));
                total += count;
                System.out.println("Verified " + table.getKey() + " rows=" + count);
            }
            pg.commit();
        }
        if (total != 8706000L) throw new IllegalStateException("SF1 total mismatch");
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("status", "verified_complete");
        result.put("rows", total);
        result.put("tables", tables);
        result.put("files", files);
        result.put(
                "verification",
                "independent PG load from original CSV; all Parquet values and row identities"
                    + " compared");
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
}
