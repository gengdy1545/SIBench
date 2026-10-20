package org.sibench.capture;

import com.google.gson.Gson;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.sibench.generation.*;

import java.io.BufferedWriter;
import java.nio.file.*;
import java.security.MessageDigest;
import java.sql.*;
import java.util.*;

/**
 * Export committed PG values in primary-key order, then read every Parquet row for verification.
 */
public final class PgBaselineExport {
    public static final Map<String, String> TABLES = new LinkedHashMap<>();

    static {
        TABLES.put("customer", "custid");
        TABLES.put("company", "companyid");
        TABLES.put("savingaccount", "accountid");
        TABLES.put("checkingaccount", "accountid");
        TABLES.put("transfer", "id");
        TABLES.put("checking", "id");
        TABLES.put("loanapps", "id");
        TABLES.put("loantrans", "id");
    }

    private PgBaselineExport() {}

    public static void main(String[] args) throws Exception {
        if (args.length == 3 && args[0].equals("--config")) {
            CaptureConfig config = CaptureConfig.read(Path.of(args[1]), Path.of(args[2]));
            export(config.jdbc(), config.layout().output(), config.layout());
            return;
        }
        if (args.length != 2)
            throw new IllegalArgumentException("PgBaselineExport JDBC_URL OUTPUT_DIRECTORY");
        Path output = Path.of(args[1]).toAbsolutePath();
        GenerationConfig config =
                new GenerationConfig(
                        Path.of("").toAbsolutePath(),
                        Map.of(),
                        "hybench",
                        0,
                        268435456,
                        1048576,
                        67108864,
                        1048576,
                        1048576,
                        true,
                        org.apache.parquet.hadoop.metadata.CompressionCodecName.ZSTD,
                        3,
                        1,
                        0,
                        output,
                        21474836480L);
        export(args[0], output, config);
    }

    public static void export(String jdbc, Path output, GenerationConfig config) throws Exception {
        Files.createDirectory(output);
        Files.writeString(
                output.resolve("INCOMPLETE"),
                "PG baseline export in progress\n",
                StandardOpenOption.CREATE_NEW);
        Gson json = new Gson();
        long fileId = 1, rowId = 1, total = 0;
        List<Map<String, Object>> tables = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection(jdbc, "postgres", "");
                BufferedWriter files = Files.newBufferedWriter(output.resolve("files.jsonl"));
                BufferedWriter locators = Files.newBufferedWriter(output.resolve("locators.tsv"))) {
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            connection.setReadOnly(true);
            for (var table : TABLES.entrySet()) {
                String name = table.getKey(), key = table.getValue();
                MessageDigest digest = MessageDigest.getInstance("SHA-256");
                long count = 0;
                List<Path> tableFiles = new ArrayList<>();
                try (Statement statement = connection.createStatement()) {
                    statement.setFetchSize(8192);
                    try (ResultSet rows =
                            statement.executeQuery(
                                    "SELECT ctid::text AS _sib_pg_tid, * FROM "
                                            + name
                                            + " ORDER BY "
                                            + key)) {
                        ResultSetMetaData columns = rows.getMetaData();
                        StringBuilder schemaText = new StringBuilder("message " + name + " { ");
                        for (int col = 2; col <= columns.getColumnCount(); col++) {
                            int type = columns.getColumnType(col);
                            schemaText
                                    .append("optional ")
                                    .append(
                                            type == Types.INTEGER
                                                            || type == Types.BIGINT
                                                            || type == Types.SMALLINT
                                                    ? "int64 "
                                                    : type == Types.REAL
                                                                    || type == Types.FLOAT
                                                                    || type == Types.DOUBLE
                                                            ? "double "
                                                            : "binary ")
                                    .append(columns.getColumnName(col))
                                    .append(
                                            type == Types.INTEGER
                                                            || type == Types.BIGINT
                                                            || type == Types.SMALLINT
                                                            || type == Types.REAL
                                                            || type == Types.FLOAT
                                                            || type == Types.DOUBLE
                                                    ? "; "
                                                    : " (UTF8); ");
                        }
                        schemaText.append(
                                "required int64 _sib_row_id; required int64 _sib_timestamp; }");
                        MessageType schema =
                                MessageTypeParser.parseMessageType(schemaText.toString());
                        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
                        TableData descriptor = new TableData(name, 0, rowId, key, schema, null);
                        ParquetDataFile writer = null;
                        Path path = null;
                        long fileRows = 0, firstKey = 0, lastKey = 0;
                        try {
                            while (rows.next()) {
                                long pk = rows.getLong(key);
                                lastKey = pk;
                                if (writer == null) {
                                    path =
                                            output.resolve(
                                                    String.format(
                                                            Locale.ROOT,
                                                            "%020d-%s.parquet",
                                                            fileId,
                                                            name));
                                    writer =
                                            new ParquetDataFile(
                                                    path,
                                                    descriptor,
                                                    config,
                                                    Map.of(
                                                            "sibench.file_id",
                                                            Long.toString(fileId),
                                                            "sibench.source",
                                                            "postgresql"));
                                    fileRows = 0;
                                    firstKey = pk;
                                    tableFiles.add(path);
                                }
                                Group group = factory.newGroup();
                                for (int col = 2; col <= columns.getColumnCount(); col++) {
                                    String value = rows.getString(col);
                                    String column = columns.getColumnName(col);
                                    if (value == null) continue;
                                    switch (schema.getType(column)
                                            .asPrimitiveType()
                                            .getPrimitiveTypeName()) {
                                        case INT64 -> group.add(column, rows.getLong(col));
                                        case DOUBLE -> group.add(column, rows.getDouble(col));
                                        default -> group.add(column, value);
                                    }
                                }
                                group.add("_sib_row_id", rowId++);
                                group.add("_sib_timestamp", 0L);
                                updateDigest(digest, group, schema);
                                locators.write(
                                        name
                                                + "\t"
                                                + pk
                                                + "\t"
                                                + rows.getString(1)
                                                + "\t"
                                                + fileId
                                                + "\t"
                                                + fileRows
                                                + "\n");
                                boolean full = writer.write(group);
                                fileRows++;
                                count++;
                                if (count % 4096 == 0
                                        && Files.getFileStore(output).getUsableSpace()
                                                < config.minFreeBytes())
                                    throw new java.io.IOException("Disk reserve reached");
                                if (full) {
                                    writeFile(
                                            files,
                                            json,
                                            writer,
                                            path,
                                            name,
                                            key,
                                            fileId,
                                            fileRows,
                                            firstKey,
                                            pk,
                                            config.maxRows());
                                    writer.close();
                                    writer = null;
                                    fileId++;
                                }
                            }
                            if (writer != null) {
                                writeFile(
                                        files,
                                        json,
                                        writer,
                                        path,
                                        name,
                                        key,
                                        fileId,
                                        fileRows,
                                        firstKey,
                                        lastKey,
                                        config.maxRows());
                                writer.close();
                                writer = null;
                                fileId++;
                            }
                        } finally {
                            if (writer != null) writer.close();
                        }
                        String expected = HexFormat.of().formatHex(digest.digest());
                        MessageDigest readDigest = MessageDigest.getInstance("SHA-256");
                        long readCount = 0;
                        for (Path file : tableFiles) {
                            try (var reader =
                                    org.apache.parquet.hadoop.ParquetReader.builder(
                                                    new org.apache.parquet.hadoop.example
                                                            .GroupReadSupport(),
                                                    new org.apache.hadoop.fs.Path(file.toUri()))
                                            .build()) {
                                Group group;
                                while ((group = reader.read()) != null) {
                                    updateDigest(readDigest, group, schema);
                                    readCount++;
                                }
                            }
                        }
                        if (readCount != count
                                || !expected.equals(HexFormat.of().formatHex(readDigest.digest())))
                            throw new IllegalStateException("PG/Parquet mismatch: " + name);
                        tables.add(
                                Map.of(
                                        "table",
                                        name,
                                        "rows",
                                        count,
                                        "sha256",
                                        expected,
                                        "verified",
                                        true,
                                        "schema",
                                        schema.toString()));
                        System.out.println(name + " verified rows=" + count);
                        total += count;
                    }
                }
            }
            connection.commit();
        }
        Files.writeString(
                output.resolve("manifest.json"),
                json.toJson(
                        Map.of(
                                "status",
                                "complete",
                                "rows",
                                total,
                                "tables",
                                tables,
                                "files",
                                fileId - 1)));
        Files.delete(output.resolve("INCOMPLETE"));
    }

    private static void writeFile(
            BufferedWriter out,
            Gson json,
            ParquetDataFile writer,
            Path path,
            String table,
            String key,
            long id,
            long rows,
            long first,
            long last,
            long maxRows)
            throws Exception {
        var footer = writer.finish();
        if (footer.getBlocks().stream().mapToLong(b -> b.getRowCount()).sum() != rows)
            throw new IllegalStateException("Footer mismatch");
        for (var block : footer.getBlocks())
            if (block.getRowCount() > maxRows) throw new IllegalStateException("Row group limit");
        out.write(
                json.toJson(
                        Map.of(
                                "file_id",
                                id,
                                "path",
                                path.getFileName().toString(),
                                "table",
                                table,
                                "key",
                                key,
                                "rows",
                                rows,
                                "first_key",
                                first,
                                "last_key",
                                last,
                                "row_groups",
                                footer.getBlocks().stream()
                                        .map(
                                                b ->
                                                        Map.of(
                                                                "rows",
                                                                b.getRowCount(),
                                                                "compressed_bytes",
                                                                b.getCompressedSize()))
                                        .toList())));
        out.newLine();
        out.flush();
    }

    private static void updateDigest(MessageDigest digest, Group group, MessageType schema) {
        for (int i = 0; i < schema.getFieldCount(); i++) {
            if (group.getFieldRepetitionCount(i) == 0) {
                digest.update((byte) 0);
                continue;
            }
            digest.update((byte) 1);
            byte[] value =
                    group.getValueToString(i, 0).getBytes(java.nio.charset.StandardCharsets.UTF_8);
            digest.update(java.nio.ByteBuffer.allocate(4).putInt(value.length).array());
            digest.update(value);
        }
    }
}
