package org.sibench.capture;

import com.google.gson.Gson;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.sibench.generation.*;
import org.sibench.schema.PgSchemaCheck;
import org.sibench.schema.SourceSchema;

import java.io.BufferedWriter;
import java.nio.file.*;
import java.security.MessageDigest;
import java.sql.*;
import java.util.*;

/**
 * Export committed PG values in primary-key order. {@link PgBaselineVerify} then compares every
 * Parquet value with PG.
 */
public final class PgBaselineExport {
    private PgBaselineExport() {}

    public static void export(String jdbc, Path output, GenerationConfig config) throws Exception {
        Map<String, Object> document = config.document();
        var source = SourceSchema.read(config.root(), document);
        var preflight = PgSchemaCheck.check(config.root(), document, jdbc, "postgres");
        Files.createDirectory(output);
        Files.writeString(
                output.resolve("schema-preflight.json"),
                PgSchemaCheck.json(preflight),
                StandardOpenOption.CREATE_NEW);
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
            for (var table : source.tables()) {
                String name = table.name(), key = SourceRow.primary(table).name();
                boolean integerKey = SourceRow.primary(table).type().equals("integer");
                MessageDigest digest = MessageDigest.getInstance("SHA-256");
                long count = 0;
                List<Path> tableFiles = new ArrayList<>();
                try (Statement statement = connection.createStatement()) {
                    statement.setFetchSize(8192);
                    try (ResultSet rows =
                            statement.executeQuery(
                                    "SELECT ctid::text AS _sib_pg_tid, * FROM \""
                                            + name
                                            + "\" ORDER BY \""
                                            + key
                                            + "\""
                                            // Same C ordering PgBaselineVerify applies to text keys.
                                            + (integerKey ? "" : " COLLATE \"C\""))) {
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
                        long fileRows = 0;
                        Object firstKey = null, lastKey = null;
                        try {
                            while (rows.next()) {
                                Object pk = integerKey ? (Object) rows.getLong(key) : rows.getString(key);
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
                        // Every written value is compared with PG by PgBaselineVerify, which the
                        // export stage always runs next; reading the files back here as well only
                        // repeated a weaker check.
                        String expected = HexFormat.of().formatHex(digest.digest());
                        tables.add(
                                Map.of(
                                        "table",
                                        name,
                                        "rows",
                                        count,
                                        "written_sha256",
                                        expected,
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
            Object first,
            Object last,
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
