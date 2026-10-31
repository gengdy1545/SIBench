package org.sibench.generation;

import static org.sibench.config.ConfigValues.*;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

public final class SourceTables {
    private SourceTables() {}

    public static List<TableData> describe(GenerationConfig config) throws IOException {
        return switch (config.benchmark()) {
            case "hybench" -> new HybenchTables(config).describe();
            case "ycsb" -> ycsb(config);
            case "sysbench" -> sysbench(config);
            default -> throw new IllegalArgumentException("Unknown benchmark");
        };
    }

    static MessageType schema(String name, String columns) {
        return MessageTypeParser.parseMessageType(
                "message "
                        + name
                        + " { "
                        + columns
                        + " required int64 _sib_row_id; required int64 _sib_timestamp; }");
    }

    static TableData table(
            GenerationConfig config,
            String name,
            long rows,
            long firstRowId,
            String key,
            String columns,
            BiFunction<Long, Group, Group> builder) {
        MessageType schema = schema(name, columns);
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        return new TableData(
                name,
                rows,
                firstRowId,
                key,
                schema,
                ordinal -> {
                    require(ordinal >= 0 && ordinal < rows, "Row ordinal out of range");
                    Group row = builder.apply(ordinal, factory.newGroup());
                    row.add("_sib_row_id", Math.addExact(firstRowId, ordinal));
                    row.add("_sib_timestamp", config.baselineTimestamp());
                    return row;
                });
    }

    private static List<TableData> ycsb(GenerationConfig config) {
        Map<String, Object> n = object(config.document(), "native");
        long rows = positive(n, "recordcount");
        require(
                integer(n, "insertstart") == 0 && positive(n, "insertcount") == rows,
                "YCSB baseline requires complete insert range");
        int fields = Math.toIntExact(positive(n, "fieldcount"));
        int length = Math.toIntExact(positive(n, "fieldlength"));
        require(fields <= 1024 && length <= 1048576, "YCSB row shape exceeds supported limits");
        require(
                Math.multiplyExact((long) fields, length) <= config.groupBytes(),
                "A YCSB row must fit within row_group_buffer_bytes");
        require(
                string(n, "fieldlengthdistribution").equals("constant"),
                "Only constant fieldlength is supported");
        String order = string(n, "insertorder");
        require(order.equals("hashed") || order.equals("ordered"), "Invalid insertorder");
        int padding = Math.toIntExact(integer(n, "zeropadding"));
        require(padding >= 0 && padding <= 64, "Invalid zeropadding");
        StringBuilder columns = new StringBuilder("required binary YCSB_KEY (UTF8); ");
        for (int f = 0; f < fields; f++)
            columns.append("required binary FIELD").append(f).append("; ");
        return List.of(
                table(
                        config,
                        "usertable",
                        rows,
                        1,
                        "YCSB_KEY",
                        columns.toString(),
                        (i, row) -> {
                            row.add("YCSB_KEY", ycsbKey(i, padding, order.equals("ordered")));
                            var random =
                                    new DeterministicRandom(config.seed(), "ycsb/usertable", i);
                            for (int f = 0; f < fields; f++) {
                                byte[] value = new byte[length];
                                // Preserve the upstream six-character bit-mask distribution, with a
                                // fixed RNG.
                                int[] masks = {31, 63, 95, 31, 63, 95};
                                for (int offset = 0; offset < length; offset += 6) {
                                    int bits = (int) random.next();
                                    for (int k = 0; k < 6 && offset + k < length; k++) {
                                        value[offset + k] =
                                                (byte) (((bits >> (5 * k)) & masks[k]) + 32);
                                    }
                                }
                                row.add(
                                        "FIELD" + f,
                                        org.apache.parquet.io.api.Binary.fromConstantByteArray(
                                                value));
                            }
                            return row;
                        }));
    }

    public static String ycsbKey(long ordinal, int padding, boolean ordered) {
        long value = ordinal;
        if (!ordered) {
            long hash = 0xcbf29ce484222325L;
            for (int b = 0; b < 8; b++) {
                hash = (hash ^ (value & 255)) * 1099511628211L;
                value >>= 8;
            }
            value = Math.abs(hash);
        }
        String suffix = Long.toString(value);
        return "user" + "0".repeat(Math.max(0, padding - suffix.length())) + suffix;
    }

    private static List<TableData> sysbench(GenerationConfig config) {
        Map<String, Object> n = object(config.document(), "native");
        int count = Math.toIntExact(positive(n, "tables"));
        long rows = positive(n, "table-size");
        require(count <= 10000 && rows <= Integer.MAX_VALUE, "sysbench table or ID limit exceeded");
        require(
                string(n, "rand-type").equals("uniform"),
                "Baseline adapter supports uniform sysbench distribution");
        require(
                integer(n, "rand-seed") == config.seed(),
                "sysbench seed differs from generation seed");
        List<TableData> tables = new ArrayList<>();
        for (int t = 1; t <= count; t++) {
            String name = "sbtest" + t;
            long start = Math.addExact(Math.multiplyExact(t - 1L, rows), 1);
            tables.add(
                    table(
                            config,
                            name,
                            rows,
                            start,
                            "id",
                            "required int32 id; required int32 k; required binary c (UTF8);"
                                + " required binary pad (UTF8);",
                            (i, row) -> {
                                var random =
                                        new DeterministicRandom(
                                                config.seed(), "sysbench/" + name, i);
                                row.add("id", Math.toIntExact(i + 1));
                                row.add("k", Math.toIntExact(random.between(1, rows + 1)));
                                row.add("c", digits(random, 10));
                                row.add("pad", digits(random, 5));
                                return row;
                            }));
        }
        return List.copyOf(tables);
    }

    private static String digits(DeterministicRandom random, int groups) {
        StringBuilder text = new StringBuilder();
        for (int g = 0; g < groups; g++) {
            if (g > 0) text.append('-');
            for (int j = 0; j < 11; j++) text.append((char) ('0' + random.between(0, 10)));
        }
        return text.toString();
    }
}
