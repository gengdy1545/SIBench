package org.sibench.capture;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.sibench.schema.SourceSchema;

/** Converts the same HyBench CSVs PostgreSQL reads into bounded, parallel Pixels LOAD inputs. */
final class PixelsInputs {
    record Prepared(PixelsSchema.Spec schema, Map<String, Long> rows) {}
    private PixelsInputs() {}

    static Prepared prepare(SourceSchema source, Path data, Path output, long shardRows) throws IOException {
        if (shardRows < 1) throw new IllegalArgumentException("Invalid LOAD shard size");
        Files.createDirectory(output);
        var files = new LinkedHashMap<String, Path>();
        for (String line : Files.readAllLines(source.source().resolve("conf/load_data_pg.sql"))) {
            if (line.isBlank()) continue;
            String[] copy = HybenchInputs.copyStatement(line);
            if (copy == null || files.put(copy[0].toLowerCase(java.util.Locale.ROOT), data.resolve(copy[1])) != null)
                throw new IOException("Invalid HyBench load statement: " + line);
        }
        var counts = new LinkedHashMap<String, Long>();
        var keyTypes = new LinkedHashMap<String, String>();
        var spec = new StringBuilder();
        for (var table : source.tables()) {
            Path input = files.remove(table.name());
            if (input == null) throw new IOException("Missing CSV for " + table.name());
            Path directory = Files.createDirectory(output.resolve(table.name()));
            var types = new ArrayList<String>();
            for (var column : table.columns()) types.add(PixelsSchema.type(column.type()));
            long[] maximum = new long[types.size()];
            long rows = 0;
            BufferedWriter writer = null;
            try (var reader = new BufferedReader(Files.newBufferedReader(input, StandardCharsets.UTF_8), 1 << 20)) {
                for (String line; (line = reader.readLine()) != null;) {
                    List<String> fields = csv(line);
                    if (fields.size() != types.size()) throw new IOException("CSV column count differs: " + table.name() + " row " + (rows + 1));
                    var text = new StringBuilder(line.length() + 32);
                    for (int i = 0; i < fields.size(); i++) {
                        if (i > 0) text.append('|');
                        String value = fields.get(i);
                        if (value == null) {
                            if (!table.columns().get(i).nullable()) throw new IOException("NULL in required column " + table.name());
                            text.append("\\N");
                            continue;
                        }
                        if (value.indexOf('|') >= 0 || value.indexOf('\\') >= 0 || value.indexOf('\r') >= 0 || value.indexOf('\n') >= 0)
                            throw new IOException("Value needs unsupported Pixels LOAD escaping: " + table.name());
                        String type = types.get(i);
                        if (type.equals("date")) {
                            try { value = java.time.LocalDate.parse(value.substring(0, Math.min(10, value.length()))).toString(); }
                            catch (RuntimeException invalid) { throw new IOException("Invalid HyBench date in " + table.name(), invalid); }
                        }
                        if (type.startsWith("char(")) {
                            int width = Integer.parseInt(type.substring(5, type.length() - 1));
                            int length = value.codePointCount(0, value.length());
                            if (length > width) {
                                int end = value.offsetByCodePoints(0, width);
                                if (!value.substring(end).chars().allMatch(c -> c == ' ')) throw new IOException("Overlong CHAR in " + table.name());
                                value = value.substring(0, end);
                            } else value += " ".repeat(width - length);
                        }
                        if (type.equals("varchar")) maximum[i] = Math.max(maximum[i], value.getBytes(StandardCharsets.UTF_8).length);
                        text.append(value);
                    }
                    if (rows % shardRows == 0) {
                        if (writer != null) writer.close();
                        writer = new BufferedWriter(Files.newBufferedWriter(directory.resolve(String.format("part-%06d.tbl", rows / shardRows)),
                                StandardCharsets.UTF_8), 1 << 20);
                    }
                    writer.append(text).append('\n');
                    rows++;
                    if (Thread.currentThread().isInterrupted()) throw new IOException("Pixels input preparation interrupted");
                }
            } finally { if (writer != null) writer.close(); }
            counts.put(table.name(), rows);
            String key = table.indexes().stream().filter(SourceSchema.Index::primary).findFirst().orElseThrow().column();
            var columns = new ArrayList<String>();
            for (int i = 0; i < types.size(); i++) {
                String type = types.get(i), name = table.columns().get(i).name();
                if (type.equals("varchar")) type += "(" + PixelsSchema.bound(maximum[i]) + ")";
                columns.add(name + ":" + type);
                if (name.equals(key)) keyTypes.put(table.name(), type.replaceAll("\\(.*", ""));
            }
            spec.append(table.name()).append('\t').append(key).append('\t').append(String.join(";", columns)).append('\n');
        }
        if (!files.isEmpty()) throw new IOException("Unexpected HyBench CSV tables: " + files.keySet());
        spec.append("sibench_marker\tid\tid:bigint\n");
        keyTypes.put("sibench_marker", "bigint");
        return new Prepared(new PixelsSchema.Spec(spec.toString(), keyTypes), Map.copyOf(counts));
    }

    /** PostgreSQL COPY CSV: unquoted empty is NULL, quoted empty is an empty string. */
    static List<String> csv(String line) throws IOException {
        if (line.indexOf('"') < 0) {
            var fields = new ArrayList<String>();
            for (String field : line.split(",", -1)) fields.add(field.isEmpty() ? null : field);
            return fields;
        }
        var fields = new ArrayList<String>();
        for (int start = 0; ;) {
            boolean quoted = start < line.length() && line.charAt(start) == '"';
            var field = new StringBuilder();
            int at = start;
            if (quoted) {
                at++;
                boolean closed = false;
                while (at < line.length()) {
                    char c = line.charAt(at++);
                    if (c != '"') field.append(c);
                    else if (at < line.length() && line.charAt(at) == '"') { field.append('"'); at++; }
                    else { closed = true; break; }
                }
                if (!closed || at < line.length() && line.charAt(at) != ',') throw new IOException("Malformed or multiline CSV field");
            } else {
                while (at < line.length() && line.charAt(at) != ',') {
                    char c = line.charAt(at++);
                    if (c == '"') throw new IOException("Quote inside unquoted CSV field");
                    field.append(c);
                }
            }
            fields.add(!quoted && field.length() == 0 ? null : field.toString());
            if (at == line.length()) return fields;
            start = at + 1;
        }
    }
}
