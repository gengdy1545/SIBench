package org.sibench.capture;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;

import org.apache.parquet.example.data.Group;
import org.sibench.schema.SourceSchema;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.Map;

/** Full row comparison follows column types, not column-name heuristics. */
final class SourceRow {
    private SourceRow() {}

    static Map<String, String> values(SourceSchema.Table table, JsonObject row) throws IOException {
        if (row == null) return null;
        if (!row.keySet()
                .equals(
                        table.columns().stream()
                                .map(SourceSchema.Column::name)
                                .collect(java.util.stream.Collectors.toSet())))
            throw new IOException("Incomplete row image for " + table.name());
        var values = new LinkedHashMap<String, String>();
        for (var column : table.columns()) {
            JsonElement value = row.get(column.name());
            if (value.isJsonNull() && !column.nullable())
                throw new IOException(
                        "Null required column: " + table.name() + "." + column.name());
            values.put(column.name(), normalize(column, value));
        }
        return values;
    }

    static String normalize(SourceSchema.Column column, JsonElement value) throws IOException {
        if (value == null || value.isJsonNull()) return null;
        try {
            String text = value.getAsString();
            return switch (column.type()) {
                case "integer" -> Long.toString(Long.parseLong(text));
                case "real" -> Integer.toString(Float.floatToIntBits(Float.parseFloat(text)));
                case "date" -> LocalDate.parse(text).toString();
                case "timestamp without time zone" ->
                        LocalDateTime.parse(text.replace(' ', 'T')).toString();
                case "character varying", "text" -> text;
                default -> {
                    if (!column.type().startsWith("character("))
                        throw new IOException("Unsupported row column type: " + column.type());
                    int end = text.length();
                    while (end > 0 && text.charAt(end - 1) == ' ') end--;
                    yield text.substring(0, end);
                }
            };
        } catch (RuntimeException error) {
            throw new IOException("Invalid row value for " + column.name(), error);
        }
    }

    static SourceSchema.Column primary(SourceSchema.Table table) {
        String name =
                table.indexes().stream()
                        .filter(SourceSchema.Index::primary)
                        .findFirst()
                        .orElseThrow()
                        .column();
        return table.columns().stream()
                .filter(c -> c.name().equals(name))
                .findFirst()
                .orElseThrow();
    }

    static String key(SourceSchema.Table table, JsonObject row) throws IOException {
        var primary = primary(table);
        String key = row == null ? null : normalize(primary, row.get(primary.name()));
        if (key == null) throw new IOException("Missing primary key for " + table.name());
        return key;
    }

    /** Validate every UPDATE attempt, including one that is subsequently rolled back. */
    static void requireStablePrimaryKey(SourceSchema.Table table, JsonObject change)
            throws IOException {
        if (!"update".equals(change.get("operation").getAsString())) return;
        JsonObject before = image(change, "old"), after = image(change, "new");
        if (!key(table, before).equals(key(table, after)))
            throw new IOException(
                    "Primary key value UPDATE is outside the standard workload: "
                            + table.name()
                            + "."
                            + primary(table).name());
    }

    static JsonObject image(JsonObject change, String field) throws IOException {
        JsonElement value = change.get(field);
        if (value == null) throw new IOException("Missing " + field + " row image");
        if (value.isJsonNull()) return null;
        if (!value.isJsonObject()) throw new IOException("Invalid " + field + " row image");
        return value.getAsJsonObject();
    }

    static JsonObject json(Group group) {
        var row = new JsonObject();
        for (int column = 0; column < group.getType().getFieldCount(); column++) {
            String name = group.getType().getFieldName(column);
            if (name.startsWith("_sib_")) continue;
            if (group.getFieldRepetitionCount(column) == 0) row.add(name, JsonNull.INSTANCE);
            else row.addProperty(name, group.getValueToString(column, 0));
        }
        return row;
    }
}
