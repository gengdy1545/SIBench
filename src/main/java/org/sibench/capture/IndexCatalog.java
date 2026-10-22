package org.sibench.capture;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import org.apache.parquet.example.data.Group;
import org.sibench.schema.SourceSchema;
import org.sibench.trace.IndexKey;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

/**
 * The supported, pinned source schema. Table IDs follow source table order from 1; index ID 0 is
 * the primary key and secondary IDs follow source DDL order within each table.
 */
public final class IndexCatalog {
    public record Definition(
            int tableId, String table, int indexId, String name, String column, String type) {
        public IndexKey encode(String value) throws IOException {
            try {
                return IndexKey.parse(type, value);
            } catch (IOException error) {
                throw new IOException("Invalid indexed value for " + name + ": " + value, error);
            }
        }

        /**
         * Encodes a scan key of PG type {@code typeOid}. Only operators of the column's own btree
         * family reach an index scan; date keys compare with a timestamp column as midnight.
         */
        public IndexKey encodeScanKey(int typeOid, String value) throws IOException {
            String keyType =
                    switch (typeOid) {
                        case 20, 21, 23 -> "int";
                        case 1114 -> "timestamp";
                        case 1082 -> "date";
                        case 1042 -> "bpchar";
                        case 25, 1043 -> "text";
                        default -> null;
                    };
            boolean compatible =
                    keyType != null
                            && switch (type) {
                                case "int" -> keyType.equals("int");
                                case "timestamp" ->
                                        keyType.equals("timestamp") || keyType.equals("date");
                                case "varchar", "text" -> keyType.equals("text");
                                default -> keyType.equals("bpchar");
                            };
            if (!compatible)
                throw new IOException(
                        "Scan key type " + typeOid + " is outside the " + type + " family of " + name);
            return IndexKey.parse(keyType, value);
        }

        public boolean textual() {
            return !type.equals("int") && !type.equals("timestamp");
        }

        public Map<String, Object> describe() {
            var result = new LinkedHashMap<String, Object>();
            result.put("table_id", tableId);
            result.put("table", table);
            result.put("index_id", indexId);
            result.put("name", name);
            result.put("column", column);
            result.put("type", type);
            return result;
        }
    }

    private final SourceSchema schema;
    private final List<Definition> definitions;
    private final Map<String, Definition> names = new HashMap<>();
    private final Map<String, Integer> tableIds = new LinkedHashMap<>();
    private final Map<Integer, Definition> primary = new HashMap<>();
    private final Map<Integer, List<Definition>> secondary = new HashMap<>();

    private IndexCatalog(SourceSchema schema, List<Definition> definitions) {
        this.schema = schema;
        this.definitions = List.copyOf(definitions);
        for (Definition definition : definitions) {
            if (names.put(definition.name(), definition) != null)
                throw new IllegalArgumentException("Duplicate source index");
            tableIds.putIfAbsent(definition.table(), definition.tableId());
            if (definition.indexId() == 0) primary.put(definition.tableId(), definition);
            else
                secondary
                        .computeIfAbsent(definition.tableId(), ignored -> new ArrayList<>())
                        .add(definition);
        }
        secondary.values().forEach(list -> list.sort(Comparator.comparing(Definition::indexId)));
    }

    /** Catalog support does not imply that the benchmark's capture/trace compiler exists. */
    public static IndexCatalog fromSchema(SourceSchema schema) throws IOException {
        var primaries = new ArrayList<Definition>();
        var secondaries = new HashMap<String, Definition>();
        int tableId = 0;
        for (var table : schema.tables()) {
            tableId++;
            int secondaryId = 0;
            for (var index : table.indexes()) {
                var column =
                        table.columns().stream()
                                .filter(c -> c.name().equals(index.column()))
                                .findFirst()
                                .orElseThrow(() -> new IOException("Unknown source index column"));
                String type =
                        switch (column.type()) {
                            case "integer", "bigint", "smallint" -> "int";
                            case "timestamp without time zone" -> "timestamp";
                            case "character varying" -> "varchar";
                            default -> column.type().replace("character(", "char(");
                        };
                if (!Set.of("int", "timestamp", "varchar", "text").contains(type)
                        && !type.matches("char\\([0-9]+\\)"))
                    throw new IOException("Unsupported indexed column type: " + type);
                var definition =
                        new Definition(
                                tableId,
                                table.name(),
                                index.primary() ? 0 : ++secondaryId,
                                index.name(),
                                index.column(),
                                type);
                if (index.primary()) primaries.add(definition);
                else secondaries.put(definition.name(), definition);
            }
        }
        // Manifest order: all PKs, then secondaries in source DDL order (HyBench history).
        var result = new ArrayList<Definition>(primaries);
        for (var index : schema.secondaryOrder()) result.add(secondaries.get(index.name()));
        return new IndexCatalog(schema, result);
    }

    /** The source identity a compiled dataset records, re-verified by {@link #fromManifest}. */
    public Map<String, Object> provenance() {
        var result = new LinkedHashMap<String, Object>();
        result.put("benchmark", schema.benchmark());
        result.put("source_commit", schema.sourceCommit());
        result.put("source_directory", schema.source().toAbsolutePath().toString());
        result.put("source_schema_sha256", schema.fingerprints());
        if (schema.nativeConfig() != null) result.put("source_native_config", schema.nativeConfig());
        result.put("index_definitions", describe());
        return result;
    }

    public static IndexCatalog fromManifest(JsonObject manifest) throws IOException {
        for (String field :
                List.of(
                        "benchmark",
                        "source_directory",
                        "source_schema_sha256",
                        "index_definitions"))
            if (!manifest.has(field)) throw new IOException("Missing pinned source provenance");
        String benchmark = manifest.get("benchmark").getAsString();
        Map<String, Object> nativeConfig =
                manifest.has("source_native_config")
                        ? plain(manifest.getAsJsonObject("source_native_config"))
                        : null;
        IndexCatalog catalog =
                fromSchema(
                        SourceSchema.of(
                                benchmark,
                                Path.of(manifest.get("source_directory").getAsString()),
                                nativeConfig));
        Gson gson = new Gson();
        if (!gson.toJsonTree(catalog.schema.fingerprints())
                .equals(manifest.get("source_schema_sha256")))
            throw new IOException("Source schema fingerprint mismatch");
        if (!gson.toJsonTree(catalog.describe()).equals(manifest.get("index_definitions")))
            throw new IOException("Declared index catalog differs from pinned source schema");
        return catalog;
    }

    /** JSON numbers back as Long or Double, as the YAML configuration delivered them. */
    private static Map<String, Object> plain(JsonObject object) {
        var result = new LinkedHashMap<String, Object>();
        for (var entry : object.entrySet()) result.put(entry.getKey(), plain(entry.getValue()));
        return result;
    }

    private static Object plain(JsonElement value) {
        if (value.isJsonObject()) return plain(value.getAsJsonObject());
        if (value.isJsonArray()) {
            var list = new ArrayList<Object>();
            for (JsonElement item : value.getAsJsonArray()) list.add(plain(item));
            return list;
        }
        var primitive = value.getAsJsonPrimitive();
        if (primitive.isBoolean()) return primitive.getAsBoolean();
        if (primitive.isString()) return primitive.getAsString();
        var number = primitive.getAsBigDecimal();
        return number.scale() <= 0 ? (Object) number.longValueExact() : number.doubleValue();
    }

    public SourceSchema schema() {
        return schema;
    }

    public int tableId(String name) throws IOException {
        Integer id = tableIds.get(name);
        if (id == null) throw new IOException("Unsupported source table: " + name);
        return id;
    }

    public Definition named(String name) throws IOException {
        Definition definition = names.get(name);
        if (definition == null) throw new IOException("Unsupported accessed index: " + name);
        return definition;
    }

    public Definition primary(int table) throws IOException {
        Definition definition = primary.get(table);
        if (definition == null) throw new IOException("Unsupported source table: " + table);
        return definition;
    }

    public List<Definition> secondary(int table) {
        return secondary.getOrDefault(table, List.of());
    }

    public List<Definition> definitions() {
        return definitions;
    }

    public List<Map<String, Object>> describe() {
        return definitions.stream().map(Definition::describe).toList();
    }

    public IndexKey primaryKey(String table, Group row) throws IOException {
        return primary(tableId(table)).encode(text(row, primary(tableId(table)).column()));
    }

    public IndexKey primaryKey(String table, JsonObject row) throws IOException {
        Definition definition = primary(tableId(table));
        JsonElement value = row.get(definition.column());
        if (value == null || value.isJsonNull())
            throw new IOException("Row lacks its primary key: " + definition.column());
        return definition.encode(value.getAsString());
    }

    public Map<Integer, IndexKey> keys(String table, Group row) throws IOException {
        var keys = new LinkedHashMap<Integer, IndexKey>();
        for (Definition definition : secondary(tableId(table)))
            keys.put(definition.indexId(), definition.encode(text(row, definition.column())));
        return keys;
    }

    public Map<Integer, IndexKey> keys(String table, JsonObject row) throws IOException {
        var keys = new LinkedHashMap<Integer, IndexKey>();
        for (Definition definition : secondary(tableId(table))) {
            JsonElement value = row.get(definition.column());
            if (value == null)
                throw new IOException("Row change missing indexed column: " + definition.column());
            keys.put(
                    definition.indexId(),
                    definition.encode(value.isJsonNull() ? null : value.getAsString()));
        }
        return keys;
    }

    private static String text(Group row, String column) throws IOException {
        if (!row.getType().containsField(column))
            throw new IOException("Row missing indexed column: " + column);
        return row.getFieldRepetitionCount(column) == 0
                ? null
                : row.getValueToString(row.getType().getFieldIndex(column), 0);
    }
}
