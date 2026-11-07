package org.sibench.capture;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

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
                            default -> column.type().startsWith("character(") ? "varchar" : column.type();
                        };
                if (!Set.of("int", "timestamp", "varchar", "text").contains(type))
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

    /** Generation records the verified source identity; consumers need no source checkout. */
    public Map<String, Object> provenance() {
        if (schema == null) throw new IllegalStateException("A consumed catalog has no source checkout");
        return provenance(schema, definitions);
    }

    /** DuckDB CHAR is a VARCHAR alias, including its significant trailing spaces. */
    public static List<Definition> duckDbDefinitions(SourceSchema schema) throws IOException {
        return fromSchema(schema).definitions();
    }

    public static Map<String, Object> provenance(SourceSchema schema, List<Definition> definitions) {
        var result = new LinkedHashMap<String, Object>();
        result.put("benchmark", schema.benchmark());
        result.put("source_commit", schema.sourceCommit());
        result.put("source_directory", schema.source().toAbsolutePath().toString());
        result.put("source_schema_sha256", schema.fingerprints());
        if (schema.nativeConfig() != null) result.put("source_native_config", schema.nativeConfig());
        result.put("index_definitions", definitions.stream().map(Definition::describe).toList());
        result.put("catalog_sha256", identity(new Gson().toJsonTree(result).getAsJsonObject()));
        return result;
    }

    /** Binds the embedded source and index definitions, independent of the original filesystem path. */
    public static String identity(JsonObject manifest) {
        var bound = new JsonObject();
        for (String name : List.of("benchmark", "source_commit", "source_schema_sha256", "source_native_config", "index_definitions"))
            if (manifest.has(name)) bound.add(name, manifest.get(name));
        var digest = org.sibench.config.Digests.digest();
        return java.util.HexFormat.of().formatHex(digest.digest(bound.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8)));
    }

    public static IndexCatalog fromManifest(JsonObject manifest) throws IOException {
        try {
            for (String field : List.of("benchmark", "source_commit", "source_schema_sha256", "index_definitions", "catalog_sha256"))
                if (!manifest.has(field)) throw new IOException("Missing source catalog provenance: " + field);
            if (!identity(manifest).equals(manifest.get("catalog_sha256").getAsString()))
                throw new IOException("Index catalog identity mismatch");
            if (!Set.of("ycsb", "sysbench", "hybench").contains(manifest.get("benchmark").getAsString())
                    || manifest.get("source_commit").getAsString().isBlank()
                    || !manifest.get("source_schema_sha256").isJsonObject())
                throw new IOException("Invalid source catalog provenance");
            var definitions = new ArrayList<Definition>();
            var tableNames = new TreeMap<Integer, String>();
            var tableIds = new HashMap<String, Integer>();
            var indexes = new HashMap<Integer, TreeSet<Integer>>();
            var names = new HashSet<String>();
            for (var item : manifest.getAsJsonArray("index_definitions")) {
                var d = item.getAsJsonObject();
                int table = d.get("table_id").getAsBigDecimal().intValueExact();
                int index = d.get("index_id").getAsBigDecimal().intValueExact();
                String name = d.get("name").getAsString(), tableName = d.get("table").getAsString();
                String column = d.get("column").getAsString(), type = d.get("type").getAsString();
                if (table < 1 || table > 65535 || index < 0 || index > 65535 || tableName.isBlank()
                        || name.isBlank() || column.isBlank() || !names.add(name)
                        || !Set.of("int", "timestamp", "varchar", "text").contains(type))
                    throw new IOException("Invalid source index definition");
                if (tableNames.containsKey(table) && !tableNames.get(table).equals(tableName)
                        || tableIds.containsKey(tableName) && tableIds.get(tableName) != table
                        || !indexes.computeIfAbsent(table, ignored -> new TreeSet<>()).add(index))
                    throw new IOException("Conflicting source index identity");
                tableNames.put(table, tableName); tableIds.put(tableName, table);
                definitions.add(new Definition(table, tableName, index, name, column, type));
            }
            if (tableNames.isEmpty() || tableNames.lastKey() != tableNames.size())
                throw new IOException("Incomplete source table identities");
            for (var ids : indexes.values())
                if (ids.first() != 0 || ids.last() != ids.size() - 1)
                    throw new IOException("Incomplete source index identities");
            return new IndexCatalog(null, definitions);
        } catch (RuntimeException invalid) {
            throw new IOException("Invalid source catalog", invalid);
        }
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

}
