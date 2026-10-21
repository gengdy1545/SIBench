package org.sibench.capture;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import org.apache.parquet.example.data.Group;
import org.sibench.generation.BaselineGenerator;
import org.sibench.schema.SourceSchema;

import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.*;

/** The supported, pinned source schema; index IDs follow source DDL order within each table. */
public final class IndexCatalog {
    public record Definition(
            int tableId, String table, int indexId, String name, String column, String type) {
        public String normalize(String value) throws IOException {
            if (value == null) return null;
            try {
                if (type.equals("int")) return Long.toString(Long.parseLong(value));
                if (type.equals("timestamp"))
                    return LocalDateTime.parse(value.replace(' ', 'T')).toString();
                if (type.startsWith("char(")) {
                    int end = value.length();
                    while (end > 0 && value.charAt(end - 1) == ' ') end--;
                    return value.substring(0, end);
                }
                if (type.equals("varchar") || type.equals("text")) return value;
            } catch (RuntimeException error) {
                throw new IOException("Invalid indexed value for " + name + ": " + value, error);
            }
            throw new IOException("Unsupported index column type: " + type);
        }

        public Map<String, Object> describe() {
            return Map.of(
                    "table_id",
                    tableId,
                    "table",
                    table,
                    "index_id",
                    indexId,
                    "name",
                    name,
                    "column",
                    column,
                    "type",
                    type);
        }
    }

    private final List<Definition> definitions;
    private final Map<Integer, List<Definition>> tables = new LinkedHashMap<>();
    private final Map<String, Definition> names = new HashMap<>();
    private final Map<Integer, List<Definition>> secondary = new HashMap<>();

    private IndexCatalog(List<Definition> definitions) {
        this.definitions = List.copyOf(definitions);
        for (Definition definition : definitions) {
            tables.computeIfAbsent(definition.tableId(), ignored -> new ArrayList<>())
                    .add(definition);
            if (names.put(definition.name(), definition) != null)
                throw new IllegalArgumentException("Duplicate source index");
        }
        tables.forEach(
                (id, indexes) ->
                        secondary.put(
                                id,
                                indexes.stream()
                                        .filter(definition -> definition.indexId() > 0)
                                        .toList()));
    }

    public static IndexCatalog read(Path source) throws IOException {
        SourceSchema schema = SourceSchema.hybench(source);
        var keys = new LinkedHashMap<String, String>();
        for (var table : schema.tables()) keys.put(table.name(), table.indexes().get(0).column());
        if (!keys.equals(PgBaselineExport.TABLES))
            throw new IOException("Source tables/primary keys differ from captured table contract");
        return fromSchema(schema);
    }

    /** Catalog support does not imply that the benchmark's capture/trace compiler exists. */
    public static IndexCatalog fromSchema(SourceSchema schema) throws IOException {
        var result = new ArrayList<Definition>();
        var primaries = new ArrayList<Definition>();
        var secondaries = new ArrayList<Definition>();
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
                            case "integer" -> "int";
                            case "timestamp without time zone" -> "timestamp";
                            case "character varying" -> "varchar";
                            default -> column.type().replace("character(", "char(");
                        };
                var definition =
                        new Definition(
                                tableId,
                                table.name(),
                                index.primary() ? 0 : ++secondaryId,
                                index.name(),
                                index.column(),
                                type);
                (index.primary() ? primaries : secondaries).add(definition);
            }
        }
        // Keep the historical HyBench manifest order: all PKs, then source-ordered secondaries.
        result.addAll(primaries);
        var byName = new HashMap<String, Definition>();
        for (Definition definition : secondaries) byName.put(definition.name(), definition);
        for (var index : schema.secondaryOrder()) result.add(byName.get(index.name()));
        for (Definition definition : result)
            if (!Set.of("int", "timestamp", "varchar", "text").contains(definition.type())
                    && !definition.type().matches("char\\([0-9]+\\)"))
                throw new IOException("Unsupported indexed column type: " + definition.type());
        return new IndexCatalog(result);
    }

    public static Map<String, String> fingerprints(Path source) throws IOException {
        return Map.of(
                "ddl_pg.sql",
                BaselineGenerator.sha256(source.resolve("conf/ddl_pg.sql")),
                "create_index_pg.sql",
                BaselineGenerator.sha256(source.resolve("conf/create_index_pg.sql")));
    }

    public static IndexCatalog fromManifest(JsonObject manifest) throws IOException {
        if (!manifest.has("source_directory")
                || !manifest.has("source_schema_sha256")
                || !manifest.has("index_definitions"))
            throw new IOException("Missing pinned source schema provenance");
        Path source = Path.of(manifest.get("source_directory").getAsString());
        IndexCatalog catalog = read(source);
        JsonObject fingerprints = manifest.getAsJsonObject("source_schema_sha256");
        for (var entry : fingerprints(source).entrySet())
            if (!fingerprints.has(entry.getKey())
                    || !entry.getValue().equals(fingerprints.get(entry.getKey()).getAsString()))
                throw new IOException("Source schema fingerprint mismatch");
        if (!new com.google.gson.Gson()
                .toJsonTree(catalog.describe())
                .equals(manifest.get("index_definitions")))
            throw new IOException("Declared index catalog differs from pinned source schema");
        return catalog;
    }

    public static int tableId(String name) throws IOException {
        int id = new ArrayList<>(PgBaselineExport.TABLES.keySet()).indexOf(name) + 1;
        if (id == 0) throw new IOException("Unsupported source table: " + name);
        return id;
    }

    public Definition named(String name) throws IOException {
        Definition definition = names.get(name);
        if (definition == null) throw new IOException("Unsupported accessed index: " + name);
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

    public Map<Integer, String> keys(String table, Group row) throws IOException {
        var keys = new LinkedHashMap<Integer, String>();
        for (Definition definition : secondary(idForTable(table))) {
            String column = definition.column();
            if (!row.getType().containsField(column))
                throw new IOException("Baseline missing indexed column: " + column);
            keys.put(
                    definition.indexId(),
                    definition.normalize(
                            row.getFieldRepetitionCount(column) == 0
                                    ? null
                                    : row.getValueToString(
                                            row.getType().getFieldIndex(column), 0)));
        }
        return keys;
    }

    public Map<Integer, String> keys(String table, JsonObject row) throws IOException {
        var keys = new LinkedHashMap<Integer, String>();
        for (Definition definition : secondary(idForTable(table))) {
            JsonElement value = row.get(definition.column());
            if (value == null)
                throw new IOException("Row change missing indexed column: " + definition.column());
            keys.put(
                    definition.indexId(),
                    definition.normalize(value.isJsonNull() ? null : value.getAsString()));
        }
        return keys;
    }

    private int idForTable(String table) throws IOException {
        for (Definition definition : definitions)
            if (definition.table().equals(table)) return definition.tableId();
        throw new IOException("Unsupported source table: " + table);
    }
}
