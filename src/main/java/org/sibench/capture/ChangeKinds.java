package org.sibench.capture;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.sibench.schema.SourceSchema;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Which index-maintenance paths a native run actually exercised, counted from the row changes of a
 * raw capture. Insert / update / delete totals alone do not say whether an UPDATE touched a
 * secondary-index column, an ordinary column, or whether a DELETE was followed by a re-insertion of
 * the same key; those three are the maintenance paths the pinned sysbench scripts distinguish
 * ({@code k} is the only secondary index column, {@code c} and {@code pad} are ordinary) and the
 * paths YCSB never reaches, because its only index is the primary key.
 *
 * <p>Pure over the change events: it reads the same {@code row_change} and transaction-boundary
 * events {@link RowStateAudit} reads, keeps the source order of one session and accounts only for
 * committed changes, because only committed modifications are published. Aborted changes are
 * discarded, like everywhere else in the capture.
 */
public final class ChangeKinds {
    private ChangeKinds() {}

    /**
     * @param inserts committed INSERTs, including the second half of a delete-then-reinsert
     * @param deletes committed DELETEs that were not followed by an INSERT of the same key
     * @param deleteThenReinsert DELETEs that a later INSERT of the same primary key paired with
     * @param indexColumnUpdates UPDATEs that changed the value of at least one secondary index column
     * @param ordinaryColumnUpdates UPDATEs that changed no indexed column value
     */
    public record Counts(
            long inserts,
            long deletes,
            long deleteThenReinsert,
            long indexColumnUpdates,
            long ordinaryColumnUpdates) {
        public static final Counts NONE = new Counts(0, 0, 0, 0, 0);

        public Map<String, Object> describe() {
            var result = new LinkedHashMap<String, Object>();
            result.put("inserts", inserts);
            result.put("deletes", deletes);
            result.put("delete_then_reinsert", deleteThenReinsert);
            result.put("index_column_updates", indexColumnUpdates);
            result.put("ordinary_column_updates", ordinaryColumnUpdates);
            return result;
        }
    }

    /** @param byTable the same counts per source table, in source table order */
    public record Report(Counts total, Map<String, Counts> byTable) {
        public Map<String, Object> describe() {
            var tables = new LinkedHashMap<String, Object>();
            for (var entry : byTable.entrySet())
                tables.put(entry.getKey(), entry.getValue().describe());
            var result = new LinkedHashMap<String, Object>();
            result.put("total", total.describe());
            result.put("by_table", tables);
            return result;
        }
    }

    /** Counts the events of one session, in the order the source produced them. */
    public static Report count(SourceSchema schema, List<JsonObject> events) throws IOException {
        Objects.requireNonNull(schema, "schema");
        var counter = new Counter(schema);
        for (JsonObject event : events) counter.accept(event);
        return counter.report();
    }

    /** Counts the raw server traces of one workload, in file order. */
    public static Report countTraces(SourceSchema schema, List<Path> traces) throws IOException {
        Objects.requireNonNull(schema, "schema");
        var counter = new Counter(schema);
        for (Path trace : traces) {
            try (var lines = Files.newBufferedReader(trace)) {
                String line;
                while ((line = lines.readLine()) != null)
                    counter.accept(JsonParser.parseString(line).getAsJsonObject());
            }
        }
        return counter.report();
    }

    private record TableKey(String table, String key) {}

    private static final class Counter {
        private final Map<String, SourceSchema.Table> tables = new LinkedHashMap<>();
        private final Map<String, Set<String>> indexed = new LinkedHashMap<>();
        private final Map<String, Tally> byTable = new LinkedHashMap<>();
        private final Map<TableKey, Integer> deleted = new HashMap<>();
        private final List<JsonObject> pending = new ArrayList<>();

        Counter(SourceSchema schema) throws IOException {
            var catalog = IndexCatalog.fromSchema(schema);
            for (var table : schema.tables()) {
                tables.put(table.name(), table);
                var columns = new LinkedHashSet<String>();
                for (var definition : catalog.secondary(catalog.tableId(table.name())))
                    columns.add(definition.column());
                indexed.put(table.name(), Set.copyOf(columns));
                byTable.put(table.name(), new Tally());
            }
        }

        void accept(JsonObject event) throws IOException {
            String type = event.get("event").getAsString();
            switch (type) {
                case "row_change" -> pending.add(event);
                case "transaction_commit" -> {
                    for (JsonObject change : pending) apply(change);
                    pending.clear();
                }
                case "transaction_abort" -> pending.clear();
                default -> {}
            }
        }

        Report report() throws IOException {
            if (!pending.isEmpty()) throw new IOException("Unfinished source row changes");
            var totals = new Tally();
            var perTable = new LinkedHashMap<String, Counts>();
            for (var entry : byTable.entrySet()) {
                perTable.put(entry.getKey(), entry.getValue().counts());
                totals.add(entry.getValue());
            }
            return new Report(totals.counts(), perTable);
        }

        private void apply(JsonObject change) throws IOException {
            String relation = change.get("relation").getAsString();
            var table = tables.get(relation);
            if (table == null) throw new IOException("Unknown source row table: " + relation);
            Tally tally = byTable.get(relation);
            JsonObject before = SourceRow.image(change, "old");
            JsonObject after = SourceRow.image(change, "new");
            String operation = change.get("operation").getAsString();
            switch (operation) {
                case "insert" -> {
                    tally.inserts++;
                    var open = new TableKey(relation, key(table, after));
                    Integer awaiting = deleted.get(open);
                    if (awaiting != null && awaiting > 0) {
                        tally.reinserted++;
                        if (awaiting == 1) deleted.remove(open);
                        else deleted.put(open, awaiting - 1);
                    }
                }
                case "update" -> {
                    String beforeKey = key(table, before), afterKey = key(table, after);
                    if (!beforeKey.equals(afterKey))
                        throw new IOException(
                                "Primary key value UPDATE is outside the standard workload: "
                                        + relation
                                        + "."
                                        + SourceRow.primary(table).name());
                    Map<String, String> oldValues = SourceRow.values(table, before);
                    Map<String, String> newValues = SourceRow.values(table, after);
                    boolean indexedChange = false;
                    for (String column : indexed.get(relation))
                        if (!Objects.equals(oldValues.get(column), newValues.get(column)))
                            indexedChange = true;
                    if (indexedChange) tally.indexColumnUpdates++;
                    else tally.ordinaryColumnUpdates++;
                }
                case "delete" -> {
                    tally.deleteEvents++;
                    deleted.merge(new TableKey(relation, key(table, before)), 1, Integer::sum);
                }
                default ->
                        throw new IOException("Unsupported source row change: " + operation);
            }
        }

        private static String key(SourceSchema.Table table, JsonObject row) throws IOException {
            if (row == null) throw new IOException("Row change lacks a row image");
            return SourceRow.key(table, row);
        }
    }

    private static final class Tally {
        private long inserts;
        private long deleteEvents;
        private long reinserted;
        private long indexColumnUpdates;
        private long ordinaryColumnUpdates;

        void add(Tally other) {
            inserts += other.inserts;
            deleteEvents += other.deleteEvents;
            reinserted += other.reinserted;
            indexColumnUpdates += other.indexColumnUpdates;
            ordinaryColumnUpdates += other.ordinaryColumnUpdates;
        }

        Counts counts() {
            return new Counts(
                    inserts,
                    deleteEvents - reinserted,
                    reinserted,
                    indexColumnUpdates,
                    ordinaryColumnUpdates);
        }
    }
}
