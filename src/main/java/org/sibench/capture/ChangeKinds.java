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

    /**
     * Counts the raw server traces of one workload, in file order. The traces are read concurrently,
     * a bounded number ahead, keeping only the lines {@link Counter#accept} acts on: a line whose
     * first field names another event type is passed over without being parsed, as accept ignores
     * it. The kept lines are applied in file order, so the counts are those of reading every line.
     * (Parsing every line of every trace on one thread held up each workload of a native run.)
     */
    public static Report countTraces(SourceSchema schema, List<Path> traces) throws IOException {
        Objects.requireNonNull(schema, "schema");
        var counter = new Counter(schema);
        int threads = Math.max(1, Math.min(traces.size(), Runtime.getRuntime().availableProcessors()));
        var pool = java.util.concurrent.Executors.newFixedThreadPool(threads, task -> {
            var thread = new Thread(task, "change-kinds");
            thread.setDaemon(true);
            return thread;
        });
        var window = new java.util.concurrent.Semaphore(threads * 2);
        try {
            var reads = new ArrayList<java.util.concurrent.Future<List<Item>>>();
            for (Path trace : traces)
                reads.add(pool.submit(() -> {
                    window.acquire();
                    return items(counter, trace);
                }));
            for (var read : reads) {
                List<Item> items;
                try {
                    items = read.get();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while counting change kinds", interrupted);
                } catch (java.util.concurrent.ExecutionException failed) {
                    if (failed.getCause() instanceof IOException io) throw io;
                    if (failed.getCause() instanceof RuntimeException runtime) throw runtime;
                    throw new IOException(failed.getCause());
                }
                window.release();
                for (Item item : items) counter.accept(item);
            }
        } finally {
            pool.shutdownNow();
        }
        return counter.report();
    }

    private static final String EVENT = "{\"event\":\"";

    /**
     * The trace's events the counter acts on, in order and already classified, without the lines
     * that name an event type it ignores; runs on the reading threads.
     */
    private static List<Item> items(Counter counter, Path trace) throws IOException {
        var kept = new ArrayList<Item>();
        try (var lines = ServerTrace.open(trace)) {
            String line;
            while ((line = lines.readLine()) != null) {
                if (line.startsWith(EVENT)
                        && !line.startsWith("row_change\"", EVENT.length())
                        && !line.startsWith("transaction_commit\"", EVENT.length())
                        && !line.startsWith("transaction_abort\"", EVENT.length()))
                    continue;
                Item item = counter.item(JsonParser.parseString(line).getAsJsonObject());
                if (item != null) kept.add(item);
            }
        }
        return kept;
    }

    /**
     * One event the counter acts on: a committed-or-not row change as its kind of maintenance
     * (table, operation and the primary key it pairs deletes and inserts by; whether an UPDATE
     * changed an indexed column), or a transaction end.
     */
    private record Item(int kind, String table, String operation, String key, boolean indexedChange, Exception error) {
        static final int CHANGE = 0, COMMIT = 1, ABORT = 2;

        Item(int kind, String table, String operation, String key, boolean indexedChange) {
            this(kind, table, operation, key, indexedChange, null);
        }
    }

    private record TableKey(String table, String key) {}

    private static final class Counter {
        private final Map<String, SourceSchema.Table> tables = new LinkedHashMap<>();
        private final Map<String, Set<String>> indexed = new LinkedHashMap<>();
        private final Map<String, Tally> byTable = new LinkedHashMap<>();
        private final Map<TableKey, Integer> deleted = new HashMap<>();
        private final List<Item> pending = new ArrayList<>();

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
            Item item = item(event);
            if (item != null) accept(item);
        }

        /** The event as the counter acts on it, or null for an event it ignores; keeps no state. */
        Item item(JsonObject event) throws IOException {
            String type = event.get("event").getAsString();
            return switch (type) {
                case "row_change" -> {
                    // A change is checked when its transaction commits, as applying it always was.
                    try {
                        yield classify(event);
                    } catch (IOException | RuntimeException error) {
                        yield new Item(Item.CHANGE, null, null, null, false, error);
                    }
                }
                case "transaction_commit" -> new Item(Item.COMMIT, null, null, null, false);
                case "transaction_abort" -> new Item(Item.ABORT, null, null, null, false);
                default -> null;
            };
        }

        void accept(Item item) throws IOException {
            switch (item.kind()) {
                case Item.CHANGE -> pending.add(item);
                case Item.COMMIT -> {
                    for (Item change : pending) apply(change);
                    pending.clear();
                }
                default -> pending.clear();
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

        /**
         * A row change as its kind of maintenance, with the checks applying it made; a change the
         * workload cannot produce fails when its transaction commits.
         */
        private Item classify(JsonObject change) throws IOException {
            String relation = change.get("relation").getAsString();
            var table = tables.get(relation);
            if (table == null) throw new IOException("Unknown source row table: " + relation);
            JsonObject before = SourceRow.image(change, "old");
            JsonObject after = SourceRow.image(change, "new");
            String operation = change.get("operation").getAsString();
            return switch (operation) {
                case "insert" -> new Item(Item.CHANGE, relation, operation, key(table, after), false);
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
                    yield new Item(Item.CHANGE, relation, operation, beforeKey, indexedChange);
                }
                case "delete" -> new Item(Item.CHANGE, relation, operation, key(table, before), false);
                default -> throw new IOException("Unsupported source row change: " + operation);
            };
        }

        private void apply(Item change) throws IOException {
            if (change.error() instanceof IOException error) throw error;
            if (change.error() instanceof RuntimeException error) throw error;
            String relation = change.table();
            Tally tally = byTable.get(relation);
            switch (change.operation()) {
                case "insert" -> {
                    tally.inserts++;
                    var open = new TableKey(relation, change.key());
                    Integer awaiting = deleted.get(open);
                    if (awaiting != null && awaiting > 0) {
                        tally.reinserted++;
                        if (awaiting == 1) deleted.remove(open);
                        else deleted.put(open, awaiting - 1);
                    }
                }
                case "update" -> {
                    if (change.indexedChange()) tally.indexColumnUpdates++;
                    else tally.ordinaryColumnUpdates++;
                }
                default -> {
                    tally.deleteEvents++;
                    deleted.merge(new TableKey(relation, change.key()), 1, Integer::sum);
                }
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
