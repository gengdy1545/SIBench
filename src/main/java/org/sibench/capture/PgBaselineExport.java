package org.sibench.capture;

import com.google.gson.Gson;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType;
import org.sibench.config.ConfigValues;
import org.sibench.generation.*;
import org.sibench.schema.PgSchemaCheck;
import org.sibench.schema.SourceSchema;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Export committed PG values in primary-key order. Independent value comparisons are an
 * explicit acceptance step through {@link PgBaselineVerify}.
 *
 * <p>Each table is read once in primary-key order from the exported snapshot. Tables are
 * encoded concurrently; only the shared Parquet byte policy decides file boundaries. RowIDs
 * follow schema/key order. FileIDs interleave tables deterministically, independently of worker
 * scheduling and the number of files each table eventually produces.
 */
public final class PgBaselineExport {
    /** Above this key/heap correlation (PG's planner statistic) key ranges read the heap in order. */
    static final double ORDERED_HEAP = 0.9;

    private PgBaselineExport() {}


    /** A table's shape, row count, and how it is read. */
    record Table(
            SourceSchema.Table source,
            String key,
            boolean integerKey,
            MessageType schema,
            long rows,
            boolean ordered,
            List<String> boundaries) {}

    /** What a written file contributes to the manifests. */
    record Written(Map<String, Object> file) {}

    /**
     * PG's sort memory for a key-ordered stream, set for that read's transaction only: a table
     * larger than memory is sorted on disk either way, but larger runs merge in fewer passes.
     */
    static String sortMemory() {
        return System.getProperty("sibench.baseline.sort_memory", "1GB");
    }

    /**
     * PG workers for a key-ordered stream, set for that read's transaction only (the capture
     * server runs queries serially): each scans and sorts a share of the table, and their sorted
     * runs merge in key order, so the rows are the same; a table larger than memory is read and
     * spilled by several processes at once, which the disk serves faster than one.
     */
    static int sortWorkers() {
        return Integer.getInteger("sibench.baseline.sort_workers", 8);
    }

    /** Bound one wide-row sort; read ranges do not define output file boundaries. */
    static long sortRangeBytes() {
        long bytes = Long.getLong("sibench.baseline.sort_range_bytes", 8L << 30);
        if (bytes < 1) throw new IllegalArgumentException("sort_range_bytes must be positive");
        return bytes;
    }

    static int threads() {
        return Integer.getInteger(
                "sibench.baseline.threads", Math.min(16, Runtime.getRuntime().availableProcessors()));
    }

    public static void export(String jdbc, Path output, GenerationConfig config) throws Exception {
        Map<String, Object> document = config.document();
        var source = SourceSchema.read(config.root(), document);
        TraceOutput strategy = TraceOutput.from(document);
        IndexCatalog catalog = IndexCatalog.fromSchema(source);
        var preflight = PgSchemaCheck.check(config.root(), document, jdbc, "postgres");
        // The schema check requires default column/index collations; these databases order text as C.
        boolean primaryOrder = config.benchmark().equals("ycsb")
                && Set.of("C", "POSIX").contains(
                        ConfigValues.string(ConfigValues.object(preflight, "environment"), "lc_collate"));
        Files.createDirectory(output);
        Files.writeString(
                output.resolve("schema-preflight.json"),
                PgSchemaCheck.json(preflight),
                StandardOpenOption.CREATE_NEW);
        Files.writeString(
                output.resolve("INCOMPLETE"),
                "PG baseline export in progress\n",
                StandardOpenOption.CREATE_NEW);
        PgBaselineVerify.Connections connections = () -> DriverManager.getConnection(jdbc, "postgres", "");
        var pool = Executors.newFixedThreadPool(threads());
        var opened = new ConcurrentLinkedQueue<Connection>();
        try (Connection coordinator = connections.open()) {
            coordinator.setAutoCommit(false);
            coordinator.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            coordinator.setReadOnly(true);
            String snapshot;
            try (Statement statement = coordinator.createStatement();
                    ResultSet rows = statement.executeQuery("SELECT pg_export_snapshot()")) {
                rows.next();
                snapshot = rows.getString(1);
            }
            var session = new Session(connections, snapshot, opened);
            var planned = new ArrayList<Future<Table>>();
            for (var table : source.tables())
                planned.add(pool.submit(() -> session.run(pg -> {
                    // YCSB's hashed keys otherwise make each range rescan the whole wide heap.
                    if (config.benchmark().equals("ycsb")) {
                        if (primaryOrder) primarySettings(pg);
                        else sortSettings(pg);
                    }
                    return plan(pg, table, config.benchmark().equals("ycsb")
                            ? Long.MAX_VALUE : sortRangeBytes());
                })));
            var tables = new ArrayList<Table>();
            for (var future : planned) {
                Table table = get(future);
                tables.add(strategy == TraceOutput.PROTO ? indexColumns(table, catalog) : table);
            }
            var actualRows = new LinkedHashMap<String, Long>();
            for (Table table : tables) actualRows.put(table.source().name(), table.rows());
            requireRows(config, actualRows);
            var written = new ArrayList<Future<List<Written>>>();
            long nextRowId = 1;
            for (int i = 0; i < tables.size(); i++) {
                Table table = tables.get(i);
                int tableIndex = i;
                long firstRowId = nextRowId;
                nextRowId = Math.addExact(nextRowId, table.rows());
                written.add(pool.submit(() -> session.run(pg -> stream(
                        pg, table, tableIndex, tables.size(), firstRowId, output, config, primaryOrder, session, strategy, catalog))));
            }
            var results = new ArrayList<Written>();
            for (var future : written) results.addAll(get(future));
            coordinator.commit();
            finish(source, output, tables, results, strategy);
        } finally {
            pool.shutdownNow();
            for (Connection pg : opened)
                try {
                    pg.close();
                } catch (SQLException ignored) {
                    // The export result is already decided.
                }
        }
        Files.delete(output.resolve("INCOMPLETE"));
    }

    /** Check the native load against the same snapshot used to plan and export its rows. */
    static void requireRows(GenerationConfig config, Map<String, Long> rows) throws IOException {
        if (!config.benchmark().equals("ycsb")) return;
        var nativeConfig = ConfigValues.object(config.document(), "native");
        if (!rows.equals(Map.of(ConfigValues.string(nativeConfig, "table"),
                        ConfigValues.positive(nativeConfig, "recordcount"))))
            throw new IOException("YCSB export found a different row count in PG: " + rows);
    }

    /** Worker connections, one per thread, each inside the exported snapshot. */
    private record Session(
            PgBaselineVerify.Connections connections, String snapshot, Queue<Connection> opened) {
        private static final ThreadLocal<Map<Session, Connection>> OWN = ThreadLocal.withInitial(IdentityHashMap::new);

        <T> T run(Work<T> work) throws Exception {
            Connection pg = OWN.get().get(this);
            if (pg == null) {
                pg = connections.open();
                pg.setAutoCommit(false);
                pg.setReadOnly(true);
                pg.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
                OWN.get().put(this, pg);
                opened.add(pg);
            }
            try (Statement statement = pg.createStatement()) {
                statement.execute("SET TRANSACTION SNAPSHOT '" + snapshot + "'");
            }
            try {
                return work.apply(pg);
            } finally {
                pg.rollback();
            }
        }
    }

    @FunctionalInterface
    private interface Work<T> {
        T apply(Connection pg) throws Exception;
    }

    private static <T> T get(Future<T> future) throws Exception {
        try {
            return future.get();
        } catch (ExecutionException failed) {
            if (failed.getCause() instanceof Exception cause) throw cause;
            throw failed;
        }
    }

    private static String quoted(String identifier) {
        return "\"" + identifier + "\"";
    }

    private static String order(Table table) {
        // Same C ordering PgBaselineVerify applies to text keys.
        return quoted(table.key()) + (table.integerKey() ? "" : " COLLATE \"C\"");
    }

    private static String literal(Table table, String key) {
        return table.integerKey() ? Long.toString(Long.parseLong(key)) : "'" + key.replace("'", "''") + "'";
    }

    static Table plan(Connection pg, SourceSchema.Table table, long rangeBytes) throws Exception {
        String name = table.name(), key = SourceRow.primary(table).name();
        boolean integerKey = SourceRow.primary(table).type().equals("integer");
        MessageType schema;
        try (Statement statement = pg.createStatement();
                ResultSet rows = statement.executeQuery("SELECT * FROM " + quoted(name) + " LIMIT 0")) {
            schema = schema(name, rows.getMetaData());
        }
        long count;
        try (Statement statement = pg.createStatement();
                ResultSet rows = statement.executeQuery("SELECT count(*) FROM " + quoted(name))) {
            rows.next();
            count = rows.getLong(1);
        }
        boolean ordered;
        try (PreparedStatement statement =
                pg.prepareStatement(
                        "SELECT correlation FROM pg_stats WHERE schemaname = current_schema()"
                                + " AND tablename = ? AND attname = ?")) {
            statement.setString(1, name);
            statement.setString(2, key);
            try (ResultSet rows = statement.executeQuery()) {
                ordered = rows.next() && rows.getObject(1) != null && Math.abs(rows.getDouble(1)) >= ORDERED_HEAP;
            }
        }
        var boundaries = new ArrayList<String>();
        var result = new Table(table, key, integerKey, schema, count, ordered, boundaries);
        if (!ordered && count > 0) {
            long bytes;
            try (PreparedStatement statement = pg.prepareStatement("SELECT pg_table_size(CAST(? AS regclass))")) {
                statement.setString(1, quoted(name));
                try (ResultSet rows = statement.executeQuery()) { rows.next(); bytes = rows.getLong(1); }
            }
            long ranges = Math.max(1, (bytes - 1) / rangeBytes + 1);
            long rangeRows = (count - 1) / ranges + 1;
            if (rangeRows < count)
                // Sort only keys to find exact, disjoint ranges, even with a non-C database collation.
                try (Statement statement = pg.createStatement();
                        ResultSet rows = statement.executeQuery(
                                "SELECT k FROM (SELECT " + quoted(key) + "::text AS k, row_number() OVER (ORDER BY "
                                        + order(result) + ") AS n FROM " + quoted(name) + ") s WHERE n % "
                                        + rangeRows + " = 0 AND n < " + count + " ORDER BY n")) {
                    while (rows.next()) boundaries.add(rows.getString(1));
                }
        }
        return result;
    }

    private static MessageType schema(String name, ResultSetMetaData columns) throws SQLException {
        StringBuilder text = new StringBuilder("message " + name + " { ");
        for (int col = 1; col <= columns.getColumnCount(); col++) {
            int type = columns.getColumnType(col);
            boolean integer = type == Types.INTEGER || type == Types.BIGINT || type == Types.SMALLINT;
            boolean real = type == Types.REAL || type == Types.FLOAT || type == Types.DOUBLE;
            text.append("optional ")
                    .append(integer ? "int64 " : real ? "double " : "binary ")
                    .append(columns.getColumnName(col))
                    .append(integer || real ? "; " : " (UTF8); ");
        }
        text.append("required int64 _sib_row_id; required int64 _sib_timestamp; }");
        return MessageTypeParser.parseMessageType(text.toString());
    }

    private static List<Written> stream(
            Connection pg, Table table, int tableIndex, int tableCount, long firstRowId,
            Path output, GenerationConfig config, boolean primaryOrder, Session session,
            TraceOutput strategy, IndexCatalog catalog) throws Exception {
        if (config.benchmark().equals("ycsb")) {
            String query = "SELECT " + columns(table) + " FROM " + quoted(table.source().name()) + " ORDER BY "
                    + (primaryOrder ? quoted(table.key()) : order(table));
            if (primaryOrder) {
                primarySettings(pg);
                int readers;
                try (Statement statement = pg.createStatement();
                        ResultSet result = statement.executeQuery("SHOW max_connections")) {
                    result.next();
                    // Leave control and health-check connections available in the capture service.
                    readers = Math.max(1, Math.min(result.getInt(1) - 8,
                            (int) Math.min(64L, Runtime.getRuntime().availableProcessors() * 2L)));
                }
                try (var input = new PrefetchRows(session, table, readers)) {
                    RunTiming.current().note("baseline_" + table.source().name() + "_prefetch_readers", readers);
                    return write(table, tableIndex, tableCount, firstRowId, input, output, config, strategy, catalog);
                }
            } else sortSettings(pg);
            // A JDBC cursor's nonzero fetch count disables PG parallel workers at execution.
            // The schema preflight guarantees that every YCSB column is varchar/text.
            try (var copy = new org.postgresql.copy.PGCopyInputStream(
                    pg.unwrap(org.postgresql.PGConnection.class),
                    "COPY (" + query + ") TO STDOUT (FORMAT binary, ENCODING 'UTF8')")) {
                var input = new PgTextCopyRows(copy, table.schema().getFieldCount() - 2);
                return write(table, tableIndex, tableCount, firstRowId, input, output, config, strategy, catalog);
            }
        }
        if (!table.ordered()) sortSettings(pg);
        try (Statement statement = pg.createStatement()) {
            statement.setFetchSize(8192);
            try (var input = new RangeRows(statement, table)) {
                return write(table, tableIndex, tableCount, firstRowId, input::next, output, config, strategy, catalog);
            }
        }
    }

    static Table indexColumns(Table table, IndexCatalog catalog) {
        var indexed = catalog.definitions().stream().filter(d -> d.table().equals(table.source().name()))
                .map(IndexCatalog.Definition::column).collect(java.util.stream.Collectors.toSet());
        var fields = table.schema().getFields().stream()
                .filter(f -> indexed.contains(f.getName()) || f.getName().startsWith("_sib_")).toList();
        return new Table(table.source(), table.key(), table.integerKey(),
                new MessageType(table.schema().getName(), fields), table.rows(), table.ordered(), table.boundaries());
    }

    private static String columns(Table table) {
        return table.schema().getFields().stream().filter(f -> !f.getName().startsWith("_sib_"))
                .map(f -> quoted(f.getName())).collect(java.util.stream.Collectors.joining(", "));
    }

    private static List<Written> write(Table table, int tableIndex, int tableCount, long firstRowId,
            Rows input, Path output, GenerationConfig config, TraceOutput strategy, IndexCatalog catalog) throws Exception {
        return strategy == TraceOutput.PROTO
                ? writeIndexTable(table, tableIndex, firstRowId, input, output, config, catalog)
                : writeTable(table, tableIndex, tableCount, firstRowId, input, output, config);
    }

    static List<Written> writeIndexTable(Table table, int tableIndex, long firstRowId, Rows input,
            Path output, GenerationConfig config, IndexCatalog catalog) throws Exception {
        Path file = output.resolve(table.source().name() + ".pb.zst");
        long rows = 0;
        String first = null, last = null;
        var writer = new ProtoTrace.Writer(file);
        int tableId = catalog.tableId(table.source().name());
        var primary = catalog.primary(tableId);
        org.sibench.trace.IndexKey previous = null;
        try (writer) {
            String[] row;
            while ((row = input.next()) != null) {
                if (rows >= table.rows() || row.length != table.schema().getFieldCount() - 2)
                    throw new IOException("Index baseline row coverage or shape mismatch");
                var value = new com.google.gson.JsonObject();
                for (int i = 0; i < row.length; i++) value.addProperty(table.schema().getFieldName(i), row[i]);
                var key = catalog.primaryKey(table.source().name(), value);
                if (previous != null && previous.compareTo(key) >= 0)
                    throw new IOException("Index baseline primary keys are not strictly ordered");
                previous = key;
                var operation = IndexOperationTrace.write(0, IndexOperationTrace.INSERT, tableId, 0,
                        key, 0, 0, 0, Math.addExact(firstRowId, rows))
                        .withWorkUnitId(1 + (firstRowId + rows - 1) / IndexOperationTrace.BLOCK_RECORDS);
                IndexMutation.emit(operation, Map.of(), catalog.keys(table.source().name(), value), writer);
                String text = value.get(primary.column()).getAsString();
                if (first == null) first = text;
                last = text;
                rows++;
                if (rows % 4096 == 0 && Files.getFileStore(output).getUsableSpace() < config.minFreeBytes())
                    throw new IOException("Disk reserve reached");
            }
            if (rows != table.rows()) throw new IOException("Index baseline row coverage mismatch");
        }
        FileDigests.written(file, writer.sha256());
        var result = new LinkedHashMap<String, Object>();
        result.put("file_id", tableIndex + 1L);
        result.put("path", file.getFileName().toString());
        result.put("table", table.source().name());
        result.put("key", table.key());
        result.put("rows", rows);
        result.put("operations", writer.count());
        result.put("logical_sha256", writer.logicalSha256());
        result.put("size_bytes", Files.size(file));
        result.put("first_key", first);
        result.put("last_key", last);
        return List.of(new Written(result));
    }

    private static void sortSettings(Connection pg) throws SQLException {
        try (Statement statement = pg.createStatement()) {
            statement.execute("SET LOCAL work_mem = '" + sortMemory() + "'");
            statement.execute("SET LOCAL max_parallel_workers_per_gather = " + sortWorkers());
        }
    }

    private static void primarySettings(Connection pg) throws SQLException {
        try (Statement statement = pg.createStatement()) {
            statement.execute("SET LOCAL work_mem = '" + sortMemory() + "'");
            statement.execute("SET LOCAL max_parallel_workers_per_gather = 0");
            statement.execute("SET LOCAL enable_indexscan = on");
            statement.execute("SET LOCAL enable_indexonlyscan = on");
            statement.execute("SET LOCAL enable_seqscan = off");
            statement.execute("SET LOCAL enable_bitmapscan = off");
            statement.execute("SET LOCAL enable_sort = off");
            statement.execute("SET LOCAL temp_file_limit = 0");
        }
    }

    static void requirePrimaryPlan(com.google.gson.JsonObject plan, String index) throws IOException {
        requirePrimaryPlan(plan, index, false);
    }

    static void requirePrimaryPlan(com.google.gson.JsonObject plan, String index, boolean indexOnly) throws IOException {
        if (!plan.has("Node Type") || !(plan.get("Node Type").getAsString().equals("Index Scan")
                        || indexOnly && plan.get("Node Type").getAsString().equals("Index Only Scan"))
                || !plan.has("Index Name") || !plan.get("Index Name").getAsString().equals(index))
            throw new IOException("YCSB baseline export requires an ordered primary index scan: " + plan);
    }

    static int prefetchCost(long bytes, int budget) throws IOException {
        if (bytes > budget)
            throw new IOException("YCSB row exceeds the per-reader prefetch byte budget");
        return (int) bytes;
    }

    /** Disjoint key ranges hide random-read latency; only the writer consumes them in key order. */
    private static final class PrefetchRows implements Rows, AutoCloseable {
        private static final int WINDOW_ROWS = 65_536;
        private static final int BUFFER_BYTES = 128 << 20;
        private static final Object END = new Object();
        private record Slot(String[] row, int bytes) {}
        private record Window(List<Range> ranges, String after) {}

        private final Session session;
        private final Table table;
        private final int readers;
        private final ExecutorService pool;
        private List<Range> ranges = List.of();
        private Future<Window> ahead;
        private int range;
        private Slot held;
        private Range heldRange;

        PrefetchRows(Session session, Table table, int readers) {
            this.session = session;
            this.table = table;
            this.readers = readers;
            pool = Executors.newFixedThreadPool(readers);
        }

        private Window window(String after) throws Exception {
            return session.run(pg -> {
                primarySettings(pg);
                return planWindow(pg, after);
            });
        }

        private Window planWindow(Connection pg, String after) throws Exception {
            var keys = new ArrayList<String>();
            String key = quoted(table.key());
            try (Statement statement = pg.createStatement()) {
                statement.setFetchSize(8192);
                try (ResultSet rows = statement.executeQuery("SELECT " + key + " FROM " + quoted(table.source().name())
                        + (after == null ? "" : " WHERE " + key + " > " + literal(table, after))
                        + " ORDER BY " + key + " LIMIT " + WINDOW_ROWS)) {
                    while (rows.next()) keys.add(rows.getString(1));
                }
            }
            if (keys.isEmpty()) return new Window(List.of(), after);
            var ownRanges = new ArrayList<Range>();
            int workers = Math.min(readers, keys.size());
            String lower = after;
            for (int i = 0; i < workers; i++) {
                int start = keys.size() * i / workers, end = keys.size() * (i + 1) / workers;
                var own = new Range(lower, keys.get(end - 1), end - start, BUFFER_BYTES / 2 / workers);
                ownRanges.add(own);
                lower = keys.get(end - 1);
                pool.submit(own::read);
            }
            return new Window(ownRanges, keys.get(keys.size() - 1));
        }

        @Override
        public String[] next() throws Exception {
            release();
            while (true) {
                if (range == ranges.size()) {
                    if (ahead == null) ahead = pool.submit(() -> window(null));
                    Window next = get(ahead);
                    if (next.ranges().isEmpty()) return null;
                    ranges = next.ranges();
                    range = 0;
                    // One window encodes while the next reads; their byte credits share the bound.
                    ahead = pool.submit(() -> window(next.after()));
                }
                Range own = ranges.get(range);
                Object item = own.take();
                if (item == END) {
                    range++;
                    continue;
                }
                held = (Slot) item;
                heldRange = own;
                return held.row();
            }
        }

        private void release() {
            if (held != null) {
                heldRange.credit.release(held.bytes());
                held = null;
            }
        }

        private final class Range {
            private final String lower, upper;
            private final int budget;
            private final Semaphore credit;
            private final BlockingQueue<Object> ahead;
            private volatile Throwable failed;

            Range(String lower, String upper, int rows, int budget) {
                this.lower = lower;
                this.upper = upper;
                this.budget = budget;
                credit = new Semaphore(budget);
                ahead = new ArrayBlockingQueue<>(rows + 1);
            }

            void read() {
                try {
                    session.run(pg -> {
                        primarySettings(pg);
                        String key = quoted(table.key());
                        String query = "SELECT " + columns(table) + " FROM " + quoted(table.source().name()) + " WHERE "
                                + (lower == null ? "" : key + " > " + literal(table, lower) + " AND ")
                                + key + " <= " + literal(table, upper) + " ORDER BY " + key;
                        String index = table.source().indexes().stream()
                                .filter(SourceSchema.Index::primary).findFirst().orElseThrow().name();
                        try (Statement statement = pg.createStatement();
                                ResultSet result = statement.executeQuery("EXPLAIN (FORMAT JSON) " + query)) {
                            result.next();
                            var plan = com.google.gson.JsonParser.parseString(result.getString(1))
                                    .getAsJsonArray().get(0).getAsJsonObject().getAsJsonObject("Plan");
                            requirePrimaryPlan(plan, index, table.schema().getFieldCount() == 3);
                        }
                        try (var copy = new org.postgresql.copy.PGCopyInputStream(
                                pg.unwrap(org.postgresql.PGConnection.class),
                                "COPY (" + query + ") TO STDOUT (FORMAT binary, ENCODING 'UTF8')")) {
                            var rows = new PgTextCopyRows(copy, table.schema().getFieldCount() - 2);
                            String[] row;
                            while ((row = rows.next()) != null) {
                                int cost = prefetchCost(rows.rowBytes(), budget);
                                credit.acquire(cost);
                                boolean queued = false;
                                try {
                                    ahead.put(new Slot(row, cost));
                                    queued = true;
                                } finally {
                                    if (!queued) credit.release(cost);
                                }
                            }
                        }
                        return null;
                    });
                    ahead.put(END);
                } catch (Throwable error) {
                    failed = error;
                    ahead.offer(END);
                }
            }

            Object take() throws Exception {
                check();
                Object item = ahead.take();
                check();
                return item;
            }

            private void check() throws Exception {
                if (failed instanceof Exception error) throw error;
                if (failed instanceof Error error) throw error;
                if (failed != null) throw new IOException(failed);
            }
        }

        @Override
        public void close() {
            release();
            pool.shutdownNow();
            ranges = List.of();
        }
    }

    /** All ranges share the caller's snapshot and the table's single encoder. */
    private static final class RangeRows implements AutoCloseable {
        private final Statement statement;
        private final Table table;
        private ResultSet rows;
        private int range;

        RangeRows(Statement statement, Table table) { this.statement = statement; this.table = table; }

        String[] next() throws SQLException {
            while (true) {
                if (rows != null && rows.next()) {
                    String[] row = new String[table.schema().getFieldCount() - 2];
                    for (int col = 0; col < row.length; col++) row[col] = rows.getString(col + 1);
                    return row;
                }
                close();
                if (range > table.boundaries().size()) return null;
                var bounds = new ArrayList<String>();
                if (range > 0) bounds.add(order(table) + " > " + literal(table, table.boundaries().get(range - 1)));
                if (range < table.boundaries().size()) bounds.add(order(table) + " <= " + literal(table, table.boundaries().get(range)));
                rows = statement.executeQuery("SELECT " + columns(table) + " FROM " + quoted(table.source().name())
                        + (bounds.isEmpty() ? "" : " WHERE " + String.join(" AND ", bounds))
                        + " ORDER BY " + order(table));
                range++;
            }
        }

        public void close() throws SQLException { if (rows != null) { rows.close(); rows = null; } }
    }

    @FunctionalInterface
    interface Rows {
        String[] next() throws Exception;
    }

    /** One semantic table stream: row groups and worker tasks never create extra tail files. */
    static List<Written> writeTable(
            Table table, int tableIndex, int tableCount, long firstRowId, Rows input,
            Path output, GenerationConfig config) throws Exception {
        var written = new ArrayList<Written>();
        PartWriter writer = null;
        long total = 0;
        try {
            String[] row;
            while ((row = input.next()) != null) {
                if (total >= table.rows())
                    throw new IllegalStateException("More rows than counted: " + table.source().name());
                if (writer == null) {
                    long fileId = Math.addExact(tableIndex + 1L, Math.multiplyExact((long) written.size(), tableCount));
                    writer = new PartWriter(table, fileId, firstRowId + total, output, config);
                }
                boolean full = writer.add(row);
                total++;
                if (full) {
                    written.add(writer.finish());
                    writer.close();
                    writer = null;
                }
            }
            if (total != table.rows())
                throw new IllegalStateException("Fewer rows than counted: " + table.source().name());
            if (writer != null) written.add(writer.finish());
            return written;
        } finally {
            if (writer != null) writer.close();
        }
    }

    /** Writes one byte-sized file before the directory is published. */
    private static final class PartWriter implements AutoCloseable {
        private final Table table;
        private final long fileId, firstRowId;
        private final MessageType schema;
        /** Per value column of a fetched row (after the tuple id): its Parquet type, and the key's. */
        private final PrimitiveType.PrimitiveTypeName[] types;
        private final int keyColumn;
        private final SimpleGroupFactory factory;
        private final ParquetDataFile writer;
        private final Path path;
        private final Path output;
        private final GenerationConfig config;
        private long rows;
        private String first, last;

        PartWriter(Table table, long fileId, long firstRowId, Path output, GenerationConfig config) throws Exception {
            this.table = table;
            this.fileId = fileId;
            this.firstRowId = firstRowId;
            this.output = output;
            this.config = config;
            schema = table.schema();
            factory = new SimpleGroupFactory(schema);
            int columns = schema.getFieldCount() - 2;
            types = new PrimitiveType.PrimitiveTypeName[columns];
            for (int i = 0; i < columns; i++) types[i] = schema.getType(i).asPrimitiveType().getPrimitiveTypeName();
            keyColumn = schema.getFieldIndex(table.key());
            path = output.resolve(String.format(Locale.ROOT, "%020d-%s.parquet", fileId, table.source().name()));
            writer = new ParquetDataFile(
                    path,
                    new TableData(table.source().name(), 0, firstRowId, table.key(), schema, null),
                    config,
                    Map.of("sibench.file_id", Long.toString(fileId), "sibench.source", "postgresql"));
        }

        /** {@code row}: the table's columns in PG order, all as PG text. */
        boolean add(String[] row) throws Exception {
            if (row.length != types.length) throw new IllegalStateException("Row shape changed");
            Group group = factory.newGroup();
            for (int i = 0; i < types.length; i++) {
                String value = row[i];
                if (value == null) continue;
                switch (types[i]) {
                    case INT64 -> group.add(i, Long.parseLong(value));
                    case DOUBLE -> group.add(i, Double.parseDouble(value));
                    default -> group.add(i, value);
                }
            }
            String pk = row[keyColumn];
            if (pk == null) throw new IllegalStateException("Null key in " + table.source().name());
            group.add("_sib_row_id", firstRowId + rows);
            group.add("_sib_timestamp", 0L);
            boolean full = writer.write(group);
            if (first == null) first = pk;
            last = pk;
            rows++;
            if (rows % 4096 == 0 && Files.getFileStore(output).getUsableSpace() < config.minFreeBytes())
                throw new java.io.IOException("Disk reserve reached");
            return full;
        }

        Written finish() throws Exception {
            var footer = writer.finish();
            if (footer.getBlocks().stream().mapToLong(b -> b.getRowCount()).sum() != rows)
                throw new IllegalStateException("Footer mismatch");
            for (var block : footer.getBlocks())
                if (block.getRowCount() > config.maxRows()) throw new IllegalStateException("Row group limit");
            Object firstKey = table.integerKey() ? (Object) Long.parseLong(first) : first;
            Object lastKey = table.integerKey() ? (Object) Long.parseLong(last) : last;
            var file = new LinkedHashMap<String, Object>();
            file.put("file_id", fileId);
            file.put("path", path.getFileName().toString());
            file.put("table", table.source().name());
            file.put("key", table.key());
            file.put("rows", rows);
            file.put("size_bytes", Files.size(path));
            file.put("file_size", writer.sizing());
            file.put("first_key", firstKey);
            file.put("last_key", lastKey);
            file.put(
                    "row_groups",
                    footer.getBlocks().stream()
                            .map(b -> {
                                var group = new LinkedHashMap<String, Object>();
                                group.put("rows", b.getRowCount());
                                group.put("compressed_bytes", b.getCompressedSize());
                                return group;
                            })
                            .toList());
            return new Written(file);
        }

        @Override
        public void close() throws Exception {
            writer.close();
        }
    }

    /** Manifests in file order. */
    private static void finish(SourceSchema source, Path output, List<Table> tables, List<Written> results, TraceOutput strategy)
            throws Exception {
        Gson json = new Gson();
        try (BufferedWriter files = Files.newBufferedWriter(output.resolve("files.jsonl"))) {
            for (Written result : results) {
                files.write(json.toJson(result.file()));
                files.newLine();
            }
        }
        var manifest = new ArrayList<Map<String, Object>>();
        long total = 0;
        for (Table table : tables) {
            var entry = new LinkedHashMap<String, Object>();
            entry.put("table", table.source().name());
            entry.put("rows", table.rows());
            entry.put("schema", table.schema().toString());
            manifest.add(entry);
            System.out.println(table.source().name() + " exported rows=" + table.rows() + " files="
                    + results.stream().filter(w -> w.file().get("table").equals(table.source().name())).count() + " as one key-ordered stream");
            total += table.rows();
        }
        // Preserve semantic table/file order; nested JSON object key order is immaterial.
        var result = new LinkedHashMap<String, Object>();
        result.put("status", "complete");
        result.put("rows", total);
        result.put("tables", manifest);
        result.put("files", results.size());
        result.put("file_size_policy", strategy == TraceOutput.PROTO
                ? "compressed_protobuf_blocks" : GenerationConfig.FILE_SIZE_POLICY);
        result.put("target_file_size_bytes", strategy == TraceOutput.PROTO || results.isEmpty()
                ? null : ((Map<?, ?>) results.get(0).file().get("file_size")).get("target_bytes"));
        Files.writeString(output.resolve("manifest.json"), json.toJson(result));
        var expected = new LinkedHashMap<String, Long>();
        for (Table table : tables) expected.put(table.source().name(), table.rows());
        BaselineManifest.publish(source, output, expected, strategy);
    }

}
