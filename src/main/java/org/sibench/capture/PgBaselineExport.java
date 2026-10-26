package org.sibench.capture;

import com.google.gson.Gson;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType;
import org.sibench.generation.*;
import org.sibench.schema.PgSchemaCheck;
import org.sibench.schema.SourceSchema;

import java.io.BufferedWriter;
import java.nio.file.*;
import java.security.MessageDigest;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Export committed PG values in primary-key order. {@link PgBaselineVerify} then compares every
 * Parquet value with PG.
 *
 * <p>Every table is cut into files of exactly {@link GenerationConfig#maxRows()} rows in key order
 * (the last file holds the rest), so the identities of every file and row follow from the table
 * row counts before a single row is written: file ids run over tables in schema order and files in
 * key order, row ids likewise. Files are therefore written concurrently, all from one exported PG
 * snapshot, and the result depends only on the source data, never on the thread count or on how
 * rows were fetched:
 *
 * <ul>
 *   <li>a table whose heap follows its key order is read by key range, one range per file and
 *       connection, which reads the heap sequentially;
 *   <li>any other table (YCSB inserts in hashed order) is read as one key-ordered stream, which PG
 *       sorts once, while the files cut from the stream are encoded concurrently; reading it by key
 *       range would touch the whole heap once per file.
 * </ul>
 */
public final class PgBaselineExport {
    /** Above this key/heap correlation (PG's planner statistic) key ranges read the heap in order. */
    static final double ORDERED_HEAP = 0.9;

    private PgBaselineExport() {}

    /** One output file: a table's rows {@code index * rows .. } in key order. */
    private record Part(Table table, int index, long fileId, long firstRowId, long rows) {}

    /** A table's shape, row count, and how it is read. */
    private record Table(
            SourceSchema.Table source,
            String key,
            boolean integerKey,
            MessageType schema,
            long rows,
            boolean ordered,
            List<String> boundaries) {}

    /** What a written file contributes to the manifests. */
    private record Written(Map<String, Object> file, byte[] digest) {}

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

    static int threads() {
        return Integer.getInteger(
                "sibench.baseline.threads", Math.min(16, Runtime.getRuntime().availableProcessors()));
    }

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
        PgBaselineVerify.Connections connections = () -> DriverManager.getConnection(jdbc, "postgres", "");
        var pool = Executors.newFixedThreadPool(threads());
        // Stream readers wait for encoders, so they never take a pool thread the encoders need.
        var readers = Executors.newCachedThreadPool();
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
            // Shapes, counts and file boundaries, every table concurrently.
            var planned = new ArrayList<Future<Table>>();
            for (var table : source.tables())
                planned.add(pool.submit(() -> session.run(pg -> plan(pg, table, config.maxRows()))));
            var tables = new ArrayList<Table>();
            for (var future : planned) tables.add(get(future));
            var parts = new ArrayList<Part>();
            long fileId = 1, rowId = 1;
            for (Table table : tables) {
                for (int index = 0; (long) index * config.maxRows() < table.rows(); index++) {
                    long rows = Math.min(config.maxRows(), table.rows() - (long) index * config.maxRows());
                    parts.add(new Part(table, index, fileId++, rowId, rows));
                    rowId += rows;
                }
            }
            var written = new ArrayList<Future<Written>>(Collections.nCopies(parts.size(), null));
            // Key ranges first: each part is one task with its own connection.
            for (int i = 0; i < parts.size(); i++) {
                Part part = parts.get(i);
                if (part.table().ordered())
                    written.set(i, pool.submit(() -> session.run(pg -> range(pg, part, output, config))));
            }
            // Streams: one connection reads the table, its files are encoded on the same pool.
            var streams = new ArrayList<Future<?>>();
            for (Table table : tables) {
                if (table.ordered()) continue;
                var own = new ArrayList<Integer>();
                for (int i = 0; i < parts.size(); i++) if (parts.get(i).table() == table) own.add(i);
                streams.add(stream(session, table, own.stream().map(parts::get).toList(), output, config, pool, readers, written, own));
            }
            var results = new ArrayList<Written>();
            for (var future : written) results.add(get(future));
            // A stream fails after its last part too, when PG has rows beyond the count.
            for (var future : streams) get(future);
            coordinator.commit();
            finish(output, tables, parts, results);
        } finally {
            pool.shutdownNow();
            readers.shutdownNow();
            for (Connection pg : opened)
                try {
                    pg.close();
                } catch (SQLException ignored) {
                    // The export result is already decided.
                }
        }
        Files.delete(output.resolve("INCOMPLETE"));
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

    /** A key literal; plain statements keep every value in PG's text format. */
    private static String literal(Table table, String key) {
        return table.integerKey() ? Long.toString(Long.parseLong(key)) : "'" + key.replace("'", "''") + "'";
    }

    private static String order(Table table) {
        // Same C ordering PgBaselineVerify applies to text keys.
        return quoted(table.key()) + (table.integerKey() ? "" : " COLLATE \"C\"");
    }

    private static Table plan(Connection pg, SourceSchema.Table table, long fileRows) throws Exception {
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
        var shape = new Table(table, key, integerKey, schema, count, ordered, boundaries);
        if (ordered && count > fileRows)
            // The last key of every full file, counted in key order by PG over the key index alone.
            try (Statement statement = pg.createStatement()) {
                statement.setFetchSize(8192);
                try (ResultSet rows = statement.executeQuery(
                        "SELECT k FROM (SELECT " + quoted(key) + "::text AS k, row_number() OVER (ORDER BY "
                                + order(shape) + ") AS n FROM " + quoted(name) + ") s WHERE n % " + fileRows
                                + " = 0 ORDER BY n")) {
                    while (rows.next()) boundaries.add(rows.getString(1));
                }
            }
        return shape;
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

    /** One part read by its key range: (previous boundary, own boundary]. */
    private static Written range(Connection pg, Part part, Path output, GenerationConfig config) throws Exception {
        Table table = part.table();
        var sql = new StringBuilder("SELECT * FROM ").append(quoted(table.source().name()));
        var bounds = new ArrayList<String>();
        if (part.index() > 0)
            bounds.add(order(table) + " > " + literal(table, table.boundaries().get(part.index() - 1)));
        if (part.index() < table.boundaries().size())
            bounds.add(order(table) + " <= " + literal(table, table.boundaries().get(part.index())));
        if (!bounds.isEmpty()) sql.append(" WHERE ").append(String.join(" AND ", bounds));
        sql.append(" ORDER BY ").append(order(table));
        try (Statement statement = pg.createStatement()) {
            statement.setFetchSize(8192);
            try (ResultSet rows = statement.executeQuery(sql.toString())) {
                int width = rows.getMetaData().getColumnCount();
                var writer = new PartWriter(part, output, config);
                try {
                    while (rows.next()) {
                        String[] row = new String[width];
                        for (int col = 0; col < width; col++) row[col] = rows.getString(col + 1);
                        writer.add(row);
                    }
                    return writer.finish();
                } finally {
                    writer.close();
                }
            }
        }
    }

    /**
     * A table read as one key-ordered stream. The reading task cuts it into the parts' row counts and
     * hands each full part to the pool, keeping at most a few parts of rows in memory.
     */
    private static Future<?> stream(
            Session session,
            Table table,
            List<Part> parts,
            Path output,
            GenerationConfig config,
            ExecutorService pool,
            ExecutorService readers,
            List<Future<Written>> written,
            List<Integer> positions)
            throws Exception {
        var futures = new ArrayList<CompletableFuture<Written>>();
        for (int i = 0; i < parts.size(); i++) futures.add(new CompletableFuture<>());
        for (int i = 0; i < parts.size(); i++) written.set(positions.get(i), futures.get(i));
        Future<?> reader =
                readers.submit(
                        () ->
                                session.run(
                                        pg -> {
                                            try (Statement statement = pg.createStatement()) {
                                                statement.execute("SET LOCAL work_mem = '" + sortMemory() + "'");
                                                statement.execute("SET LOCAL max_parallel_workers_per_gather = " + sortWorkers());
                                                statement.setFetchSize(8192);
                                                try (ResultSet rows = statement.executeQuery(
                                                        "SELECT * FROM "
                                                                + quoted(table.source().name()) + " ORDER BY " + order(table))) {
                                                    int width = rows.getMetaData().getColumnCount();
                                                    Semaphore inFlight = null;
                                                    for (int i = 0; i < parts.size(); i++) {
                                                        Part part = parts.get(i);
                                                        var batch = new ArrayList<String[]>((int) part.rows());
                                                        long chars = 0;
                                                        while (batch.size() < part.rows() && rows.next()) {
                                                            String[] row = new String[width];
                                                            for (int col = 0; col < width; col++) {
                                                                row[col] = rows.getString(col + 1);
                                                                if (row[col] != null) chars += row[col].length();
                                                            }
                                                            batch.add(row);
                                                        }
                                                        // Parts held for encoding: as many as half the heap holds
                                                        // at the first part's size, so memory stays bounded.
                                                        if (inFlight == null)
                                                            inFlight = new Semaphore(inFlight(batch.size(), width, chars));
                                                        inFlight.acquire();
                                                        Semaphore permits = inFlight;
                                                        var target = futures.get(i);
                                                        pool.submit(() -> {
                                                            try {
                                                                var writer = new PartWriter(part, output, config);
                                                                try {
                                                                    for (String[] row : batch) writer.add(row);
                                                                    target.complete(writer.finish());
                                                                } finally {
                                                                    writer.close();
                                                                }
                                                            } catch (Throwable error) {
                                                                target.completeExceptionally(error);
                                                            } finally {
                                                                permits.release();
                                                            }
                                                        });
                                                    }
                                                    // Rows beyond the count are a table that changed under the snapshot.
                                                    if (rows.next())
                                                        throw new IllegalStateException("More rows than counted: " + table.source().name());
                                                }
                                            }
                                            return null;
                                        }));
        // A failed reader fails every part it did not hand over.
        CompletableFuture.runAsync(() -> {
            try {
                reader.get();
            } catch (Exception error) {
                Throwable cause = error instanceof ExecutionException failed ? failed.getCause() : error;
                for (var future : futures) future.completeExceptionally(cause);
            }
        });
        return reader;
    }

    /** Parts of a stream held in memory at once: about half the heap at the measured row size. */
    static int inFlight(long rows, int width, long chars) {
        // A compact string costs its characters plus about 56 bytes of header and reference.
        long bytes = chars + rows * (16L + 56L * width);
        long fit = bytes == 0 ? threads() : Runtime.getRuntime().maxMemory() / 2 / bytes;
        return (int) Math.max(1, Math.min(threads(), fit));
    }

    /** Writes one part: its Parquet file under its final name, and its digest. */
    private static final class PartWriter implements AutoCloseable {
        private final Part part;
        private final MessageType schema;
        /** Per value column of a fetched row (after the tuple id): its Parquet type, and the key's. */
        private final PrimitiveType.PrimitiveTypeName[] types;
        private final int keyColumn;
        private final SimpleGroupFactory factory;
        private final ParquetDataFile writer;
        private final MessageDigest digest;
        private final Path path;
        private final Path output;
        private final GenerationConfig config;
        private long rows;
        private String first, last;

        PartWriter(Part part, Path output, GenerationConfig config) throws Exception {
            this.part = part;
            this.output = output;
            this.config = config;
            Table table = part.table();
            schema = table.schema();
            factory = new SimpleGroupFactory(schema);
            int columns = schema.getFieldCount() - 2;
            types = new PrimitiveType.PrimitiveTypeName[columns];
            for (int i = 0; i < columns; i++) types[i] = schema.getType(i).asPrimitiveType().getPrimitiveTypeName();
            keyColumn = schema.getFieldIndex(table.key());
            path = output.resolve(String.format(Locale.ROOT, "%020d-%s.parquet", part.fileId(), table.source().name()));
            writer = new ParquetDataFile(
                    path,
                    new TableData(table.source().name(), 0, part.firstRowId(), table.key(), schema, null),
                    config,
                    Map.of("sibench.file_id", Long.toString(part.fileId()), "sibench.source", "postgresql"));
            digest = MessageDigest.getInstance("SHA-256");
        }

        /** {@code row}: the table's columns in PG order, all as PG text. */
        void add(String[] row) throws Exception {
            if (rows == part.rows()) throw new IllegalStateException("More rows than counted: " + part.table().source().name());
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
            if (pk == null) throw new IllegalStateException("Null key in " + part.table().source().name());
            group.add("_sib_row_id", part.firstRowId() + rows);
            group.add("_sib_timestamp", 0L);
            updateDigest(digest, group, schema);
            writer.write(group);
            if (first == null) first = pk;
            last = pk;
            rows++;
            if (rows % 4096 == 0 && Files.getFileStore(output).getUsableSpace() < config.minFreeBytes())
                throw new java.io.IOException("Disk reserve reached");
        }

        Written finish() throws Exception {
            if (rows != part.rows())
                throw new IllegalStateException(
                        "Fewer rows than counted: " + part.table().source().name() + " part " + part.index());
            var footer = writer.finish();
            if (footer.getBlocks().stream().mapToLong(b -> b.getRowCount()).sum() != rows)
                throw new IllegalStateException("Footer mismatch");
            for (var block : footer.getBlocks())
                if (block.getRowCount() > config.maxRows()) throw new IllegalStateException("Row group limit");
            Object firstKey = part.table().integerKey() ? (Object) Long.parseLong(first) : first;
            Object lastKey = part.table().integerKey() ? (Object) Long.parseLong(last) : last;
            var file = new LinkedHashMap<String, Object>();
            file.put("file_id", part.fileId());
            file.put("path", path.getFileName().toString());
            file.put("table", part.table().source().name());
            file.put("key", part.table().key());
            file.put("rows", rows);
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
            return new Written(file, digest.digest());
        }

        @Override
        public void close() throws Exception {
            writer.close();
        }
    }

    /** Manifests in file order. */
    private static void finish(Path output, List<Table> tables, List<Part> parts, List<Written> results)
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
            // The table digest folds its files' row digests in file order.
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (int i = 0; i < parts.size(); i++) if (parts.get(i).table() == table) digest.update(results.get(i).digest());
            var entry = new LinkedHashMap<String, Object>();
            entry.put("table", table.source().name());
            entry.put("rows", table.rows());
            entry.put("written_sha256", HexFormat.of().formatHex(digest.digest()));
            entry.put("schema", table.schema().toString());
            manifest.add(entry);
            System.out.println(table.source().name() + " exported rows=" + table.rows() + " files="
                    + parts.stream().filter(p -> p.table() == table).count() + (table.ordered() ? " by key range" : " as one stream"));
            total += table.rows();
        }
        // Ordered maps: the manifests of one still database are identical byte for byte.
        var result = new LinkedHashMap<String, Object>();
        result.put("status", "complete");
        result.put("rows", total);
        result.put("tables", manifest);
        result.put("files", parts.size());
        Files.writeString(output.resolve("manifest.json"), json.toJson(result));
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
