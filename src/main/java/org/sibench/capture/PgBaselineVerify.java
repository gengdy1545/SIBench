package org.sibench.capture;

import com.google.gson.*;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.sibench.generation.BaselineGenerator;
import org.sibench.schema.SourceSchema;

import java.io.IOException;
import java.nio.file.*;
import java.security.MessageDigest;
import java.sql.*;
import java.util.*;

/** Independently compare every baseline file with an unchanged source database. */
public final class PgBaselineVerify {
    private PgBaselineVerify() {}

    /** Row groups are verified concurrently, each against the same exported PG snapshot. */
    private static int threads() {
        return Integer.getInteger(
                "sibench.baseline.threads", Math.min(16, Runtime.getRuntime().availableProcessors()));
    }

    /** One Parquet row group and the PG rows whose primary keys fall in (after, through]. */
    private record Task(
            SourceSchema.Table table,
            JsonObject file,
            Path path,
            long start,
            long end,
            long rows,
            long firstRowId,
            Object after,
            Object through) {}

    private record Result(long rows, byte[] digest) {}

    /** Opens a fresh autocommit connection to the source database. */
    @FunctionalInterface
    interface Connections {
        Connection open() throws SQLException;
    }

    static void verify(String jdbc, String user, SourceSchema schema, Path directory)
            throws Exception {
        String password = System.getenv().getOrDefault("PGPASSWORD", "");
        verify(() -> DriverManager.getConnection(jdbc, user, password), schema, directory);
    }

    static void verify(Connections connections, SourceSchema schema, Path directory) throws Exception {
        if (Files.exists(directory.resolve("verified-manifest.json")))
            throw new IOException("Baseline verification already exists");
        List<JsonObject> files =
                Files.readAllLines(directory.resolve("files.jsonl")).stream()
                        .map(line -> JsonParser.parseString(line).getAsJsonObject())
                        .toList();
        Set<String> expectedTables =
                schema.tables().stream()
                        .map(SourceSchema.Table::name)
                        .collect(java.util.stream.Collectors.toSet());
        for (JsonObject file : files)
            if (!expectedTables.contains(file.get("table").getAsString()))
                throw new IOException("Unknown baseline manifest table");
        List<Task> tasks = tasks(schema, files, directory);
        try (Connection coordinator = connections.open()) {
            if (!coordinator.getAutoCommit())
                throw new IOException("Baseline verification requires a fresh autocommit connection");
            String snapshot;
            try {
                coordinator.setAutoCommit(false);
                coordinator.setReadOnly(true);
                coordinator.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
                try (Statement statement = coordinator.createStatement();
                        ResultSet rows = statement.executeQuery("SELECT pg_export_snapshot()")) {
                    rows.next();
                    snapshot = rows.getString(1);
                }
                var results = run(connections, coordinator, snapshot, tasks, files, directory);
                coordinator.commit();
                finish(schema, files, directory, tasks, results);
            } catch (Exception error) {
                try {
                    coordinator.rollback();
                } catch (SQLException rollback) {
                    error.addSuppressed(rollback);
                }
                throw error;
            }
        }
    }

    /**
     * Every row group of every file, in the order the baseline assigns row identities: tables in
     * schema order, files in manifest order, groups in file order. Keys must be strictly
     * increasing across groups, so the ranges (previous maximum, own maximum] partition the
     * table: each PG row belongs to exactly one group, and the first and last ranges are open.
     */
    private static List<Task> tasks(SourceSchema schema, List<JsonObject> files, Path directory)
            throws IOException {
        var tasks = new ArrayList<Task>();
        long rowId = 1;
        for (var table : schema.tables()) {
            String key = SourceRow.primary(table).name();
            Comparable<Object> previous = null;
            var own = new ArrayList<Task>();
            for (JsonObject file : files) {
                if (!file.get("table").getAsString().equals(table.name())) continue;
                Path path = directory.resolve(file.get("path").getAsString());
                long fileRows = 0;
                try (ParquetFileReader reader =
                        ParquetFileReader.open(HadoopInputFile.fromPath(
                                new org.apache.hadoop.fs.Path(path.toUri()),
                                new org.apache.hadoop.conf.Configuration(false)))) {
                    var blocks = reader.getFooter().getBlocks();
                    for (int i = 0; i < blocks.size(); i++) {
                        var block = blocks.get(i);
                        var statistics =
                                block.getColumns().stream()
                                        .filter(c -> c.getPath().toDotString().equals(key))
                                        .findFirst()
                                        .orElseThrow(() -> new IOException("Key column missing in " + path))
                                        .getStatistics();
                        if (statistics == null || !statistics.hasNonNullValue())
                            throw new IOException("Key statistics missing in " + path);
                        @SuppressWarnings("unchecked")
                        Comparable<Object> min = (Comparable<Object>) statistics.genericGetMin();
                        @SuppressWarnings("unchecked")
                        Comparable<Object> max = (Comparable<Object>) statistics.genericGetMax();
                        if (previous != null && min.compareTo(previous) <= 0)
                            throw new IOException("Baseline keys are not strictly increasing: " + table.name());
                        long start = block.getStartingPos();
                        long end =
                                i + 1 < blocks.size()
                                        ? blocks.get(i + 1).getStartingPos()
                                        : Files.size(path);
                        own.add(
                                new Task(table, file, path, start, end, block.getRowCount(), rowId + fileRows,
                                        previous == null ? null : key(table, previous), key(table, max)));
                        fileRows += block.getRowCount();
                        previous = max;
                    }
                }
                if (fileRows != file.get("rows").getAsLong())
                    throw new IllegalStateException("File count mismatch");
                rowId += fileRows;
            }
            // The last group takes every larger key, so rows beyond the baseline are not missed.
            if (!own.isEmpty()) {
                var last = own.remove(own.size() - 1);
                own.add(new Task(last.table(), last.file(), last.path(), last.start(), last.end(), last.rows(),
                        last.firstRowId(), last.after(), null));
            }
            tasks.addAll(own);
        }
        return tasks;
    }

    private static Object key(SourceSchema.Table table, Object statistic) {
        if (SourceRow.primary(table).type().equals("integer")) return ((Number) statistic).longValue();
        return ((org.apache.parquet.io.api.Binary) statistic).toStringUsingUTF8();
    }

    /** The result of every task, in task order. */
    private static List<Result> run(
            Connections connections,
            Connection coordinator,
            String snapshot,
            List<Task> tasks,
            List<JsonObject> files,
            Path directory)
            throws Exception {
        var pool = java.util.concurrent.Executors.newFixedThreadPool(threads());
        // Stream readers wait for comparisons, so they never take a pool thread those need.
        var readers = java.util.concurrent.Executors.newCachedThreadPool();
        var perThread = new ThreadLocal<Connection>();
        var opened = new java.util.concurrent.ConcurrentLinkedQueue<Connection>();
        Work open = () -> {
            Connection pg = perThread.get();
            if (pg == null) {
                pg = connections.open();
                pg.setAutoCommit(false);
                pg.setReadOnly(true);
                pg.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
                perThread.set(pg);
                opened.add(pg);
            }
            try (Statement statement = pg.createStatement()) {
                statement.execute("SET TRANSACTION SNAPSHOT '" + snapshot + "'");
            }
            return pg;
        };
        try {
            // By position: a task names its manifest entry, which gains its digest while it runs.
            var futures = new ArrayList<java.util.concurrent.Future<Result>>(java.util.Collections.nCopies(tasks.size(), null));
            var streamed = new java.util.LinkedHashMap<SourceSchema.Table, List<Integer>>();
            var ordered = new java.util.HashMap<SourceSchema.Table, Boolean>();
            for (int i = 0; i < tasks.size(); i++) {
                Task task = tasks.get(i);
                if (!ordered.computeIfAbsent(task.table(), table -> ordered(coordinator, table))) {
                    streamed.computeIfAbsent(task.table(), ignored -> new ArrayList<>()).add(i);
                    continue;
                }
                futures.set(i, pool.submit(() -> {
                    Connection pg = open.connection();
                    try (Statement statement = pg.createStatement()) {
                        statement.setFetchSize(8192);
                        try (ResultSet expected = statement.executeQuery(query(task.table(), task))) {
                            return compare(task, rows(expected, task.table().columns().size()));
                        }
                    } finally {
                        pg.rollback();
                    }
                }));
            }
            var streams = new ArrayList<java.util.concurrent.Future<?>>();
            for (var entry : streamed.entrySet())
                streams.add(
                        Boolean.getBoolean(ORDERED_STREAM)
                                ? stream(open, entry.getKey(), entry.getValue(), tasks, futures, pool, readers)
                                : unordered(connections, coordinator, entry.getKey(), entry.getValue(), tasks, futures, pool, readers));
            var results = new ArrayList<Result>();
            try {
                for (var future : futures) results.add(future.get());
                // A stream fails after its last group too, when PG has rows beyond them.
                for (var future : streams) future.get();
            } catch (java.util.concurrent.ExecutionException error) {
                futures.forEach(f -> f.cancel(true));
                if (error.getCause() instanceof Exception cause) throw cause;
                throw error;
            }
            // Whole-file digests are independent of the row comparison and run in the same pool.
            var recorded = BaselineDigests.of(directory);
            var digests = new ArrayList<java.util.concurrent.Future<String>>();
            for (JsonObject file : files) {
                Path path = directory.resolve(file.get("path").getAsString());
                digests.add(pool.submit(() -> recorded.read(path)));
            }
            for (int i = 0; i < files.size(); i++) {
                JsonObject file = files.get(i);
                file.addProperty("size_bytes", Files.size(directory.resolve(file.get("path").getAsString())));
                file.addProperty("sha256", digests.get(i).get());
            }
            recorded.save();
            return results;
        } finally {
            pool.shutdownNow();
            readers.shutdownNow();
            for (Connection pg : opened)
                try {
                    pg.close();
                } catch (SQLException ignored) {
                    // The comparison result is already decided.
                }
        }
    }

    @FunctionalInterface
    private interface Work {
        /** The calling thread's connection, inside the exported snapshot. */
        Connection connection() throws Exception;
    }

    /**
     * Whether key ranges read the table's heap in order, as {@link PgBaselineExport} decides it;
     * either way every row is compared, only the reading differs.
     */
    private static boolean ordered(Connection coordinator, SourceSchema.Table table) {
        try (PreparedStatement statement =
                coordinator.prepareStatement(
                        "SELECT correlation FROM pg_stats WHERE schemaname = current_schema()"
                                + " AND tablename = ? AND attname = ?")) {
            statement.setString(1, table.name());
            statement.setString(2, SourceRow.primary(table).name());
            try (ResultSet rows = statement.executeQuery()) {
                return rows.next()
                        && rows.getObject(1) != null
                        && Math.abs(rows.getDouble(1)) >= PgBaselineExport.ORDERED_HEAP;
            }
        } catch (SQLException error) {
            throw new IllegalStateException(error);
        }
    }

    /** The table's columns in schema order, within the task's key range when one is given. */
    private static String query(SourceSchema.Table table, Task task) {
        var primary = SourceRow.primary(table);
        boolean integerKey = primary.type().equals("integer");
        var columns = table.columns();
        var sql = new StringBuilder("SELECT ");
        for (int i = 0; i < columns.size(); i++)
            sql.append(i == 0 ? "" : ", ").append('"').append(columns.get(i).name()).append('"');
        sql.append(" FROM \"").append(table.name()).append('"');
        String keyExpression = "\"" + primary.name() + "\"" + (integerKey ? "" : " COLLATE \"C\"");
        var bounds = new ArrayList<String>();
        if (task != null && task.after() != null) bounds.add(keyExpression + " > " + literal(integerKey, task.after()));
        if (task != null && task.through() != null) bounds.add(keyExpression + " <= " + literal(integerKey, task.through()));
        if (!bounds.isEmpty()) sql.append(" WHERE ").append(String.join(" AND ", bounds));
        return sql.append(" ORDER BY ").append(keyExpression).toString();
    }

    /** A key literal; plain statements keep every value in PG's text format. */
    private static String literal(boolean integerKey, Object key) {
        return integerKey ? Long.toString((Long) key) : "'" + ((String) key).replace("'", "''") + "'";
    }

    /** The rows of a result as text, read as they are consumed. */
    private static java.util.Iterator<String[]> rows(ResultSet result, int width) {
        return new java.util.Iterator<>() {
            private String[] next;
            private boolean done;

            @Override
            public boolean hasNext() {
                if (next == null && !done)
                    try {
                        if (result.next()) {
                            next = new String[width];
                            for (int i = 0; i < width; i++) next[i] = result.getString(i + 1);
                        } else done = true;
                    } catch (SQLException error) {
                        throw new IllegalStateException(error);
                    }
                return next != null;
            }

            @Override
            public String[] next() {
                if (!hasNext()) throw new java.util.NoSuchElementException();
                String[] row = next;
                next = null;
                return row;
            }
        };
    }

    /**
     * A table read as one key-ordered stream, cut into its row groups' counts, each compared on the
     * pool; a few groups of rows are held at once. Rows beyond the last group are missing from
     * Parquet; a short stream leaves its group with extra Parquet rows.
     */
    private static java.util.concurrent.Future<?> stream(
            Work open,
            SourceSchema.Table table,
            List<Integer> positions,
            List<Task> tasks,
            List<java.util.concurrent.Future<Result>> futures,
            java.util.concurrent.ExecutorService pool,
            java.util.concurrent.ExecutorService readers) {
        var own = new ArrayList<java.util.concurrent.CompletableFuture<Result>>();
        for (int position : positions) {
            var future = new java.util.concurrent.CompletableFuture<Result>();
            own.add(future);
            futures.set(position, future);
        }
        int width = table.columns().size();
        var inFlight = new java.util.concurrent.Semaphore(Math.max(2, threads()));
        var reader = readers.submit(() -> {
            Connection pg = open.connection();
            try (Statement statement = pg.createStatement()) {
                statement.execute("SET LOCAL work_mem = '" + PgBaselineExport.sortMemory() + "'");
                statement.setFetchSize(8192);
                try (ResultSet result = statement.executeQuery(query(table, null))) {
                    var rows = rows(result, width);
                    for (int i = 0; i < positions.size(); i++) {
                        Task task = tasks.get(positions.get(i));
                        var batch = new ArrayList<String[]>((int) Math.min(task.rows(), 1 << 20));
                        while (batch.size() < task.rows() && rows.hasNext()) batch.add(rows.next());
                        inFlight.acquire();
                        var target = own.get(i);
                        pool.submit(() -> {
                            try {
                                target.complete(compare(task, batch.iterator()));
                            } catch (Throwable error) {
                                target.completeExceptionally(error);
                            } finally {
                                inFlight.release();
                            }
                        });
                    }
                    if (rows.hasNext()) throw new IllegalStateException("Missing Parquet rows: " + table.name());
                }
            } finally {
                pg.rollback();
            }
            return null;
        });
        java.util.concurrent.CompletableFuture.runAsync(() -> {
            try {
                reader.get();
            } catch (Exception error) {
                Throwable cause = error instanceof java.util.concurrent.ExecutionException failed ? failed.getCause() : error;
                for (var future : own) future.completeExceptionally(cause);
            }
        });
        return reader;
    }

    /**
     * Compares a table whose key does not follow its heap as one PG key-ordered stream, row by row,
     * instead of by digests; names the first differing row. The default ({@link #unordered}) needs
     * no sort of the table.
     */
    static final String ORDERED_STREAM = "sibench.baseline.verify_ordered";

    /**
     * A table whose key does not follow its heap, compared without sorting it. An ordered read
     * of such a table is an external sort of all of it by PG (the 95 GB YCSB 87m table spilled
     * 87 GB), and the export has already paid for one. Instead PG's rows are read in heap order
     * by block ranges in parallel ({@link FinalState}) and the Parquet row groups in parallel, and
     * the two sides are compared as order-independent digests of the normalized rows. The groups
     * check, row by row, that their keys strictly increase and their row identities are the
     * consecutive ones; the groups' key statistics already increase across groups ({@link #tasks}).
     * With unique keys on both sides, equal digests are the same rows in the same key order, so
     * the comparison proves what the ordered one does.
     */
    private static java.util.concurrent.Future<?> unordered(
            Connections connections,
            Connection coordinator,
            SourceSchema.Table table,
            List<Integer> positions,
            List<Task> tasks,
            List<java.util.concurrent.Future<Result>> futures,
            java.util.concurrent.ExecutorService pool,
            java.util.concurrent.ExecutorService readers) {
        var digests = new FinalState.Digest[positions.size()];
        var groups = new ArrayList<java.util.concurrent.Future<Result>>();
        for (int i = 0; i < positions.size(); i++) {
            Task task = tasks.get(positions.get(i));
            int own = i;
            var future = pool.submit(() -> {
                var digest = new FinalState.Digest();
                Result result = group(task, digest);
                digests[own] = digest;
                return result;
            });
            groups.add(future);
            futures.set(positions.get(i), future);
        }
        // The coordinator exports the snapshot again for the scan's workers; it is otherwise idle.
        return readers.submit(() -> {
            FinalState.Digest postgres = FinalState.scan(coordinator, connections::open, table);
            var parquet = new FinalState.Digest();
            for (int i = 0; i < groups.size(); i++) {
                groups.get(i).get();
                parquet.addAll(digests[i]);
            }
            if (!parquet.same(postgres))
                throw new IOException(
                        "Baseline rows differ from the source: "
                                + table.name()
                                + ": Parquet "
                                + parquet
                                + ", PostgreSQL "
                                + postgres
                                + "; verify again with -D"
                                + ORDERED_STREAM
                                + "=true to name the first differing row");
            return null;
        });
    }

    /**
     * One row group's rows into {@code digest}, normalized as {@link #compare} compares them, with
     * the checks that comparison made through PG's order: required values present, keys strictly
     * increasing, consecutive row identities; and the group's own digest, as {@link #compare}.
     */
    private static Result group(Task task, FinalState.Digest digest) throws Exception {
        var table = task.table();
        var columns = table.columns();
        var primary = SourceRow.primary(table);
        boolean integerKey = primary.type().equals("integer");
        int keyIndex = columns.indexOf(primary);
        MessageDigest rowsDigest = MessageDigest.getInstance("SHA-256");
        long count = 0, rowId = task.firstRowId();
        String previous = null;
        try (ParquetReader<Group> reader =
                ParquetReader.builder(new GroupReadSupport(), new org.apache.hadoop.fs.Path(task.path().toUri()))
                        .withFileRange(task.start(), task.end())
                        .build()) {
            int[] fields = null;
            Group actual;
            var values = new LinkedHashMap<String, String>();
            while ((actual = reader.read()) != null) {
                if (fields == null) fields = fields(table, actual.getType());
                values.clear();
                String key = null;
                for (int i = 0; i < columns.size(); i++) {
                    var column = columns.get(i);
                    String value =
                            actual.getFieldRepetitionCount(fields[i]) == 0
                                    ? null
                                    : actual.getValueToString(fields[i], 0);
                    if (value == null && !column.nullable())
                        throw new IOException("Null required column: " + table.name() + "." + column.name());
                    if (i == keyIndex) key = value;
                    values.put(column.name(), SourceRow.normalize(column, value));
                }
                if (previous != null
                        && (integerKey
                                ? Long.parseLong(previous) >= Long.parseLong(key)
                                : Arrays.compareUnsigned(
                                                previous.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                                                key.getBytes(java.nio.charset.StandardCharsets.UTF_8))
                                        >= 0))
                    throw new IOException("Baseline keys are not strictly increasing: " + table.name());
                previous = key;
                digest.add(table, values);
                if (actual.getLong("_sib_row_id", 0) != rowId++ || actual.getLong("_sib_timestamp", 0) != 0)
                    throw new IllegalStateException("Baseline identity mismatch");
                byte[] data = actual.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                rowsDigest.update(java.nio.ByteBuffer.allocate(4).putInt(data.length).array());
                rowsDigest.update(data);
                count++;
            }
        }
        if (count != task.rows()) throw new IllegalStateException("File count mismatch");
        return new Result(count, rowsDigest.digest());
    }

    /**
     * The Parquet field of each table column, in table order. The file's own columns (all but the
     * SIBench identity fields) must be exactly the table's.
     */
    private static int[] fields(SourceSchema.Table table, org.apache.parquet.schema.GroupType type)
            throws IOException {
        var own = new java.util.HashSet<String>();
        for (var field : type.getFields())
            if (!field.getName().startsWith("_sib_")) own.add(field.getName());
        var names = new java.util.HashSet<String>();
        for (var column : table.columns()) names.add(column.name());
        if (!own.equals(names)) throw new IOException("Incomplete row image for " + table.name());
        int[] fields = new int[table.columns().size()];
        for (int i = 0; i < fields.length; i++) fields[i] = type.getFieldIndex(table.columns().get(i).name());
        return fields;
    }

    /** One row group against the PG rows of its key range, row by row and in order. */
    private static Result compare(Task task, java.util.Iterator<String[]> expected) throws Exception {
        var table = task.table();
        var columns = table.columns();
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        long count = 0, rowId = task.firstRowId();
        try (ParquetReader<Group> reader =
                ParquetReader.builder(new GroupReadSupport(), new org.apache.hadoop.fs.Path(task.path().toUri()))
                        .withFileRange(task.start(), task.end())
                        .build()) {
            int[] fields = null;
            Group actual;
            while ((actual = reader.read()) != null) {
                if (!expected.hasNext()) throw new IllegalStateException("Extra parquet row: " + table.name());
                String[] source = expected.next();
                // One schema per file: its columns must be the table's, once.
                if (fields == null) fields = fields(table, actual.getType());
                for (int i = 0; i < columns.size(); i++) {
                    var column = columns.get(i);
                    String value =
                            actual.getFieldRepetitionCount(fields[i]) == 0
                                    ? null
                                    : actual.getValueToString(fields[i], 0);
                    if ((source[i] == null || value == null) && !column.nullable())
                        throw new IOException("Null required column: " + table.name() + "." + column.name());
                    // Normalization is a function of the text, so equal texts need it once.
                    String normalized = SourceRow.normalize(column, source[i]);
                    String other =
                            java.util.Objects.equals(source[i], value) ? normalized : SourceRow.normalize(column, value);
                    if (!java.util.Objects.equals(normalized, other))
                        throw new IOException(
                                "Baseline row mismatch: " + table.name() + "/" + (rowId - task.firstRowId()));
                }
                if (actual.getLong("_sib_row_id", 0) != rowId++ || actual.getLong("_sib_timestamp", 0) != 0)
                    throw new IllegalStateException("Baseline identity mismatch");
                byte[] data = actual.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                digest.update(java.nio.ByteBuffer.allocate(4).putInt(data.length).array());
                digest.update(data);
                count++;
            }
            if (expected.hasNext()) throw new IllegalStateException("Missing Parquet rows: " + table.name());
        }
        if (count != task.rows()) throw new IllegalStateException("File count mismatch");
        return new Result(count, digest.digest());
    }

    private static void finish(
            SourceSchema schema,
            List<JsonObject> files,
            Path directory,
            List<Task> tasks,
            List<Result> results)
            throws Exception {
        List<Map<String, Object>> tables = new ArrayList<>();
        long total = 0;
        for (var table : schema.tables()) {
            // The table digest folds the row-group digests in baseline order.
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            long count = 0;
            for (int i = 0; i < tasks.size(); i++) {
                if (tasks.get(i).table() != table) continue;
                Result result = results.get(i);
                count += result.rows();
                digest.update(result.digest());
            }
            var entry = new LinkedHashMap<String, Object>();
            entry.put("table", table.name());
            entry.put("rows", count);
            entry.put("row_digest", HexFormat.of().formatHex(digest.digest()));
            entry.put("verified", true);
            tables.add(entry);
            total += count;
            System.out.println("Verified " + table.name() + " rows=" + count);
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("status", "verified_complete");
        result.put("rows", total);
        result.put("tables", tables);
        result.put("files", files);
        result.put("source_schema", schema.tables());
        result.put("source_schema_sha256", schema.fingerprints());
        result.put(
                "verification",
                "all Parquet values and row identities compared with the unchanged source PG");
        result.put(
                "files_manifest_sha256",
                BaselineGenerator.sha256(directory.resolve("files.jsonl")));
        Path partial = directory.resolve("verified-manifest.json.partial");
        Files.writeString(
                partial,
                new GsonBuilder().setPrettyPrinting().create().toJson(result),
                StandardOpenOption.CREATE_NEW);
        Files.move(
                partial,
                directory.resolve("verified-manifest.json"),
                StandardCopyOption.ATOMIC_MOVE);
    }

    static void verifyFiles(Path baseline) throws Exception {
        Path verified = baseline.resolve("verified-manifest.json");
        if (!Files.exists(verified))
            throw new IOException("Independent verified baseline manifest required");
        JsonObject verification =
                JsonParser.parseString(Files.readString(verified)).getAsJsonObject();
        if (!verification.get("status").getAsString().equals("verified_complete"))
            throw new IOException("Baseline is not verified");
        if (!FileDigests.sha256(baseline.resolve("files.jsonl"))
                .equals(verification.get("files_manifest_sha256").getAsString()))
            throw new IOException("Baseline file manifest changed");
        var data = new java.util.ArrayList<Path>();
        for (JsonElement item : verification.getAsJsonArray("files")) {
            Path path = baseline.resolve(item.getAsJsonObject().get("path").getAsString()).normalize();
            if (!path.startsWith(baseline.normalize())) throw new IOException("Baseline data changed");
            data.add(path);
        }
        // A large baseline is tens of GB: files with a recorded digest are not read again
        // (BaselineDigests), the rest are read concurrently.
        var recorded = BaselineDigests.of(baseline);
        var unread = new java.util.ArrayList<Path>();
        for (Path path : data) if (recorded.get(path) == null) unread.add(path);
        FileDigests.prefetch(unread);
        var files = verification.getAsJsonArray("files");
        for (int i = 0; i < data.size(); i++) {
            String digest = recorded.get(data.get(i));
            if (digest == null) digest = FileDigests.sha256(data.get(i));
            if (!digest.equals(files.get(i).getAsJsonObject().get("sha256").getAsString()))
                throw new IOException("Baseline data changed");
            recorded.put(data.get(i), digest);
        }
        recorded.save();
    }
}
