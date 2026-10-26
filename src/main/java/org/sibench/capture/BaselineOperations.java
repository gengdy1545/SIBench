package org.sibench.capture;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.sibench.capture.IndexOperationTrace.Operation;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The baseline index load of a trace: one primary-key INSERT per baseline row and its secondary
 * index entries, in baseline file order. It is a function of the baseline Parquet and the index
 * definitions alone, so the trace does not store it: compilation records only its count and the
 * SHA256 of the file it would form ({@code baseline_trace_sha256}), and every reader derives it
 * again from the verified baseline and checks that digest when it ends.
 *
 * <p>A trace compiled before this keeps its {@code baseline.ops}, which is read as it is.
 */
public final class BaselineOperations {
    static final String FILE = "baseline.ops";

    private BaselineOperations() {}

    /** What a compiler records per baseline file and row while the load is derived. */
    interface Rows {
        void file(String table, int tableId) throws IOException;

        /** {@code type}: the Parquet schema of the file the row is in, all of its columns. */
        void row(String table, org.apache.parquet.schema.GroupType type, Operation primary) throws IOException;

        void fileFinished() throws IOException;
    }

    /** The derived load's extent: its rows and the largest row and file identities in it. */
    record Extent(long rows, long maximumRowId, long maximumFileId) {}

    /** One baseline row: its primary write and its whole maintenance unit, the primary first. */
    private record Row(Operation primary, List<Operation> operations) {}

    /**
     * One row group of a baseline file: its byte range, its rows and where they start in the file
     * and in the whole baseline (row identities and work units follow from these alone).
     */
    private record Group(int file, long start, long end, long rows, long filePosition, long firstRow) {}

    /**
     * A row group's rows as the load needs them, read ahead of encoding: row identities and the
     * text of the primary and secondary index columns, in the catalog's order.
     */
    private record Columns(long[] rowIds, String[][] texts, long maximumRowId) {}

    /** One run of a row group's rows, encoded; the group's last run carries its read columns. */
    private record Chunk(int group, List<Row> rows, Columns columns, boolean last) {}

    /** Rows encoded per task, and encoded runs held ahead of the consumer. */
    static final int CHUNK_ROWS = 8192, CHUNKS_AHEAD = 96;

    /** Row groups read ahead: each is a group's index texts, so memory is bounded. */
    static final int GROUPS_AHEAD = 6;

    /** Threads that encode rows; their output still reaches the consumer in file order. */
    static int threads() {
        return Integer.getInteger("sibench.baseline.derive_threads", Runtime.getRuntime().availableProcessors());
    }

    /**
     * Derives the load from the baseline directory, in file order. {@code rows} sees each row
     * before its operations reach {@code consumer}. Row groups are read a few ahead, each by a
     * thread of its own and only for the row identity and the indexed columns; their rows are then
     * encoded in runs by a pool, a bounded number of runs ahead, and handed over in order. A row's
     * position, row identity and work unit follow from the rows before it, so the stream is the
     * one a single reader produces. (Decoding a whole group per task with a bounded queue each
     * stalled every group but the one being consumed: the load was derived by one thread.)
     */
    static Extent derive(Path baseline, IndexCatalog catalog, IndexOperationTrace.Consumer consumer, Rows rows)
            throws IOException {
        var files = new ArrayList<JsonObject>();
        for (String line : Files.readAllLines(baseline.resolve("files.jsonl")))
            files.add(JsonParser.parseString(line).getAsJsonObject());
        var types = new ArrayList<MessageType>();
        var groups = new ArrayList<Group>();
        long before = 0, maximumFileId = 0;
        for (int i = 0; i < files.size(); i++) {
            Path path = baseline.resolve(files.get(i).get("path").getAsString());
            try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(
                    new org.apache.hadoop.fs.Path(path.toUri()), new org.apache.hadoop.conf.Configuration(false)))) {
                types.add(reader.getFooter().getFileMetaData().getSchema());
                var blocks = reader.getFooter().getBlocks();
                long position = 0;
                for (int b = 0; b < blocks.size(); b++) {
                    long start = blocks.get(b).getStartingPos();
                    long end = b + 1 < blocks.size() ? blocks.get(b + 1).getStartingPos() : Files.size(path);
                    groups.add(new Group(i, start, end, blocks.get(b).getRowCount(), position, before + position));
                    position += blocks.get(b).getRowCount();
                }
                if (position != files.get(i).get("rows").getAsLong())
                    throw new IOException("Baseline manifest row mismatch");
                before += position;
            }
        }
        var readers = java.util.concurrent.Executors.newFixedThreadPool(GROUPS_AHEAD, daemon("baseline-read"));
        var encoders = java.util.concurrent.Executors.newFixedThreadPool(Math.max(1, threads()), daemon("baseline-encode"));
        var reads = new ArrayList<java.util.concurrent.Future<Columns>>();
        var readWindow = new java.util.concurrent.Semaphore(GROUPS_AHEAD);
        var chunks = new java.util.concurrent.ArrayBlockingQueue<java.util.concurrent.Future<Chunk>>(CHUNKS_AHEAD);
        var stopped = new java.util.concurrent.atomic.AtomicBoolean();
        // Reads go in group order behind a window; the submitter turns each read group into runs.
        for (int i = 0; i < groups.size(); i++) reads.add(new java.util.concurrent.CompletableFuture<>());
        var submitter = new Thread(() -> {
            try {
                var readStarter = new Thread(() -> {
                    try {
                        for (int i = 0; i < groups.size() && !stopped.get(); i++) {
                            readWindow.acquire();
                            int index = i;
                            var target = (java.util.concurrent.CompletableFuture<Columns>) reads.get(index);
                            readers.execute(() -> {
                                try {
                                    target.complete(read(baseline, catalog, files.get(groups.get(index).file()),
                                            types.get(groups.get(index).file()), groups.get(index)));
                                } catch (Throwable failure) {
                                    target.completeExceptionally(failure);
                                }
                            });
                        }
                    } catch (InterruptedException closed) {
                        Thread.currentThread().interrupt();
                    }
                }, "baseline-read-start");
                readStarter.setDaemon(true);
                readStarter.start();
                for (int i = 0; i < groups.size() && !stopped.get(); i++) {
                    Group group = groups.get(i);
                    JsonObject file = files.get(group.file());
                    Columns columns;
                    try {
                        columns = reads.get(i).get();
                    } catch (java.util.concurrent.ExecutionException failed) {
                        chunks.put(java.util.concurrent.CompletableFuture.failedFuture(failed.getCause()));
                        return;
                    }
                    int index = i;
                    for (long from = 0; from < group.rows() || from == 0; from += CHUNK_ROWS) {
                        long to = Math.min(group.rows(), from + CHUNK_ROWS);
                        boolean last = to == group.rows();
                        long first = from;
                        chunks.put(encoders.submit(() -> new Chunk(index,
                                encode(catalog, file, group, columns, (int) first, (int) to), last ? columns : null, last)));
                        if (last) break;
                    }
                }
            } catch (InterruptedException closed) {
                Thread.currentThread().interrupt();
            }
        }, "baseline-submit");
        submitter.setDaemon(true);
        submitter.start();
        try {
            long count = 0, maximumRowId = 0;
            int file = -1;
            for (int i = 0; i < groups.size(); i++) {
                Group group = groups.get(i);
                if (group.file() != file) {
                    if (file >= 0) rows.fileFinished();
                    file = group.file();
                    JsonObject entry = files.get(file);
                    String table = entry.get("table").getAsString();
                    maximumFileId = Math.max(maximumFileId, entry.get("file_id").getAsLong());
                    rows.file(table, catalog.tableId(table));
                }
                String table = files.get(file).get("table").getAsString();
                MessageType type = types.get(file);
                long groupRows = 0;
                while (true) {
                    Chunk chunk;
                    try {
                        chunk = chunks.take().get();
                    } catch (java.util.concurrent.ExecutionException failed) {
                        Throwable cause = failed.getCause();
                        if (cause instanceof IOException io) throw io;
                        if (cause instanceof RuntimeException runtime) throw runtime;
                        if (cause instanceof Error error) throw error;
                        throw new IOException(cause);
                    }
                    if (chunk.group() != i) throw new IllegalStateException("Baseline runs out of order");
                    for (Row row : chunk.rows()) {
                        rows.row(table, type, row.primary());
                        for (Operation operation : row.operations()) consumer.accept(operation);
                    }
                    groupRows += chunk.rows().size();
                    if (!chunk.last()) continue;
                    if (groupRows != group.rows()) throw new IOException("Baseline row group count mismatch");
                    maximumRowId = Math.max(maximumRowId, chunk.columns().maximumRowId());
                    break;
                }
                count += groupRows;
                reads.set(i, null);
                readWindow.release();
            }
            if (file >= 0) rows.fileFinished();
            return new Extent(count, maximumRowId, maximumFileId);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while deriving the baseline load", interrupted);
        } finally {
            stopped.set(true);
            submitter.interrupt();
            readers.shutdownNow();
            encoders.shutdownNow();
        }
    }

    private static java.util.concurrent.ThreadFactory daemon(String name) {
        return task -> {
            var thread = new Thread(task, name);
            thread.setDaemon(true);
            return thread;
        };
    }

    /**
     * The columns the load needs of a table's rows, those of them the file has: the row identity
     * and every indexed column. A column the file lacks stays absent, so it fails as before.
     */
    private static MessageType projection(IndexCatalog catalog, String table, MessageType type) {
        var needed = new java.util.LinkedHashSet<String>();
        needed.add("_sib_row_id");
        for (var definition : catalog.definitions())
            if (definition.table().equals(table)) needed.add(definition.column());
        var fields = new ArrayList<org.apache.parquet.schema.Type>();
        for (var field : type.getFields()) if (needed.contains(field.getName())) fields.add(field);
        return new MessageType(type.getName(), fields);
    }

    /** The indexed columns of a table: its primary key's first, then its secondaries in order. */
    private static List<IndexCatalog.Definition> indexed(IndexCatalog catalog, int tableId) throws IOException {
        var result = new ArrayList<IndexCatalog.Definition>();
        result.add(catalog.primary(tableId));
        result.addAll(catalog.secondary(tableId));
        return result;
    }

    /** One row group's row identities and index texts, read as {@link IndexCatalog} reads them. */
    private static Columns read(Path baseline, IndexCatalog catalog, JsonObject file, MessageType type, Group group)
            throws IOException {
        String table = file.get("table").getAsString();
        var definitions = indexed(catalog, catalog.tableId(table));
        var configuration = new org.apache.hadoop.conf.Configuration(false);
        configuration.set(org.apache.parquet.hadoop.api.ReadSupport.PARQUET_READ_SCHEMA,
                projection(catalog, table, type).toString());
        int rows = Math.toIntExact(group.rows());
        long[] rowIds = new long[rows];
        String[][] texts = new String[definitions.size()][rows];
        long maximumRowId = 0;
        int n = 0;
        try (ParquetReader<org.apache.parquet.example.data.Group> reader =
                ParquetReader.builder(
                                new GroupReadSupport(),
                                new org.apache.hadoop.fs.Path(baseline.resolve(file.get("path").getAsString()).toUri()))
                        .withConf(configuration)
                        .withFileRange(group.start(), group.end())
                        .build()) {
            org.apache.parquet.example.data.Group row;
            int[] fields = null;
            while ((row = reader.read()) != null) {
                if (n == rows) throw new IOException("Baseline row group count mismatch");
                if (fields == null) {
                    fields = new int[definitions.size()];
                    for (int d = 0; d < fields.length; d++) {
                        String column = definitions.get(d).column();
                        if (!row.getType().containsField(column))
                            throw new IOException("Row missing indexed column: " + column);
                        fields[d] = row.getType().getFieldIndex(column);
                    }
                }
                long rowId = row.getLong("_sib_row_id", 0);
                rowIds[n] = rowId;
                maximumRowId = Math.max(maximumRowId, rowId);
                for (int d = 0; d < fields.length; d++)
                    texts[d][n] = row.getFieldRepetitionCount(fields[d]) == 0 ? null : row.getValueToString(fields[d], 0);
                n++;
            }
        }
        if (n != rows) throw new IOException("Baseline row group count mismatch");
        return new Columns(rowIds, texts, maximumRowId);
    }

    /** Rows {@code from} to {@code to} of a read group as their operations; runs on the pool. */
    private static List<Row> encode(IndexCatalog catalog, JsonObject file, Group group, Columns columns, int from, int to)
            throws IOException {
        String table = file.get("table").getAsString();
        int tableId = catalog.tableId(table);
        long fileId = file.get("file_id").getAsLong();
        var definitions = indexed(catalog, tableId);
        var result = new ArrayList<Row>(to - from);
        for (int n = from; n < to; n++) {
            long position = group.filePosition() + n;
            Operation primary =
                    IndexOperationTrace.write(
                                    0,
                                    IndexOperationTrace.INSERT,
                                    tableId,
                                    0,
                                    definitions.get(0).encode(columns.texts()[0][n]),
                                    0,
                                    fileId,
                                    position,
                                    columns.rowIds()[n])
                            .withWorkUnitId(1 + (group.firstRow() + n) / IndexOperationTrace.BLOCK_RECORDS);
            var keys = new java.util.LinkedHashMap<Integer, org.sibench.trace.IndexKey>();
            for (int d = 1; d < definitions.size(); d++)
                keys.put(definitions.get(d).indexId(), definitions.get(d).encode(columns.texts()[d][n]));
            var operations = new ArrayList<Operation>(4);
            IndexMutation.emit(primary, Map.of(), keys, operations::add);
            result.add(new Row(primary, operations));
        }
        return result;
    }

    private static final Rows NONE =
            new Rows() {
                @Override
                public void file(String table, int tableId) {}

                @Override
                public void row(String table, org.apache.parquet.schema.GroupType type, Operation primary) {}

                @Override
                public void fileFinished() {}
            };

    /**
     * The baseline load of a compiled trace: its {@code baseline.ops} when it has one, otherwise
     * derived from the baseline directory its manifest names. A derived load requires the baseline
     * files to be the verified ones and, once read, to be the stream the trace was compiled with.
     */
    public static IndexOperationTrace.Source of(Path trace) {
        Path file = trace.resolve(FILE);
        if (Files.exists(file)) return IndexOperationTrace.Source.file(file);
        return consumer -> {
            JsonObject manifest = JsonParser.parseString(Files.readString(trace.resolve("manifest.json"))).getAsJsonObject();
            for (String field : java.util.List.of("baseline_directory", "baseline_trace_sha256", "baseline_operations", "baseline_verified_manifest_sha256"))
                if (!manifest.has(field)) throw new IOException("Trace manifest cannot derive its baseline load: " + field);
            Path baseline = Path.of(manifest.get("baseline_directory").getAsString());
            if (!FileDigests.sha256(baseline.resolve("verified-manifest.json"))
                    .equals(manifest.get("baseline_verified_manifest_sha256").getAsString()))
                throw new IOException("The baseline is not the one the trace was compiled with: " + baseline);
            try {
                PgBaselineVerify.verifyFiles(baseline);
            } catch (IOException error) {
                throw error;
            } catch (Exception error) {
                throw new IOException(error);
            }
            IndexCatalog catalog = IndexCatalog.fromManifest(manifest);
            var digest = new WriterThread(new IndexOperationTrace.Writer(null), "baseline-digest");
            try (digest) {
                derive(baseline, catalog, operation -> {
                    digest.append(operation);
                    consumer.accept(operation);
                }, NONE);
            }
            if (digest.count() != manifest.get("baseline_operations").getAsLong()
                    || !digest.sha256().equals(manifest.get("baseline_trace_sha256").getAsString()))
                throw new IOException("Derived baseline load differs from the compiled trace");
            return digest.count();
        };
    }
}
