package org.sibench.generation;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import org.rocksdb.*;

/** Native timestamped rows and stable identities in one fresh RocksDB database. */
public final class RocksDbRows implements AutoCloseable {
    public record Settings(long cacheBytes, long writeBufferBytes, int backgroundJobs) {
        public Settings {
            if (cacheBytes < 1 << 20 || writeBufferBytes < 1 << 20 || backgroundJobs < 2 || backgroundJobs > 64)
                throw new IllegalArgumentException("Invalid RocksDB memory/background settings");
        }
    }
    public record Row(long id, String key, byte[][] fields) {}
    public record Event(String kind, String key, long rowId, long version, long readVersion,
                        long connection, long query, long count, long limit, boolean committed) {}
    @FunctionalInterface public interface Events { void accept(Event event) throws IOException; }
    @FunctionalInterface public interface Baseline { void accept(Row row) throws IOException; }

    private final TransactionDB database;
    private final DBOptions options;
    private final TransactionDBOptions transactions;
    private final ColumnFamilyOptions metadataOptions, rowOptions;
    private final List<ColumnFamilyHandle> handles = new ArrayList<>();
    private final Cache cache;
    private final BloomFilter bloom;
    private final WriteOptions writes = new WriteOptions().setDisableWAL(true).setSync(false);
    private final AtomicLong identities = new AtomicLong();
    private final int fields;
    private final long workloadWriteBufferBytes;
    private final Events events;
    private volatile long version;
    private volatile boolean loading = true, sealed, closed;
    private volatile Exception failure;
    private Map<String, RocksDbData.File> writtenFiles;

    public RocksDbRows(Path directory, int fields, Settings settings, Events events) throws IOException, RocksDBException {
        if (fields < 1 || fields > 1024) throw new IllegalArgumentException("Invalid field count");
        if (Files.exists(directory)) throw new IOException("RocksDB output already exists: " + directory);
        Files.createDirectories(directory.toAbsolutePath().getParent());
        this.fields = fields; this.events = Objects.requireNonNull(events);
        workloadWriteBufferBytes = Math.min(settings.writeBufferBytes(), 2L << 30);
        cache = new LRUCache(settings.cacheBytes());
        bloom = new BloomFilter(10, false);
        // Trim flushed memtables on subsequent writes; native transaction snapshots are unused.
        var history = new Properties(); history.setProperty("max_write_buffer_size_to_maintain", "1");
        metadataOptions = ColumnFamilyOptions.getColumnFamilyOptionsFromProps(history)
                .setWriteBufferSize(Math.min(settings.writeBufferBytes(), 16L << 20))
                .setMaxWriteBufferNumber(3).setCompressionType(CompressionType.LZ4_COMPRESSION)
                .setTableFormatConfig(new BlockBasedTableConfig().setBlockCache(cache).setFilterPolicy(bloom));
        rowOptions = ColumnFamilyOptions.getColumnFamilyOptionsFromProps(history).setWriteBufferSize(settings.writeBufferBytes())
                // Retain finite capture history without rewriting full rows in background merges.
                .setCompactionStyle(CompactionStyle.NONE)
                .setLevel0SlowdownWritesTrigger(Integer.MAX_VALUE).setLevel0StopWritesTrigger(Integer.MAX_VALUE)
                .setMaxWriteBufferNumber(3)
                .setCompressionType(CompressionType.ZSTD_COMPRESSION)
                .setTableFormatConfig(new BlockBasedTableConfig().setBlockCache(cache).setFilterPolicy(bloom));
        try (var compression = new CompressionOptions().setLevel(1)) { rowOptions.setCompressionOptions(compression); }
        RocksDbTimestamps.configure(rowOptions);
        options = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true)
                .setErrorIfExists(true).setMaxBackgroundJobs(settings.backgroundJobs())
                .setMaxSubcompactions(settings.backgroundJobs() - 1);
        transactions = new TransactionDBOptions().setTransactionLockTimeout(30000);
        RocksDbTimestamps.fileChecksums(options);
        try {
            database = TransactionDB.open(options, transactions, directory.toString(), descriptors(metadataOptions, rowOptions), handles);
        } catch (RocksDBException | RuntimeException error) {
            handles.forEach(ColumnFamilyHandle::close);
            writes.close(); transactions.close(); options.close(); rowOptions.close(); metadataOptions.close(); bloom.close(); cache.close();
            throw error;
        }
    }

    static List<ColumnFamilyDescriptor> descriptors(ColumnFamilyOptions metadata, ColumnFamilyOptions rows) {
        return List.of(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, metadata),
                new ColumnFamilyDescriptor(bytes("rows"), rows));
    }
    private ColumnFamilyHandle rows() { return handles.get(1); }

    /** Batches only baseline ingestion; workload calls always retain their own atomic boundary. */
    public void load(List<Row> input, Baseline baseline) throws Exception {
        check(); if (!loading) throw new IllegalStateException("Baseline is closed");
        try (var transaction = database.beginTransaction(writes);
             var read = new ReadOptions()) {
            var loaded = new ArrayList<Row>(input.size());
            for (Row supplied : input) {
                byte[] key = bytes(supplied.key());
                if (transaction.getForUpdate(read, rows(), key, true, false) != null)
                    throw new IOException("Duplicate baseline key: " + supplied.key());
                var row = new Row(nextIdentity(), supplied.key(), supplied.fields());
                transaction.put(rows(), key, encode(row, fields));
                transaction.put(identityKey(row.id()), key);
                loaded.add(row);
            }
            RocksDbTimestamps.commitTimestamp(transaction, 0);
            transaction.commit();
            synchronized (this) { check(); for (Row row : loaded) baseline.accept(row); }
        } catch (Exception error) { failure = error; throw error; }
    }

    public synchronized void finishLoad() throws Exception {
        check(); if (!loading) throw new IllegalStateException("Baseline already closed");
        // Finish the large sorted baseline flush before restoring small workload memtables.
        try (var flush = new FlushOptions().setWaitForFlush(true)) { database.flush(flush, handles); }
        database.setOptions(rows(), MutableColumnFamilyOptions.builder()
                .setWriteBufferSize(workloadWriteBufferBytes).setMaxWriteBufferNumber(3).build());
        loading = false;
    }
    public long baselineRows() { return identities.get(); }
    public long version() { return version; }

    public Map<String, String> statistics() throws RocksDBException {
        return Map.of("metadata", database.getProperty(handles.get(0), "rocksdb.stats"),
                "rows", database.getProperty(rows(), "rocksdb.stats"));
    }

    public List<Row> read(String key, int limit, long connection, long query) throws Exception {
        checkWorkload(); if (limit < 0) throw new IllegalArgumentException("Negative scan limit");
        long snapshot = version;
        var found = new ArrayList<Row>();
        try (var timestamp = new Slice(RocksDbTimestamps.timestamp(snapshot));
             var read = new ReadOptions().setTimestamp(timestamp)) {
            if (limit == 0) {
                byte[] value = database.get(rows(), read, bytes(key));
                if (value != null) found.add(decode(key, value, fields));
            } else try (var iterator = database.newIterator(rows(), read)) {
                for (iterator.seek(bytes(key)); iterator.isValid() && found.size() < limit; iterator.next())
                    found.add(decode(new String(iterator.key(), StandardCharsets.UTF_8), iterator.value(), fields));
                iterator.status();
            }
            synchronized (this) {
                check(); long next = Math.incrementExact(version);
                database.put(writes, pointKey(connection, query), number(snapshot));
                events.accept(new Event(limit == 0 ? "read" : "scan", key, 0, next, snapshot,
                        connection, query, found.size(), limit, true));
                version = next;
            }
            return found;
        } catch (Exception error) { failure = error; throw error; }
    }

    /** Returns null for an absent update/delete or a duplicate insertion, without inventing a write. */
    public Row change(String kind, String key, Map<Integer, byte[]> updates, long connection, long query) throws Exception {
        checkWorkload();
        if (!Set.of("insert", "update", "delete").contains(kind)) throw new IllegalArgumentException("Unknown row change");
        try (var transaction = database.beginTransaction(writes);
             var read = new ReadOptions()) {
            byte[] encodedKey = bytes(key);
            byte[] old = transaction.getForUpdate(read, rows(), encodedKey, true, false);
            Row previous = old == null ? null : decode(key, old, fields);
            boolean duplicate = kind.equals("insert") && previous != null;
            boolean changed = !duplicate && (kind.equals("insert") || previous != null);
            Row row = null;
            if (changed) {
                if (kind.equals("delete")) { row = previous; transaction.delete(rows(), encodedKey); }
                else {
                    byte[][] values = previous == null ? new byte[fields][] : previous.fields().clone();
                    for (var field : updates.entrySet()) {
                        if (field.getKey() < 0 || field.getKey() >= fields || field.getValue() == null)
                            throw new IllegalArgumentException("Invalid row field");
                        values[field.getKey()] = field.getValue();
                    }
                    row = new Row(previous == null ? nextIdentity() : previous.id(), key, values);
                    transaction.put(rows(), encodedKey, encode(row, fields));
                    if (previous == null) transaction.put(identityKey(row.id()), encodedKey);
                }
            }
            synchronized (this) {
                check(); long snapshot = version, next = Math.incrementExact(snapshot);
                if (duplicate) {
                    transaction.rollback();
                    database.put(writes, pointKey(connection, query), number(snapshot));
                } else {
                    transaction.put(pointKey(connection, query), number(snapshot));
                    RocksDbTimestamps.commitTimestamp(transaction, next);
                    transaction.commit();
                }
                events.accept(new Event(kind, key, row == null ? 0 : row.id(), next, snapshot,
                        connection, query, changed ? 1 : 0, 0, !duplicate));
                version = next;
            }
            return row;
        } catch (Exception error) { failure = error; throw error; }
    }

    public synchronized void seal() throws Exception {
        checkWorkload();
        database.put(writes, bytes("sealed"), ByteBuffer.allocate(20).putInt(fields).putLong(version).putLong(identities.get()).array());
        try (var flush = new FlushOptions().setWaitForFlush(true)) { database.flush(flush, handles); }
        sealed = true;
    }
    private long nextIdentity() throws IOException {
        long id = identities.incrementAndGet(); if (id <= 0) throw new IOException("Row identity overflow"); return id;
    }
    private void check() throws IOException {
        if (closed || sealed) throw new IOException("RocksDB generation is closed");
        if (failure != null) throw new IOException("RocksDB generation already failed", failure);
    }
    private void checkWorkload() throws IOException {
        check(); if (loading) throw new IOException("Baseline has not completed");
    }
    @Override public synchronized void close() throws RocksDBException {
        if (closed) return; closed = true;
        try {
            handles.forEach(ColumnFamilyHandle::close); database.closeE();
            if (sealed) writtenFiles = Map.copyOf(RocksDbTimestamps.writtenFiles(options));
        }
        finally { writes.close(); transactions.close(); options.close(); rowOptions.close(); metadataOptions.close(); bloom.close(); cache.close(); }
    }
    public Map<String, RocksDbData.File> writtenFiles() {
        if (writtenFiles == null) throw new IllegalStateException("RocksDB has not sealed and closed successfully");
        return writtenFiles;
    }

    static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }
    static byte[] number(long value) { return ByteBuffer.allocate(8).putLong(value).array(); }
    static byte[] identityKey(long id) { return ByteBuffer.allocate(9).put((byte) 'i').putLong(id).array(); }
    static byte[] pointKey(long connection, long query) {
        if (connection <= 0 || query <= 0) throw new IllegalArgumentException("Invalid source operation identity");
        return ByteBuffer.allocate(17).put((byte) 'p').putLong(connection).putLong(query).array();
    }
    static byte[] encode(Row row, int fields) throws IOException {
        if (row.id() <= 0 || row.fields().length != fields) throw new IOException("Invalid native row layout");
        int size = 8 + 4 * fields;
        for (byte[] value : row.fields()) if (value != null) size = Math.addExact(size, value.length);
        var buffer = ByteBuffer.allocate(size).putLong(row.id());
        for (byte[] value : row.fields()) { buffer.putInt(value == null ? -1 : value.length); if (value != null) buffer.put(value); }
        return buffer.array();
    }
    static Row decode(String key, byte[] value, int fields) throws IOException {
        try {
            var buffer = ByteBuffer.wrap(value); long id = buffer.getLong();
            if (id <= 0) throw new IOException("Invalid native row identity");
            var result = new byte[fields][];
            for (int i = 0; i < fields; i++) {
                int size = buffer.getInt();
                if (size < -1 || size > buffer.remaining()) throw new IOException("Invalid native row field length");
                if (size >= 0) { result[i] = new byte[size]; buffer.get(result[i]); }
            }
            if (buffer.hasRemaining()) throw new IOException("Unexpected native row tail");
            return new Row(id, key, result);
        } catch (java.nio.BufferUnderflowException error) { throw new IOException("Truncated native row", error); }
    }
}
