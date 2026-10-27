package org.sibench.adapters;

import com.google.protobuf.ByteString;

import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.WriteOptions;
import org.sibench.adapters.proto.Bound;
import org.sibench.adapters.proto.DescribeResponse;
import org.sibench.adapters.proto.Result;
import org.sibench.adapters.proto.Search;
import org.sibench.adapters.proto.Value;
import org.sibench.adapters.proto.ValueKind;
import org.sibench.adapters.proto.Write;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A versioned index on one RocksDB instance, in the trace contract's KV layout:
 * {@code table(u32) ++ index(u32) ++ [indexed value] ++ primary key ++ ~version(u64)}, all big
 * endian, so the versions of one entry sort newest first right after each other. The value is
 * {@code present(1) ++ indexed value length(u32) ++ file(i64) ++ position(i64) ++ row id(i64)}; a
 * logical delete is a version that is not present.
 *
 * <p>Baseline inserts skip the WAL and become durable at {@link #flush()}; every other write uses
 * the WAL without fsync.
 */
public final class RocksDbBackend implements IndexBackend {
    static {
        RocksDB.loadLibrary();
    }

    private static final int HEADER = 8, VERSION = 8, VALUE = 1 + 4 + 8 + 8 + 8;
    private final Options options;
    private final RocksDB db;
    private final WriteOptions logged = new WriteOptions(), unlogged = new WriteOptions().setDisableWAL(true);
    private final ValueKind valueKind;

    private RocksDbBackend(Options options, RocksDB db, ValueKind valueKind) {
        this.options = options;
        this.db = db;
        this.valueKind = valueKind;
    }

    /** A new, empty index in {@code path}; an existing directory is refused, never reused. */
    public static RocksDbBackend create(Path path, ValueKind valueKind) throws IOException {
        if (Files.exists(path)) throw new IOException("Index directory already exists: " + path);
        if (valueKind != ValueKind.LOCATOR && valueKind != ValueKind.ROW_ID)
            throw new IOException("Unknown value kind");
        Files.createDirectories(path.getParent());
        var options = new Options().setCreateIfMissing(true).setErrorIfExists(true);
        try {
            return new RocksDbBackend(options, RocksDB.open(options, path.toString()), valueKind);
        } catch (RocksDBException error) {
            options.close();
            throw new IOException(error);
        }
    }

    @Override
    public DescribeResponse describe() {
        return DescribeResponse.newBuilder()
                .setName("rocksdb-" + RocksDB.rocksdbVersion())
                .setProtocolVersion(1)
                .setValueKind(valueKind)
                .setVersionedReads(true)
                .setOrderedSameVersionOverwrite(true)
                .setTransactionLocalReads(true)
                .setSearchGroups(true)
                .setDurability("acknowledged writes are in the memtable and WAL without fsync;"
                        + " baseline inserts skip the WAL until Flush")
                .setWriteAck(org.sibench.adapters.proto.Acknowledgement.ACK_WAL)
                .setBaselineDurableOnlyAtFlush(true)
                .setFlushMakesDurable(true)
                .build();
    }

    @Override
    public void write(Write write) throws IOException {
        var key = write.getKey();
        if (key.getTable() < 1 || (key.getIndex() == 0) != key.getIndexedValue().isEmpty()
                || key.getPrimaryKey().isEmpty())
            throw new IllegalArgumentException("Invalid entry key");
        byte[] entry = entry(key.getTable(), key.getIndex(), key.getIndexedValue(), key.getPrimaryKey());
        byte[] versioned = versioned(entry, write.getVersion());
        try {
            switch (write.getKind()) {
                case INSERT -> {
                    if (write.getVersion() != 0) throw new IllegalArgumentException("Insert outside the baseline");
                    db.put(unlogged, versioned, value(true, key.getIndexedValue().size(), write.getValue()));
                }
                case UPSERT -> db.put(logged, versioned, value(true, key.getIndexedValue().size(), write.getValue()));
                case LOGICAL_DELETE ->
                        db.put(logged, versioned, value(false, key.getIndexedValue().size(), Value.getDefaultInstance()));
                case RELOCATE -> {
                    byte[] current = db.get(versioned);
                    if (current == null || current[0] == 0)
                        throw new IllegalArgumentException("Relocation requires an existing exact version");
                    if (valueKind == ValueKind.ROW_ID
                            && ByteBuffer.wrap(current).getLong(1 + 4 + 16) != write.getValue().getRowId())
                        throw new IllegalArgumentException("Relocation changed the row id");
                    db.put(logged, versioned, value(true, key.getIndexedValue().size(), write.getValue()));
                }
                default -> throw new IllegalArgumentException("Unknown write kind");
            }
        } catch (RocksDBException error) {
            throw new IOException(error);
        }
    }

    @Override
    public List<Result> search(Search search) throws IOException {
        if (search.getTable() < 1) throw new IllegalArgumentException("Invalid search table");
        var results = new ArrayList<Result>();
        byte[] prefix = entry(search.getTable(), search.getIndex(), ByteString.EMPTY, ByteString.EMPTY);
        if (!search.getPinnedPrimaryKey().isEmpty()) {
            if (search.getIndex() == 0 || !search.hasLower() || !search.getLower().equals(search.getUpper()))
                throw new IllegalArgumentException("A pinned search names one secondary entry");
            byte[] entry = entry(search.getTable(), search.getIndex(), search.getLower().getKey(), search.getPinnedPrimaryKey());
            try (var iterator = db.newIterator()) {
                Result result = visible(iterator, entry, search.getReadVersion(), search.getLocalVersion());
                if (result != null) results.add(result);
            } catch (RocksDBException error) {
                throw new IOException(error);
            }
            return results;
        }
        byte[] start = search.hasLower() ? concat(prefix, lower(search.getLower())) : prefix;
        // A bound without a successor is past every entry of the index.
        if (start == null) return results;
        byte[] end = search.hasUpper() ? concat(prefix, upper(search.getUpper())) : null;
        if (end == null) end = successor(prefix);
        if (end != null && Arrays.compareUnsigned(start, end) >= 0) return results;
        long limit = search.getLimit();
        try (var startSlice = new Slice(start);
                var endSlice = end == null ? null : new Slice(end);
                var bounded = new ReadOptions().setIterateLowerBound(startSlice);
                var versions = db.newIterator()) {
            if (endSlice != null) bounded.setIterateUpperBound(endSlice);
            try (var iterator = db.newIterator(bounded)) {
                if (!search.getDescending()) {
                    iterator.seek(start);
                    while (iterator.isValid() && (limit == 0 || results.size() < limit)) {
                        byte[] entry = logical(iterator.key());
                        Result result = visible(versions, entry, search.getReadVersion(), search.getLocalVersion());
                        if (result != null) results.add(result);
                        // Past the oldest possible version of this entry.
                        iterator.seek(versioned(entry, 0));
                        if (iterator.isValid() && Arrays.equals(logical(iterator.key()), entry)) iterator.next();
                    }
                } else {
                    if (end == null) iterator.seekToLast();
                    else iterator.seekForPrev(end);
                    while (iterator.isValid() && end != null && Arrays.compareUnsigned(iterator.key(), end) >= 0)
                        iterator.prev();
                    while (iterator.isValid() && (limit == 0 || results.size() < limit)) {
                        byte[] entry = logical(iterator.key());
                        Result result = visible(versions, entry, search.getReadVersion(), search.getLocalVersion());
                        if (result != null) results.add(result);
                        // Every version of the entry sorts after the entry itself.
                        iterator.seekForPrev(entry);
                    }
                }
                iterator.status();
            }
        } catch (RocksDBException error) {
            throw new IOException(error);
        }
        return results;
    }

    /**
     * The newest version of {@code entry} at or below the read version, or the exact local version
     * when a transaction-local search asks for one above it, if that state is present.
     */
    private static Result visible(
            RocksIterator versions, byte[] entry, long readVersion, long localVersion)
            throws RocksDBException {
        byte[] selected = versioned(entry, Math.max(readVersion, localVersion));
        versions.seek(selected);
        if (!versions.isValid()) {
            versions.status();
            return null;
        }
        byte[] key = versions.key();
        if (key.length != entry.length + VERSION || !Arrays.equals(key, 0, entry.length, entry, 0, entry.length))
            return null;
        if (localVersion > readVersion
                && ~ByteBuffer.wrap(key, entry.length, VERSION).getLong() != localVersion) {
            // The transaction did not write this entry; read the statement snapshot instead.
            versions.seek(versioned(entry, readVersion));
            if (!versions.isValid()) {
                versions.status();
                return null;
            }
            key = versions.key();
            if (key.length != entry.length + VERSION
                    || !Arrays.equals(key, 0, entry.length, entry, 0, entry.length)) return null;
        }
        ByteBuffer value = ByteBuffer.wrap(versions.value());
        if (value.get(0) == 0) return null;
        int indexedValue = value.getInt(1);
        return Result.newBuilder()
                .setPrimaryKey(ByteString.copyFrom(key, HEADER + indexedValue, entry.length - HEADER - indexedValue))
                .setVersion(~ByteBuffer.wrap(key, entry.length, VERSION).getLong())
                .setValue(Value.newBuilder()
                        .setFile(value.getLong(5))
                        .setPosition(value.getLong(13))
                        .setRowId(value.getLong(21)))
                .build();
    }

    private static byte[] entry(int table, int index, ByteString indexedValue, ByteString primaryKey) {
        return ByteBuffer.allocate(HEADER + indexedValue.size() + primaryKey.size())
                .putInt(table)
                .putInt(index)
                .put(indexedValue.asReadOnlyByteBuffer())
                .put(primaryKey.asReadOnlyByteBuffer())
                .array();
    }

    private static byte[] versioned(byte[] entry, long version) {
        return ByteBuffer.allocate(entry.length + VERSION).put(entry).putLong(~version).array();
    }

    private static byte[] logical(byte[] key) {
        return Arrays.copyOf(key, key.length - VERSION);
    }

    private static byte[] value(boolean present, int indexedValue, Value value) {
        return ByteBuffer.allocate(VALUE)
                .put((byte) (present ? 1 : 0))
                .putInt(indexedValue)
                .putLong(value.getFile())
                .putLong(value.getPosition())
                .putLong(value.getRowId())
                .array();
    }

    /** Components delimit themselves, so the successor of a component bounds everything after it. */
    private static byte[] lower(Bound bound) {
        byte[] key = bound.getKey().toByteArray();
        return bound.getInclusive() ? key : successor(key);
    }

    private static byte[] upper(Bound bound) {
        byte[] key = bound.getKey().toByteArray();
        return bound.getInclusive() ? successor(key) : key;
    }

    /** The smallest byte string greater than every string starting with {@code bytes}. */
    static byte[] successor(byte[] bytes) {
        byte[] result = bytes.clone();
        for (int i = result.length - 1; i >= 0; i--) {
            if (result[i] != (byte) 0xff) {
                result[i]++;
                return Arrays.copyOf(result, i + 1);
            }
        }
        return null;
    }

    private static byte[] concat(byte[] a, byte[] b) {
        if (b == null) return null;
        byte[] result = Arrays.copyOf(a, a.length + b.length);
        System.arraycopy(b, 0, result, a.length, b.length);
        return result;
    }

    @Override
    public void flush() throws IOException {
        try (var flush = new FlushOptions().setWaitForFlush(true)) {
            db.flush(flush);
            db.flushWal(true);
        } catch (RocksDBException error) {
            throw new IOException(error);
        }
    }

    @Override
    public void close() {
        logged.close();
        unlogged.close();
        db.close();
        options.close();
    }
}
