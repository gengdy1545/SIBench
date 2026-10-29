package org.sibench.adapters;

import com.google.protobuf.ByteString;

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
 * A versioned index on one RocksDB instance (local, or rocksdb-cloud), in the trace contract's KV
 * layout:
 * {@code table(u32) ++ index(u32) ++ [indexed value] ++ primary key ++ ~version(u64)}, all big
 * endian, so the versions of one entry sort newest first right after each other. The value is
 * {@code present(1) ++ indexed value length(u32) ++ file(i64) ++ position(i64) ++ row id(i64)}; a
 * logical delete is a version that is not present.
 *
 * <p>Baseline inserts skip the WAL and become durable at {@link #flush()}; every other write uses
 * the WAL without fsync. Table 0, which no entry uses, holds the index's value kind and its
 * recorded complete-commit watermark, so a reopened index is checked and resumes publication.
 */
public final class RocksDbBackend implements IndexBackend {
    private static final int HEADER = 8, VERSION = 8, VALUE = 1 + 4 + 8 + 8 + 8;
    private static final byte[] VALUE_KIND = metadata("value_kind"),
            PUBLISHED_VERSION = metadata("published_version"),
            TRACE_BINDING = metadata("trace_binding");
    private final RocksStore store;
    private final ValueKind valueKind;
    /** One JSON line per Flush: the engine's state and object-storage requests so far. */
    private final Path engineStats;
    private long flushes;

    private RocksDbBackend(RocksStore store, ValueKind valueKind, Path path) {
        this.store = store;
        this.valueKind = valueKind;
        this.engineStats = path.resolveSibling(path.getFileName() + ".engine-stats.jsonl");
    }

    /** A new, empty index in {@code path}; an existing directory is refused, never reused. */
    public static RocksDbBackend create(Path path, ValueKind valueKind) throws IOException {
        prepare(path, valueKind);
        return created(LocalRocksStore.create(path), valueKind, path);
    }

    /** A new, empty rocksdb-cloud index: local files in {@code path}, objects in the store. */
    static RocksDbBackend cloud(Path path, ValueKind valueKind, ObjectStore objects)
            throws IOException {
        prepare(path, valueKind);
        return created(CloudRocksStore.create(path, objects), valueKind, path);
    }

    /** An index this class created in {@code path}, reopened, e.g. after a restart. */
    public static RocksDbBackend open(Path path, ValueKind valueKind) throws IOException {
        if (!Files.isDirectory(path)) throw new IOException("No index directory: " + path);
        return reopened(LocalRocksStore.open(path), valueKind, path);
    }

    /**
     * A rocksdb-cloud index reopened from its local files, or, when {@code path} does not exist,
     * from the bucket alone (a cold start).
     */
    static RocksDbBackend openCloud(Path path, ValueKind valueKind, ObjectStore objects)
            throws IOException {
        Files.createDirectories(path.getParent());
        return reopened(CloudRocksStore.open(path, objects), valueKind, path);
    }

    private static void prepare(Path path, ValueKind valueKind) throws IOException {
        if (Files.exists(path)) throw new IOException("Index directory already exists: " + path);
        if (valueKind != ValueKind.LOCATOR && valueKind != ValueKind.ROW_ID)
            throw new IOException("Unknown value kind");
        Files.createDirectories(path.getParent());
    }

    private static RocksDbBackend created(RocksStore store, ValueKind valueKind, Path path)
            throws IOException {
        try {
            store.put(VALUE_KIND, new byte[] {(byte) valueKind.getNumber()}, true);
        } catch (IOException | RuntimeException error) {
            store.close();
            throw error;
        }
        return new RocksDbBackend(store, valueKind, path);
    }

    private static RocksDbBackend reopened(RocksStore store, ValueKind valueKind, Path path)
            throws IOException {
        byte[] stored = store.get(VALUE_KIND);
        if (stored == null || stored.length != 1 || stored[0] != valueKind.getNumber()) {
            store.close();
            throw new IOException("Reopened index does not hold " + valueKind + " values");
        }
        return new RocksDbBackend(store, valueKind, path);
    }

    private static byte[] metadata(String name) {
        byte[] text = name.getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        return ByteBuffer.allocate(HEADER + text.length).putInt(0).putInt(0).put(text).array();
    }

    @Override
    public DescribeResponse describe() {
        return DescribeResponse.newBuilder()
                .setName(store.name())
                .setProtocolVersion(2)
                .setValueKind(valueKind)
                .setVersionedReads(true)
                .setOrderedSameVersionOverwrite(true)
                .setTransactionLocalReads(true)
                .setSearchGroups(true)
                .setDurability(store.durability())
                .setWriteAck(org.sibench.adapters.proto.Acknowledgement.ACK_WAL)
                .setBaselineDurableOnlyAtFlush(true)
                .setFlushMakesDurable(true)
                .setBaselineWriteBatches(true)
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
        switch (write.getKind()) {
            case INSERT -> {
                if (write.getVersion() != 0) throw new IllegalArgumentException("Insert outside the baseline");
                store.put(versioned, value(true, key.getIndexedValue().size(), write.getValue()), false);
            }
            case UPSERT -> store.put(versioned, value(true, key.getIndexedValue().size(), write.getValue()), true);
            case LOGICAL_DELETE ->
                    store.put(versioned, value(false, key.getIndexedValue().size(), Value.getDefaultInstance()), true);
            case RELOCATE -> {
                byte[] current = store.get(versioned);
                if (current == null || current[0] == 0)
                    throw new IllegalArgumentException("Relocation requires an existing exact version");
                if (valueKind == ValueKind.ROW_ID
                        && ByteBuffer.wrap(current).getLong(1 + 4 + 16) != write.getValue().getRowId())
                    throw new IllegalArgumentException("Relocation changed the row id");
                store.put(versioned, value(true, key.getIndexedValue().size(), write.getValue()), true);
            }
            default -> throw new IllegalArgumentException("Unknown write kind");
        }
    }

    @Override
    public void search(Search search, ResultSink sink) throws IOException {
        if (search.getTable() < 1) throw new IllegalArgumentException("Invalid search table");
        long found = 0;
        byte[] prefix = entry(search.getTable(), search.getIndex(), ByteString.EMPTY, ByteString.EMPTY);
        if (!search.getPinnedPrimaryKey().isEmpty()) {
            if (search.getIndex() == 0 || !search.hasLower() || !search.getLower().equals(search.getUpper()))
                throw new IllegalArgumentException("A pinned search names one secondary entry");
            byte[] entry = entry(search.getTable(), search.getIndex(), search.getLower().getKey(), search.getPinnedPrimaryKey());
            try (var iterator = store.cursor(null, null)) {
                Result result = visible(iterator, entry, search.getReadVersion(), search.getLocalVersion());
                if (result != null) sink.accept(result);
            }
            return;
        }
        byte[] start = search.hasLower() ? concat(prefix, lower(search.getLower())) : prefix;
        // A bound without a successor is past every entry of the index.
        if (start == null) return;
        byte[] end = search.hasUpper() ? concat(prefix, upper(search.getUpper())) : null;
        if (end == null) end = successor(prefix);
        if (end != null && Arrays.compareUnsigned(start, end) >= 0) return;
        long limit = search.getLimit();
        try (var versions = store.cursor(null, null);
                var iterator = store.cursor(start, end)) {
            if (!search.getDescending()) {
                iterator.seek(start);
                while (iterator.valid() && (limit == 0 || found < limit)) {
                    byte[] entry = logical(iterator.key());
                    Result result = visible(versions, entry, search.getReadVersion(), search.getLocalVersion());
                    if (result != null) {
                        sink.accept(result);
                        found++;
                    }
                    // Past the oldest possible version of this entry.
                    iterator.seek(versioned(entry, 0));
                    if (iterator.valid() && Arrays.equals(logical(iterator.key()), entry)) iterator.next();
                }
            } else {
                if (end == null) iterator.seekToLast();
                else iterator.seekForPrev(end);
                while (iterator.valid() && end != null && Arrays.compareUnsigned(iterator.key(), end) >= 0)
                    iterator.prev();
                while (iterator.valid() && (limit == 0 || found < limit)) {
                    byte[] entry = logical(iterator.key());
                    Result result = visible(versions, entry, search.getReadVersion(), search.getLocalVersion());
                    if (result != null) {
                        sink.accept(result);
                        found++;
                    }
                    // Every version of the entry sorts after the entry itself.
                    iterator.seekForPrev(entry);
                }
            }
        }
    }

    /**
     * The newest version of {@code entry} at or below the read version, or the exact local version
     * when a transaction-local search asks for one above it, if that state is present.
     */
    private static Result visible(
            RocksStore.Cursor versions, byte[] entry, long readVersion, long localVersion)
            throws IOException {
        byte[] selected = versioned(entry, Math.max(readVersion, localVersion));
        versions.seek(selected);
        if (!versions.valid()) return null;
        byte[] key = versions.key();
        if (key.length != entry.length + VERSION || !Arrays.equals(key, 0, entry.length, entry, 0, entry.length))
            return null;
        if (localVersion > readVersion
                && ~ByteBuffer.wrap(key, entry.length, VERSION).getLong() != localVersion) {
            // The transaction did not write this entry; read the statement snapshot instead.
            versions.seek(versioned(entry, readVersion));
            if (!versions.valid()) return null;
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
        store.flush();
        recordEngineStats();
    }

    private static final List<String> ENGINE_PROPERTIES =
            List.of(
                    "rocksdb.estimate-num-keys",
                    "rocksdb.total-sst-files-size",
                    "rocksdb.live-sst-files-size",
                    "rocksdb.estimate-pending-compaction-bytes",
                    "rocksdb.num-running-compactions",
                    "rocksdb.num-running-flushes",
                    "rocksdb.stats");

    private synchronized void recordEngineStats() throws IOException {
        var line = new java.util.LinkedHashMap<String, Object>();
        line.put("time_millis", System.currentTimeMillis());
        line.put("flush", ++flushes);
        line.put("engine", store.name());
        for (String name : ENGINE_PROPERTIES) line.put(name, store.property(name));
        var requests = store.objectRequests();
        if (requests == null) line.put("object_requests", "N/A");
        else {
            var byType = new java.util.LinkedHashMap<String, Object>();
            requests.forEach(
                    (type, counts) ->
                            byType.put(type, objectRequestStats(counts)));
            line.put("object_requests", byType);
        }
        Files.writeString(
                engineStats,
                new com.google.gson.Gson().toJson(line) + "\n",
                java.nio.file.StandardOpenOption.CREATE,
                java.nio.file.StandardOpenOption.APPEND);
    }

    static java.util.Map<String, Object> objectRequestStats(long[] counts) {
        long[] buckets = Arrays.copyOfRange(counts, 4, 36);
        long[] upperBounds = new long[32];
        for (int i = 0; i < 31; i++) upperBounds[i] = 1L << i;
        upperBounds[31] = Long.MAX_VALUE;
        long samples = Arrays.stream(buckets).sum();
        return java.util.Map.of(
                "requests", counts[0], "failed", counts[1], "bytes", counts[2], "micros", counts[3],
                "latency_micros", java.util.Map.of(
                        "samples", samples, "bucket_counts", buckets,
                        "bucket_upper_bounds", upperBounds, "overflow_after", 1L << 30,
                        "p50_upper_bound", latencyBound(buckets, samples, 50),
                        "p95_upper_bound", latencyBound(buckets, samples, 95),
                        "p99_upper_bound", latencyBound(buckets, samples, 99)));
    }

    private static long latencyBound(long[] buckets, long samples, int percentile) {
        if (samples == 0) return 0;
        long rank = (samples / 100) * percentile + ((samples % 100) * percentile + 99) / 100;
        for (int i = 0; i < buckets.length; i++) {
            rank -= buckets[i];
            if (rank <= 0) return i == 31 ? Long.MAX_VALUE : 1L << i;
        }
        throw new IllegalArgumentException("Incomplete request latency histogram");
    }

    @Override
    public synchronized void bind(String binding) throws IOException {
        if (binding == null || binding.isEmpty())
            throw new IllegalArgumentException("A Flush must name the trace the index holds");
        byte[] stored = store.get(TRACE_BINDING);
        if (stored == null)
            store.put(TRACE_BINDING, binding.getBytes(java.nio.charset.StandardCharsets.UTF_8), true);
        else if (!binding.equals(new String(stored, java.nio.charset.StandardCharsets.UTF_8)))
            throw new IOException(
                    "Index holds "
                            + new String(stored, java.nio.charset.StandardCharsets.UTF_8)
                            + ", not "
                            + binding);
    }

    @Override
    public String location() {
        return store.location();
    }

    @Override
    public long publishedVersion() throws IOException {
        byte[] value = store.get(PUBLISHED_VERSION);
        return value == null ? 0 : ByteBuffer.wrap(value).getLong();
    }

    @Override
    public void recordPublishedVersion(long version) throws IOException {
        store.put(PUBLISHED_VERSION, ByteBuffer.allocate(8).putLong(version).array(), true);
    }

    @Override
    public void close() throws IOException {
        store.close();
    }
}
