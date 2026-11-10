package org.sibench.adapters;

import com.google.protobuf.ByteString;

import org.sibench.adapters.proto.Bound;
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
 * A versioned index on one ordered byte store, in the trace contract's KV
 * layout:
 * {@code table(u32) ++ index(u32) ++ [indexed value] ++ primary key ++ ~version(u64)}, all big
 * endian, so the versions of one entry sort newest first right after each other. The value is
 * {@code present(1) ++ indexed value length(u32) ++ file(i64) ++ position(i64) ++ row id(i64)}; a
 * logical delete is a version that is not present.
 *
 * <p>All writes have the same acknowledgment contract; persistence is asynchronous and its level is
 * declared by the store. Table 0, which no entry uses, holds the index's value kind and its
 * Trace binding, so a reopened index is checked before reuse.
 */
public final class VersionedIndexBackend implements IndexBackend {
    private static final int HEADER = 8, VERSION = 8, VALUE = 1 + 4 + 8 + 8 + 8;
    private static final byte[] VALUE_KIND = metadata("value_kind"),
            TRACE_BINDING = metadata("trace_binding");
    private final OrderedStore store;
    private final ValueKind valueKind;
    /** One JSON line per internal persistence cycle: the engine's state and object-storage requests so far. */
    private final Path engineStats;
    private long flushes;

    private VersionedIndexBackend(OrderedStore store, ValueKind valueKind, Path path) {
        this.store = store;
        this.valueKind = valueKind;
        this.engineStats = path.resolveSibling(path.getFileName() + ".engine-stats.jsonl");
    }

    /** A new, empty index in {@code path}; an existing directory is refused, never reused. */
    public static VersionedIndexBackend create(Path path, ValueKind valueKind) throws IOException {
        prepare(path, valueKind);
        return created(LocalRocksStore.create(path), valueKind, path);
    }

    /** A new, empty rocksdb-cloud index: local files in {@code path}, objects in the store. */
    static VersionedIndexBackend cloud(Path path, ValueKind valueKind, ObjectStore objects)
            throws IOException {
        prepare(path, valueKind);
        return created(CloudRocksStore.create(path, objects), valueKind, path);
    }

    /** An index this class created in {@code path}, reopened, e.g. after a restart. */
    public static VersionedIndexBackend open(Path path, ValueKind valueKind) throws IOException {
        if (!Files.isDirectory(path)) throw new IOException("No index directory: " + path);
        return reopened(LocalRocksStore.open(path), valueKind, path);
    }

    /**
     * A rocksdb-cloud index reopened from its local files, or, when {@code path} does not exist,
     * from the bucket alone (a cold start).
     */
    static VersionedIndexBackend openCloud(Path path, ValueKind valueKind, ObjectStore objects)
            throws IOException {
        Files.createDirectories(path.getParent());
        return reopened(CloudRocksStore.open(path, objects), valueKind, path);
    }

    static VersionedIndexBackend slate(Path path, ValueKind valueKind, ObjectStore objects,
            boolean reopen) throws IOException {
        if (reopen) {
            Files.createDirectories(path.getParent());
            return reopened(SlateDbStore.open(path, objects), valueKind, path);
        }
        prepare(path, valueKind);
        return created(SlateDbStore.create(path, objects), valueKind, path);
    }

    private static void prepare(Path path, ValueKind valueKind) throws IOException {
        if (Files.exists(path)) throw new IOException("Index directory already exists: " + path);
        if (valueKind != ValueKind.LOCATOR && valueKind != ValueKind.ROW_ID)
            throw new IOException("Unknown value kind");
        Files.createDirectories(path.getParent());
    }

    private static VersionedIndexBackend created(OrderedStore store, ValueKind valueKind, Path path)
            throws IOException {
        try {
            store.put(VALUE_KIND, new byte[] {(byte) valueKind.getNumber()}, true);
        } catch (IOException | RuntimeException error) {
            store.close();
            throw error;
        }
        return new VersionedIndexBackend(store, valueKind, path);
    }

    private static VersionedIndexBackend reopened(OrderedStore store, ValueKind valueKind, Path path)
            throws IOException {
        byte[] stored = store.get(VALUE_KIND);
        if (stored == null || stored.length != 1 || stored[0] != valueKind.getNumber()) {
            store.close();
            throw new IOException("Reopened index does not hold " + valueKind + " values");
        }
        var backend = new VersionedIndexBackend(store, valueKind, path);
        byte[] prefix = new byte[HEADER];
        try (var entries = store.cursor(prefix, successor(prefix))) {
            entries.seek(prefix);
            while (entries.valid()) {
                if (!Arrays.equals(entries.key(), VALUE_KIND) && !Arrays.equals(entries.key(), TRACE_BINDING))
                    throw new IOException("Unsupported index metadata");
                entries.next();
            }
        } catch (IOException | RuntimeException error) {
            store.close();
            throw error;
        }
        return backend;
    }

    private static byte[] metadata(String name) {
        byte[] text = name.getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        return ByteBuffer.allocate(HEADER + text.length).putInt(0).putInt(0).put(text).array();
    }

    @Override
    public IndexDescription describe() {
        return new IndexDescription(store.name(), valueKind, store.writeAck(), store.durability(), store.location());
    }

    @Override
    public void write(Write write) throws IOException {
        var key = write.getKey();
        IndexExecution.validateWrite(write, valueKind);
        byte[] entry = entry(key.getTable(), key.getIndex(), key.getIndexedValue(), key.getPrimaryKey());
        byte[] versioned = versioned(entry, write.getVersion());
        switch (write.getKind()) {
            case INSERT -> {
                if (write.getVersion() != 0) throw new IllegalArgumentException("Insert outside the baseline");
                store.put(versioned, value(true, key.getIndexedValue().size(), write.getValue()), true);
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
    public void writeBatch(java.util.List<Write> writes) throws IOException {
        var keys = new java.util.ArrayList<byte[]>(writes.size());
        var values = new java.util.ArrayList<byte[]>(writes.size());
        for (var write : writes) {
            IndexExecution.validateWrite(write, valueKind);
            if (write.getKind() == Write.Kind.RELOCATE)
                throw new IllegalArgumentException("Relocate is not a batch mutation");
            var key = write.getKey();
            keys.add(versioned(entry(key.getTable(), key.getIndex(), key.getIndexedValue(), key.getPrimaryKey()), write.getVersion()));
            boolean present = write.getKind() != Write.Kind.LOGICAL_DELETE;
            values.add(value(present, key.getIndexedValue().size(), present ? write.getValue() : Value.getDefaultInstance()));
        }
        store.putAll(keys, values);
    }

    @Override
    public void search(Search search, ResultSink sink) throws IOException {
        if (!search.getUnknownFields().asMap().isEmpty() || !search.getLower().getUnknownFields().asMap().isEmpty()
                || !search.getUpper().getUnknownFields().asMap().isEmpty())
            throw new IllegalArgumentException("Unsupported search fields");
        if (search.getTable() < 1 || search.getIndex() < 0 || search.getReadVersion() < 0 || search.getLimit() < 0)
            throw new IllegalArgumentException("Invalid search table, index, version or limit");
        if (search.hasLower()) IndexExecution.component(search.getLower().getKey());
        if (search.hasUpper()) IndexExecution.component(search.getUpper().getKey());
        long found = 0;
        byte[] prefix = entry(search.getTable(), search.getIndex(), ByteString.EMPTY, ByteString.EMPTY);
        if (!search.getPinnedPrimaryKey().isEmpty()) {
            if (search.getIndex() == 0 || !search.hasLower() || !search.getLower().equals(search.getUpper()))
                throw new IllegalArgumentException("A pinned search names one secondary entry");
            byte[] entry = entry(search.getTable(), search.getIndex(), search.getLower().getKey(), search.getPinnedPrimaryKey());
            try (var iterator = store.cursor(null, null)) {
                Result result = visible(iterator, entry, search.getReadVersion());
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
        try (var iterator = store.cursor(start, end)) {
            if (!search.getDescending()) {
                iterator.seek(start);
                while (iterator.valid() && (limit == 0 || found < limit)) {
                    byte[] key = iterator.key();
                    byte[] entry = logical(key);
                    long version = ~ByteBuffer.wrap(key, entry.length, VERSION).getLong();
                    long selected = search.getReadVersion();
                    if (version > selected) iterator.seek(versioned(entry, selected));
                    Result result = visibleAt(iterator, entry, search.getReadVersion());
                    if (result != null) {
                        sink.accept(result);
                        found++;
                    }
                    // Most entries have one version. Keep long histories bounded by one skip seek.
                    if (iterator.valid() && sameEntry(iterator.key(), entry)) {
                        iterator.next();
                        if (iterator.valid() && sameEntry(iterator.key(), entry)) {
                            iterator.seek(versioned(entry, 0));
                            if (iterator.valid() && sameEntry(iterator.key(), entry)) iterator.next();
                        }
                    }
                }
            } else try (var versions = store.cursor(null, null)) {
                if (end == null) iterator.seekToLast();
                else iterator.seekForPrev(end);
                while (iterator.valid() && end != null && Arrays.compareUnsigned(iterator.key(), end) >= 0)
                    iterator.prev();
                while (iterator.valid() && (limit == 0 || found < limit)) {
                    byte[] entry = logical(iterator.key());
                    Result result = visible(versions, entry, search.getReadVersion());
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
     * The newest committed version of {@code entry} at or below the read version.
     */
    private Result visible(
            OrderedStore.Cursor versions, byte[] entry, long readVersion)
            throws IOException {
        byte[] selected = versioned(entry, readVersion);
        versions.seek(selected);
        return visibleAt(versions, entry, readVersion);
    }

    private static boolean sameEntry(byte[] key, byte[] entry) {
        return key.length == entry.length + VERSION
                && Arrays.equals(key, 0, entry.length, entry, 0, entry.length);
    }

    private Result visibleAt(
            OrderedStore.Cursor versions, byte[] entry, long readVersion)
            throws IOException {
        if (!versions.valid()) return null;
        byte[] key = versions.key();
        if (!sameEntry(key, entry)) return null;
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
            throw new IllegalArgumentException("An index must name the trace it holds");
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
    public void close() throws IOException {
        store.close();
    }
}
