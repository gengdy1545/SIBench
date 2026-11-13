package org.sibench.adapters;

import org.sibench.config.ObjectStore;

import com.google.protobuf.ByteString;

import org.sibench.adapters.proto.*;
import org.sibench.state.ByteKeys;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Versioned opaque KV over an ordered store. Escaped keys delimit histories, including key prefixes. */
public final class VersionedIndexBackend implements IndexBackend {
    private static final int VERSION = 8;
    private static final byte[] VALUE_KIND = metadata("value_kind"),
            TRACE_BINDING = metadata("trace_binding");
    private final OrderedStore store;
    private final ValueKind valueKind;
    /** One JSON line per internal persistence cycle: the engine's state and object-storage requests so far. */
    private final Path engineStats;
    private long flushes;
    private volatile String bound;

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
    static VersionedIndexBackend cloud(Path path, ValueKind valueKind, ObjectStore objects, boolean wal)
            throws IOException {
        prepare(path, valueKind);
        return created(CloudRocksStore.create(path, objects, wal), valueKind, path);
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
    static VersionedIndexBackend openCloud(Path path, ValueKind valueKind, ObjectStore objects, boolean wal)
            throws IOException {
        Files.createDirectories(path.getParent());
        return reopened(CloudRocksStore.open(path, objects, wal), valueKind, path);
    }

    static VersionedIndexBackend slate(Path path, ValueKind valueKind, ObjectStore objects,
            boolean reopen, boolean wal) throws IOException {
        if (reopen) {
            Files.createDirectories(path.getParent());
            return reopened(SlateDbStore.open(path, objects, wal), valueKind, path);
        }
        prepare(path, valueKind);
        return created(SlateDbStore.create(path, objects, wal), valueKind, path);
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
            store.put(VALUE_KIND, new byte[] {valueKindByte(valueKind)});
        } catch (IOException | RuntimeException error) {
            store.close();
            throw error;
        }
        return new VersionedIndexBackend(store, valueKind, path);
    }

    private static VersionedIndexBackend reopened(OrderedStore store, ValueKind valueKind, Path path)
            throws IOException {
        byte[] stored = store.get(VALUE_KIND);
        if (stored == null || stored.length != 1 || stored[0] != valueKindByte(valueKind)) {
            store.close();
            throw new IOException("Reopened index does not hold " + valueKind + " values");
        }
        var backend = new VersionedIndexBackend(store, valueKind, path);
        byte[] prefix = new byte[] {0};
        try (var entries = store.cursor(prefix, ByteKeys.successor(prefix))) {
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

    private static byte valueKindByte(ValueKind kind) {
        return switch (kind) { case LOCATOR -> 1; case ROW_ID -> 2; };
    }

    private static byte[] metadata(String name) {
        byte[] text = name.getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        return ByteBuffer.allocate(1 + text.length).put((byte) 0).put(text).array();
    }

    @Override
    public IndexDescription describe() {
        return new IndexDescription(store.name(), valueKind, store.writeAck(), store.wal(), store.durability(), store.location());
    }

    @Override
    public void write(Mutation mutation) throws IOException {
        IndexExecution.validateMutation(mutation);
        boolean put = mutation.hasPut();
        ByteString key = put ? mutation.getPut().getKey() : mutation.getDelete().getKey();
        long ts = put ? mutation.getPut().getTs() : mutation.getDelete().getTs();
        store.put(versioned(entry(key), ts), value(put, mutation.getPut().getValue()));
    }

    @Override
    public void writeBatch(List<Mutation> mutations) throws IOException {
        var keys = new ArrayList<byte[]>(mutations.size());
        var values = new ArrayList<byte[]>(mutations.size());
        for (var mutation : mutations) {
            IndexExecution.validateMutation(mutation);
            boolean put = mutation.hasPut();
            keys.add(versioned(entry(put ? mutation.getPut().getKey() : mutation.getDelete().getKey()),
                    put ? mutation.getPut().getTs() : mutation.getDelete().getTs()));
            values.add(value(put, mutation.getPut().getValue()));
        }
        store.putAll(keys, values);
    }

    @Override
    public ByteString get(GetRequest get) throws IOException {
        IndexExecution.validateGet(get);
        try (var cursor = store.cursor(null, null)) {
            return visible(cursor, entry(get.getKey()), get.getReadTs());
        }
    }

    @Override
    public void scan(ScanRequest scan, ValueSink sink) throws IOException {
        IndexExecution.validateScan(scan);
        byte[] start = scan.hasStartKey() ? entry(scan.getStartKey()) : new byte[] {1};
        byte[] end = scan.hasEndKey() ? entry(scan.getEndKey()) : new byte[] {2};
        if (Arrays.compareUnsigned(start, end) >= 0) return;
        long found = 0, limit = scan.getLimit();
        try (var cursor = store.cursor(start, end)) {
            if (!scan.getReverse()) {
                cursor.seek(start);
                while (cursor.valid() && (limit == 0 || found < limit)) {
                    byte[] key = cursor.key(), entry = logical(key);
                    long ts = ~ByteBuffer.wrap(key, entry.length, VERSION).getLong();
                    if (ts > scan.getReadTs()) cursor.seek(versioned(entry, scan.getReadTs()));
                    ByteString result = visibleAt(cursor, entry);
                    if (result != null) { sink.accept(result); found++; }
                    if (cursor.valid() && sameEntry(cursor.key(), entry)) {
                        cursor.next();
                        if (cursor.valid() && sameEntry(cursor.key(), entry)) {
                            cursor.seek(versioned(entry, 0));
                            if (cursor.valid() && sameEntry(cursor.key(), entry)) cursor.next();
                        }
                    }
                }
            } else try (var versions = store.cursor(null, null)) {
                cursor.seekForPrev(end);
                while (cursor.valid() && Arrays.compareUnsigned(cursor.key(), end) >= 0) cursor.prev();
                while (cursor.valid() && (limit == 0 || found < limit)) {
                    byte[] entry = logical(cursor.key());
                    ByteString result = visible(versions, entry, scan.getReadTs());
                    if (result != null) { sink.accept(result); found++; }
                    cursor.seekForPrev(entry);
                }
            }
        }
    }

    private ByteString visible(OrderedStore.Cursor cursor, byte[] entry, long readTs) throws IOException {
        cursor.seek(versioned(entry, readTs));
        return visibleAt(cursor, entry);
    }

    private static boolean sameEntry(byte[] key, byte[] entry) {
        return key.length == entry.length + VERSION && Arrays.equals(key, 0, entry.length, entry, 0, entry.length);
    }

    private ByteString visibleAt(OrderedStore.Cursor cursor, byte[] entry) throws IOException {
        if (!cursor.valid() || !sameEntry(cursor.key(), entry)) return null;
        byte[] value = cursor.value();
        if (value.length == 0 || (value[0] != 0 && value[0] != 1) || value[0] == 0 && value.length != 1)
            throw new IOException("Invalid stored KV value");
        if (value[0] == 0) return null;
        ByteBuffer input = ByteBuffer.wrap(entry, 1, entry.length - 1);
        ByteKeys.decode(input);
        if (input.hasRemaining()) throw new IOException("Invalid stored KV key");
        return ByteString.copyFrom(value, 1, value.length - 1);
    }

    private static byte[] entry(ByteString key) {
        return ByteKeys.concat(new byte[] {1}, ByteKeys.encode(key.toByteArray()));
    }
    private static byte[] versioned(byte[] entry, long ts) {
        return ByteBuffer.allocate(entry.length + VERSION).put(entry).putLong(~ts).array();
    }
    private static byte[] logical(byte[] key) throws IOException {
        if (key.length < 3 + VERSION || key[0] != 1) throw new IOException("Invalid stored versioned key");
        return Arrays.copyOf(key, key.length - VERSION);
    }
    private static byte[] value(boolean present, ByteString value) {
        return present ? ByteKeys.concat(new byte[] {1}, value.toByteArray()) : new byte[] {0};
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
    public void bind(String binding) throws IOException {
        // Every harness call names its Trace; once accepted, the stored binding cannot change for this instance.
        if (binding != null && binding.equals(bound)) return;
        synchronized (this) {
            if (binding == null || binding.isEmpty())
                throw new IllegalArgumentException("An index must name the trace it holds");
            byte[] stored = store.get(TRACE_BINDING);
            if (stored == null)
                store.put(TRACE_BINDING, binding.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            else if (!binding.equals(new String(stored, java.nio.charset.StandardCharsets.UTF_8)))
                throw new IOException(
                        "Index holds "
                                + new String(stored, java.nio.charset.StandardCharsets.UTF_8)
                                + ", not "
                                + binding);
            bound = binding;
        }
    }

    @Override
    public void close() throws IOException {
        store.close();
    }
}
