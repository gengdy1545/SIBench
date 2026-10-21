package org.sibench.capture;

import org.sibench.capture.IndexOperationTrace.Operation;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

/** Compact baseline slots and sparse full-key histories; not a PG MVCC or storage engine. */
public final class IndexReferenceReplay {
    private record Namespace(int table, int index) {}

    private record Entry(int table, int index, long key, String indexedValue) {}

    private record StoredVersion(long version, long value, boolean present) {}

    private record WritePosition(long version, long transaction, long ordinal) {}

    public record LookupResult(long key, long commitVersion, long value) {}

    private static final int SLOT_BITS = 12, SLOT_COUNT = 1 << SLOT_BITS;

    private static final class Slots {
        final long[] values = new long[SLOT_COUNT];
        final int[] buckets = new int[SLOT_COUNT];
        final long[] next = new long[SLOT_COUNT];
        final BitSet present = new BitSet(SLOT_COUNT);
    }

    private static final class Store {
        final NavigableMap<Long, Slots> pages = new TreeMap<>();
        final Map<String, Integer> bucketIds = new HashMap<>();
        final List<Long> heads = new ArrayList<>(List.of(0L));
        final Map<String, Set<Long>> added = new HashMap<>();

        Slots page(long key, boolean create) {
            long page = key >>> SLOT_BITS;
            return create ? pages.computeIfAbsent(page, ignored -> new Slots()) : pages.get(page);
        }

        void baseline(long key, String indexedValue, long value, boolean secondary)
                throws IOException {
            Slots slots = page(key, true);
            int offset = (int) (key & (SLOT_COUNT - 1));
            if (slots.present.get(offset)) throw new IOException("Duplicate baseline index key");
            slots.present.set(offset);
            slots.values[offset] = value;
            if (secondary) {
                int bucket =
                        bucketIds.computeIfAbsent(
                                indexedValue,
                                ignored -> {
                                    heads.add(0L);
                                    return heads.size() - 1;
                                });
                slots.buckets[offset] = bucket;
                slots.next[offset] = heads.get(bucket);
                heads.set(bucket, key);
            }
        }

        StoredVersion baseline(Entry entry) {
            Slots slots = page(entry.key(), false);
            if (slots == null) return null;
            int offset = (int) (entry.key() & (SLOT_COUNT - 1));
            if (!slots.present.get(offset)) return null;
            if (entry.index() > 0
                    && !Objects.equals(bucketIds.get(entry.indexedValue()), slots.buckets[offset]))
                return null;
            return new StoredVersion(0, slots.values[offset], true);
        }

        void candidates(String predicate, Set<Long> output) {
            Integer bucket = bucketIds.get(predicate);
            if (bucket != null)
                for (long key = heads.get(bucket); key != 0; ) {
                    output.add(key);
                    Slots slots = page(key, false);
                    key = slots.next[(int) (key & (SLOT_COUNT - 1))];
                }
            output.addAll(added.getOrDefault(predicate, Set.of()));
        }
    }

    private final boolean rowIds, secondaryIndexes;
    private final Map<Namespace, Store> stores = new LinkedHashMap<>();
    private final Map<Entry, NavigableMap<Long, StoredVersion>> history = new HashMap<>();
    private final Map<Entry, WritePosition> writePositions = new HashMap<>();
    private long relocationWrites, queries, checkedResults, checksum;
    private long selectedVersionChecksum, snapshotQueries, retainedVersions, lastAppliedVersion;

    public IndexReferenceReplay(boolean rowIds) {
        this(rowIds, true);
    }

    public IndexReferenceReplay(boolean rowIds, boolean secondaryIndexes) {
        this.rowIds = rowIds;
        this.secondaryIndexes = secondaryIndexes;
    }

    private static Entry identity(Operation operation) {
        return new Entry(
                operation.table(),
                operation.index(),
                operation.key(),
                operation.index() == 0 ? null : operation.indexedValue());
    }

    private Store store(Entry entry) {
        return stores.computeIfAbsent(
                new Namespace(entry.table(), entry.index()), ignored -> new Store());
    }

    public void accept(Operation operation) throws IOException {
        if (IndexOperationTrace.isControl(operation))
            throw new IOException("Source controls must not reach the index");
        if (operation.table() < 1
                || operation.index() < 0
                || (operation.index() > 0 && !secondaryIndexes))
            throw new IOException("Invalid or disabled index identity");
        if (operation.kind() == IndexOperationTrace.SEARCH) {
            validateSearch(operation);
            List<LookupResult> results;
            if (operation.index() == 0) {
                LookupResult result =
                        lookupPrimary(
                                operation.table(), operation.key(), operation.readTimestamp());
                results = result == null ? List.of() : List.of(result);
            } else
                results =
                        lookupSecondary(
                                operation.table(),
                                operation.index(),
                                operation.indexedValue(),
                                operation.readTimestamp());
            if (results.size() != operation.expectedCount())
                throw new IOException(
                        "Search count mismatch at source event "
                                + operation.sourceEvent()
                                + ": expected "
                                + operation.expectedCount()
                                + ", found "
                                + results.size());
            for (LookupResult result : results) checked(operation, result);
            checkedResults += results.size();
            queries++;
            if ((operation.flags() & IndexOperationTrace.SNAPSHOT_SEARCH) != 0) snapshotQueries++;
            return;
        }
        if (operation.key() < 1 || operation.version() < 0 || operation.rowId() < 1)
            throw new IOException("Invalid index key/version");
        Entry entry = identity(operation);
        long value =
                operation.kind() == IndexOperationTrace.LOGICAL_DELETE
                        ? 0
                        : rowIds
                                ? operation.rowId()
                                : locator(operation.file(), operation.position());
        if (operation.kind() == IndexOperationTrace.RELOCATE) {
            relocate(operation, entry, value);
            return;
        }
        if (operation.kind() == IndexOperationTrace.INSERT) {
            if (operation.version() != 0 || current(entry) != null)
                throw new IOException("Invalid baseline insertion");
            store(entry).baseline(entry.key(), entry.indexedValue(), value, entry.index() > 0);
            retainedVersions++;
            return;
        }
        if (!IndexOperationTrace.isMutation(operation) || operation.version() == 0)
            throw new IOException("Unsupported index write");
        StoredVersion previous = current(entry);
        if (operation.kind() == IndexOperationTrace.LOGICAL_DELETE
                && (previous == null || !previous.present()))
            throw new IOException("Deletion of absent index entry");
        if (previous != null
                && (operation.version() < previous.version()
                        || (operation.version() == previous.version()
                                && !orderedOverwrite(operation, entry))))
            throw new IOException("Non-increasing index version");
        var chain = history.computeIfAbsent(entry, ignored -> new TreeMap<>());
        if (previous != null) chain.putIfAbsent(previous.version(), previous);
        if (chain.put(
                        operation.version(),
                        new StoredVersion(
                                operation.version(),
                                value,
                                operation.kind() != IndexOperationTrace.LOGICAL_DELETE))
                == null) retainedVersions++;
        if (entry.index() > 0)
            store(entry)
                    .added
                    .computeIfAbsent(entry.indexedValue(), ignored -> new HashSet<>())
                    .add(entry.key());
        else store(entry);
        writePositions.put(
                entry,
                new WritePosition(
                        operation.version(),
                        operation.transactionId(),
                        operation.mutationOrdinal()));
        lastAppliedVersion = Math.max(lastAppliedVersion, operation.version());
    }

    private boolean orderedOverwrite(Operation operation, Entry entry) {
        WritePosition previous = writePositions.get(entry);
        return previous != null
                && operation.transactionId() > 0
                && previous.transaction() == operation.transactionId()
                && previous.version() == operation.version()
                && operation.mutationOrdinal() > previous.ordinal();
    }

    private StoredVersion current(Entry entry) {
        var chain = history.get(entry);
        if (chain != null && !chain.isEmpty()) return chain.lastEntry().getValue();
        Store store = stores.get(new Namespace(entry.table(), entry.index()));
        return store == null ? null : store.baseline(entry);
    }

    private StoredVersion visible(Entry entry, long timestamp) {
        var chain = history.get(entry);
        if (chain != null) {
            var selected = chain.floorEntry(timestamp);
            if (selected != null) return selected.getValue();
        }
        Store store = stores.get(new Namespace(entry.table(), entry.index()));
        return store == null ? null : store.baseline(entry);
    }

    public LookupResult lookupPrimary(int table, long key, long timestamp) throws IOException {
        if (table < 1 || key < 1 || timestamp < 0) throw new IOException("Invalid primary lookup");
        StoredVersion selected = visible(new Entry(table, 0, key, null), timestamp);
        return selected == null || !selected.present()
                ? null
                : new LookupResult(key, selected.version(), selected.value());
    }

    public LookupResult lookupEntry(
            int table, int index, long key, String indexedValue, long timestamp)
            throws IOException {
        if (table < 1 || index < 0 || key < 1 || timestamp < 0 || (index > 0 && !secondaryIndexes))
            throw new IOException("Invalid full index identity lookup");
        StoredVersion selected =
                visible(new Entry(table, index, key, index == 0 ? null : indexedValue), timestamp);
        return selected == null || !selected.present()
                ? null
                : new LookupResult(key, selected.version(), selected.value());
    }

    public List<LookupResult> lookupSecondary(
            int table, int index, String predicate, long timestamp) throws IOException {
        if (!secondaryIndexes || table < 1 || index < 1 || timestamp < 0)
            throw new IOException("Invalid secondary lookup");
        Store store = stores.get(new Namespace(table, index));
        if (store == null) return List.of();
        Set<Long> candidates = new TreeSet<>();
        store.candidates(predicate, candidates);
        var results = new ArrayList<LookupResult>();
        for (long key : candidates) {
            StoredVersion selected = visible(new Entry(table, index, key, predicate), timestamp);
            if (selected != null && selected.present())
                results.add(new LookupResult(key, selected.version(), selected.value()));
        }
        return results;
    }

    /** Numeric transfer lookup for historical trace callers. */
    public List<LookupResult> lookupSecondary(int index, int key, long timestamp)
            throws IOException {
        return lookupSecondary(5, index, Long.toString(key), timestamp);
    }

    private void validateSearch(Operation operation) throws IOException {
        int allowed =
                IndexOperationTrace.SNAPSHOT_SEARCH
                        | IndexOperationTrace.DERIVED_SEARCH
                        | IndexOperationTrace.TRANSACTION_LOCAL_SEARCH;
        if (operation.version() != 0
                || operation.readTimestamp() < 0
                || operation.expectedCount() < 0
                || (operation.flags() & ~allowed) != 0)
            throw new IOException("Invalid search metadata");
        if ((operation.flags() & IndexOperationTrace.TRANSACTION_LOCAL_SEARCH) != 0
                && (operation.transactionId() <= 0
                        || operation.mutationOrdinal() <= 0
                        || operation.readTimestamp() != lastAppliedVersion))
            throw new IOException("Invalid ordered transaction-local search");
    }

    private void checked(Operation operation, LookupResult result) {
        checksum ^= result.value();
        selectedVersionChecksum =
                Long.rotateLeft(selectedVersionChecksum, 1)
                        ^ result.commitVersion()
                        ^ Long.rotateLeft(result.key(), 17)
                        ^ ((long) operation.table() << 48)
                        ^ ((long) operation.index() << 40);
    }

    private void relocate(Operation operation, Entry entry, long value) throws IOException {
        StoredVersion selected = visible(entry, operation.version());
        if (selected == null || !selected.present() || selected.version() != operation.version())
            throw new IOException("Relocation requires an existing exact version");
        if (rowIds) {
            if (selected.value() != value) throw new IOException("Relocation changed RowID");
            return;
        }
        if (operation.version() == 0) {
            Slots slots = store(entry).page(entry.key(), false);
            slots.values[(int) (entry.key() & (SLOT_COUNT - 1))] = value;
            var chain = history.get(entry);
            if (chain != null) chain.put(0L, new StoredVersion(0, value, true));
        } else
            history.get(entry)
                    .put(operation.version(), new StoredVersion(operation.version(), value, true));
        relocationWrites++;
    }

    public long value(int table, int key) {
        StoredVersion selected = current(new Entry(table, 0, key, null));
        return selected == null ? 0 : selected.value();
    }

    public long stateChecksum() {
        long result = 0;
        for (var namespace : stores.entrySet()) {
            if (namespace.getKey().index() != 0) continue;
            int table = namespace.getKey().table();
            for (var page : namespace.getValue().pages.entrySet())
                for (int offset = page.getValue().present.nextSetBit(0);
                        offset >= 0;
                        offset = page.getValue().present.nextSetBit(offset + 1)) {
                    long key = (page.getKey() << SLOT_BITS) + offset;
                    StoredVersion current = current(new Entry(table, 0, key, null));
                    result ^= stateHash(table, key, current);
                }
            for (Entry entry : history.keySet())
                if (entry.table() == table
                        && entry.index() == 0
                        && namespace.getValue().baseline(entry) == null)
                    result ^= stateHash(table, entry.key(), current(entry));
        }
        return result;
    }

    private static long stateHash(int table, long key, StoredVersion version) {
        return Long.rotateLeft(key, 17)
                ^ Long.rotateLeft(version.value(), 31)
                ^ version.version()
                ^ ((long) table << 48)
                ^ (version.present() ? 1 : 0);
    }

    public long relocationWrites() {
        return relocationWrites;
    }

    public long queries() {
        return queries;
    }

    public long checkedResults() {
        return checkedResults;
    }

    public long checksum() {
        return checksum;
    }

    public long selectedVersionChecksum() {
        return selectedVersionChecksum;
    }

    public long snapshotQueries() {
        return snapshotQueries;
    }

    public long retainedVersions() {
        return retainedVersions;
    }

    public static long locator(long file, long position) throws IOException {
        if (file < 1 || file > Integer.MAX_VALUE || position < 0 || position > 0xffffffffL)
            throw new IOException("Reference packed locator width exceeded");
        return (file << 32) | position;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3)
            throw new IllegalArgumentException(
                    "IndexReferenceReplay BASELINE_OPS WORKLOAD_OPS REPORT");
        org.sibench.replay.ReferenceReplay.runFiles(
                Path.of(args[0]),
                Path.of(args[1]),
                org.sibench.replay.ReplayConfig.defaults(),
                Path.of(args[2]));
    }
}
