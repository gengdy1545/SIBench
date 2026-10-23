package org.sibench.capture;

import org.sibench.capture.IndexOperationTrace.Operation;
import org.sibench.capture.IndexOperationTrace.Range;
import org.sibench.trace.IndexKey;

import java.io.IOException;
import java.util.*;

/**
 * An ordered-key reference service, not a PG MVCC or storage engine. Each (table, index) holds
 * entries keyed by the concatenated encoded components (indexed value, PK), which sort as the
 * tuple: an immutable sorted baseline run plus sorted per-key version histories.
 */
public final class IndexReferenceReplay {
    public record LookupResult(IndexKey key, long commitVersion, long value) {}

    private record Namespace(int table, int index) {}

    private record StoredVersion(long version, long value, boolean present) {}

    private record WritePosition(long version, long transaction, long ordinal) {}

    /** Encoded (value, PK) bytes; {@code split} is where the PK starts. */
    private record Composite(byte[] bytes, int split) implements Comparable<Composite> {
        static Composite of(IndexKey value, IndexKey key) {
            byte[] pk = key.bytes();
            if (value == null) return new Composite(pk, 0);
            byte[] prefix = value.bytes(), bytes = Arrays.copyOf(prefix, prefix.length + pk.length);
            System.arraycopy(pk, 0, bytes, prefix.length, pk.length);
            return new Composite(bytes, prefix.length);
        }

        IndexKey key() throws IOException {
            return IndexKey.decode(Arrays.copyOfRange(bytes, split, bytes.length));
        }

        @Override
        public int compareTo(Composite other) {
            return Arrays.compareUnsigned(bytes, other.bytes);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof Composite c && Arrays.equals(bytes, c.bytes);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(bytes);
        }
    }

    private static final class Store {
        byte[][] keys = new byte[1024][];
        int[] splits = new int[1024];
        long[] values = new long[1024];
        int size;
        boolean sealed;
        final TreeMap<Composite, NavigableMap<Long, StoredVersion>> history = new TreeMap<>();

        void add(Composite entry, long value) throws IOException {
            if (sealed) throw new IOException("Baseline insertion after runtime writes or reads");
            if (size == keys.length) {
                int capacity = Math.multiplyExact(size, 2);
                keys = Arrays.copyOf(keys, capacity);
                splits = Arrays.copyOf(splits, capacity);
                values = Arrays.copyOf(values, capacity);
            }
            keys[size] = entry.bytes();
            splits[size] = entry.split();
            values[size++] = value;
        }

        /** Baseline rows arrive in PK order, so secondary runs are the only ones to sort. */
        void seal() throws IOException {
            if (sealed) return;
            sealed = true;
            boolean ordered = true;
            for (int i = 1; i < size && ordered; i++)
                ordered = Arrays.compareUnsigned(keys[i - 1], keys[i]) < 0;
            if (!ordered) {
                Integer[] order = new Integer[size];
                for (int i = 0; i < size; i++) order[i] = i;
                Arrays.parallelSort(order, (a, b) -> Arrays.compareUnsigned(keys[a], keys[b]));
                byte[][] sortedKeys = new byte[size][];
                int[] sortedSplits = new int[size];
                long[] sortedValues = new long[size];
                for (int i = 0; i < size; i++) {
                    sortedKeys[i] = keys[order[i]];
                    sortedSplits[i] = splits[order[i]];
                    sortedValues[i] = values[order[i]];
                }
                keys = sortedKeys;
                splits = sortedSplits;
                values = sortedValues;
            }
            for (int i = 1; i < size; i++)
                if (Arrays.compareUnsigned(keys[i - 1], keys[i]) == 0)
                    throw new IOException("Duplicate baseline index key");
        }

        /** First baseline position whose key is not below {@code bound}. */
        int lowerBound(byte[] bound) {
            int low = 0, high = size;
            while (low < high) {
                int middle = (low + high) >>> 1;
                if (Arrays.compareUnsigned(keys[middle], bound) < 0) low = middle + 1;
                else high = middle;
            }
            return low;
        }

        int find(Composite entry) {
            int position = lowerBound(entry.bytes());
            return position < size && Arrays.equals(keys[position], entry.bytes()) ? position : -1;
        }

        StoredVersion baseline(Composite entry) {
            int position = find(entry);
            return position < 0 ? null : new StoredVersion(0, values[position], true);
        }
    }

    private final boolean rowIds, secondaryIndexes;
    private final Map<Namespace, Store> stores = new LinkedHashMap<>();
    private final Map<Composite, WritePosition> writePositions = new HashMap<>();
    private long relocationWrites, queries, checkedResults, checksum;
    private long selectedVersionChecksum, snapshotQueries, retainedVersions, lastAppliedVersion;

    public IndexReferenceReplay(boolean rowIds) {
        this(rowIds, true);
    }

    public IndexReferenceReplay(boolean rowIds, boolean secondaryIndexes) {
        this.rowIds = rowIds;
        this.secondaryIndexes = secondaryIndexes;
    }

    private Store store(int table, int index) {
        return stores.computeIfAbsent(new Namespace(table, index), ignored -> new Store());
    }

    public void accept(Operation operation) throws IOException {
        if (!groupMembers.isEmpty() && !operation.grouped())
            throw new IOException("Search group interrupted before its last member");
        if (IndexOperationTrace.isControl(operation))
            throw new IOException("Source controls must not reach the index");
        if (operation.table() < 1
                || operation.index() < 0
                || (operation.index() > 0 && !secondaryIndexes))
            throw new IOException("Invalid or disabled index identity");
        if (operation.kind() == IndexOperationTrace.SEARCH) {
            validateSearch(operation);
            List<LookupResult> results = results(operation);
            if (operation.grouped()) {
                group(operation, results);
                return;
            }
            expect(operation, results.size());
            for (LookupResult result : results) checked(operation, result);
            checkedResults += results.size();
            queries++;
            if ((operation.flags() & IndexOperationTrace.SNAPSHOT_SEARCH) != 0) snapshotQueries++;
            return;
        }
        if (operation.key() == null || operation.version() < 0 || operation.rowId() < 1)
            throw new IOException("Invalid index key/version");
        Store store = store(operation.table(), operation.index());
        Composite entry =
                Composite.of(
                        operation.index() == 0 ? null : operation.indexedValue(), operation.key());
        long value =
                operation.kind() == IndexOperationTrace.LOGICAL_DELETE
                        ? 0
                        : rowIds
                                ? operation.rowId()
                                : locator(operation.file(), operation.position());
        if (operation.kind() == IndexOperationTrace.INSERT) {
            if (operation.version() != 0) throw new IOException("Invalid baseline insertion");
            store.add(entry, value);
            retainedVersions++;
            return;
        }
        store.seal();
        if (operation.kind() == IndexOperationTrace.RELOCATE) {
            relocate(operation, store, entry, value);
            return;
        }
        if (!IndexOperationTrace.isMutation(operation) || operation.version() == 0)
            throw new IOException("Unsupported index write");
        StoredVersion previous = current(store, entry);
        if (operation.kind() == IndexOperationTrace.LOGICAL_DELETE
                && (previous == null || !previous.present()))
            throw new IOException("Deletion of absent index entry");
        if (previous != null
                && (operation.version() < previous.version()
                        || (operation.version() == previous.version()
                                && !orderedOverwrite(operation, entry))))
            throw new IOException("Non-increasing index version");
        var chain = store.history.computeIfAbsent(entry, ignored -> new TreeMap<>());
        if (previous != null) chain.putIfAbsent(previous.version(), previous);
        if (chain.put(
                        operation.version(),
                        new StoredVersion(
                                operation.version(),
                                value,
                                operation.kind() != IndexOperationTrace.LOGICAL_DELETE))
                == null) retainedVersions++;
        writePositions.put(
                entry,
                new WritePosition(
                        operation.version(),
                        operation.transactionId(),
                        operation.mutationOrdinal()));
        lastAppliedVersion = Math.max(lastAppliedVersion, operation.version());
    }

    private boolean orderedOverwrite(Operation operation, Composite entry) {
        WritePosition previous = writePositions.get(entry);
        return previous != null
                && operation.transactionId() > 0
                && previous.transaction() == operation.transactionId()
                && previous.version() == operation.version()
                && operation.mutationOrdinal() > previous.ordinal();
    }

    private static StoredVersion current(Store store, Composite entry) {
        var chain = store.history.get(entry);
        if (chain != null && !chain.isEmpty()) return chain.lastEntry().getValue();
        return store.baseline(entry);
    }

    private static StoredVersion visible(Store store, Composite entry, long timestamp) {
        var chain = store.history.get(entry);
        if (chain != null) {
            var selected = chain.floorEntry(timestamp);
            // A history holds the baseline version once the key has been written.
            return selected == null ? null : selected.getValue();
        }
        return store.baseline(entry);
    }

    private StoredVersion visible(int table, int index, Composite entry, long timestamp)
            throws IOException {
        Store store = stores.get(new Namespace(table, index));
        if (store == null) return null;
        store.seal();
        return visible(store, entry, timestamp);
    }

    public LookupResult lookupPrimary(int table, IndexKey key, long timestamp) throws IOException {
        if (table < 1 || key == null || timestamp < 0)
            throw new IOException("Invalid primary lookup");
        StoredVersion selected = visible(table, 0, Composite.of(null, key), timestamp);
        return selected == null || !selected.present()
                ? null
                : new LookupResult(key, selected.version(), selected.value());
    }

    public LookupResult lookupEntry(
            int table, int index, IndexKey key, IndexKey indexedValue, long timestamp)
            throws IOException {
        if (table < 1
                || index < 0
                || key == null
                || timestamp < 0
                || (index > 0 && (!secondaryIndexes || indexedValue == null)))
            throw new IOException("Invalid full index identity lookup");
        StoredVersion selected =
                visible(table, index, Composite.of(index == 0 ? null : indexedValue, key), timestamp);
        return selected == null || !selected.present()
                ? null
                : new LookupResult(key, selected.version(), selected.value());
    }

    public List<LookupResult> lookupSecondary(
            int table, int index, IndexKey predicate, long timestamp) throws IOException {
        if (!secondaryIndexes || index < 1) throw new IOException("Invalid secondary lookup");
        return search(table, index, Range.equal(predicate), timestamp);
    }

    /**
     * Visible entries of one index whose leading component lies in {@code range}, in key order,
     * stopping at the range limit. Returned keys are PKs.
     */
    public List<LookupResult> search(int table, int index, Range range, long timestamp)
            throws IOException {
        if (table < 1 || index < 0 || timestamp < 0 || (index > 0 && !secondaryIndexes))
            throw new IOException("Invalid index search");
        var results = new ArrayList<LookupResult>();
        Store store = stores.get(new Namespace(table, index));
        if (store == null) return results;
        store.seal();
        byte[] start =
                range.lower() == null
                        ? new byte[0]
                        : range.lowerInclusive()
                                ? range.lower().bytes()
                                : range.lower().prefixSuccessor();
        byte[] end =
                range.upper() == null
                        ? null
                        : range.upperInclusive()
                                ? range.upper().prefixSuccessor()
                                : range.upper().bytes();
        if (end != null && Arrays.compareUnsigned(start, end) >= 0) return results;
        int first = store.lowerBound(start), last = end == null ? store.size : store.lowerBound(end);
        NavigableMap<Composite, NavigableMap<Long, StoredVersion>> changed =
                end == null
                        ? store.history.tailMap(new Composite(start, 0), true)
                        : store.history.subMap(
                                new Composite(start, 0), true, new Composite(end, 0), false);
        if (range.descending()) changed = changed.descendingMap();
        var overlay = changed.entrySet().iterator();
        var pending = overlay.hasNext() ? overlay.next() : null;
        int step = range.descending() ? -1 : 1;
        int position = range.descending() ? last - 1 : first;
        while (range.limit() == 0 || results.size() < range.limit()) {
            boolean baselineLeft = position >= first && position < last;
            if (!baselineLeft && pending == null) break;
            int order =
                    !baselineLeft
                            ? 1
                            : pending == null
                                    ? -1
                                    : step
                                            * Arrays.compareUnsigned(
                                                    store.keys[position], pending.getKey().bytes());
            StoredVersion selected;
            Composite entry;
            if (order < 0) {
                entry = new Composite(store.keys[position], store.splits[position]);
                selected = new StoredVersion(0, store.values[position], true);
                position += step;
            } else {
                entry = pending.getKey();
                var floor = pending.getValue().floorEntry(timestamp);
                selected = floor == null ? null : floor.getValue();
                if (order == 0) position += step;
                pending = overlay.hasNext() ? overlay.next() : null;
            }
            if (selected != null && selected.present())
                results.add(new LookupResult(entry.key(), selected.version(), selected.value()));
        }
        return results;
    }

    /** The entries one search names: a range, or the single entry a pinned search names. */
    private List<LookupResult> results(Operation operation) throws IOException {
        if (operation.key() == null)
            return search(
                    operation.table(), operation.index(), operation.range(), operation.readTimestamp());
        LookupResult entry =
                lookupEntry(
                        operation.table(),
                        operation.index(),
                        operation.key(),
                        operation.range().lower(),
                        operation.readTimestamp());
        return entry == null ? List.of() : List.of(entry);
    }

    private static void expect(Operation operation, long found) throws IOException {
        if (found != operation.expectedCount())
            throw new IOException(
                    "Search count mismatch at source event "
                            + operation.sourceEvent()
                            + ": expected "
                            + operation.expectedCount()
                            + ", found "
                            + found);
    }

    /** The members of the open search group, with each member's results. */
    private final List<Operation> groupMembers = new ArrayList<>();
    private final List<List<LookupResult>> groupResults = new ArrayList<>();

    /**
     * Collects one group member; the last member combines the members' results by primary key and
     * checks the group's count. An intersection's members may search different indexes of one
     * table; a union's members are points of one index.
     */
    private void group(Operation operation, List<LookupResult> results) throws IOException {
        if (operation.expansionPosition() != groupMembers.size())
            throw new IOException("Search group member out of order");
        if (!groupMembers.isEmpty()) {
            Operation first = groupMembers.get(0);
            if (operation.sourceEvent() != first.sourceEvent()
                    || operation.table() != first.table()
                    || groupFlags(operation) != groupFlags(first)
                    || operation.readTimestamp() != first.readTimestamp()
                    || operation.expectedCount() != first.expectedCount()
                    || operation.expansionSize() != first.expansionSize()
                    || operation.transactionId() != first.transactionId()
                    || operation.mutationOrdinal() != first.mutationOrdinal()
                    || operation.workUnitId() != first.workUnitId()
                    || (operation.flags() & IndexOperationTrace.UNION_GROUP) != 0
                            && operation.index() != first.index())
                throw new IOException("Inconsistent search group member");
        }
        groupMembers.add(operation);
        groupResults.add(results);
        if (groupMembers.size() < operation.expansionSize()) return;
        boolean intersect = (operation.flags() & IndexOperationTrace.INTERSECT_GROUP) != 0;
        Map<IndexKey, LookupResult> combined = new LinkedHashMap<>();
        for (LookupResult result : groupResults.get(0)) combined.put(result.key(), result);
        for (int i = 1; i < groupResults.size(); i++) {
            var keys = new HashSet<IndexKey>();
            for (LookupResult result : groupResults.get(i)) {
                keys.add(result.key());
                if (!intersect) combined.putIfAbsent(result.key(), result);
            }
            if (intersect) combined.keySet().retainAll(keys);
        }
        groupMembers.clear();
        groupResults.clear();
        expect(operation, combined.size());
        for (LookupResult result : combined.values()) checked(operation, result);
        checkedResults += combined.size();
        queries++;
        if ((operation.flags() & IndexOperationTrace.SNAPSHOT_SEARCH) != 0) snapshotQueries++;
    }

    /** The flags every member of a group shares; range flags belong to each member. */
    private static int groupFlags(Operation operation) {
        return operation.flags()
                & ~(IndexOperationTrace.LOWER_INCLUSIVE
                        | IndexOperationTrace.UPPER_INCLUSIVE
                        | IndexOperationTrace.DESCENDING);
    }

    private void validateSearch(Operation operation) throws IOException {
        if (operation.version() != 0
                || operation.readTimestamp() < 0
                || operation.expectedCount() < 0
                || (operation.flags() & ~IndexOperationTrace.SEARCH_FLAGS) != 0)
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
                        ^ Long.rotateLeft(result.key().hashCode(), 17)
                        ^ ((long) operation.table() << 48)
                        ^ ((long) operation.index() << 40);
    }

    private void relocate(Operation operation, Store store, Composite entry, long value)
            throws IOException {
        StoredVersion selected = visible(store, entry, operation.version());
        if (selected == null || !selected.present() || selected.version() != operation.version())
            throw new IOException("Relocation requires an existing exact version");
        if (rowIds) {
            if (selected.value() != value) throw new IOException("Relocation changed RowID");
            return;
        }
        var chain = store.history.get(entry);
        if (operation.version() == 0) {
            int position = store.find(entry);
            if (position >= 0) store.values[position] = value;
            if (chain != null) chain.put(0L, new StoredVersion(0, value, true));
        } else chain.put(operation.version(), new StoredVersion(operation.version(), value, true));
        relocationWrites++;
    }

    /** The current value of a primary key, or 0 when it has none. */
    public long value(int table, IndexKey key) throws IOException {
        Store store = stores.get(new Namespace(table, 0));
        if (store == null) return 0;
        store.seal();
        StoredVersion selected = current(store, Composite.of(null, key));
        return selected == null ? 0 : selected.value();
    }

    /** Current primary state of every table, independent of history layout. */
    public long stateChecksum() throws IOException {
        long result = 0;
        for (var namespace : stores.entrySet()) {
            if (namespace.getKey().index() != 0) continue;
            int table = namespace.getKey().table();
            Store store = namespace.getValue();
            store.seal();
            for (int i = 0; i < store.size; i++) {
                Composite entry = new Composite(store.keys[i], 0);
                result ^= stateHash(table, entry, current(store, entry));
            }
            for (var entry : store.history.keySet())
                if (store.baseline(entry) == null)
                    result ^= stateHash(table, entry, current(store, entry));
        }
        return result;
    }

    private static long stateHash(int table, Composite key, StoredVersion version) {
        return Long.rotateLeft(key.hashCode(), 17)
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

}
