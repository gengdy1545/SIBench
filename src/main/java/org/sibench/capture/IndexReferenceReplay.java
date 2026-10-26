package org.sibench.capture;

import org.sibench.capture.IndexOperationTrace.Operation;
import org.sibench.capture.IndexOperationTrace.Range;
import org.sibench.state.ExternalSort;
import org.sibench.state.SpillingMap;
import org.sibench.trace.IndexKey;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Supplier;

/**
 * An ordered-key reference service, not a PG MVCC or storage engine. Each (table, index) holds
 * entries keyed by the concatenated encoded components (indexed value, PK), which sort as the
 * tuple: an immutable sorted baseline plus an overlay of per-key version histories.
 *
 * <p>Memory is bounded. Baselines load into arrays until a shared entry budget is spent, after
 * which the largest still-loading baseline is sorted out of core into an immutable on-disk run;
 * version histories live in an ordered spilling map keyed by (entry, version). Behaviour does not
 * depend on where an entry happens to live.
 */
public final class IndexReferenceReplay implements AutoCloseable {
    /**
     * The entry a lookup selected. {@code value} is the RowID, or the FileID of a Locator whose row
     * position is {@code position}; both words are int64 and compared as a pair.
     */
    public record LookupResult(IndexKey key, long commitVersion, long value, long position) {
        public LookupResult(IndexKey key, long commitVersion, long value) {
            this(key, commitVersion, value, 0);
        }
    }

    /**
     * Where state may live and how much stays in memory. {@code directory} is where each model
     * makes its own working directory when it first spills (null: the system temporary directory);
     * the model removes that directory on {@link #close}.
     */
    public record Settings(Path directory, long baselineEntries, int overlayEntries, long sortBytes) {
        /**
         * The standard budgets. The system properties {@code sibench.reference.directory},
         * {@code .baseline_entries}, {@code .overlay_entries} and {@code .sort_bytes} override
         * them, for runs that must not or cannot hold the model in memory.
         */
        public static Settings defaults() {
            String directory = System.getProperty("sibench.reference.directory");
            return new Settings(
                    directory == null ? null : Path.of(directory),
                    Long.getLong("sibench.reference.baseline_entries", 64_000_000L),
                    Integer.getInteger("sibench.reference.overlay_entries", 500_000),
                    Long.getLong("sibench.reference.sort_bytes", 64L << 20));
        }
    }

    private record Namespace(int table, int index) {}

    private record StoredVersion(long version, long value, long position, boolean present) {}

    private record WritePosition(long version, long transaction, long ordinal) {}

    /** An entry of one index: the same bytes in two indexes are two different entries. */
    private record EntryId(Namespace namespace, Composite entry) {}

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
            return org.sibench.trace.KeyHash.of(bytes);
        }
    }

    // ---- storage ----

    /**
     * The files all stores of one model share, created when first needed. Every model works in a
     * directory of its own, below the configured one when there is one, so models that run one
     * after another or side by side never meet each other's files; it is removed on close.
     */
    private static final class Workspace implements Closeable {
        private final Settings settings;
        private Path root;

        Workspace(Settings settings) {
            this.settings = settings;
        }

        /**
         * A directory for one store's files. Nothing is created here: the first file written below
         * it creates the model's directory, so a model that never spills touches no disk.
         */
        Supplier<Path> directory(String name) {
            return () -> {
                try {
                    return root().resolve(name);
                } catch (IOException error) {
                    throw new UncheckedIOException(error);
                }
            };
        }

        private Path root() throws IOException {
            if (root == null) {
                if (settings.directory() == null)
                    root = Files.createTempDirectory("sibench-reference-");
                else {
                    Files.createDirectories(settings.directory());
                    root = Files.createTempDirectory(settings.directory(), "model-");
                }
            }
            return root;
        }

        @Override
        public void close() throws IOException {
            if (root == null || !Files.exists(root)) return;
            try (var paths = Files.walk(root)) {
                for (Path path : paths.sorted(Comparator.reverseOrder()).toList())
                    Files.deleteIfExists(path);
            }
        }
    }

    /** The in-memory baseline entries all loading stores of a model may hold together. */
    private static final class Budget {
        final long limit;
        long used;
        final List<Baseline> members = new ArrayList<>();

        Budget(long limit) {
            this.limit = limit;
        }

        /** Moves the largest loading in-memory baseline out of core until under the limit. */
        void reclaim() throws IOException {
            while (used > limit) {
                Baseline largest = null;
                for (Baseline member : members)
                    if (member.inMemory() && !member.sealed && (largest == null || member.size > largest.size))
                        largest = member;
                if (largest == null) return;
                largest.spill();
            }
        }
    }

    private static byte[] encode(long value, long position, int split) {
        return ByteBuffer.allocate(20).putLong(value).putLong(position).putInt(split).array();
    }

    private static byte[] encode(StoredVersion version, int split) {
        return ByteBuffer.allocate(21)
                .putLong(version.value())
                .putLong(version.position())
                .putInt(split)
                .put((byte) (version.present() ? 1 : 0))
                .array();
    }

    /** One entry of a baseline scan; the key bytes are the composite. */
    private interface BaselineScan extends Closeable {
        boolean next() throws IOException;

        byte[] key();

        int split();

        long value();

        long position();
    }

    /**
     * The immutable sorted entries of one index: arrays while they fit the shared budget, an
     * on-disk run once they do not. Entries are added in any order and sorted on first read.
     */
    private static final class Baseline implements Closeable {
        private final Budget budget;
        private final Workspace workspace;
        private final String name;
        private final long sortBytes;
        private final boolean locators;
        byte[][] keys = new byte[1024][];
        int[] splits = new int[1024];
        long[] values = new long[1024];
        long[] positions;
        int size;
        long count;
        boolean sealed;
        private ExternalSort sort;
        private SpillingMap run;

        Baseline(Budget budget, Workspace workspace, String name, long sortBytes, boolean locators) {
            this.budget = budget;
            this.workspace = workspace;
            this.name = name;
            this.sortBytes = sortBytes;
            this.locators = locators;
            if (locators) positions = new long[1024];
            budget.members.add(this);
        }

        boolean inMemory() {
            return sort == null && run == null;
        }

        void add(Composite entry, long value, long position) throws IOException {
            if (sealed) throw new IOException("Baseline insertion after runtime writes or reads");
            count++;
            if (!inMemory()) {
                sort.add(entry.bytes(), encode(value, position, entry.split()));
                return;
            }
            if (size == keys.length) {
                int capacity = Math.multiplyExact(size, 2);
                keys = Arrays.copyOf(keys, capacity);
                splits = Arrays.copyOf(splits, capacity);
                values = Arrays.copyOf(values, capacity);
                if (positions != null) positions = Arrays.copyOf(positions, capacity);
            }
            keys[size] = entry.bytes();
            splits[size] = entry.split();
            if (positions != null) positions[size] = position;
            values[size++] = value;
            budget.used++;
            if (budget.used > budget.limit) budget.reclaim();
        }

        /** Hands every in-memory entry to an external sort and keeps loading there. */
        void spill() throws IOException {
            sort = new ExternalSort(workspace.directory(name + "-sort"), sortBytes);
            for (int i = 0; i < size; i++)
                sort.add(keys[i], encode(values[i], positions == null ? 0 : positions[i], splits[i]));
            budget.used -= size;
            keys = new byte[0][];
            splits = new int[0];
            values = new long[0];
            positions = null;
            size = 0;
        }

        /** Sorts the entries; the baseline is read-only afterwards. */
        void seal() throws IOException {
            if (sealed) return;
            sealed = true;
            if (sort != null) {
                run = new SpillingMap(workspace.directory(name + "-run"), 1, 2);
                try (var sorted = sort.sorted();
                        var bulk = run.bulk()) {
                    byte[] previous = null;
                    while (sorted.hasNext()) {
                        byte[] key = sorted.peekKey();
                        if (previous != null && Arrays.equals(previous, key))
                            throw new IOException("Duplicate baseline index key");
                        bulk.append(key, sorted.peekValue());
                        previous = key;
                        sorted.advance();
                    }
                    bulk.finish();
                }
                sort.close();
                sort = null;
                return;
            }
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
                long[] sortedPositions = positions == null ? null : new long[size];
                for (int i = 0; i < size; i++) {
                    sortedKeys[i] = keys[order[i]];
                    sortedSplits[i] = splits[order[i]];
                    sortedValues[i] = values[order[i]];
                    if (sortedPositions != null) sortedPositions[i] = positions[order[i]];
                }
                keys = sortedKeys;
                splits = sortedSplits;
                values = sortedValues;
                positions = sortedPositions;
            }
            for (int i = 1; i < size; i++)
                if (Arrays.compareUnsigned(keys[i - 1], keys[i]) == 0)
                    throw new IOException("Duplicate baseline index key");
        }

        /** First in-memory position whose key is not below {@code bound}. */
        private int lowerBound(byte[] bound) {
            int low = 0, high = size;
            while (low < high) {
                int middle = (low + high) >>> 1;
                if (Arrays.compareUnsigned(keys[middle], bound) < 0) low = middle + 1;
                else high = middle;
            }
            return low;
        }

        StoredVersion find(Composite entry) throws IOException {
            if (run != null) {
                byte[] found = run.getBytes(entry.bytes());
                if (found == null) return null;
                ByteBuffer value = ByteBuffer.wrap(found);
                return new StoredVersion(0, value.getLong(), value.getLong(), true);
            }
            int position = lowerBound(entry.bytes());
            if (position >= size || !Arrays.equals(keys[position], entry.bytes())) return null;
            return new StoredVersion(
                    0, values[position], positions == null ? 0 : positions[position], true);
        }

        /** The entries with {@code start <= key < end} (a null end is open) in the given order. */
        BaselineScan scan(byte[] start, byte[] end, boolean descending) throws IOException {
            if (run != null) {
                var entries = run.scan(start, end, descending);
                return new BaselineScan() {
                    private ByteBuffer value;

                    @Override
                    public boolean next() throws IOException {
                        if (!entries.next()) return false;
                        value = ByteBuffer.wrap(entries.value());
                        return true;
                    }

                    @Override
                    public byte[] key() {
                        return entries.key();
                    }

                    @Override
                    public int split() {
                        return value.getInt(16);
                    }

                    @Override
                    public long value() {
                        return value.getLong(0);
                    }

                    @Override
                    public long position() {
                        return value.getLong(8);
                    }

                    @Override
                    public void close() throws IOException {
                        entries.close();
                    }
                };
            }
            int first = lowerBound(start), last = end == null ? size : lowerBound(end);
            int step = descending ? -1 : 1;
            return new BaselineScan() {
                private int at = descending ? last : first - 1;

                @Override
                public boolean next() {
                    at += step;
                    return descending ? at >= first : at < last;
                }

                @Override
                public byte[] key() {
                    return keys[at];
                }

                @Override
                public int split() {
                    return splits[at];
                }

                @Override
                public long value() {
                    return values[at];
                }

                @Override
                public long position() {
                    return positions == null ? 0 : positions[at];
                }

                @Override
                public void close() {}
            };
        }

        @Override
        public void close() throws IOException {
            if (sort != null) sort.close();
            if (run != null) run.close();
            budget.members.remove(this);
            if (inMemory()) budget.used -= size;
        }
    }

    /**
     * The version histories of one index: an ordered map from (entry, version) to the stored
     * value. An entry with any key here has a history, whose versions at and below a timestamp
     * decide what that timestamp sees; an entry without one reads from the baseline.
     */
    private static final class Overlay implements Closeable {
        final SpillingMap map;

        Overlay(Workspace workspace, String name, int memoryEntries) throws IOException {
            map = new SpillingMap(workspace.directory(name + "-over"), memoryEntries, 4);
        }

        boolean empty() {
            return map.memoryEntries() == 0 && map.runCount() == 0;
        }

        static byte[] key(Composite entry, long version) {
            return ByteBuffer.allocate(entry.bytes().length + 8)
                    .put(entry.bytes())
                    .putLong(version)
                    .array();
        }

        static boolean same(byte[] key, Composite entry) {
            int length = entry.bytes().length;
            return key.length == length + 8 && Arrays.equals(key, 0, length, entry.bytes(), 0, length);
        }

        static StoredVersion decode(byte[] key, byte[] value) {
            long version = ByteBuffer.wrap(key).getLong(key.length - 8);
            ByteBuffer v = ByteBuffer.wrap(value);
            return new StoredVersion(version, v.getLong(), v.getLong(), value[20] == 1);
        }

        /** The latest version of the entry, or null when it has no history. */
        StoredVersion latest(Composite entry) throws IOException {
            if (empty()) return null;
            var found = map.floor(key(entry, Long.MAX_VALUE));
            return found != null && same(found.getKey(), entry)
                    ? decode(found.getKey(), found.getValue())
                    : null;
        }

        /** The version at or below the timestamp; {@code exists} tells a history from none. */
        StoredVersion at(Composite entry, long timestamp, boolean[] exists) throws IOException {
            exists[0] = false;
            if (empty()) return null;
            var found = map.floor(key(entry, timestamp));
            if (found != null && same(found.getKey(), entry)) {
                exists[0] = true;
                return decode(found.getKey(), found.getValue());
            }
            // Nothing at or below the timestamp: only later versions, or no history at all.
            exists[0] = latest(entry) != null;
            return null;
        }

        boolean has(Composite entry, long version) throws IOException {
            return map.getBytes(key(entry, version)) != null;
        }

        void put(Composite entry, StoredVersion version) throws IOException {
            map.putBytes(key(entry, version.version()), encode(version, entry.split()));
        }

        @Override
        public void close() throws IOException {
            map.close();
        }
    }

    private static final class Store implements Closeable {
        final Baseline baseline;
        final Overlay overlay;

        Store(Baseline baseline, Overlay overlay) {
            this.baseline = baseline;
            this.overlay = overlay;
        }

        void seal() throws IOException {
            baseline.seal();
        }

        /** The newest state of the entry: its latest version, else the baseline. */
        StoredVersion current(Composite entry) throws IOException {
            StoredVersion latest = overlay.latest(entry);
            return latest != null ? latest : baseline.find(entry);
        }

        /** The state a reader at the timestamp sees; null when the entry did not exist then. */
        StoredVersion visible(Composite entry, long timestamp) throws IOException {
            var exists = new boolean[1];
            StoredVersion selected = overlay.at(entry, timestamp, exists);
            return exists[0] ? selected : baseline.find(entry);
        }

        @Override
        public void close() throws IOException {
            baseline.close();
            overlay.close();
        }
    }

    private final boolean rowIds, secondaryIndexes;
    private final Settings settings;
    private final Workspace workspace;
    private final Budget budget;
    private final Map<Namespace, Store> stores = new LinkedHashMap<>();
    /** Where entries were last written, kept for the transaction being applied only. */
    private final Map<EntryId, WritePosition> writePositions = new HashMap<>();
    private long positionsTransaction;
    private long relocationWrites, queries, checkedResults, checksum;
    private long selectedVersionChecksum, snapshotQueries, retainedVersions, lastAppliedVersion;

    public IndexReferenceReplay(boolean rowIds) {
        this(rowIds, true);
    }

    public IndexReferenceReplay(boolean rowIds, boolean secondaryIndexes) {
        this(rowIds, secondaryIndexes, Settings.defaults());
    }

    public IndexReferenceReplay(boolean rowIds, boolean secondaryIndexes, Settings settings) {
        this.rowIds = rowIds;
        this.secondaryIndexes = secondaryIndexes;
        this.settings = settings;
        this.workspace = new Workspace(settings);
        this.budget = new Budget(settings.baselineEntries());
    }

    private Store store(int table, int index) throws IOException {
        Namespace key = new Namespace(table, index);
        Store store = stores.get(key);
        if (store == null) {
            String name = "t%d-i%d".formatted(table, index);
            store =
                    new Store(
                            new Baseline(budget, workspace, name, settings.sortBytes(), !rowIds),
                            new Overlay(workspace, name, settings.overlayEntries()));
            stores.put(key, store);
        }
        return store;
    }

    /** Baseline entries that live outside memory, for reporting. */
    public long spilledBaselineEntries() {
        long total = 0;
        for (Store store : stores.values()) if (!store.baseline.inMemory()) total += store.baseline.count;
        return total;
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
        EntryId id = new EntryId(new Namespace(operation.table(), operation.index()), entry);
        boolean delete = operation.kind() == IndexOperationTrace.LOGICAL_DELETE;
        if (!delete && !rowIds && (operation.file() < 1 || operation.position() < 0))
            throw new IOException("Invalid Locator");
        long value = delete ? 0 : rowIds ? operation.rowId() : operation.file();
        long position = delete || rowIds ? 0 : operation.position();
        if (operation.kind() == IndexOperationTrace.INSERT) {
            if (operation.version() != 0) throw new IOException("Invalid baseline insertion");
            store.baseline.add(entry, value, position);
            retainedVersions++;
            return;
        }
        store.seal();
        if (operation.kind() == IndexOperationTrace.RELOCATE) {
            relocate(operation, store, entry, value, position);
            return;
        }
        if (!IndexOperationTrace.isMutation(operation) || operation.version() == 0)
            throw new IOException("Unsupported index write");
        StoredVersion previous = store.current(entry);
        if (operation.kind() == IndexOperationTrace.LOGICAL_DELETE
                && (previous == null || !previous.present()))
            throw new IOException("Deletion of absent index entry");
        if (operation.transactionId() != positionsTransaction) {
            writePositions.clear();
            positionsTransaction = operation.transactionId();
        }
        if (previous != null
                && (operation.version() < previous.version()
                        || (operation.version() == previous.version()
                                && !orderedOverwrite(operation, id))))
            throw new IOException("Non-increasing index version");
        // The history keeps the state it replaces, so older readers still find it.
        if (previous != null && !store.overlay.has(entry, previous.version()))
            store.overlay.put(entry, previous);
        boolean existed = store.overlay.has(entry, operation.version());
        store.overlay.put(
                entry,
                new StoredVersion(
                        operation.version(),
                        value,
                        position,
                        operation.kind() != IndexOperationTrace.LOGICAL_DELETE));
        if (!existed) retainedVersions++;
        writePositions.put(
                id,
                new WritePosition(
                        operation.version(),
                        operation.transactionId(),
                        operation.mutationOrdinal()));
        lastAppliedVersion = Math.max(lastAppliedVersion, operation.version());
    }

    private boolean orderedOverwrite(Operation operation, EntryId id) {
        WritePosition previous = writePositions.get(id);
        return previous != null
                && operation.transactionId() > 0
                && previous.transaction() == operation.transactionId()
                && previous.version() == operation.version()
                && operation.mutationOrdinal() > previous.ordinal();
    }

    private StoredVersion visible(int table, int index, Composite entry, long timestamp)
            throws IOException {
        Store store = stores.get(new Namespace(table, index));
        if (store == null) return null;
        store.seal();
        return store.visible(entry, timestamp);
    }

    public LookupResult lookupPrimary(int table, IndexKey key, long timestamp) throws IOException {
        if (table < 1 || key == null || timestamp < 0)
            throw new IOException("Invalid primary lookup");
        StoredVersion selected = visible(table, 0, Composite.of(null, key), timestamp);
        return selected == null || !selected.present()
                ? null
                : new LookupResult(key, selected.version(), selected.value(), selected.position());
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
                : new LookupResult(key, selected.version(), selected.value(), selected.position());
    }

    public List<LookupResult> lookupSecondary(
            int table, int index, IndexKey predicate, long timestamp) throws IOException {
        if (!secondaryIndexes || index < 1) throw new IOException("Invalid secondary lookup");
        return search(table, index, Range.equal(predicate), timestamp);
    }

    /** The next entry of the overlay in scan order with the version a timestamp selects. */
    private static final class History {
        final byte[] entry;
        final int split;
        StoredVersion selected;

        History(byte[] entry, int split) {
            this.entry = entry;
            this.split = split;
        }
    }

    /**
     * Groups an overlay scan by entry. Versions of one entry arrive consecutively, ascending or
     * descending; the group selects the newest version at or below the timestamp.
     */
    private static final class HistoryScan implements Closeable {
        private final SpillingMap.Entries entries;
        private final long timestamp;
        private boolean pending;

        HistoryScan(SpillingMap.Entries entries, long timestamp) throws IOException {
            this.entries = entries;
            this.timestamp = timestamp;
            pending = entries.next();
        }

        History next() throws IOException {
            if (!pending) return null;
            byte[] first = entries.key();
            int length = first.length - 8;
            History history = new History(Arrays.copyOf(first, length), ByteBuffer.wrap(entries.value()).getInt(16));
            while (pending && entries.key().length - 8 == length
                    && Arrays.equals(entries.key(), 0, length, history.entry, 0, length)) {
                StoredVersion version = Overlay.decode(entries.key(), entries.value());
                if (version.version() <= timestamp
                        && (history.selected == null
                                || version.version() > history.selected.version()))
                    history.selected = version;
                pending = entries.next();
            }
            return history;
        }

        @Override
        public void close() throws IOException {
            entries.close();
        }
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
        boolean descending = range.descending();
        int step = descending ? -1 : 1;
        try (BaselineScan baseline = store.baseline.scan(start, end, descending);
                HistoryScan overlay =
                        new HistoryScan(
                                store.overlay.map.scan(start, end, descending), timestamp)) {
            boolean baselineLeft = baseline.next();
            History pending = overlay.next();
            while (range.limit() == 0 || results.size() < range.limit()) {
                if (!baselineLeft && pending == null) break;
                int order =
                        !baselineLeft
                                ? 1
                                : pending == null
                                        ? -1
                                        : step * Arrays.compareUnsigned(baseline.key(), pending.entry);
                StoredVersion selected;
                byte[] entry;
                int split;
                if (order < 0) {
                    entry = baseline.key();
                    split = baseline.split();
                    selected = new StoredVersion(0, baseline.value(), baseline.position(), true);
                    baselineLeft = baseline.next();
                } else {
                    entry = pending.entry;
                    split = pending.split;
                    selected = pending.selected;
                    if (order == 0) baselineLeft = baseline.next();
                    pending = overlay.next();
                }
                if (selected != null && selected.present())
                    results.add(
                            new LookupResult(
                                    new Composite(entry, split).key(),
                                    selected.version(),
                                    selected.value(),
                                    selected.position()));
            }
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
        checksum ^= word(result);
        selectedVersionChecksum =
                Long.rotateLeft(selectedVersionChecksum, 1)
                        ^ result.commitVersion()
                        ^ Long.rotateLeft(result.key().checksumWord(), 17)
                        ^ ((long) operation.table() << 48)
                        ^ ((long) operation.index() << 40);
    }

    /** One checksum word per result; equals the old packed Locator while both parts fit it. */
    private long word(LookupResult result) {
        return rowIds ? result.value() : (result.value() << 32) | result.position();
    }

    private void relocate(
            Operation operation, Store store, Composite entry, long value, long position)
            throws IOException {
        StoredVersion selected = store.visible(entry, operation.version());
        if (selected == null || !selected.present() || selected.version() != operation.version())
            throw new IOException("Relocation requires an existing exact version");
        if (rowIds) {
            if (selected.value() != value) throw new IOException("Relocation changed RowID");
            return;
        }
        // Baseline entries are immutable; the moved location becomes their version-0 history.
        store.overlay.put(entry, new StoredVersion(operation.version(), value, position, true));
        relocationWrites++;
    }

    /** The current RowID (or Locator FileID) of a primary key, or 0 when it has none. */
    public long value(int table, IndexKey key) throws IOException {
        StoredVersion selected = currentPrimary(table, key);
        return selected == null ? 0 : selected.value();
    }

    /** The current Locator row position of a primary key, or 0 when it has none. */
    public long position(int table, IndexKey key) throws IOException {
        StoredVersion selected = currentPrimary(table, key);
        return selected == null ? 0 : selected.position();
    }

    private StoredVersion currentPrimary(int table, IndexKey key) throws IOException {
        Store store = stores.get(new Namespace(table, 0));
        if (store == null) return null;
        store.seal();
        return store.current(Composite.of(null, key));
    }

    /** Current primary state of every table, independent of history layout. */
    public long stateChecksum() throws IOException {
        long result = 0;
        for (var namespace : stores.entrySet()) {
            if (namespace.getKey().index() != 0) continue;
            int table = namespace.getKey().table();
            Store store = namespace.getValue();
            store.seal();
            try (BaselineScan scan = store.baseline.scan(new byte[0], null, false)) {
                while (scan.next()) {
                    Composite entry = new Composite(scan.key(), 0);
                    result ^= stateHash(table, entry, store.current(entry));
                }
            }
            // Histories of keys that were never in the baseline.
            try (var entries = store.overlay.map.scan(null, null, false)) {
                byte[] group = null;
                byte[] lastKey = null, lastValue = null;
                while (true) {
                    boolean more = entries.next();
                    byte[] entry = more ? Arrays.copyOf(entries.key(), entries.key().length - 8) : null;
                    if (group != null && (!more || !Arrays.equals(group, entry))) {
                        Composite composite = new Composite(group, 0);
                        if (store.baseline.find(composite) == null)
                            result ^=
                                    stateHash(
                                            table,
                                            composite,
                                            Overlay.decode(lastKey, lastValue));
                    }
                    if (!more) break;
                    group = entry;
                    lastKey = entries.key();
                    lastValue = entries.value();
                }
            }
        }
        return result;
    }

    private static long stateHash(int table, Composite key, StoredVersion version) {
        // The checksum's key word is fixed, independent of the hash the maps use.
        return Long.rotateLeft(Arrays.hashCode(key.bytes), 17)
                ^ Long.rotateLeft(version.value(), 31)
                ^ Long.rotateLeft(version.position(), 43)
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

    /** Releases files and memory; a model that never spilled holds no files. */
    @Override
    public void close() throws IOException {
        IOException failure = null;
        for (Store store : stores.values()) {
            try {
                store.close();
            } catch (IOException error) {
                if (failure == null) failure = error;
                else failure.addSuppressed(error);
            }
        }
        stores.clear();
        try {
            workspace.close();
        } catch (IOException error) {
            if (failure == null) failure = error;
            else failure.addSuppressed(error);
        }
        if (failure != null) throw failure;
    }
}
