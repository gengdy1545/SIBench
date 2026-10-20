package org.sibench.capture;

import org.sibench.capture.IndexOperationTrace.Operation;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;

/** Serial versioned correctness reference, not a PG MVCC or storage engine implementation. */
public final class IndexReferenceReplay {
    private static final int[] CAPACITY = {
        0, 300001, 302001, 302001, 302001, 6100001, 600001, 600001, 600001
    };
    private final boolean rowIds;
    private final boolean secondaryIndexes;
    private final boolean versionedReads;
    private final long[][] values = new long[9][], versions = new long[9][];
    private final BitSet[] present = new BitSet[9];
    private final int[][] heads = new int[3][], next = new int[3][], indexed = new int[3][];
    private final long[][] secondaryValues = new long[3][], secondaryVersions = new long[3][];
    private final BitSet[] secondaryPresent = new BitSet[3];
    private long relocationWrites, queries, checkedResults, checksum;
    private long selectedVersionChecksum, snapshotQueries, retainedVersions;
    private long lastAppliedVersion;

    private record Entry(int table, int index, long key) {}

    private record WritePosition(long version, long transactionId, long ordinal) {}

    private final Map<Entry, WritePosition> writePositions = new HashMap<>();
    // Baseline stays in dense slots; only mutated keys need sparse historical chains.
    private final Map<Entry, NavigableMap<Long, StoredVersion>> history = new HashMap<>();

    private record StoredVersion(long version, long value, boolean present) {}

    public record LookupResult(long key, long commitVersion, long value) {}

    public long relocationWrites() {
        return relocationWrites;
    }

    public long value(int table, int key) {
        return values[table][key];
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

    public long stateChecksum() {
        long result = 0;
        for (int table = 1; table <= 8; table++)
            for (int key = 1; key < CAPACITY[table]; key++)
                result =
                        Long.rotateLeft(result, 1)
                                ^ values[table][key]
                                ^ versions[table][key]
                                ^ (present[table].get(key) ? key : 0);
        return result;
    }

    public IndexReferenceReplay(boolean rowIds) {
        this(rowIds, true);
    }

    public IndexReferenceReplay(boolean rowIds, boolean secondaryIndexes) {
        this(rowIds, secondaryIndexes, true);
    }

    public IndexReferenceReplay(boolean rowIds, boolean secondaryIndexes, boolean versionedReads) {
        this.rowIds = rowIds;
        this.secondaryIndexes = secondaryIndexes;
        this.versionedReads = versionedReads;
        for (int table = 1; table <= 8; table++) {
            values[table] = new long[CAPACITY[table]];
            versions[table] = new long[CAPACITY[table]];
            present[table] = new BitSet(CAPACITY[table]);
        }
        if (secondaryIndexes) {
            for (int index = 1; index <= 2; index++) {
                heads[index] = new int[302001];
                next[index] = new int[6100001];
                indexed[index] = new int[6100001];
                secondaryValues[index] = new long[6100001];
                secondaryVersions[index] = new long[6100001];
                secondaryPresent[index] = new BitSet(6100001);
            }
        }
    }

    public void accept(Operation o) throws IOException {
        if (IndexOperationTrace.isControl(o))
            throw new IOException("Source transaction controls must not reach the index");
        int table = o.table(), key = Math.toIntExact(o.key());
        if (table < 1 || table > 8 || o.index() < 0 || o.index() > 2)
            throw new IOException("Invalid reference index identity");
        if (key < 1 || key >= CAPACITY[table])
            throw new IOException("Invalid key or PoC key capacity exceeded");
        if (o.version() < 0
                || (o.kind() == IndexOperationTrace.INSERT && o.version() != 0)
                || ((o.kind() == IndexOperationTrace.UPSERT
                                || o.kind() == IndexOperationTrace.LOGICAL_DELETE)
                        && o.version() == 0)) throw new IOException("Invalid logical version");
        if (o.index() > 0) {
            if (!secondaryIndexes)
                throw new IOException("Secondary operation dispatched while disabled");
            if (table != 5) throw new IOException("Unsupported secondary index");
            acceptSecondary(o, key);
            return;
        }
        if (o.kind() == IndexOperationTrace.SEARCH) {
            validateSearch(o);
            long count = 0;
            LookupResult result = lookupPrimary(table, key, o.readTimestamp());
            if (result != null) {
                count = 1;
                checked(o, result);
            }
            if (count != o.expectedCount())
                throw new IOException("Search count mismatch at source event " + o.sourceEvent());
            checkedResults += count;
            queries++;
            if ((o.flags() & IndexOperationTrace.SNAPSHOT_SEARCH) != 0) snapshotQueries++;
            return;
        }
        long value =
                o.kind() == IndexOperationTrace.LOGICAL_DELETE
                        ? 0
                        : rowIds ? o.rowId() : locator(o.file(), o.position());
        if (o.kind() == IndexOperationTrace.RELOCATE) {
            if (versionedReads) {
                relocate(o, value);
                return;
            }
            if (!present[table].get(key) || versions[table][key] != o.version())
                throw new IOException("Relocation version mismatch");
            if (!rowIds) {
                values[table][key] = value;
                relocationWrites++;
            } else if (values[table][key] != value)
                throw new IOException("Relocation changed RowID");
            return;
        }
        if (o.kind() != IndexOperationTrace.INSERT
                && o.kind() != IndexOperationTrace.UPSERT
                && o.kind() != IndexOperationTrace.LOGICAL_DELETE)
            throw new IOException("Unsupported reference operation");
        boolean existed = present[table].get(key);
        if (o.kind() == IndexOperationTrace.LOGICAL_DELETE && !existed)
            throw new IOException("Deletion of absent primary key");
        if (o.kind() == IndexOperationTrace.INSERT && existed)
            throw new IOException("Duplicate baseline key");
        if ((existed || versions[table][key] != 0)
                && (o.version() < versions[table][key]
                        || (o.version() == versions[table][key] && !orderedOverwrite(o))))
            throw new IOException("Non-increasing logical version");
        remember(o, value, present[table].get(key), values[table][key], versions[table][key]);
        present[table].set(key, o.kind() != IndexOperationTrace.LOGICAL_DELETE);
        values[table][key] = value;
        versions[table][key] = o.version();
        recordWrite(o);
    }

    private void acceptSecondary(Operation o, int key) throws IOException {
        int index = o.index();
        if (o.kind() == IndexOperationTrace.SEARCH) {
            validateSearch(o);
            if (key >= heads[index].length) throw new IOException("Invalid secondary search");
            long count = 0;
            for (LookupResult result : lookupSecondary(index, key, o.readTimestamp())) {
                count++;
                checked(o, result);
            }
            if (count != o.expectedCount())
                throw new IOException("Search count mismatch at source event " + o.sourceEvent());
            queries++;
            if ((o.flags() & IndexOperationTrace.SNAPSHOT_SEARCH) != 0) snapshotQueries++;
            checkedResults += count;
            return;
        }
        int account = Math.toIntExact(index == 1 ? o.secondarySource() : o.secondaryTarget());
        if (account < 1 || account >= heads[index].length)
            throw new IOException("Invalid transfer account");
        if (indexed[index][key] != 0 && indexed[index][key] != account)
            throw new IOException("Transfer index-column updates are outside this PoC");
        boolean existed = secondaryPresent[index].get(key);
        if (o.kind() == IndexOperationTrace.LOGICAL_DELETE && !existed)
            throw new IOException("Deletion of absent secondary entry");
        long value =
                o.kind() == IndexOperationTrace.LOGICAL_DELETE
                        ? 0
                        : rowIds ? o.rowId() : locator(o.file(), o.position());
        if (o.kind() == IndexOperationTrace.RELOCATE) {
            if (versionedReads) {
                if (indexed[index][key] != account)
                    throw new IOException("Secondary relocation identity mismatch");
                relocate(o, value);
                return;
            }
            if (!existed || secondaryVersions[index][key] != o.version())
                throw new IOException("Secondary relocation version mismatch");
            if (!rowIds) {
                secondaryValues[index][key] = value;
                relocationWrites++;
            } else if (secondaryValues[index][key] != value)
                throw new IOException("Relocation changed RowID");
            return;
        }
        if (o.kind() != IndexOperationTrace.INSERT
                && o.kind() != IndexOperationTrace.UPSERT
                && o.kind() != IndexOperationTrace.LOGICAL_DELETE)
            throw new IOException("Unsupported secondary operation");
        if (o.kind() == IndexOperationTrace.INSERT && existed)
            throw new IOException("Duplicate secondary baseline key");
        long previousVersion = secondaryVersions[index][key];
        if ((existed || previousVersion != 0)
                && (o.version() < previousVersion
                        || (o.version() == previousVersion && !orderedOverwrite(o))))
            throw new IOException("Non-increasing secondary version");
        if (indexed[index][key] == 0) {
            indexed[index][key] = account;
            next[index][key] = heads[index][account];
            heads[index][account] = key;
        }
        remember(o, value, existed, secondaryValues[index][key], previousVersion);
        secondaryPresent[index].set(key, o.kind() != IndexOperationTrace.LOGICAL_DELETE);
        secondaryValues[index][key] = value;
        secondaryVersions[index][key] = o.version();
        recordWrite(o);
    }

    private boolean orderedOverwrite(Operation o) {
        WritePosition previous = writePositions.get(identity(o));
        return previous != null
                && o.transactionId() > 0
                && previous.version() == o.version()
                && previous.transactionId() == o.transactionId()
                && o.mutationOrdinal() > previous.ordinal();
    }

    private void recordWrite(Operation o) {
        if (o.transactionId() > 0)
            writePositions.put(
                    identity(o),
                    new WritePosition(o.version(), o.transactionId(), o.mutationOrdinal()));
        lastAppliedVersion = Math.max(lastAppliedVersion, o.version());
    }

    private static Entry identity(Operation o) {
        return new Entry(o.table(), o.index(), o.key());
    }

    public LookupResult lookupPrimary(int table, int key, long readTimestamp) throws IOException {
        if (table < 1 || table > 8 || key < 1 || key >= CAPACITY[table] || readTimestamp < 0)
            throw new IOException("Invalid point lookup timestamp");
        StoredVersion selected = visible(new Entry(table, 0, key), readTimestamp);
        return selected == null || !selected.present()
                ? null
                : new LookupResult(key, selected.version(), selected.value());
    }

    public List<LookupResult> lookupSecondary(int index, int key, long readTimestamp)
            throws IOException {
        if (!secondaryIndexes
                || index < 1
                || index > 2
                || key < 1
                || key >= heads[index].length
                || readTimestamp < 0) throw new IOException("Invalid secondary lookup");
        List<LookupResult> results = new ArrayList<>();
        for (int id = heads[index][key]; id != 0; id = next[index][id]) {
            StoredVersion selected = visible(new Entry(5, index, id), readTimestamp);
            if (selected != null && selected.present())
                results.add(new LookupResult(id, selected.version(), selected.value()));
        }
        return results;
    }

    private void validateSearch(Operation o) throws IOException {
        if (o.version() != 0
                || o.readTimestamp() < 0
                || o.expectedCount() < 0
                || (o.flags()
                                & ~(IndexOperationTrace.SNAPSHOT_SEARCH
                                        | IndexOperationTrace.DERIVED_SEARCH
                                        | IndexOperationTrace.TRANSACTION_LOCAL_SEARCH))
                        != 0) throw new IOException("Invalid search metadata");
        if (!versionedReads && (o.flags() & IndexOperationTrace.SNAPSHOT_SEARCH) != 0)
            throw new IOException("Legacy projection has no transaction snapshot contract");
        if ((o.flags() & IndexOperationTrace.TRANSACTION_LOCAL_SEARCH) != 0
                && (o.transactionId() <= 0
                        || o.mutationOrdinal() <= 0
                        || o.readTimestamp() != lastAppliedVersion))
            throw new IOException("Invalid ordered transaction-local search");
    }

    private void checked(Operation o, LookupResult result) {
        checksum ^= result.value();
        selectedVersionChecksum =
                Long.rotateLeft(selectedVersionChecksum, 1)
                        ^ result.commitVersion()
                        ^ Long.rotateLeft(result.key(), 17)
                        ^ ((long) o.table() << 48)
                        ^ ((long) o.index() << 40);
    }

    private StoredVersion current(Entry entry) {
        int key = Math.toIntExact(entry.key());
        if (entry.index() == 0)
            return new StoredVersion(
                    versions[entry.table()][key],
                    values[entry.table()][key],
                    present[entry.table()].get(key));
        return new StoredVersion(
                secondaryVersions[entry.index()][key],
                secondaryValues[entry.index()][key],
                secondaryPresent[entry.index()].get(key));
    }

    private StoredVersion visible(Entry entry, long readTimestamp) {
        StoredVersion current = current(entry);
        if (!versionedReads || current.version() <= readTimestamp) return current;
        var chain = history.get(entry);
        var selected = chain == null ? null : chain.floorEntry(readTimestamp);
        return selected == null ? null : selected.getValue();
    }

    private void remember(
            Operation o, long value, boolean oldPresent, long oldValue, long oldVersion) {
        boolean newPresent = o.kind() != IndexOperationTrace.LOGICAL_DELETE;
        if (o.kind() == IndexOperationTrace.INSERT) {
            retainedVersions++;
            return;
        }
        if (!versionedReads) return;
        var chain = history.computeIfAbsent(identity(o), key -> new TreeMap<>());
        if (oldPresent || oldVersion != 0)
            chain.putIfAbsent(oldVersion, new StoredVersion(oldVersion, oldValue, oldPresent));
        if (chain.put(o.version(), new StoredVersion(o.version(), value, newPresent)) == null)
            retainedVersions++;
    }

    private void relocate(Operation o, long value) throws IOException {
        Entry entry = identity(o);
        StoredVersion latest = current(entry), selected;
        var chain = history.get(entry);
        selected =
                o.version() == latest.version()
                        ? latest
                        : chain == null ? null : chain.get(o.version());
        if (selected == null || !selected.present())
            throw new IOException("Relocation requires an existing exact committed version");
        if (rowIds) {
            if (selected.value() != value) throw new IOException("Relocation changed RowID");
            return;
        }
        if (chain != null) chain.put(o.version(), new StoredVersion(o.version(), value, true));
        if (o.version() == latest.version()) {
            int key = Math.toIntExact(o.key());
            if (o.index() == 0) values[o.table()][key] = value;
            else secondaryValues[o.index()][key] = value;
        }
        relocationWrites++;
    }

    public static long locator(long file, long position) throws IOException {
        if (file < 1 || file > Integer.MAX_VALUE || position < 0 || position > 0xffffffffL)
            throw new IOException("PoC locator width exceeded");
        return (file << 32) | position;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3)
            throw new IllegalArgumentException(
                    "IndexReferenceReplay BASELINE_OPS WORKLOAD_OPS REPORT");
        org.sibench.replay.ReferenceReplay.runFiles(
                Path.of(args[0]), Path.of(args[1]),
                org.sibench.replay.ReplayConfig.defaults(), Path.of(args[2]));
    }
}
