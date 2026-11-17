package org.sibench.capture;

import org.sibench.trace.IndexEntry;
import org.sibench.trace.IndexKey;

import java.io.IOException;

/** Normalized operation views shared by generation, Trace validation and replay. */
public final class IndexOperationTrace {
    public static final int INSERT = 1, SEARCH = 2, UPSERT = 3, LOGICAL_DELETE = 5;
    public static final int TRANSACTION_BEGIN = 6, TRANSACTION_COMMIT = 7, TRANSACTION_ABORT = 8;
    public static final int BUSINESS_INSERT = 1, BUSINESS_UPDATE = 2;
    public static final int SNAPSHOT_SEARCH = 1;
    public static final int TRANSACTION_LOCAL_SEARCH = 4;
    public static final int LOWER_INCLUSIVE = 8, UPPER_INCLUSIVE = 16, DESCENDING = 32;
    /**
     * A search group: consecutive searches of one source access whose expected count is the size
     * of the intersection (a BitmapAnd) or union (an = ANY array) of their results by primary key.
     * Every member carries the group's count; a member's own count is not a source fact.
     */
    public static final int INTERSECT_GROUP = 64, UNION_GROUP = 128;
    public static final int SEARCH_FLAGS =
            SNAPSHOT_SEARCH
                    | TRANSACTION_LOCAL_SEARCH
                    | LOWER_INCLUSIVE
                    | UPPER_INCLUSIVE
                    | DESCENDING
                    | INTERSECT_GROUP
                    | UNION_GROUP;
    public static final int BLOCK_RECORDS = 4096;

    private IndexOperationTrace() {}

    public static String kindName(int kind) {
        return switch (kind) {
            case INSERT -> "insert";
            case SEARCH -> "search";
            case UPSERT -> "upsert";
            case LOGICAL_DELETE -> "logical_delete";
            case TRANSACTION_BEGIN -> "transaction_begin";
            case TRANSACTION_COMMIT -> "transaction_commit";
            case TRANSACTION_ABORT -> "transaction_abort";
            default -> throw new IllegalArgumentException("Unknown operation kind: " + kind);
        };
    }

    /**
     * The searched interval of one index component: the PK for index 0, the indexed value
     * otherwise. A null bound is unbounded. Matches are visited in ascending (value, PK) order, or
     * descending; a positive limit stops after that many visible matches.
     */
    public record Range(
            IndexKey lower,
            boolean lowerInclusive,
            IndexKey upper,
            boolean upperInclusive,
            boolean descending,
            long limit) {
        public static Range equal(IndexKey value) {
            return new Range(value, true, value, true, false, 0);
        }

        public boolean contains(IndexKey value) {
            if (lower != null) {
                int order = value.compareTo(lower);
                if (order < 0 || order == 0 && !lowerInclusive) return false;
            }
            if (upper != null) {
                int order = value.compareTo(upper);
                if (order > 0 || order == 0 && !upperInclusive) return false;
            }
            return true;
        }

        public boolean point() {
            return lower != null && lower.equals(upper) && lowerInclusive && upperInclusive;
        }

        int flags() {
            return (lowerInclusive ? LOWER_INCLUSIVE : 0)
                    | (upperInclusive ? UPPER_INCLUSIVE : 0)
                    | (descending ? DESCENDING : 0);
        }
    }

    /**
     * Writes carry the encoded PK in {@code key} and, for a secondary index, the encoded indexed
     * value; searches carry a {@link Range} in lower/upper/flags/limit instead. A secondary point
     * search may also carry a PK in {@code key}: it then names exactly one index entry, the row a
     * scan its consumer stopped early returned among equal indexed values.
     *
     * One canonical index operation. SEARCH has {@code version} 0; a transaction-local read is
     * identified by {@code transactionId} and {@code mutationOrdinal}, and exported reads use explicit
     * private contexts. Writes use {@code version} as the new entry version.
     */
    public record Operation(
            long sourceEvent,
            int kind,
            int table,
            int index,
            int flags,
            IndexKey key,
            IndexKey indexedValue,
            long version,
            long file,
            long position,
            long rowId,
            long expectedCount,
            long transactionId,
            long mutationOrdinal,
            long readTimestamp,
            long workUnitId,
            int expansionPosition,
            int expansionSize,
            IndexKey lower,
            IndexKey upper,
            long limit,
            java.util.List<Operation> localWrites) {

        public Operation { localWrites = java.util.List.copyOf(localWrites); }

        public Range range() {
            return new Range(
                    lower,
                    (flags & LOWER_INCLUSIVE) != 0,
                    upper,
                    (flags & UPPER_INCLUSIVE) != 0,
                    (flags & DESCENDING) != 0,
                    limit);
        }

        public Operation withIndex(int id, IndexKey value, int operationKind) {
            return new Operation(
                    sourceEvent,
                    operationKind,
                    table,
                    id,
                    flags,
                    key,
                    value,
                    version,
                    operationKind == LOGICAL_DELETE ? 0 : file,
                    operationKind == LOGICAL_DELETE ? 0 : position,
                    rowId,
                    expectedCount,
                    transactionId,
                    mutationOrdinal,
                    readTimestamp,
                    workUnitId,
                    expansionPosition,
                    expansionSize,
                    lower,
                    upper,
                    limit, localWrites);
        }

        public Operation withExpansion(int ordinal, int count) {
            return new Operation(
                    sourceEvent,
                    kind,
                    table,
                    index,
                    flags,
                    key,
                    indexedValue,
                    version,
                    file,
                    position,
                    rowId,
                    expectedCount,
                    transactionId,
                    mutationOrdinal,
                    readTimestamp,
                    workUnitId,
                    ordinal,
                    count,
                    lower,
                    upper,
                    limit, localWrites);
        }

        public Operation withWorkUnitId(long id) {
            return new Operation(
                    sourceEvent,
                    kind,
                    table,
                    index,
                    flags,
                    key,
                    indexedValue,
                    version,
                    file,
                    position,
                    rowId,
                    expectedCount,
                    transactionId,
                    mutationOrdinal,
                    readTimestamp,
                    id,
                    expansionPosition,
                    expansionSize,
                    lower,
                    upper,
                    limit, localWrites);
        }

        public Operation withLocalWrites(java.util.List<Operation> writes) {
            return new Operation(sourceEvent, kind, table, index, flags, key, indexedValue, version, file, position, rowId,
                    expectedCount, transactionId, mutationOrdinal, readTimestamp, workUnitId, expansionPosition,
                    expansionSize, lower, upper, limit, writes);
        }

        public boolean containsWrite(Operation write) {
            return table == write.table() && index == write.index()
                    && range().contains(index == 0 ? write.key() : write.indexedValue())
                    && (key == null || key.equals(write.key()));
        }

        public boolean grouped() {
            return (flags & (INTERSECT_GROUP | UNION_GROUP)) != 0;
        }

        public Operation withTransaction(long id, long ordinal) {
            return new Operation(
                    sourceEvent,
                    kind,
                    table,
                    index,
                    flags,
                    key,
                    indexedValue,
                    version,
                    file,
                    position,
                    rowId,
                    expectedCount,
                    id,
                    ordinal,
                    readTimestamp,
                    workUnitId,
                    expansionPosition,
                    expansionSize,
                    lower,
                    upper,
                    limit, localWrites);
        }
    }

    /** A primary-key write; {@link IndexMutation} expands it to the secondary indexes. */
    public static Operation write(
            long source,
            int kind,
            int table,
            int flags,
            IndexKey key,
            long version,
            long file,
            long position,
            long rowId) {
        return new Operation(
                source, kind, table, 0, flags, key, null, version, file, position, rowId, 0, 0, 0,
                -1, 0, 0, 0, null, null, 0, java.util.List.of());
    }

    /**
     * One search. {@code pinned} is the PK of the single secondary entry a pinned point search
     * names, or null; {@code groupFlags} and the group position mark a member of a search group.
     */
    public static Operation search(
            long source,
            int table,
            int index,
            Range range,
            IndexKey pinned,
            long readTimestamp,
            long count,
            int flags,
            int groupPosition,
            int groupSize) {
        return new Operation(
                source,
                SEARCH,
                table,
                index,
                flags | range.flags(),
                pinned,
                null,
                0,
                0,
                0,
                0,
                count,
                0,
                0,
                readTimestamp,
                0,
                groupPosition,
                groupSize,
                range.lower(),
                range.upper(),
                range.limit(), java.util.List.of());
    }

    public static Operation search(
            long source, int table, int index, Range range, long readTimestamp, long count, int flags) {
        return search(source, table, index, range, null, readTimestamp, count, flags, 0, 0);
    }

    public static Operation transaction(long source, int kind, long id, long version, long rows) {
        return new Operation(
                source, kind, 0, 0, 0, null, null, version, 0, 0, 0, rows, id, 0, -1, 0, 0, 0,
                null, null, 0, java.util.List.of());
    }

    public static boolean isMutation(Operation o) {
        return o.kind() == UPSERT || o.kind() == LOGICAL_DELETE;
    }

    /** A version-zero baseline row entry, outside every source transaction. */
    public static boolean isBaselineInsert(Operation o) {
        return o.kind() == INSERT && o.version() == 0 && o.transactionId() == 0;
    }

    public static boolean isControl(Operation o) {
        return o.kind() == TRANSACTION_BEGIN || o.kind() == TRANSACTION_COMMIT || o.kind() == TRANSACTION_ABORT;
    }

    /** Format-level invariants of an exported operation, including its private read context. */
    static void validate(Operation o) throws IOException {
        validateShape(o);
        if (!o.localWrites().isEmpty()) {
            if (o.kind() != SEARCH || o.localWrites().size() > CommittedOperations.MAX_TRANSACTION_OPERATIONS)
                throw new IOException("Invalid private read context");
            com.google.protobuf.ByteString previous = null;
            for (var write : o.localWrites()) {
                if (!isMutation(write) || !write.localWrites().isEmpty() || write.version() != 0
                        || write.transactionId() != 0 || write.mutationOrdinal() != 0 || write.flags() != 0
                        || write.sourceEvent() != 0 || write.workUnitId() != 0 || write.expectedCount() != 0
                        || write.rowId() < 0 || write.file() < 0 || write.position() < 0
                        || write.expansionPosition() != 0 || write.expansionSize() != 1)
                    throw new IOException("Invalid private write");
                validateShape(write);
                if (!o.containsWrite(write)) throw new IOException("Private write outside its read scope");
                var encoded = new org.sibench.trace.IndexEntry(write.table(), write.index(), write.indexedValue(), write.key()).bytes();
                if (previous != null && com.google.protobuf.ByteString.unsignedLexicographicalComparator().compare(previous, encoded) >= 0)
                    throw new IOException("Private writes must have unique ordered keys");
                previous = encoded;
            }
        }
    }

    /**
     * The operation's own fields. A decoded Trace checks private writes against the encoded read
     * request instead (IndexExecution.validateGet/validateScan), so its reader needs only this.
     */
    static void validateShape(Operation o) throws IOException {
        boolean control = isControl(o), search = o.kind() == SEARCH;
        boolean valid =
                (o.kind() == INSERT || o.kind() == SEARCH || isMutation(o) || control)
                        && (control ? o.table() == 0 : o.table() >= 1 && o.table() <= IndexEntry.MAX_TABLE)
                        && o.index() >= 0
                        && o.index() <= IndexEntry.MAX_INDEX
                        && o.workUnitId() >= 0
                        && (!control || o.workUnitId() == 0 && o.index() == 0)
                        && (search
                                ? validSearch(o)
                                : o.readTimestamp() == -1
                                        && o.lower() == null
                                        && o.upper() == null
                                        && o.limit() == 0
                                        && (control
                                                ? o.key() == null && o.indexedValue() == null
                                                : o.key() != null
                                                        && (o.index() == 0)
                                                                == (o.indexedValue() == null)));
        if (!valid) throw new IOException("Invalid index operation");
    }

    static Operation privateWrite(Operation write) {
        return write(0, write.kind(), write.table(), 0, write.key(), 0,
                write.file(), write.position(), write.kind() == LOGICAL_DELETE ? 0 : write.rowId())
                .withIndex(write.index(), write.indexedValue(), write.kind()).withExpansion(0, 1);
    }

    private static boolean validSearch(Operation o) {
        int group = o.flags() & (INTERSECT_GROUP | UNION_GROUP);
        boolean pinned = o.key() != null;
        boolean local = (o.flags() & TRANSACTION_LOCAL_SEARCH) != 0;
        return (!local
                        || o.transactionId() > 0
                                && o.mutationOrdinal() > 0
                                && (o.flags() & SNAPSHOT_SEARCH) == 0)
                && o.version() == 0
                && o.readTimestamp() >= 0
                && o.indexedValue() == null
                && (o.flags() & ~SEARCH_FLAGS) == 0
                && (o.lower() != null || (o.flags() & LOWER_INCLUSIVE) == 0)
                && (o.upper() != null || (o.flags() & UPPER_INCLUSIVE) == 0)
                && o.limit() >= 0
                && o.expectedCount() >= 0
                && (group != 0 || o.index() != 0 || !o.range().point() || o.expectedCount() <= 1)
                && (o.limit() == 0 || o.expectedCount() <= o.limit())
                && group != (INTERSECT_GROUP | UNION_GROUP)
                && (group == 0
                        ? o.expansionPosition() == 0 && o.expansionSize() == 0
                        : o.expansionSize() >= 2
                                && o.expansionPosition() >= 0
                                && o.expansionPosition() < o.expansionSize()
                                && o.limit() == 0
                                && !pinned)
                && (!pinned
                        || o.index() > 0
                                && o.range().point()
                                && o.limit() == 0
                                && o.expectedCount() == 1);
    }

    public interface Consumer {
        void accept(Operation operation) throws IOException;

        /** Keeps the stored service payload beside the views used by semantic validators. */
        default void accept(org.sibench.adapters.proto.TraceRecord record, java.util.List<Operation> views)
                throws IOException {
            for (Operation operation : views) accept(operation);
        }
    }

    /** A stream of operations: a trace file, or one derived when it is read. Returns the count. */
    @FunctionalInterface
    public interface Source {
        long read(Consumer consumer) throws IOException;
    }

}
