package org.sibench.capture;

import org.sibench.capture.IndexOperationTrace.Operation;
import org.sibench.capture.IndexOperationTrace.Range;
import org.sibench.trace.IndexKey;

/**
 * Hand-built operations for tests, in the positional shape the fixtures were written in. Integer
 * keys are encoded; a search on a secondary index looks up the integer value {@code key}, and a
 * secondary write carries {@code secondarySource} for index 1 and {@code secondaryTarget} for 2.
 */
public final class TestOps {
    private TestOps() {}

    public static Operation op(
            long sourceEvent,
            int kind,
            int table,
            int index,
            int flags,
            long key,
            long version,
            long file,
            long position,
            long rowId,
            long secondarySource,
            long secondaryTarget,
            long expectedCount) {
        return op(
                sourceEvent, kind, table, index, flags, key, version, file, position, rowId,
                secondarySource, secondaryTarget, expectedCount, 0, 0);
    }

    public static Operation op(
            long sourceEvent,
            int kind,
            int table,
            int index,
            int flags,
            long key,
            long version,
            long file,
            long position,
            long rowId,
            long secondarySource,
            long secondaryTarget,
            long expectedCount,
            long transactionId,
            long mutationOrdinal) {
        if (kind == IndexOperationTrace.SEARCH)
            return IndexOperationTrace.search(
                            sourceEvent,
                            table,
                            index,
                            Range.equal(IndexKey.ofInt(key)),
                            version,
                            expectedCount,
                            flags)
                    .withTransaction(transactionId, mutationOrdinal);
        Operation write =
                IndexOperationTrace.write(
                                sourceEvent,
                                kind,
                                table,
                                flags,
                                IndexKey.ofInt(key),
                                version,
                                file,
                                position,
                                rowId)
                        .withTransaction(transactionId, mutationOrdinal);
        return index == 0
                ? write
                : write.withIndex(
                        index, IndexKey.ofInt(index == 1 ? secondarySource : secondaryTarget), kind);
    }

    /** A primary write as a complete, single-index maintenance unit. */
    public static Operation unit(Operation primary) {
        return primary.withExpansion(0, 1);
    }

    public static IndexKey key(long value) {
        return IndexKey.ofInt(value);
    }
}
