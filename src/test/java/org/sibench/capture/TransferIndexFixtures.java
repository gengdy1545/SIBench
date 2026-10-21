package org.sibench.capture;

import org.sibench.capture.IndexOperationTrace.Operation;

import java.io.IOException;

/** Explicit transfer secondary operations for tests that hand-build traces; the compiler derives them from the source catalog. */
public final class TransferIndexFixtures {
    private TransferIndexFixtures() {}

    public static void expandRow(Operation row, IndexOperationTrace.Consumer consumer)
            throws IOException {
        consumer.accept(row);
        if (row.table() == 5 && row.index() == 0 && row.kind() != IndexOperationTrace.SEARCH)
            for (int index = 1; index <= 2; index++)
                consumer.accept(
                        row.withIndex(
                                index,
                                Long.toString(
                                        index == 1 ? row.secondarySource() : row.secondaryTarget()),
                                row.kind()));
    }

    public static void appendRow(IndexOperationTrace.Writer writer, Operation row)
            throws IOException {
        expandRow(row, writer::append);
    }
}
