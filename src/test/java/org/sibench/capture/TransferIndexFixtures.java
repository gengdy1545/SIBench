package org.sibench.capture;

import org.sibench.capture.IndexOperationTrace.Operation;
import org.sibench.trace.IndexKey;

import java.io.IOException;
import java.util.Map;

/**
 * Transfer rows with sourceid 10 and targetid 20 as complete maintenance units for tests that
 * hand-build traces; the compiler derives units from the source catalog.
 */
public final class TransferIndexFixtures {
    private TransferIndexFixtures() {}

    public static void expandRow(Operation row, IndexOperationTrace.Consumer consumer)
            throws IOException {
        if (row.kind() == IndexOperationTrace.SEARCH || row.index() != 0) {
            consumer.accept(row);
            return;
        }
        Map<Integer, IndexKey> values =
                row.table() == 5 ? Map.of(1, IndexKey.ofInt(10), 2, IndexKey.ofInt(20)) : Map.of();
        boolean delete = row.kind() == IndexOperationTrace.LOGICAL_DELETE;
        IndexMutation.emit(row, delete ? values : Map.of(), delete ? Map.of() : values, consumer);
    }

    public static void appendRow(IndexOperationTrace.Writer writer, Operation row)
            throws IOException {
        expandRow(row, writer::append);
    }
}
