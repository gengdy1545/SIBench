package org.sibench.replay;

import static org.junit.Assert.*;

import org.junit.Test;
import org.sibench.capture.IndexOperationTrace;
import org.sibench.capture.IndexOperationTrace.Operation;

import java.util.ArrayList;
import java.util.List;
import java.io.IOException;

public class ReplayProjectionTest {
    @Test
    public void disablesAllSecondaryKindsWithoutChangingPrimaryIdentityOrOrder() throws Exception {
        List<Operation> primary = new ArrayList<>(), received = new ArrayList<>();
        var projection = new ReplayProjection(false, received::add);
        for (int kind : new int[] {1, 2, 3, 4, 5}) {
            var row = new Operation(9, kind, 5, 0, 0, 7, 4, 8, 12, 20, 3, 6, 1);
            primary.add(row);
            if (kind == IndexOperationTrace.SEARCH) {
                projection.accept(row);
                projection.accept(new Operation(9, kind, 5, 1, 0, 3, 4, 0, 0, 0, 0, 0, 1));
                projection.accept(new Operation(9, kind, 5, 2, 0, 6, 4, 0, 0, 0, 0, 0, 1));
            } else IndexOperationTrace.expandRow(row, projection);
        }
        assertEquals(primary, received);
        assertEquals(15L, projection.describe().get("source_operations"));
        assertEquals(10L, projection.describe().get("filtered_secondary_operations"));
        assertEquals(5, projection.dispatched());
        var all = new ReplayProjection(true, operation -> {});
        for (Operation row : primary) all.accept(row);
        assertEquals(0L, all.describe().get("filtered_secondary_operations"));
    }

    private static Operation mutation(int table, long key, long version, long tx, long ordinal, int kind) {
        return new Operation(ordinal, kind, table, 0, 0, key, version,
                kind == 5 ? 0 : 9, ordinal, key + 100, 10, 20, 0, tx, ordinal);
    }

    @Test
    public void coalescesOnlySameFullIndexKeyWithinOneTransactionAndKeepsLastOccurrenceOrder()
            throws Exception {
        for (boolean coalesce : new boolean[] {true, false}) {
            for (boolean secondary : new boolean[] {true, false}) {
                List<Operation> received = new ArrayList<>();
                var config = new ReplayConfig(secondary, "none", 0, 0, 0, 0, coalesce);
                var projection = new ReplayProjection(config, true, received::add);
                projection.accept(IndexOperationTrace.transaction(10, 6, 10, 1, 0));
                IndexOperationTrace.expandRow(mutation(5, 1, 1, 10, 1, 3), projection);
                IndexOperationTrace.expandRow(mutation(5, 2, 1, 10, 2, 3), projection);
                IndexOperationTrace.expandRow(mutation(5, 1, 1, 10, 3, 3), projection);
                projection.accept(mutation(3, 1, 1, 10, 4, 3));
                projection.accept(mutation(3, 1, 1, 10, 5, 5));
                assertTrue(received.isEmpty());
                projection.accept(IndexOperationTrace.transaction(10, 7, 10, 1, 5));
                projection.finish();
                var writes = received.stream().filter(IndexOperationTrace::isMutation).toList();
                int transferWidth = secondary ? 3 : 1;
                assertEquals(coalesce ? transferWidth * 2 + 1 : transferWidth * 3 + 2, writes.size());
                if (coalesce) {
                    assertEquals(2, writes.get(0).key());
                    assertEquals(1, writes.get(transferWidth).key());
                    assertEquals(5, writes.get(writes.size() - 1).kind());
                }
                assertEquals(coalesce ? (long) transferWidth + 1 : 0L,
                        projection.describe().get("coalesced_operations"));
                projection.accept(IndexOperationTrace.transaction(11, 6, 11, 2, 0));
                projection.accept(mutation(3, 1, 2, 11, 1, 3));
                projection.accept(IndexOperationTrace.transaction(11, 7, 11, 2, 1));
                assertEquals(writes.size() + 1L, projection.dispatched());
                assertEquals(2L, projection.describe().get("committed_transactions"));
                assertEquals("committed_only", projection.describe().get("change_delivery"));
            }
        }
    }

    @Test
    public void rejectsMalformedBoundariesEvenWhenSecondaryWritesAreFiltered() throws Exception {
        var config = new ReplayConfig(false, "none", 0, 0, 0, 0);
        var projection = new ReplayProjection(config, true, operation -> {});
        assertThrows(IOException.class, () -> projection.accept(mutation(3, 1, 1, 10, 1, 3)));
        projection.accept(IndexOperationTrace.transaction(10, 6, 10, 1, 0));
        assertThrows(IOException.class, projection::finish);
        assertThrows(IOException.class, () -> projection.accept(
                new Operation(2, 2, 3, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1)));
        assertThrows(IOException.class, () -> projection.accept(mutation(3, 1, 2, 10, 1, 3)));
        assertThrows(IOException.class, () -> projection.accept(IndexOperationTrace.transaction(10, 7, 11, 1, 0)));
        projection.accept(mutation(5, 1, 1, 10, 1, 3));
        assertThrows(IOException.class, () -> projection.accept(IndexOperationTrace.transaction(10, 7, 10, 1, 1)));
    }
}
