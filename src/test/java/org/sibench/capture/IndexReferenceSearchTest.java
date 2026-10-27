package org.sibench.capture;

import static org.junit.Assert.*;

import org.junit.Test;
import org.sibench.capture.IndexOperationTrace.Operation;
import org.sibench.capture.IndexOperationTrace.Range;
import org.sibench.trace.IndexKey;

import java.util.*;

public class IndexReferenceSearchTest {
    private static IndexKey key(long value) {
        return IndexKey.ofInt(value);
    }

    private static List<Long> keys(List<IndexReferenceReplay.LookupResult> results)
            throws Exception {
        var keys = new ArrayList<Long>();
        for (var result : results) keys.add(result.key().intValue());
        return keys;
    }

    /** Rows 1..10 with secondary value (id % 3); then version 1 deletes 3, moves 5 to 0, adds 11. */
    private static IndexReferenceReplay model(boolean rowIds) throws Exception {
        var model = new IndexReferenceReplay(rowIds);
        for (long id = 1; id <= 10; id++)
            IndexMutation.emit(
                    IndexOperationTrace.write(0, IndexOperationTrace.INSERT, 1, 0, key(id), 0, 1, id, id),
                    Map.of(),
                    Map.of(1, key(id % 3)),
                    model::accept);
        Operation delete =
                IndexOperationTrace.write(1, IndexOperationTrace.LOGICAL_DELETE, 1, 0, key(3), 1, 0, 0, 3)
                        .withTransaction(1, 1);
        IndexMutation.emit(delete, Map.of(1, key(0)), Map.of(), model::accept);
        Operation update =
                IndexOperationTrace.write(
                                2, IndexOperationTrace.UPSERT, 1, IndexOperationTrace.BUSINESS_UPDATE,
                                key(5), 1, 2, 0, 5)
                        .withTransaction(1, 2);
        IndexMutation.emit(update, Map.of(1, key(2)), Map.of(1, key(0)), model::accept);
        Operation insert =
                IndexOperationTrace.write(
                                3, IndexOperationTrace.UPSERT, 1, IndexOperationTrace.BUSINESS_INSERT,
                                key(11), 1, 2, 1, 11)
                        .withTransaction(1, 3);
        IndexMutation.emit(insert, Map.of(), Map.of(1, key(2)), model::accept);
        return model;
    }

    @Test
    public void primaryRangesMergeBaselineAndHistoryInKeyOrder() throws Exception {
        for (boolean rowIds : new boolean[] {false, true}) {
            var model = model(rowIds);
            Range closed = new Range(key(2), true, key(6), true, false, 0);
            assertEquals(List.of(2L, 3L, 4L, 5L, 6L), keys(model.search(1, 0, closed, 0)));
            assertEquals(List.of(2L, 4L, 5L, 6L), keys(model.search(1, 0, closed, 1)));
            Range open = new Range(key(2), false, key(6), false, false, 0);
            assertEquals(List.of(4L, 5L), keys(model.search(1, 0, open, 1)));
            // Descending with a limit returns the highest keys first; deleted keys are skipped.
            Range tail = new Range(null, false, key(4), true, true, 2);
            assertEquals(List.of(4L, 2L), keys(model.search(1, 0, tail, 1)));
            Range from = new Range(key(10), true, null, false, false, 5);
            assertEquals(List.of(10L), keys(model.search(1, 0, from, 0)));
            assertEquals(List.of(10L, 11L), keys(model.search(1, 0, from, 1)));
            assertTrue(model.search(1, 0, new Range(key(6), true, key(5), true, false, 0), 1).isEmpty());
            assertEquals(5, model.search(1, 0, new Range(null, false, null, false, false, 5), 1).size());
            assertEquals(10, model.search(1, 0, new Range(null, false, null, false, false, 0), 1).size());
        }
    }

    @Test
    public void oldV9LocalSearchFormStillReadsItsOwnVersion() throws Exception {
        var model = new IndexReferenceReplay(true);
        IndexMutation.emit(
                IndexOperationTrace.write(0, IndexOperationTrace.INSERT, 1, 0, key(1), 0, 1, 0, 1),
                Map.of(),
                Map.of(1, key(0)),
                model::accept);
        var update =
                IndexOperationTrace.write(
                                1, IndexOperationTrace.UPSERT, 1, 0, key(1), 1, 1, 1, 1)
                        .withTransaction(1, 1);
        IndexMutation.emit(update, Map.of(1, key(0)), Map.of(1, key(0)), model::accept);
        // Historical v9 form: no SEARCH.version, read point in readTimestamp.
        var old =
                IndexOperationTrace.search(
                                2,
                                1,
                                0,
                                Range.equal(key(1)),
                                1,
                                1,
                                IndexOperationTrace.TRANSACTION_LOCAL_SEARCH)
                        .withTransaction(1, 1);
        model.accept(old);
        assertEquals(1, model.queries());
    }

    @Test
    public void secondaryResultsFollowValueThenPrimaryKeyAtEachReadPoint() throws Exception {
        var model = model(true);
        // Value 0 holds 3, 6, 9 at baseline; 3 is deleted and 5 moves in at version 1.
        assertEquals(List.of(3L, 6L, 9L), keys(model.lookupSecondary(1, 1, key(0), 0)));
        assertEquals(List.of(5L, 6L, 9L), keys(model.lookupSecondary(1, 1, key(0), 1)));
        Range low = new Range(key(0), true, key(1), true, false, 0);
        assertEquals(List.of(5L, 6L, 9L, 1L, 4L, 7L, 10L), keys(model.search(1, 1, low, 1)));
        Range high = new Range(key(1), false, null, false, true, 0);
        assertEquals(List.of(11L, 8L, 2L), keys(model.search(1, 1, high, 1)));
        assertEquals(List.of(8L, 5L, 2L), keys(model.search(1, 1, high, 0)));
    }
}
