package org.sibench.capture;

import static org.junit.Assert.*;
import static org.sibench.capture.IndexOperationTrace.*;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.trace.IndexKey;

public class IndexReferenceAbortTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    private static Operation row(int kind, long key, long version, long ordinal, long rowId) {
        return write(version + 1, kind, 1, 0, IndexKey.ofInt(key), version, version + 1, ordinal, rowId)
                .withTransaction(version == 0 ? 0 : version + 100, ordinal);
    }

    private static void abort(IndexReferenceReplay model, long version) throws Exception {
        model.accept(transaction(version + 1, TRANSACTION_ABORT, version + 100, version, 0));
    }

    @Test public void abortRestoresSecondaryKeysAndRejectsInvalidResolution() throws Exception {
        for (boolean spill : new boolean[]{false, true}) {
            var settings = new IndexReferenceReplay.Settings(temporary.newFolder().toPath(),
                    spill ? 1 : 100, spill ? 1 : 100, 4096);
            try (var model = new IndexReferenceReplay(true, true, settings)) {
                IndexMutation.emit(row(INSERT, 1, 0, 0, 1), Map.of(), Map.of(1, IndexKey.ofInt(10)), model::accept);
                IndexMutation.emit(row(UPSERT, 1, 1, 1, 1), Map.of(1, IndexKey.ofInt(10)),
                        Map.of(1, IndexKey.ofInt(20)), model::accept);
                assertEquals(1, model.search(1, 1, Range.equal(IndexKey.ofInt(20)), 0, 1, 101, 1).size());
                assertTrue(model.search(1, 1, Range.equal(IndexKey.ofInt(10)), 0, 1, 101, 1).isEmpty());
                assertThrows(IOException.class, () -> model.accept(transaction(2, TRANSACTION_ABORT, 999, 1, 0)));
                abort(model, 1);
                assertEquals(1, model.lookupSecondary(1, 1, IndexKey.ofInt(10), 1).size());
                assertTrue(model.lookupSecondary(1, 1, IndexKey.ofInt(20), 1).isEmpty());
                assertThrows(IOException.class, () -> abort(model, 1));
                assertThrows(IOException.class, () -> model.accept(row(UPSERT, 1, 1, 2, 1)));
            }
        }
    }

    @Test public void abortedInsertUpdateDeleteNeverEnterLaterSnapshotsOrLongHistoryScans() throws Exception {
        for (boolean spill : new boolean[]{false, true}) {
            Path directory = temporary.newFolder().toPath();
            var settings = new IndexReferenceReplay.Settings(directory, spill ? 1 : 100, spill ? 1 : 100, 4096);
            try (var model = new IndexReferenceReplay(true, true, settings)) {
                model.accept(row(INSERT, 1, 0, 0, 1));
                model.accept(row(INSERT, 2, 0, 0, 2));
                model.accept(row(UPSERT, 1, 1, 1, 1));
                model.accept(row(LOGICAL_DELETE, 2, 1, 2, 2));
                model.accept(row(UPSERT, 3, 1, 3, 3));
                var all = new Range(null, false, null, false, false, 0);
                assertEquals(List.of(IndexKey.ofInt(1), IndexKey.ofInt(3)),
                        model.search(1, 0, all, 0, 1, 101, 3).stream().map(IndexReferenceReplay.LookupResult::key).toList());
                assertEquals(List.of(IndexKey.ofInt(1), IndexKey.ofInt(2)),
                        model.search(1, 0, all, 0).stream().map(IndexReferenceReplay.LookupResult::key).toList());
                abort(model, 1);
                assertEquals(0, model.lookupPrimary(1, IndexKey.ofInt(1), 1).commitVersion());
                assertEquals(0, model.lookupPrimary(1, IndexKey.ofInt(2), 1).commitVersion());
                assertNull(model.lookupPrimary(1, IndexKey.ofInt(3), 1));
                // A rolled-back delete cannot make the next real delete target an absent row.
                model.accept(row(LOGICAL_DELETE, 2, 2, 1, 2));
                model.accept(row(UPSERT, 3, 2, 2, 4));
                abort(model, 3); // Read-only Abort.
                for (long version = 4; version <= 20; version++) {
                    model.accept(row(UPSERT, 1, version, 1, 1));
                    abort(model, version);
                }
                for (boolean descending : new boolean[]{false, true}) {
                    var range = new Range(null, false, null, false, descending, 1);
                    var result = model.search(1, 0, range, 20);
                    assertEquals(1, result.size());
                    assertEquals(IndexKey.ofInt(descending ? 3 : 1), result.get(0).key());
                    assertEquals(descending ? 2 : 0, result.get(0).commitVersion());
                }
                assertEquals(0, model.lookupPrimary(1, IndexKey.ofInt(1), 20).commitVersion());
                assertNull(model.lookupPrimary(1, IndexKey.ofInt(2), 20));
                assertEquals(4, model.lookupPrimary(1, IndexKey.ofInt(3), 20).value());
                assertTrue(model.skippedHistories() > 0);
                var visible=new java.util.ArrayList<IndexReferenceReplay.LookupResult>();
                model.scanVisible(20,(table,index,key,value,result)->visible.add(result));
                assertEquals(List.of(IndexKey.ofInt(1),IndexKey.ofInt(3)),
                        visible.stream().map(IndexReferenceReplay.LookupResult::key).toList());
                assertEquals(List.of(0L,2L),visible.stream().map(IndexReferenceReplay.LookupResult::commitVersion).toList());
            }
        }
    }
}
