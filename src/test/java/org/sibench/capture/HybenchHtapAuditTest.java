package org.sibench.capture;

import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.*;

public class HybenchHtapAuditTest {
    @Test
    public void resolvesPerKeyCausalityWithoutFilenameCommitOrdering() throws Exception {
        JsonObject initial = row(1), middle = row(2), last = row(3);
        assertEquals(last, HybenchHtapAudit.applyRowChanges(initial,
                List.of(change(middle, last), change(initial, middle))));
    }

    @Test
    public void insertUpdateDeleteAndReinsertKeepExactState() throws Exception {
        JsonObject inserted = row(1), updated = row(2), reinserted = row(3);
        assertEquals(reinserted, HybenchHtapAudit.applyRowChanges(null, List.of(
                change(null, inserted), change(inserted, updated),
                change(updated, null), change(null, reinserted))));
        assertNull(HybenchHtapAudit.applyRowChanges(inserted, List.of(change(inserted, null))));
    }

    @Test
    public void rejectsMissingBeforeImagesInsteadOfAcceptingFinalState() {
        assertThrows(IOException.class, () -> HybenchHtapAudit.applyRowChanges(row(1),
                List.of(change(row(2), row(3)))));
        assertThrows(IOException.class, () -> HybenchHtapAudit.applyRowChanges(null,
                List.of(change(row(1), row(2)))));
    }

    private static JsonObject row(int value) {
        JsonObject row = new JsonObject();
        row.addProperty("id", 1);
        row.addProperty("value", value);
        return row;
    }

    private static JsonObject change(JsonObject before, JsonObject after) {
        JsonObject change = new JsonObject();
        change.add("old", before == null ? JsonNull.INSTANCE : before);
        change.add("new", after == null ? JsonNull.INSTANCE : after);
        return change;
    }
}
