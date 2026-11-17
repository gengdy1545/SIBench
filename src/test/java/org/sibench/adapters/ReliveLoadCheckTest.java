package org.sibench.adapters;

import static org.junit.Assert.*;

import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import org.junit.Test;

public class ReliveLoadCheckTest {
    private static ByteString value(long number) {
        return ByteString.copyFrom(ByteBuffer.allocate(8).putLong(number).array());
    }

    @Test public void extensionKeepsOldInputAndAssignsDistinctPredeclaredVersions() throws Exception {
        var digest = MessageDigest.getInstance("SHA-256");
        for (var mutation : ReliveLoadCheck.plan(64)) {
            digest.update(ByteBuffer.allocate(4).putInt(mutation.getSerializedSize()).array());
            digest.update(mutation.toByteArray());
        }
        assertEquals("6547707338c00dd7138c932c5f1e6aec67fcc88b4edb44158f191e55422dbb10",
                HexFormat.of().formatHex(digest.digest()));
        var seen = new java.util.HashSet<Long>();
        var plan = ReliveLoadCheck.plan(1024);
        for (var mutation : plan) {
            long ts = mutation.hasPut() ? mutation.getPut().getTs() : mutation.getDelete().getTs();
            assertTrue(ts >= 1 && ts <= 1024);
            assertTrue(seen.add(ts));
        }
        assertEquals(1024, seen.size());
        for (int invalid : new int[]{0, 63, 65, 8193})
            assertThrows(IllegalArgumentException.class, () -> ReliveLoadCheck.plan(invalid));
    }

    @Test public void handHistoriesDistinguishTombstonesAndLaterReinsertion() {
        assertEquals(List.of(value(200008), value(200015), value(200014), value(200013),
                value(200012), value(200010), value(200009)), ReliveLoadCheck.expected(15, 1024));
        assertEquals(List.of(value(201024), value(201022), value(201021), value(201020),
                value(201019), value(201018), value(201017)), ReliveLoadCheck.expected(1024, 1024));
        assertEquals(List.of(value(100000), value(100001), value(100002), value(100003),
                value(100004), value(100005), value(100006), value(100007)), ReliveLoadCheck.expected(0, 1024));
    }

    @Test public void drainRejectsEachKindOfOutstandingMaintenanceAndMissingEvidence() {
        var idle = com.google.gson.JsonParser.parseString("""
                {"memoryCharge":0,"frozenMemtables":0,"compactionEligibleBytes":0,
                 "backgroundFlushing":false,"backgroundCompacting":false}
                """).getAsJsonObject();
        assertTrue(ReliveLoadCheck.drained(idle));
        for (String key : List.of("memoryCharge", "frozenMemtables", "compactionEligibleBytes",
                "backgroundFlushing", "backgroundCompacting")) {
            var active = idle.deepCopy();
            if (key.startsWith("background")) active.addProperty(key, true);
            else active.addProperty(key, 1);
            assertFalse(ReliveLoadCheck.drained(active));
            active.remove(key);
            assertThrows(RuntimeException.class, () -> ReliveLoadCheck.drained(active));
        }
    }
}
