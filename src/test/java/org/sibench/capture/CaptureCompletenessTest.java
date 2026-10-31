package org.sibench.capture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;

public class CaptureCompletenessTest {
    @SuppressWarnings("unchecked")
    private static Map<String, Object> map(Object value) {
        return (Map<String, Object>) value;
    }

    @SuppressWarnings("unchecked")
    private static List<Object> list(Object value) {
        return (List<Object>) value;
    }

    private static JsonObject snapshot(String indexes) {
        return snapshot("sbtest", indexes);
    }

    private static JsonObject snapshot(String database, String indexes) {
        return JsonParser.parseString(
                        """
                        {"schema_version":2,"source":"pg_stat_user_indexes","profile":"config/x.yaml",
                         "database":"%s","track_counts":"on","indexes":[%s]}
                        """
                                .formatted(database, indexes))
                .getAsJsonObject();
    }

    private static String counters(
            String schema, String table, String index, long scan, long read, long fetch) {
        return """
                {"schemaname":"%s","relname":"%s","indexrelname":"%s","idx_scan":%d,
                 "idx_tup_read":%d,"idx_tup_fetch":%d}
                """
                .formatted(schema, table, index, scan, read, fetch);
    }

    private static JsonObject ledger(String indexes) {
        return JsonParser.parseString(
                        """
                        {"schema_version":%d,"benchmark":"sysbench",
                         "indexes":[%s]}
                        """
                                .formatted(AccessLedger.VERSION, indexes))
                .getAsJsonObject();
    }

    /**
     * One ledger index tally: 50 access units whose 120 candidates include 30 bitmap TIDs, so only
     * 90 of the 118 visible rows were fetched through index_fetch_heap.
     */
    private static String tally(
            String index, long candidates, long heapVisible, long descentsMin, long descentsMax) {
        return """
                {"index":"%s","table":"sbtest1","column":"id","role":"primary","access_units":50,
                 "candidates":%d,"visible_rows":118,"heap_fetches":95,"heap_fetch_visible":%d,
                 "recheck_flagged_candidates":0,"tree_descents_min":%d,"tree_descents_max":%d,
                 "index_insert_attempts":3}
                """
                .formatted(index, candidates, heapVisible, descentsMin, descentsMax);
    }

    private static Map<String, Object> compare(
            String ledgerIndexes, String before, String after) {
        return CaptureCompleteness.compare(
                ledger(ledgerIndexes), snapshot(before), snapshot(after));
    }

    private static Map<String, Object> one(Map<String, Object> report) {
        return map(list(report.get("indexes")).get(0));
    }

    private static List<String> reasons(Map<String, Object> report) {
        return list(one(report).get("reasons")).stream().map(Object::toString).toList();
    }

    private static final String PK = "sbtest1_pkey";
    private static final String BEFORE = counters("public", "sbtest1", PK, 0, 0, 0);
    private static final String AFTER = counters("public", "sbtest1", PK, 40, 120, 90);
    private static final String TALLY = tally(PK, 120, 90, 38, 44);

    @Test
    public void countersThatMatchTheLedgerAreConsistent() {
        var report = compare(TALLY, BEFORE, AFTER);
        assertEquals("consistent", report.get("status"));
        assertEquals(List.of(), report.get("inconsistent_indexes"));
        assertEquals("sysbench", report.get("benchmark"));
        assertEquals("sbtest", report.get("database"));
        var index = one(report);
        assertEquals(PK, index.get("index"));
        assertEquals("public", index.get("schemaname"));
        assertEquals("sbtest1", index.get("table"));
        assertEquals(40L, index.get("idx_scan_delta"));
        assertEquals(120L, index.get("idx_tup_read_delta"));
        assertEquals(90L, index.get("idx_tup_fetch_delta"));
        assertEquals(50L, index.get("access_units"));
        assertEquals(38L, index.get("tree_descents_min"));
        assertEquals(44L, index.get("tree_descents_max"));
        assertEquals(120L, index.get("candidates"));
        assertEquals(118L, index.get("visible_rows"));
        assertEquals(90L, index.get("heap_fetch_visible"));
        assertEquals(3L, index.get("index_insert_attempts"));
        assertEquals(true, index.get("consistent"));
        assertEquals(List.of(), index.get("reasons"));
    }

    @Test
    public void aCounterThatWentBackwardsIsInconsistent() {
        var report =
                compare(
                        TALLY,
                        counters("public", "sbtest1", PK, 40, 130, 118),
                        counters("public", "sbtest1", PK, 40, 120, 100));
        assertEquals("inconsistent", report.get("status"));
        assertEquals(List.of(PK), report.get("inconsistent_indexes"));
        var reasons = reasons(report);
        assertTrue(reasons.contains("idx_tup_read went backwards by 10"));
        assertTrue(reasons.contains("idx_tup_fetch went backwards by 18"));
        assertEquals(false, one(report).get("consistent"));
    }

    @Test
    public void candidatesAndHeapFetchedRowsMustMatchTheLedger() {
        var report = compare(tally(PK, 100, 80, 38, 44), BEFORE, AFTER);
        assertEquals("inconsistent", report.get("status"));
        var reasons = reasons(report);
        assertTrue(reasons.contains("idx_tup_read delta 120 != ledger candidates 100"));
        assertTrue(reasons.contains("idx_tup_fetch delta 90 != ledger heap_fetch_visible 80"));
        assertEquals(2, reasons.size());
    }

    @Test
    public void bitmapAndAllVisibleRowsAreNotComparedWithIdxTupFetch() {
        // 118 visible rows, but bitmap heap checks and all-visible index-only rows never reach
        // index_fetch_heap: a fetch delta of 90 is the expected one, 118 would be a lost event.
        var report = compare(TALLY, BEFORE, counters("public", "sbtest1", PK, 40, 120, 118));
        assertEquals(
                List.of("idx_tup_fetch delta 118 != ledger heap_fetch_visible 90"), reasons(report));
    }

    @Test
    public void idxScanMustLieWithinTheLedgersTreeDescents() {
        assertEquals(
                "consistent",
                compare(TALLY, BEFORE, counters("public", "sbtest1", PK, 38, 120, 90))
                        .get("status"));
        assertEquals(
                "consistent",
                compare(TALLY, BEFORE, counters("public", "sbtest1", PK, 44, 120, 90))
                        .get("status"));
        // More descents than the ledger can explain: a scan the instrumentation never reported.
        assertEquals(
                List.of("idx_scan delta 45 outside ledger tree descents [38, 44]"),
                reasons(compare(TALLY, BEFORE, counters("public", "sbtest1", PK, 45, 120, 90))));
        // Fewer than the scans without array keys must have made: lost counters.
        assertEquals(
                List.of("idx_scan delta 37 outside ledger tree descents [38, 44]"),
                reasons(compare(TALLY, BEFORE, counters("public", "sbtest1", PK, 37, 120, 90))));
    }

    @Test
    public void anIndexMissingFromEitherSideIsInconsistent() {
        var report =
                compare(
                        TALLY,
                        BEFORE,
                        AFTER + "," + counters("public", "sbtest1", "k_1", 5, 20, 20));
        assertEquals("inconsistent", report.get("status"));
        assertEquals(List.of("k_1"), report.get("inconsistent_indexes"));
        var missing = map(list(report.get("indexes")).get(0));
        assertEquals("k_1", missing.get("index"));
        var reasons = list(missing.get("reasons")).stream().map(Object::toString).toList();
        assertTrue(reasons.contains("index is absent from the before snapshot"));
        assertTrue(reasons.contains("index is absent from the access ledger"));
        assertNull(missing.get("candidates"));

        var dropped = compare(TALLY, "", AFTER);
        assertEquals("inconsistent", dropped.get("status"));
        assertEquals("index is absent from the before snapshot", reasons(dropped).get(0));
        assertNull(one(dropped).get("idx_scan_delta"));
        assertEquals(false, one(dropped).get("consistent"));
    }

    @Test
    public void anUntouchedIndexIsComparedAsZero() {
        String untouched = tally("k_1", 0, 0, 0, 0);
        var report =
                compare(
                        TALLY + "," + untouched,
                        BEFORE + "," + counters("public", "sbtest1", "k_1", 7, 9, 9),
                        AFTER + "," + counters("public", "sbtest1", "k_1", 7, 9, 9));
        assertEquals("consistent", report.get("status"));
    }

    @Test
    public void staleInputsAreRejectedBeforeComparison() {
        var oldLedger =
                JsonParser.parseString("{\"schema_version\":3,\"indexes\":[]}").getAsJsonObject();
        var error =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                CaptureCompleteness.compare(
                                        oldLedger, snapshot(BEFORE), snapshot(AFTER)));
        assertTrue(error.getMessage().contains("rerun access-ledger"));
        var oldSnapshot =
                JsonParser.parseString("{\"schema_version\":1,\"indexes\":[]}").getAsJsonObject();
        assertThrows(
                IllegalArgumentException.class,
                () -> CaptureCompleteness.compare(ledger(TALLY), oldSnapshot, snapshot(AFTER)));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        CaptureCompleteness.compare(
                                ledger(TALLY), snapshot(BEFORE), snapshot("other", AFTER)));
    }

    @Test
    public void theReportStatesTheCounterMappingAndItsPreconditions() {
        var report = compare(TALLY, BEFORE, AFTER);
        var semantics =
                list(report.get("counter_semantics")).stream().map(Object::toString).toList();
        assertEquals(3, semantics.size());
        assertTrue(semantics.get(0).startsWith("idx_scan counts _bt_first descents"));
        assertTrue(semantics.get(1).contains("ledger candidates"));
        assertTrue(semantics.get(2).contains("heap_fetch_visible"));
        var preconditions =
                list(report.get("preconditions")).stream().map(Object::toString).toList();
        assertEquals(3, preconditions.size());
        assertTrue(preconditions.get(0).contains("no other session"));
        assertTrue(preconditions.get(1).contains("Only the native run"));
        assertTrue(preconditions.get(2).contains("UDP"));
    }

    /** A database whose sessions leave and whose counters settle on a scripted schedule. */
    private static final class Scripted implements CaptureCompleteness.CounterSource {
        final Deque<Long> sessions;
        final Deque<List<Map<String, Object>>> reads;
        long clock;

        Scripted(List<Long> sessions, List<List<Map<String, Object>>> reads) {
            this.sessions = new ArrayDeque<>(sessions);
            this.reads = new ArrayDeque<>(reads);
        }

        @Override
        public long otherSessions() {
            return sessions.size() > 1 ? sessions.poll() : sessions.peek();
        }

        @Override
        public List<Map<String, Object>> counters() {
            return reads.size() > 1 ? reads.poll() : reads.peek();
        }

        @Override
        public void sleep(long millis) {
            clock += millis;
        }

        @Override
        public long now() {
            return clock;
        }
    }

    private static List<Map<String, Object>> read(long scan) {
        return List.<Map<String, Object>>of(Map.of("indexrelname", PK, "idx_scan", scan));
    }

    @Test
    public void aSnapshotWaitsForSessionsToLeaveAndCountersToSettle() throws Exception {
        // An exiting backend is still connected twice; its counters arrive over two reads.
        var source = new Scripted(List.of(1L, 1L, 0L), List.of(read(10), read(12), read(13), read(13)));
        var result = CaptureCompleteness.settle(source, 60_000);
        assertEquals(read(13), result.get("indexes"));
        var settled = map(result.get("settled"));
        assertEquals(400L, settled.get("waited_for_other_sessions_ms"));
        assertEquals(4, settled.get("reads"));
        assertEquals(CaptureCompleteness.READ_INTERVAL_MS, settled.get("read_interval_ms"));
    }

    @Test
    public void aSnapshotThatCannotSettleFails() {
        var connected = new Scripted(List.of(1L), List.of(read(1)));
        var error =
                assertThrows(IOException.class, () -> CaptureCompleteness.settle(connected, 1_000));
        assertTrue(error.getMessage().contains("remain connected"));
        var moving =
                new Scripted(
                        List.of(0L),
                        java.util.stream.LongStream.range(0, 100).mapToObj(CaptureCompletenessTest::read).toList());
        error = assertThrows(IOException.class, () -> CaptureCompleteness.settle(moving, 3_000));
        assertTrue(error.getMessage().contains("did not settle"));
    }
}
