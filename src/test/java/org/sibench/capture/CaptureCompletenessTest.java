package org.sibench.capture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.junit.Test;

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
        return JsonParser.parseString(
                        """
                        {"schema_version":1,"source":"pg_stat_user_indexes","profile":"config/x.yaml",
                         "database":"sbtest","indexes":[%s]}
                        """
                                .formatted(indexes))
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
                        {"schema_version":2,"benchmark":"sysbench",
                         "indexes":[%s]}
                        """
                                .formatted(indexes))
                .getAsJsonObject();
    }

    private static String tally(
            String index, String table, long units, long candidates, long visible, long inserts) {
        return """
                {"index":"%s","table":"%s","column":"id","role":"primary","access_units":%d,
                 "candidates":%d,"visible_rows":%d,"heap_fetches":0,
                 "recheck_flagged_candidates":0,"index_insert_attempts":%d}
                """
                .formatted(index, table, units, candidates, visible, inserts);
    }

    private static Map<String, Object> compare(
            String ledgerIndexes, String before, String after) {
        return CaptureCompleteness.compare(
                ledger(ledgerIndexes), snapshot(before), snapshot(after));
    }

    private static Map<String, Object> one(Map<String, Object> report) {
        return map(list(report.get("indexes")).get(0));
    }

    private static final String PK = "sbtest1_pkey";
    private static final String BEFORE = counters("public", "sbtest1", PK, 0, 0, 0);
    private static final String AFTER = counters("public", "sbtest1", PK, 40, 120, 118);
    private static final String TALLY = tally(PK, "sbtest1", 50, 120, 118, 3);

    @Test
    public void countersThatMatchTheLedgerAreConsistent() {
        var report = compare(TALLY, BEFORE, AFTER);
        assertEquals("consistent", report.get("status"));
        assertEquals(List.of(), report.get("inconsistent_indexes"));
        assertEquals("sysbench", report.get("benchmark"));
        var index = one(report);
        assertEquals(PK, index.get("index"));
        assertEquals("public", index.get("schemaname"));
        assertEquals("sbtest1", index.get("table"));
        assertEquals(40L, index.get("idx_scan_delta"));
        assertEquals(120L, index.get("idx_tup_read_delta"));
        assertEquals(118L, index.get("idx_tup_fetch_delta"));
        assertEquals(50L, index.get("access_units"));
        assertEquals(120L, index.get("candidates"));
        assertEquals(118L, index.get("visible_rows"));
        assertEquals(3L, index.get("index_insert_attempts"));
        // idx_scan counts tree descents: 40 of the 50 access units fetched at least once.
        assertEquals(0.8, index.get("idx_scan_ratio"));
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
        var reasons = list(one(report).get("reasons")).stream().map(Object::toString).toList();
        assertTrue(reasons.contains("idx_tup_read went backwards by 10"));
        assertTrue(reasons.contains("idx_tup_fetch went backwards by 18"));
        assertEquals(false, one(report).get("consistent"));
    }

    @Test
    public void candidatesAndVisibleRowsMustMatchTheLedger() {
        var report = compare(tally(PK, "sbtest1", 50, 100, 90, 0), BEFORE, AFTER);
        assertEquals("inconsistent", report.get("status"));
        var reasons = list(one(report).get("reasons")).stream().map(Object::toString).toList();
        assertTrue(reasons.contains("idx_tup_read delta 120 != ledger candidates 100"));
        assertTrue(reasons.contains("idx_tup_fetch delta 118 != ledger visible_rows 90"));
        assertEquals(2, reasons.size());
    }

    @Test
    public void idxScanCountsDescentsNotEveryAccessUnit() {
        // 60 descents for 50 access units cannot happen; equality is not the assertion.
        var report =
                compare(TALLY, BEFORE, counters("public", "sbtest1", PK, 60, 120, 118));
        assertEquals("inconsistent", report.get("status"));
        var reasons = list(one(report).get("reasons")).stream().map(Object::toString).toList();
        assertTrue(reasons.contains("idx_scan delta 60 > ledger access_units 50"));
        // Fewer descents than access units is expected and stays consistent.
        var fewer =
                compare(TALLY, BEFORE, counters("public", "sbtest1", PK, 10, 120, 118));
        assertEquals("consistent", fewer.get("status"));
        assertEquals(0.2, one(fewer).get("idx_scan_ratio"));
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
        assertEquals(
                "index is absent from the before snapshot",
                list(one(dropped).get("reasons")).get(0));
        assertNull(one(dropped).get("idx_scan_delta"));
        assertEquals(0.0, one(dropped).get("idx_scan_ratio"));
        assertEquals(false, one(dropped).get("consistent"));
    }

    @Test
    public void theReportStatesTheCounterMappingAndItsPreconditions() {
        var report = compare(TALLY, BEFORE, AFTER);
        var semantics =
                list(report.get("counter_semantics")).stream().map(Object::toString).toList();
        assertEquals(3, semantics.size());
        assertTrue(
                semantics
                        .get(0)
                        .startsWith("idx_scan counts descents of the index tree (PG14 nbtsearch.c)"));
        assertTrue(semantics.get(1).contains("idx_tup_read corresponds to candidate index entries"));
        assertTrue(semantics.get(2).contains("idx_tup_fetch corresponds to heap-visible rows"));
        var preconditions =
                list(report.get("preconditions")).stream().map(Object::toString).toList();
        assertEquals(4, preconditions.size());
        assertTrue(preconditions.get(0).contains("No other session"));
        assertTrue(preconditions.get(1).contains("ordered scans are excluded"));
        assertTrue(preconditions.get(2).contains("BitmapAnd"));
        assertTrue(preconditions.get(3).contains("idx_scan is not a count of all access units"));
    }
}
