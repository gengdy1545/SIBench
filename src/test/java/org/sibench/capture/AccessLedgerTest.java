package org.sibench.capture;

import static org.junit.Assert.*;

import com.google.gson.JsonParser;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.schema.SourceSchema;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AccessLedgerTest {
    @Rule public TemporaryFolder folder = new TemporaryFolder();

    private static final String PK = "sbtest1_pkey", K = "k_1";
    private static final String KEY =
            "{\"attribute\":%d,\"strategy\":%d,\"flags\":%d,\"type_oid\":23,\"value\":%s}";
    private static final String EQUALITY = "[" + KEY.formatted(1, 3, 0, "\"5\"") + "]";
    private static final String RANGE =
            "[" + KEY.formatted(1, 4, 0, "\"5\"") + "," + KEY.formatted(1, 2, 0, "\"8\"") + "]";
    private static final String PROBE = "[" + KEY.formatted(1, 0, 129, "null") + "]";

    /** sbtest1 with its id primary key and k secondary index, as the pinned sysbench creates. */
    private static SourceSchema schema() throws IOException {
        return SourceSchema.of("sysbench", Path.of("benchmark/sysbench"), Map.of("tables", 1L));
    }

    /** Builds one backend trace with request ordinals, executor depth and query identifiers. */
    private static final class Trace {
        final List<String> lines = new ArrayList<>();
        long req;
        int depth;

        Trace event(String type, String relation, String detail) {
            lines.add(
                    "{\"event\":\""
                            + type
                            + "\",\"pid\":7,\"xid\":0,\"cid\":0,\"req\":"
                            + req
                            + ",\"depth\":"
                            + depth
                            + ",\"application\":\"sibench:x\""
                            + (relation == null ? "" : ",\"relation\":\"" + relation + "\"")
                            + (detail == null ? "" : "," + detail)
                            + "}");
            return this;
        }

        Trace start(String sql) {
            if (depth == 0) req++;
            depth++;
            return event(
                    "executor_start",
                    null,
                    "\"sql\":\"" + sql + "\",\"query_id\":" + (sql.hashCode() | 1));
        }

        Trace end(long processed) {
            event("executor_run_end", null, "\"processed\":" + processed);
            event("executor_end", null, "\"processed\":" + processed);
            depth--;
            return this;
        }

        Trace plan(String scan, String index, int node, String kind) {
            return event("plan_scan", index, "\"scan\":\"" + scan + "\",\"plan_node\":" + node + ",\"kind\":\"" + kind + "\"");
        }

        Trace rescan(String scan, String index, String keys) {
            return event("scan_rescan", index, "\"scan\":\"" + scan + "\",\"found\":false,\"recheck\":false,\"keys\":" + keys);
        }

        Trace next(String scan, String index, boolean found, boolean recheck) {
            return event("scan_next", index, "\"scan\":\"" + scan + "\",\"direction\":1,\"found\":" + found + ",\"recheck\":" + recheck);
        }

        int rows;

        /** A heap verdict; a visible row carries its identity (indexed value 5, a fresh PK). */
        Trace visible(String scan, String index, boolean found) {
            return found ? visible(scan, index, "5", Integer.toString(++rows)) : event("heap_visibility", index, "\"scan\":\"" + scan + "\",\"found\":false,\"recheck\":false");
        }

        Trace visible(String scan, String index, String value, String pk) {
            return event(
                    "heap_visibility",
                    index,
                    "\"scan\":\""
                            + scan
                            + "\",\"found\":true,\"recheck\":false,\"row\":{\"indexed\":[\""
                            + value
                            + "\"],\"pk\":[\""
                            + pk
                            + "\"]}");
        }

        int tids;

        Trace tid(String scan, String index) {
            return tid(scan, index, "(0," + ++tids + ")");
        }

        Trace tid(String scan, String index, String tid) {
            return event("bitmap_candidate", index, "\"scan\":\"" + scan + "\",\"found\":true,\"recheck\":false,\"tid\":\"" + tid + "\"");
        }

        Trace bitmapEnd(String scan, String index) {
            return event("bitmap_end", index, "\"scan\":\"" + scan + "\",\"found\":false,\"recheck\":false");
        }

        /** A bitmap heap node over one scan, or a flat BitmapOr of several. */
        Trace bitmapHeap(String heap, String... scans) {
            return combined(heap, "or", scans);
        }

        Trace combined(String heap, String combination, String... scans) {
            String list = "\"" + String.join("\",\"", scans) + "\"";
            String tree = scans.length == 1 ? list : "{\"" + combination + "\":[" + list + "]}";
            return event("plan_bitmap_heap", "sbtest1", "\"heap\":\"" + heap + "\",\"plan_node\":9,\"tree\":" + tree + ",\"scans\":[" + list + "]");
        }

        Trace verdict(String heap, String tid, boolean visible) {
            return event("bitmap_heap_candidate", "sbtest1", "\"heap\":\"" + heap + "\",\"tid\":\"" + tid + "\",\"found\":" + visible);
        }

        Trace lossy(String heap) {
            return event("bitmap_heap_lossy", "sbtest1", "\"heap\":\"" + heap + "\"");
        }

        Trace scanEnd(String scan, String index) {
            return event("scan_end", index, "\"scan\":\"" + scan + "\",\"found\":false,\"recheck\":false");
        }

        Trace row() {
            return event("result_row", null, "\"values\":[\"x\"]");
        }

        Trace commit() {
            return event("transaction_commit", null, null);
        }

        /** PG forgets every active executor when a transaction aborts. */
        Trace abort() {
            event("transaction_abort", null, null);
            depth = 0;
            return this;
        }

        Path write(Path directory, String name) throws IOException {
            Path file = directory.resolve(name);
            Files.write(file, lines);
            return file;
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> map(Object value) {
        return (Map<String, Object>) value;
    }

    private Map<String, Object> analyze(Trace trace, String name) throws Exception {
        return AccessLedger.analyze(schema(), trace.write(folder.getRoot().toPath(), name), true);
    }

    @Test
    public void claimsEveryEventOnceAndKeepsLayersSeparate() throws Exception {
        var t = new Trace();
        // The planner probes the index endpoint before the executor starts.
        t.event("transaction_commit", null, null);
        t.rescan("p", PK, PROBE).next("p", PK, true, false).visible("p", PK, true).scanEnd("p", PK);
        t.start("SELECT c FROM sbtest1 WHERE id = $1").plan("a", PK, 0, "index").rescan("a", PK, EQUALITY);
        t.next("a", PK, true, false).visible("a", PK, true).row().next("a", PK, false, false);
        t.end(1).scanEnd("a", PK);
        // A range scan over three candidates, one of them dead.
        t.start("SELECT c FROM sbtest1 WHERE id BETWEEN $1 AND $2").plan("b", PK, 0, "index").rescan("b", PK, RANGE);
        t.next("b", PK, true, false).visible("b", PK, true).row();
        t.next("b", PK, true, false).visible("b", PK, false);
        t.next("b", PK, true, false).visible("b", PK, true).row().next("b", PK, false, false);
        t.end(2).scanEnd("b", PK);
        // A bitmap reports TIDs before visibility; the heap finds three of the four visible.
        t.start("SELECT count(*) FROM sbtest1 WHERE k = $1").plan("c", K, 2, "bitmap").bitmapHeap("h", "c").rescan("c", K, EQUALITY);
        for (int i = 1; i <= 4; i++) t.tid("c", K, "(7," + i + ")");
        t.bitmapEnd("c", K);
        for (int i = 1; i <= 4; i++) t.verdict("h", "(7," + i + ")", i != 2);
        t.row();
        t.end(1).scanEnd("c", K);
        // A nested loop rescans one inner scan three times.
        t.start("SELECT * FROM a JOIN sbtest1 ON a.x = sbtest1.id").plan("d", PK, 1, "index");
        for (int i = 0; i < 3; i++) {
            t.rescan("d", PK, EQUALITY).next("d", PK, true, false).visible("d", PK, true).row();
            t.next("d", PK, false, false);
        }
        t.end(3).scanEnd("d", PK);
        // A write maintains two indexes and searches one.
        t.start("UPDATE sbtest1 SET k = 1 WHERE id = 2").plan("e", PK, 1, "index").rescan("e", PK, EQUALITY);
        t.next("e", PK, true, false).visible("e", PK, true);
        t.event("index_insert_attempt", PK, "\"values\":[\"2\"]");
        t.event("index_insert_attempt", K, "\"values\":[\"1\"]");
        t.event("row_change", "sbtest1", "\"operation\":\"update\"");
        t.next("e", PK, false, false);
        t.end(1).scanEnd("e", PK);
        // A sequential scan touches no index.
        t.start("SELECT count(*) FROM sbtest1").row().end(1);
        t.commit();

        var report = analyze(t, "pg-7.jsonl");
        assertEquals(3, report.get("schema_version"));
        assertEquals((long) t.lines.size(), ((Number) map(report.get("partition")).get("sum")).longValue());
        var units = map(report.get("access_units"));
        assertEquals(8L, units.get("total"));
        assertEquals(1L, map(units.get("by_source")).get("planner_endpoint_probe"));
        assertEquals(7L, map(units.get("by_source")).get("executor"));
        assertEquals(1L, map(units.get("by_kind")).get("bitmap"));
        // Candidates: probe 1, equality 1, range 3, bitmap TIDs 4, nested loop 3, write lookup 1.
        assertEquals(13L, units.get("candidates"));
        // Visible rows are a separate layer: heap-checked rows and bitmap heap rows.
        assertEquals(11L, units.get("visible_rows"));
        assertEquals(9L, units.get("heap_fetches"));
        var projection = map(report.get("projection"));
        assertEquals("complete", projection.get("status"));
        assertEquals(Map.of("equality", 6L, "range", 1L), projection.get("projected"));
        assertEquals(Map.of("planner_endpoint_probe", 1L), projection.get("excluded"));
        var requests = map(report.get("requests"));
        assertEquals(6L, requests.get("total"));
        assertEquals(1L, requests.get("without_index_access"));
        assertEquals(1L, requests.get("planner_endpoint_probes_assigned_to_next_request"));
        var templates = (List<?>) report.get("templates");
        var range = templates.stream().map(AccessLedgerTest::map)
                .filter(x -> x.get("sql").toString().contains("between")).findFirst().orElseThrow();
        assertEquals(3L, range.get("candidates"));
        assertEquals(2L, range.get("visible_rows"));
        assertEquals(2L, range.get("returned_rows"));
        assertEquals(1, range.get("plan_variants"));
        var path = map(((List<?>) range.get("access_paths")).get(0));
        assertEquals("range", path.get("decision"));
        assertEquals(PK, path.get("index"));
        var write = templates.stream().map(AccessLedgerTest::map)
                .filter(x -> x.get("sql").toString().startsWith("update")).findFirst().orElseThrow();
        assertEquals(2L, write.get("index_insert_attempts"));
        assertEquals(1L, write.get("row_changes"));
        assertEquals(1L, write.get("access_units"));
    }

    @Test
    public void everyUnitIsProjectedOrRejectedWithItsReason() throws Exception {
        var t = new Trace().start("SELECT 1");
        int[] node = {0};
        java.util.function.BiConsumer<String, String> exhausted =
                (index, keys) -> {
                    String id = "s" + node[0]++;
                    t.plan(id, index, node[0], "index").rescan(id, index, keys).next(id, index, false, false).scanEnd(id, index);
                };
        exhausted.accept(PK, EQUALITY);
        exhausted.accept(PK, "[" + KEY.formatted(1, 5, 0, "\"1\"") + "]");
        exhausted.accept(PK, RANGE);
        exhausted.accept(PK, "[]");
        exhausted.accept(K, "[" + KEY.formatted(1, 0, 0x41, "null") + "]");
        exhausted.accept(K, "[" + KEY.formatted(1, 0, 0x81, "null") + "]");
        exhausted.accept(PK, "[" + KEY.formatted(2, 3, 0, "\"1\"") + "]");
        exhausted.accept(PK, "[" + KEY.formatted(1, 3, 1, "null") + "]");
        exhausted.accept(PK, "[" + KEY.formatted(1, 3, 0x20, "null").replace("}", ",\"unsupported_key\":true}") + "]");
        exhausted.accept(PK, "[{\"attribute\":1,\"strategy\":3,\"flags\":0,\"type_oid\":25,\"value\":\"x\"}]");
        // Consumers that stop early: a limit on the primary key, the returned rows on a secondary.
        t.plan("lp", PK, 20, "index").rescan("lp", PK, RANGE).next("lp", PK, true, false).visible("lp", PK, true).scanEnd("lp", PK);
        t.plan("ls", K, 21, "index").rescan("ls", K, EQUALITY).next("ls", K, true, false).visible("ls", K, true).scanEnd("ls", K);
        // A BitmapOr: one TID is on both sides, so each index gets its verdict.
        t.plan("o1", K, 22, "bitmap").plan("o2", PK, 23, "bitmap").bitmapHeap("or", "o1", "o2");
        t.rescan("o1", K, EQUALITY).tid("o1", K, "(1,1)").tid("o1", K, "(1,2)").bitmapEnd("o1", K);
        t.rescan("o2", PK, EQUALITY).tid("o2", PK, "(1,2)").bitmapEnd("o2", PK);
        t.verdict("or", "(1,1)", true).verdict("or", "(1,2)", true).scanEnd("o1", K).scanEnd("o2", PK);
        // Candidates the heap never checked, and a lossy page.
        t.plan("b1", K, 24, "bitmap").bitmapHeap("h1", "b1").rescan("b1", K, EQUALITY).tid("b1", K).bitmapEnd("b1", K).scanEnd("b1", K);
        t.plan("b2", K, 25, "bitmap").bitmapHeap("h2", "b2").rescan("b2", K, EQUALITY).tid("b2", K).bitmapEnd("b2", K)
                .lossy("h2").scanEnd("b2", K);
        // A rescan that was never fetched from searched nothing.
        t.plan("n", PK, 24, "index").rescan("n", PK, EQUALITY).rescan("n", PK, EQUALITY).next("n", PK, false, false).scanEnd("n", PK);
        t.end(0);
        var projection = map(analyze(t, "pg-12.jsonl").get("projection"));
        assertEquals("has_rejected_units", projection.get("status"));
        assertEquals(
                Map.of(
                        "equality", 5L,
                        "range", 3L,
                        "full_scan", 1L,
                        "limited_range", 1L,
                        "stopped_equality", 1L),
                projection.get("projected"));
        assertEquals(
                Map.of(
                        "composite_key", 1L,
                        "null_comparison", 1L,
                        "row_comparison", 1L,
                        "unsupported_key_type", 1L,
                        "bitmap_heap_stopped_early", 1L,
                        "bitmap_heap_lossy_page", 1L),
                projection.get("rejected"));
        assertEquals(Map.of("rescan_without_fetch", 1L), projection.get("excluded"));
    }

    /**
     * The rows a secondary scan returned before its consumer stopped: the values before the last
     * one are a fully consumed prefix, the last value's rows are pinned by their primary keys.
     */
    @Test
    public void stoppedSecondaryScansKeepTheirReturnedRows() throws Exception {
        var t = new Trace().start("SELECT 1");
        String atLeastFive = "[" + KEY.formatted(1, 4, 0, "\"5\"") + "]";
        t.plan("s", K, 1, "index").rescan("s", K, atLeastFive);
        // k = 5 twice, then k = 6 once; k = 6 has more rows the consumer never asked for.
        t.next("s", K, true, false).visible("s", K, "5", "11");
        t.next("s", K, true, false).visible("s", K, "5", "3");
        t.next("s", K, true, false).visible("s", K, "6", "9").scanEnd("s", K);
        // An index-only scan returns all-visible rows without reading the heap: no identity.
        t.plan("i", K, 2, "index_only").rescan("i", K, atLeastFive);
        t.next("i", K, true, false).event("vm_visible", K, "\"scan\":\"i\",\"found\":true").scanEnd("i", K);
        t.end(0);
        var projection = map(analyze(t, "pg-16.jsonl").get("projection"));
        assertEquals(Map.of("stopped_range", 1L), projection.get("projected"));
        assertEquals(Map.of("stopped_scan_without_row_identity", 1L), projection.get("rejected"));
        // One prefix [5, 6) and one pinned (6, 9).
        assertEquals(2L, projection.get("search_operations"));
    }

    /**
     * A BitmapAnd's inputs of one cycle are one group. PostgreSQL skips the remaining inputs once
     * the intersection is empty, so a later cycle may hold fewer inputs than the plan has.
     */
    @Test
    public void bitmapAndInputsFormOneGroupPerCycle() throws Exception {
        var t = new Trace().start("SELECT 1");
        // Cycle 1 in PostgreSQL's order: constant keys reach both indexes while the plan is
        // initialized, before the heap node binds them; (2,2) is only on one side.
        t.plan("x", K, 1, "bitmap").rescan("x", K, EQUALITY);
        t.plan("y", PK, 2, "bitmap").rescan("y", PK, RANGE).combined("h", "and", "x", "y");
        t.tid("x", K, "(2,1)").tid("x", K, "(2,2)").bitmapEnd("x", K);
        t.tid("y", PK, "(2,1)").bitmapEnd("y", PK);
        t.verdict("h", "(2,1)", true);
        // Cycle 2: x is empty, so y never runs.
        t.rescan("x", K, EQUALITY).bitmapEnd("x", K);
        // Cycle 3: both again; the heap stops before checking the shared TID.
        t.rescan("x", K, EQUALITY).tid("x", K, "(3,1)").bitmapEnd("x", K);
        t.rescan("y", PK, RANGE).tid("y", PK, "(3,1)").bitmapEnd("y", PK);
        t.scanEnd("x", K).scanEnd("y", PK);
        // A nested combination has no flat projection.
        t.plan("p", K, 3, "bitmap").plan("q", PK, 4, "bitmap").plan("r", K, 5, "bitmap");
        t.event(
                "plan_bitmap_heap",
                "sbtest1",
                "\"heap\":\"n\",\"plan_node\":6,\"tree\":{\"and\":[\"p\",{\"or\":[\"q\",\"r\"]}]},\"scans\":[\"p\",\"q\",\"r\"]");
        t.rescan("p", K, EQUALITY).bitmapEnd("p", K).scanEnd("p", K);
        t.end(0);
        var report = analyze(t, "pg-17.jsonl");
        var projection = map(report.get("projection"));
        // Two inputs of cycle 1 as one intersection, x alone in cycle 2.
        assertEquals(Map.of("intersection", 2L, "equality", 1L), projection.get("projected"));
        assertEquals(
                Map.of("bitmap_heap_stopped_early", 2L, "nested_bitmap_combination", 1L),
                projection.get("rejected"));
        assertEquals(Map.of("intersection_groups", 2L, "rejected_groups", 1L), projection.get("bitmap_and_groups"));
    }

    /** An = ANY array is one point per distinct non-null element, as one union. */
    @Test
    public void arrayKeysAreUnionsOfTheirElements() throws Exception {
        var t = new Trace().start("SELECT 1");
        String array =
                "[{\"attribute\":1,\"strategy\":3,\"flags\":32,\"type_oid\":23,\"value\":null,"
                        + "\"array_type_oid\":23,\"array\":[\"9\",\"4\",null,\"9\"]}]";
        t.plan("a", K, 1, "bitmap").bitmapHeap("h", "a").rescan("a", K, array);
        t.tid("a", K, "(1,1)").bitmapEnd("a", K).verdict("h", "(1,1)", true).scanEnd("a", K);
        // Both null: = ANY of an array holding only nulls never matches, so PG does not search.
        String nulls =
                "[{\"attribute\":1,\"strategy\":3,\"flags\":32,\"type_oid\":23,\"value\":null,"
                        + "\"array_type_oid\":23,\"array\":[null]}]";
        t.plan("b", K, 2, "index").rescan("b", K, nulls).next("b", K, false, false).scanEnd("b", K);
        t.end(0);
        var projection = map(analyze(t, "pg-18.jsonl").get("projection"));
        assertEquals(Map.of("union", 1L), projection.get("projected"));
        assertEquals(2L, projection.get("search_operations"));
        assertEquals(
                Map.of("contradictory_scan_keys", 1L), projection.get("excluded"));
    }

    @Test
    public void textIntervalsNeedTheCCollationButEqualityDoesNot() throws Exception {
        var schema =
                SourceSchema.of(
                        "ycsb",
                        Path.of("benchmark/YCSB"),
                        Map.of("fieldcount", 2L, "table", "usertable"));
        String text = "{\"attribute\":1,\"strategy\":%d,\"flags\":0,\"type_oid\":25,\"value\":\"user1\"}";
        var t = new Trace().start("SELECT 1");
        for (int strategy : new int[] {3, 4}) {
            String id = "u" + strategy;
            t.plan(id, "usertable_pkey", strategy, "index").rescan(id, "usertable_pkey", "[" + text.formatted(strategy) + "]");
            t.next(id, "usertable_pkey", false, false).scanEnd(id, "usertable_pkey");
        }
        t.end(0);
        Path file = t.write(folder.getRoot().toPath(), "pg-13.jsonl");
        var ordered = map(AccessLedger.analyze(schema, file, true).get("projection"));
        assertEquals(Map.of("equality", 1L, "range", 1L), ordered.get("projected"));
        var unordered = map(AccessLedger.analyze(schema, file, false).get("projection"));
        assertEquals(Map.of("equality", 1L), unordered.get("projected"));
        assertEquals(Map.of("text_interval_requires_c_collation", 1L), unordered.get("rejected"));
    }

    @Test
    public void literalsDoNotMultiplyTemplates() {
        assertEquals(
                "insert into sbtest1 (k, c, pad) values (?, ?, ?)",
                AccessLedger.template(
                        "insert into sbtest1 (k, c, pad) values (148, '27327-0680', 'it''s')"));
        assertEquals(
                "select c from sbtest2 where id between $1 and $2 and k = ?",
                AccessLedger.template("select c from sbtest2 where id between $1 and $2 and k = -7"));
        // Identifiers with digits and bind markers are untouched.
        assertEquals("update t2 set c3=$1 where k_1=$2", AccessLedger.template("update t2 set c3=$1 where k_1=$2"));
        assertEquals("select ?, ?", AccessLedger.template("select 1.5, 'x'"));
    }

    @Test
    public void rejectsEventsThatCannotBeClaimedExactlyOnce() throws Exception {
        // A trace without request ordinals cannot be attributed to requests.
        Path unnumbered = folder.newFile("pg-0.jsonl").toPath();
        Files.writeString(unnumbered, "{\"event\":\"executor_start\",\"pid\":1,\"sql\":\"SELECT 1\"}\n");
        assertTrue(
                assertThrows(IOException.class, () -> AccessLedger.analyze(schema(), unnumbered, true))
                        .getMessage()
                        .contains("request ordinals"));

        var anonymous = new Trace();
        anonymous.depth = 1;
        anonymous.req = 1;
        anonymous.event("executor_start", null, "\"sql\":\"SELECT 1\"");
        assertTrue(failure(anonymous, "pg-14.jsonl").contains("query identifiers"));

        var orphan = new Trace().start("SELECT 1");
        orphan.next("a", PK, true, false);
        assertTrue(failure(orphan, "pg-1.jsonl").contains("without an access unit"));

        var unbound = new Trace().start("SELECT 1");
        unbound.rescan("a", PK, EQUALITY);
        assertTrue(failure(unbound, "pg-2.jsonl").contains("without plan binding"));

        var repeated = new Trace().start("SELECT 1");
        repeated.plan("a", PK, 0, "index").plan("a", PK, 0, "index");
        assertTrue(failure(repeated, "pg-3.jsonl").contains("Repeated live scan identity"));

        var outside = new Trace();
        outside.rescan("a", PK, EQUALITY);
        assertTrue(failure(outside, "pg-4.jsonl").contains("outside any executor"));

        var mismatch = new Trace().start("SELECT c FROM t");
        mismatch.row().end(2);
        assertTrue(failure(mismatch, "pg-5.jsonl").contains("differ from executor return"));

        var unknown = new Trace().start("SELECT 1");
        unknown.event("mystery", null, null);
        assertTrue(failure(unknown, "pg-6.jsonl").contains("Unclassified raw event"));

        var foreign = new Trace().start("SELECT 1");
        foreign.plan("a", "other_pkey", 0, "index").rescan("a", "other_pkey", EQUALITY);
        assertTrue(failure(foreign, "pg-15.jsonl").contains("Unsupported accessed index"));

        var lingering = new Trace().start("SELECT 1");
        lingering.plan("a", PK, 0, "index").rescan("a", PK, EQUALITY).end(0);
        lingering.start("SELECT 2").end(0);
        assertTrue(failure(lingering, "pg-7.jsonl").contains("left open before its request ended"));

        var survived = new Trace().start("SELECT 1");
        survived.plan("a", PK, 0, "index").rescan("a", PK, EQUALITY).end(0);
        survived.start("SELECT 2").end(0).abort();
        assertTrue(failure(survived, "pg-11.jsonl").contains("survived the abort"));
    }

    @Test
    public void failedStatementScansEndWithTheAbort() throws Exception {
        var t = new Trace().start("INSERT INTO t VALUES ($1)");
        t.plan("a", PK, 0, "index").rescan("a", PK, EQUALITY).abort();
        t.start("SELECT 1").end(0).commit();
        assertEquals(1L, map(analyze(t, "pg-8.jsonl").get("access_units")).get("open_when_statement_aborted"));
    }

    @Test
    public void connectionCloseMayLeaveOnlyTheLastRequestOpen() throws Exception {
        var t = new Trace().start("SELECT 1");
        t.plan("a", PK, 0, "index").rescan("a", PK, EQUALITY).next("a", PK, true, false);
        assertEquals(1L, map(analyze(t, "pg-9.jsonl").get("access_units")).get("open_at_connection_close"));
    }

    @Test
    public void reportIsDeterministicAndNeverOverwritten() throws Exception {
        var t = new Trace().start("SELECT 1");
        t.plan("a", PK, 0, "index").rescan("a", PK, EQUALITY).next("a", PK, false, false).end(0).scanEnd("a", PK);
        Path file = t.write(folder.getRoot().toPath(), "pg-10.jsonl");
        Path report = folder.getRoot().toPath().resolve("ledger.json");
        AccessLedger.audit(schema(), file, report);
        String first = Files.readString(report);
        assertTrue(JsonParser.parseString(first).isJsonObject());
        assertThrows(IOException.class, () -> AccessLedger.audit(schema(), file, report));
        Path second = folder.getRoot().toPath().resolve("ledger-2.json");
        AccessLedger.audit(schema(), file, second);
        assertEquals(first, Files.readString(second));
    }

    private String failure(Trace trace, String name) throws Exception {
        Path file = trace.write(folder.getRoot().toPath(), name);
        return assertThrows(IOException.class, () -> AccessLedger.analyze(schema(), file, true))
                .getMessage();
    }
}
