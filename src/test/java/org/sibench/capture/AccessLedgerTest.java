package org.sibench.capture;

import static org.junit.Assert.*;

import com.google.gson.JsonParser;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AccessLedgerTest {
    @Rule public TemporaryFolder folder = new TemporaryFolder();

    private static final String EQUALITY = "[{\"attribute\":1,\"strategy\":3,\"flags\":0,\"value\":\"5\"}]";
    private static final String RANGE =
            "[{\"attribute\":1,\"strategy\":4,\"flags\":0,\"value\":\"5\"},"
                    + "{\"attribute\":1,\"strategy\":2,\"flags\":0,\"value\":\"8\"}]";
    private static final String PROBE = "[{\"attribute\":1,\"strategy\":0,\"flags\":129,\"value\":null}]";

    /** Builds one backend trace with request ordinals and executor depth. */
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
            return event("executor_start", null, "\"sql\":\"" + sql + "\"");
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
            return event("scan_next", index, "\"scan\":\"" + scan + "\",\"found\":" + found + ",\"recheck\":" + recheck);
        }

        Trace visible(String scan, String index, boolean found) {
            return event("heap_visibility", index, "\"scan\":\"" + scan + "\",\"found\":" + found + ",\"recheck\":false");
        }

        Trace tid(String scan, String index) {
            return event("bitmap_candidate", index, "\"scan\":\"" + scan + "\",\"found\":true,\"recheck\":false");
        }

        Trace bitmapEnd(String scan, String index) {
            return event("bitmap_end", index, "\"scan\":\"" + scan + "\",\"found\":false,\"recheck\":false");
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

    @Test
    public void claimsEveryEventOnceAndKeepsLayersSeparate() throws Exception {
        var t = new Trace();
        // The planner probes the index endpoint before the executor starts.
        t.event("transaction_commit", null, null);
        t.rescan("p", "t_pkey", PROBE).next("p", "t_pkey", true, false).visible("p", "t_pkey", true).scanEnd("p", "t_pkey");
        t.start("SELECT c FROM t WHERE id = $1").plan("a", "t_pkey", 0, "index").rescan("a", "t_pkey", EQUALITY);
        t.next("a", "t_pkey", true, false).visible("a", "t_pkey", true).row().next("a", "t_pkey", false, false);
        t.end(1).scanEnd("a", "t_pkey");
        // A range scan over three candidates, one of them dead.
        t.start("SELECT c FROM t WHERE id BETWEEN $1 AND $2").plan("b", "t_pkey", 0, "index").rescan("b", "t_pkey", RANGE);
        t.next("b", "t_pkey", true, false).visible("b", "t_pkey", true).row();
        t.next("b", "t_pkey", true, false).visible("b", "t_pkey", false);
        t.next("b", "t_pkey", true, false).visible("b", "t_pkey", true).row().next("b", "t_pkey", false, false);
        t.end(2).scanEnd("b", "t_pkey");
        // A bitmap scan reports TIDs before visibility; SQL returns one aggregate row.
        t.start("SELECT count(*) FROM t WHERE k = $1").plan("c", "t_k", 2, "bitmap").rescan("c", "t_k", EQUALITY);
        for (int i = 0; i < 4; i++) t.tid("c", "t_k");
        t.bitmapEnd("c", "t_k").row();
        t.end(1).scanEnd("c", "t_k");
        // A nested loop rescans one inner scan three times.
        t.start("SELECT * FROM a JOIN t ON a.x = t.id").plan("d", "t_pkey", 1, "index");
        for (int i = 0; i < 3; i++) {
            t.rescan("d", "t_pkey", EQUALITY).next("d", "t_pkey", true, false).visible("d", "t_pkey", true).row();
            t.next("d", "t_pkey", false, false);
        }
        t.end(3).scanEnd("d", "t_pkey");
        // A write maintains two indexes and searches none.
        t.start("UPDATE t SET k = 1 WHERE id = 2").plan("e", "t_pkey", 1, "index").rescan("e", "t_pkey", EQUALITY);
        t.next("e", "t_pkey", true, false).visible("e", "t_pkey", true);
        t.event("index_insert_attempt", "t_pkey", "\"values\":[\"2\"]");
        t.event("index_insert_attempt", "t_k", "\"values\":[\"1\"]");
        t.event("row_change", "t", "\"operation\":\"update\"");
        t.end(1).scanEnd("e", "t_pkey");
        // A sequential scan touches no index.
        t.start("SELECT count(*) FROM t").row().end(1);
        t.commit();

        Path file = t.write(folder.getRoot().toPath(), "pg-7.jsonl");
        var report = AccessLedger.analyze(null, file);

        long events = t.lines.size();
        var partition = map(report.get("partition"));
        assertEquals(events, ((Number) partition.get("sum")).longValue());
        var units = map(report.get("access_units"));
        assertEquals(8L, units.get("total"));
        // 1 probe + 1 equality + 1 range + 1 bitmap + 3 nested-loop rescans + 1 write lookup.
        assertEquals(1L, map(units.get("by_source")).get("planner_endpoint_probe"));
        assertEquals(7L, map(units.get("by_source")).get("executor"));
        assertEquals(1L, map(units.get("by_kind")).get("bitmap"));
        assertEquals(1L, map(units.get("by_predicate_shape")).get("range"));
        var eligibility = map(units.get("predicate_eligibility"));
        assertEquals(6L, eligibility.get("single_column_equality"));
        assertEquals(1L, eligibility.get("unsupported_range"));
        assertEquals(1L, eligibility.get("excluded_planner_endpoint_probe"));
        // Candidates: probe 1, equality 1, range 3, bitmap TIDs 4, nested loop 3, write lookup 1.
        assertEquals(13L, units.get("candidates"));
        // Visible rows are a separate attribute: bitmap TIDs are not visibility checked.
        assertEquals(7L, units.get("units_with_visibility_observed"));
        assertEquals(8L, units.get("heap_visible_rows"));
        assertEquals(9L, units.get("heap_fetches"));
        var requests = map(report.get("requests"));
        assertEquals(6L, requests.get("total"));
        assertEquals(1L, requests.get("without_index_access"));
        assertEquals(1L, requests.get("planner_endpoint_probes_assigned_to_next_request"));
        var templates = (List<?>) report.get("templates");
        var range = templates.stream().map(AccessLedgerTest::map)
                .filter(x -> x.get("sql").toString().contains("between")).findFirst().orElseThrow();
        assertEquals(3L, range.get("candidates"));
        assertEquals(2L, range.get("heap_visible_rows"));
        assertEquals(2L, range.get("returned_rows"));
        var write = templates.stream().map(AccessLedgerTest::map)
                .filter(x -> x.get("sql").toString().startsWith("update")).findFirst().orElseThrow();
        assertEquals(2L, write.get("index_insert_attempts"));
        assertEquals(1L, write.get("row_changes"));
        assertEquals(1L, write.get("access_units"));
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
                assertThrows(IOException.class, () -> AccessLedger.analyze(null, unnumbered))
                        .getMessage()
                        .contains("request ordinals"));

        var orphan = new Trace().start("SELECT 1");
        orphan.next("a", "t_pkey", true, false);
        assertTrue(failure(orphan, "pg-1.jsonl").contains("without an access unit"));

        var unbound = new Trace().start("SELECT 1");
        unbound.rescan("a", "t_pkey", EQUALITY);
        assertTrue(failure(unbound, "pg-2.jsonl").contains("without plan binding"));

        var repeated = new Trace().start("SELECT 1");
        repeated.plan("a", "t_pkey", 0, "index").plan("a", "t_pkey", 0, "index");
        assertTrue(failure(repeated, "pg-3.jsonl").contains("Repeated live scan identity"));

        var outside = new Trace();
        outside.rescan("a", "t_pkey", EQUALITY);
        assertTrue(failure(outside, "pg-4.jsonl").contains("outside any executor"));

        var mismatch = new Trace().start("SELECT c FROM t");
        mismatch.row().end(2);
        assertTrue(failure(mismatch, "pg-5.jsonl").contains("differ from executor return"));

        var unknown = new Trace().start("SELECT 1");
        unknown.event("mystery", null, null);
        assertTrue(failure(unknown, "pg-6.jsonl").contains("Unclassified raw event"));

        var lingering = new Trace().start("SELECT 1");
        lingering.plan("a", "t_pkey", 0, "index").rescan("a", "t_pkey", EQUALITY).end(0);
        lingering.start("SELECT 2").end(0);
        assertTrue(failure(lingering, "pg-7.jsonl").contains("left open before its request ended"));

        var survived = new Trace().start("SELECT 1");
        survived.plan("a", "t_pkey", 0, "index").rescan("a", "t_pkey", EQUALITY).end(0);
        survived.start("SELECT 2").end(0).abort();
        assertTrue(failure(survived, "pg-11.jsonl").contains("survived the abort"));
    }

    @Test
    public void failedStatementScansEndWithTheAbort() throws Exception {
        var t = new Trace().start("INSERT INTO t VALUES ($1)");
        t.plan("a", "t_pkey", 0, "index").rescan("a", "t_pkey", EQUALITY).abort();
        t.start("SELECT 1").end(0).commit();
        var report = AccessLedger.analyze(null, t.write(folder.getRoot().toPath(), "pg-8.jsonl"));
        assertEquals(1L, map(report.get("access_units")).get("open_when_statement_aborted"));
    }

    @Test
    public void connectionCloseMayLeaveOnlyTheLastRequestOpen() throws Exception {
        var t = new Trace().start("SELECT 1");
        t.plan("a", "t_pkey", 0, "index").rescan("a", "t_pkey", EQUALITY).next("a", "t_pkey", true, false);
        var report = AccessLedger.analyze(null, t.write(folder.getRoot().toPath(), "pg-9.jsonl"));
        assertEquals(1L, map(report.get("access_units")).get("open_at_connection_close"));
    }

    @Test
    public void reportIsDeterministicAndNeverOverwritten() throws Exception {
        var t = new Trace().start("SELECT 1");
        t.plan("a", "t_pkey", 0, "index").rescan("a", "t_pkey", EQUALITY).next("a", "t_pkey", false, false).end(0).scanEnd("a", "t_pkey");
        Path file = t.write(folder.getRoot().toPath(), "pg-10.jsonl");
        Path report = folder.getRoot().toPath().resolve("ledger.json");
        AccessLedger.audit(null, file, report);
        String first = Files.readString(report);
        assertTrue(JsonParser.parseString(first).isJsonObject());
        assertThrows(IOException.class, () -> AccessLedger.audit(null, file, report));
        Path second = folder.getRoot().toPath().resolve("ledger-2.json");
        AccessLedger.audit(null, file, second);
        assertEquals(first, Files.readString(second));
    }

    private String failure(Trace trace, String name) throws Exception {
        Path file = trace.write(folder.getRoot().toPath(), name);
        return assertThrows(IOException.class, () -> AccessLedger.analyze(null, file)).getMessage();
    }

    @Test
    public void shapesAreClassifiedFromRuntimeKeys() throws Exception {
        String key = "{\"attribute\":%d,\"strategy\":%d,\"flags\":%d,\"value\":\"1\"%s}";
        var shapes =
                List.of(
                        "[" + key.formatted(1, 3, 0, "") + "]",
                        "[" + key.formatted(1, 5, 0, "") + "]",
                        "[" + key.formatted(1, 4, 0, "") + "," + key.formatted(1, 2, 0, "") + "]",
                        "[" + key.formatted(1, 3, 0, "") + "," + key.formatted(2, 3, 0, "") + "]",
                        "[" + key.formatted(1, 3, 0, "") + "," + key.formatted(2, 1, 0, "") + "]",
                        "[" + key.formatted(1, 3, 1, "") + "]",
                        "[" + key.formatted(1, 3, 0, ",\"unsupported_key\":true") + "]",
                        "[]");
        var t = new Trace().start("SELECT 1");
        int i = 0;
        for (String keys : shapes) {
            String id = "s" + i++;
            t.plan(id, "t_pkey", i, "index").rescan(id, "t_pkey", keys).scanEnd(id, "t_pkey");
        }
        t.end(0);
        var report = AccessLedger.analyze(null, t.write(folder.getRoot().toPath(), "pg-12.jsonl"));
        var shape = map(map(report.get("access_units")).get("by_predicate_shape"));
        assertEquals(1L, shape.get("equality"));
        assertEquals(2L, shape.get("range"));
        assertEquals(1L, shape.get("composite_equality"));
        assertEquals(1L, shape.get("composite_mixed"));
        assertEquals(1L, shape.get("null_or_flagged"));
        assertEquals(1L, shape.get("array_or_row"));
        assertEquals(1L, shape.get("no_key"));
        var eligibility = map(map(report.get("access_units")).get("predicate_eligibility"));
        assertEquals(1L, eligibility.get("single_column_equality"));
        assertEquals(7L, eligibility.values().stream().mapToLong(v -> (Long) v).sum() - 1L);
    }
}
