package org.sibench.capture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.junit.Test;

import java.util.List;
import java.util.Map;

public class LedgerComparisonTest {
    @SuppressWarnings("unchecked")
    private static Map<String, Object> map(Object value) {
        return (Map<String, Object>) value;
    }

    @SuppressWarnings("unchecked")
    private static List<Object> list(Object value) {
        return (List<Object>) value;
    }

    private static JsonObject ledger(String templates) {
        return JsonParser.parseString(
                        """
                        {"schema_version":2,"benchmark":"sysbench","requests":{"total":12},
                         "access_units":{"total":9},
                         "projection":{"projected":{"equality":8},"rejected":{"bitmap_heap_lossy_page":1}},
                         "templates":[%s]}
                        """
                                .formatted(templates))
                .getAsJsonObject();
    }

    private static String template(String queryId, int variants, String plans, String paths) {
        // Distinct text by default, so a template is only paired by SQL when a test asks for it.
        return template(
                queryId, "select c from sbtest1 where id = $1 /* %s */".formatted(queryId),
                variants, plans, paths);
    }

    private static String template(
            String queryId, String sql, int variants, String plans, String paths) {
        return """
                {"query_id":"%s","sql":"%s","requests":6,
                 "plan_variants":%d,"plans":[%s],"access_paths":[%s]}
                """
                .formatted(queryId, sql, variants, plans, paths);
    }

    private static String plan(String signature, long requests) {
        return "{\"plan\":\"" + signature + "\",\"requests\":" + requests + "}";
    }

    private static String path(
            String source,
            int node,
            String index,
            String kind,
            String decision,
            long units,
            long candidates,
            long visible,
            long heap,
            long recheck) {
        return """
                {"source":"%s","plan_node":%d,"index":"%s","kind":"%s","decision":"%s",
                 "access_units":%d,"candidates":%d,"visible_rows":%d,"heap_fetches":%d,
                 "recheck_flagged_candidates":%d}
                """
                .formatted(
                        source, node, index, kind, decision, units, candidates, visible, heap,
                        recheck);
    }

    private static final String PK = "sbtest1_pkey";
    private static final String POINT =
            path("executor", 0, PK, "index", "equality", 6, 6, 6, 6, 0);
    private static final String CUSTOM = "index:0:" + PK + ":equality";
    private static final String GENERIC = "index:0:" + PK + ":full_scan";

    private static Map<String, Object> first(Map<String, Object> report) {
        return map(list(report.get("templates")).get(0));
    }

    @Test
    public void identicalLedgersShareEveryPlanAndPath() {
        String template = template("123456789", 1, plan(CUSTOM, 6), POINT);
        var report = LedgerComparison.compare(ledger(template), ledger(template));
        assertEquals("identical_plans", report.get("status"));
        assertEquals(List.of("123456789"), report.get("identical"));
        assertEquals(List.of(), report.get("only_in_a"));
        assertEquals(List.of(), report.get("only_in_b"));
        var template45 = first(report);
        assertEquals("123456789", template45.get("query_id"));
        assertEquals(1L, template45.get("plan_variants_a"));
        assertEquals(1L, template45.get("plan_variants_b"));
        assertEquals(List.of(), template45.get("plans_added"));
        assertEquals(List.of(), template45.get("plans_removed"));
        assertEquals(List.of(), template45.get("paths_added"));
        assertEquals(List.of(), template45.get("paths_removed"));
        var delta = map(list(template45.get("tally_delta")).get(0));
        assertEquals("executor|0|" + PK + "|index|equality", delta.get("path"));
        for (String key :
                List.of(
                        "access_units",
                        "candidates",
                        "visible_rows",
                        "heap_fetches",
                        "recheck_flagged_candidates")) assertEquals(0L, delta.get(key));
        var counts = map(report.get("counts"));
        assertEquals(12L, counts.get("requests_a"));
        assertEquals(12L, counts.get("requests_b"));
        assertEquals(9L, counts.get("access_units_a"));
        assertEquals(9L, counts.get("access_units_b"));
        assertEquals(1L, counts.get("rejected_units_a"));
        assertEquals(1L, counts.get("rejected_units_b"));
        assertEquals(8L, counts.get("projected_units_a"));
        assertEquals(8L, counts.get("projected_units_b"));
    }

    @Test
    public void aTemplateThatChangedPlanPartwayThroughIsDrift() {
        String one = template("42", 1, plan(CUSTOM, 6), POINT);
        String two =
                template(
                        "42",
                        2,
                        plan(CUSTOM, 4) + "," + plan(GENERIC, 2),
                        path("executor", 0, PK, "index", "equality", 4, 4, 4, 4, 0)
                                + ","
                                + path(
                                        "executor", 0, PK, "index", "full_scan", 2, 200, 200, 200,
                                        0));
        var report = LedgerComparison.compare(ledger(one), ledger(two));
        assertEquals("plan_drift", report.get("status"));
        assertEquals(List.of(), report.get("identical"));
        var compared = first(report);
        assertEquals(1L, compared.get("plan_variants_a"));
        assertEquals(2L, compared.get("plan_variants_b"));
        assertEquals(List.of(), compared.get("plans_removed"));
        assertEquals(List.of(Map.of("plan", GENERIC, "requests", 2L)), compared.get("plans_added"));
        assertEquals(List.of("executor|0|" + PK + "|index|full_scan"), compared.get("paths_added"));
        assertEquals(List.of(), compared.get("paths_removed"));
        var delta = map(list(compared.get("tally_delta")).get(0));
        assertEquals("executor|0|" + PK + "|index|equality", delta.get("path"));
        assertEquals(-2L, delta.get("access_units"));
        assertEquals(-2L, delta.get("candidates"));
    }

    @Test
    public void plansAndPathsOnlyOneSideHasAreSortedAndReported() {
        String one =
                template("7", 2, plan(CUSTOM, 3) + "," + plan(GENERIC, 1),
                        POINT + "," + path("executor", 0, PK, "index", "full_scan", 1, 9, 9, 9, 0));
        String two = template("7", 1, plan(CUSTOM, 4), POINT);
        String other = template("8", 1, plan(CUSTOM, 2), POINT);
        var report =
                LedgerComparison.compare(ledger(one + "," + other), ledger(two));
        assertEquals("plan_drift", report.get("status"));
        assertEquals(List.of("8"), report.get("only_in_a"));
        assertEquals(List.of(), report.get("only_in_b"));
        var compared = first(report);
        assertEquals(
                List.of(Map.of("plan", GENERIC, "requests", 1L)), compared.get("plans_removed"));
        assertEquals(List.of(), compared.get("plans_added"));
        assertEquals(
                List.of("executor|0|" + PK + "|index|full_scan"), compared.get("paths_removed"));
        assertEquals(1L, compared.get("plan_variants_b"));
        var shared = map(list(compared.get("tally_delta")).get(0));
        assertEquals("executor|0|" + PK + "|index|equality", shared.get("path"));
        assertEquals(0L, shared.get("access_units"));
    }

    @Test
    public void templatesOnlyInTheSecondReportAreListed() {
        String one = template("1", 1, plan(CUSTOM, 1), POINT);
        String two = template("2", 1, plan(GENERIC, 1), POINT);
        var report = LedgerComparison.compare(ledger(one), ledger(two));
        assertEquals(List.of("1"), report.get("only_in_a"));
        assertEquals(List.of("2"), report.get("only_in_b"));
        assertEquals(List.of(), report.get("identical"));
        assertEquals(List.of(), report.get("templates"));
        assertEquals("identical_plans", report.get("status"));
    }

    @Test
    public void everyEmittedListIsSorted() {
        String many =
                template("90", 1, plan(CUSTOM, 1), POINT)
                        + ","
                        + template("9", 1, plan(GENERIC, 1), POINT)
                        + ","
                        + template("100", 1, plan(CUSTOM, 1), POINT);
        var report = LedgerComparison.compare(ledger(many), ledger(many));
        assertEquals(List.of("100", "9", "90"), report.get("identical"));
        var ids =
                list(report.get("templates")).stream()
                        .map(x -> map(x).get("query_id").toString())
                        .toList();
        assertEquals(List.of("100", "9", "90"), ids);
        var drift =
                LedgerComparison.compare(
                        ledger(many),
                        ledger(
                                template("90", 1, plan(GENERIC, 1), POINT)
                                        + ","
                                        + template("5", 1, plan(CUSTOM, 1), POINT)));
        assertEquals(List.of("100", "9"), drift.get("only_in_a"));
        assertEquals(List.of("5"), drift.get("only_in_b"));
    }

    @Test
    public void rendersEveryAccessPathField() {
        var report =
                LedgerComparison.compare(
                        ledger(template("3", 1, plan(CUSTOM, 1), "")),
                        ledger(
                                template(
                                        "3",
                                        1,
                                        plan(CUSTOM, 1),
                                        path("planner_endpoint_probe", 4, "k_1", "bitmap", "excluded:planner_endpoint_probe", 1, 1, 0, 0, 0))));
        assertEquals("plan_drift", report.get("status"));
        assertEquals(
                List.of("planner_endpoint_probe|4|k_1|bitmap|excluded:planner_endpoint_probe"),
                first(report).get("paths_added"));
        assertTrue(list(first(report).get("tally_delta")).isEmpty());
    }

    /**
     * PostgreSQL's query identifier jumbles relation OIDs, so the same statement carries a
     * different one in every database. Across two runs the normalized SQL text has to pair them,
     * otherwise every template looks like it exists in only one run.
     */
    @Test
    public void templatesPairBySqlTextWhenQueryIdentifiersDiffer() {
        String first =
                template(
                        "-1",
                        "select c from sbtest1 where id = $1",
                        1,
                        plan("1:index:idx_a", 6),
                        path("executor", 1, "idx_a", "index", "equality", 6, 6, 6, 6, 0));
        String second =
                template(
                        "-2",
                        "select c from sbtest1 where id = $1",
                        1,
                        plan("1:index:idx_a", 6),
                        path("executor", 1, "idx_a", "index", "equality", 6, 6, 6, 6, 0));
        var report = LedgerComparison.compare(ledger(first), ledger(second));
        assertEquals("identical_plans", report.get("status"));
        assertEquals(List.of("-1"), report.get("identical"));
        assertEquals(List.of(), report.get("only_in_a"));
        assertEquals(List.of(), report.get("only_in_b"));
        assertEquals(0L, map(report.get("matched_by")).get("query_id"));
        assertEquals(1L, map(report.get("matched_by")).get("sql_text"));
    }

    @Test
    public void reportsOfDifferentSchemaVersionsAreRefused() {
        JsonObject old =
                JsonParser.parseString(
                                """
                                {"schema_version":1,"templates":[{"sql":"select 1","requests":1}]}
                                """)
                        .getAsJsonObject();
        var failure =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> LedgerComparison.compare(old, ledger(template("-1", 1, plan("1:index:idx_a", 6), path("executor", 1, "idx_a", "index", "equality", 6, 6, 6, 6, 0)))));
        assertTrue(failure.getMessage(), failure.getMessage().contains("schema version"));
    }
}
