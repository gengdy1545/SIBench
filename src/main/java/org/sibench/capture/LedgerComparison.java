package org.sibench.capture;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Aligns two access-ledger reports by PostgreSQL query identifier and reports how the plan and
 * access-path sets of one template differ between them. A template is PostgreSQL's query_id, so
 * the comparison does not depend on SQL text: a template with several plans is a workload that
 * changed plan partway through (a prepared statement moving to a generic plan, for example), and
 * comparing two runs or two scales shows whether both saw the same plans.
 *
 * <p>The comparison is pure. It reads two parsed reports, never touches the database or the file
 * system, and sorts every list it emits, so one pair of reports always yields one report.
 */
public final class LedgerComparison {
    private LedgerComparison() {}

    /** The five numeric tallies one access path carries in a ledger report. */
    private record Tally(
            long units, long candidates, long visible, long heapFetches, long recheck) {}

    /** One template of one report: the request count per plan signature and the tally per path. */
    private static final class Template {
        final Map<String, Long> plans = new TreeMap<>();
        final Map<String, Tally> paths = new TreeMap<>();
        long variants;
        String sql = "";

        /** The normalized text the ledger prints, used to pair templates across databases. */
        String key() {
            return sql;
        }
    }

    /**
     * Compares two access-ledger reports. {@code identical} lists the templates whose plan and
     * access-path sets are equal; their tallies can still differ and are reported in
     * {@code templates}, whose {@code tally_delta} is always the second report minus the first.
     */
    public static Map<String, Object> compare(JsonObject a, JsonObject b) {
        // Templates are only comparable within one ledger schema: an older report names them by
        // SQL text and carries no query identifier, which would silently look like total drift.
        long leftVersion = number(a, "schema_version"), rightVersion = number(b, "schema_version");
        if (leftVersion != rightVersion)
            throw new IllegalArgumentException(
                    "Access ledger reports differ in schema version: "
                            + leftVersion
                            + " vs "
                            + rightVersion
                            + "; compare reports produced by the same code");
        Map<String, Template> left = templates(a), right = templates(b);
        if (left.isEmpty() || right.isEmpty())
            throw new IllegalArgumentException(
                    "An access ledger report carries no query-identified templates");
        var ids = new TreeSet<String>(left.keySet());
        ids.addAll(right.keySet());
        var identical = new ArrayList<String>();
        var onlyInA = new ArrayList<String>();
        var onlyInB = new ArrayList<String>();
        var reports = new ArrayList<Map<String, Object>>();
        boolean drift = false;
        // PostgreSQL's query identifier jumbles relation OIDs, so the same statement gets a
        // different one in every database: within one capture it is the identity, across two runs
        // the normalized SQL text is the only key that survives.
        var paired = new LinkedHashMap<String, String>();
        long matchedById = 0;
        for (String id : new TreeSet<>(left.keySet()))
            if (right.containsKey(id)) {
                paired.put(id, id);
                matchedById++;
            }
        var rightBySql = new LinkedHashMap<String, String>();
        for (var entry : right.entrySet())
            rightBySql.putIfAbsent(entry.getValue().key(), entry.getKey());
        long matchedBySql = 0;
        for (String id : new TreeSet<>(left.keySet())) {
            if (paired.containsKey(id)) continue;
            String other = rightBySql.get(left.get(id).key());
            if (other != null && !paired.containsValue(other)) {
                paired.put(id, other);
                matchedBySql++;
            }
        }
        for (String id : ids) {
            if (!paired.containsKey(id) && paired.containsValue(id)) continue;
            if (!paired.containsKey(id)) {
                (left.containsKey(id) ? onlyInA : onlyInB).add(id);
                continue;
            }
            Template one = left.get(id), two = right.get(paired.get(id));
            var plansAdded = plans(two.plans, one.plans);
            var plansRemoved = plans(one.plans, two.plans);
            List<String> pathsAdded = added(two.paths.keySet(), one.paths.keySet());
            List<String> pathsRemoved = added(one.paths.keySet(), two.paths.keySet());
            var deltas = new ArrayList<Map<String, Object>>();
            var shared = new TreeSet<String>(one.paths.keySet());
            shared.retainAll(two.paths.keySet());
            for (String path : shared)
                deltas.add(delta(path, one.paths.get(path), two.paths.get(path)));
            boolean same =
                    plansAdded.isEmpty()
                            && plansRemoved.isEmpty()
                            && pathsAdded.isEmpty()
                            && pathsRemoved.isEmpty();
            if (same) identical.add(id);
            else drift = true;
            var item = new LinkedHashMap<String, Object>();
            item.put("query_id", id);
            item.put("plan_variants_a", one.variants);
            item.put("plan_variants_b", two.variants);
            item.put("plans_added", plansAdded);
            item.put("plans_removed", plansRemoved);
            item.put("paths_added", pathsAdded);
            item.put("paths_removed", pathsRemoved);
            item.put("tally_delta", deltas);
            reports.add(item);
        }
        var result = new LinkedHashMap<String, Object>();
        result.put("status", drift ? "plan_drift" : "identical_plans");
        result.put("identical", identical);
        result.put("only_in_a", onlyInA);
        result.put("only_in_b", onlyInB);
        result.put("templates", reports);
        result.put("counts", counts(a, b));
        result.put(
                "matched_by",
                Map.of(
                        "query_id",
                        matchedById,
                        "sql_text",
                        matchedBySql,
                        "note",
                        "PostgreSQL's query identifier jumbles relation OIDs, so it only pairs"
                                + " templates inside one database; across runs the normalized SQL"
                                + " text pairs them."));
        return result;
    }

    private static Map<String, Template> templates(JsonObject report) {
        var result = new TreeMap<String, Template>();
        JsonElement element = report.get("templates");
        if (element == null || !element.isJsonArray()) return result;
        for (JsonElement item : element.getAsJsonArray()) {
            if (!item.isJsonObject()) continue;
            JsonObject value = item.getAsJsonObject();
            JsonElement id = value.get("query_id");
            if (id == null) continue;
            var template = new Template();
            JsonElement text = value.get("sql");
            if (text != null) template.sql = text.getAsString();
            template.variants = number(value, "plan_variants");
            JsonElement plans = value.get("plans");
            if (plans != null && plans.isJsonArray())
                for (JsonElement plan : plans.getAsJsonArray()) {
                    JsonObject entry = plan.getAsJsonObject();
                    template.plans.put(entry.get("plan").getAsString(), number(entry, "requests"));
                }
            if (template.variants == 0) template.variants = template.plans.size();
            JsonElement paths = value.get("access_paths");
            if (paths != null && paths.isJsonArray())
                for (JsonElement path : paths.getAsJsonArray()) {
                    JsonObject entry = path.getAsJsonObject();
                    template.paths.put(render(entry), tally(entry));
                }
            result.put(id.getAsString(), template);
        }
        return result;
    }

    /** One comparable line per access path: source, plan node, index, scan kind and decision. */
    static String render(JsonObject path) {
        return String.join(
                "|",
                text(path, "source"),
                Long.toString(number(path, "plan_node")),
                text(path, "index"),
                text(path, "kind"),
                text(path, "decision"));
    }

    private static Tally tally(JsonObject path) {
        return new Tally(
                number(path, "access_units"),
                number(path, "candidates"),
                number(path, "visible_rows"),
                number(path, "heap_fetches"),
                number(path, "recheck_flagged_candidates"));
    }

    /** Plans of {@code source} that {@code other} does not have, with their request counts. */
    private static List<Map<String, Object>> plans(
            Map<String, Long> source, Map<String, Long> other) {
        var result = new ArrayList<Map<String, Object>>();
        for (String plan : new TreeSet<>(added(source.keySet(), other.keySet()))) {
            var item = new LinkedHashMap<String, Object>();
            item.put("plan", plan);
            item.put("requests", source.get(plan));
            result.add(item);
        }
        return result;
    }

    private static List<String> added(Set<String> source, Set<String> other) {
        var result = new ArrayList<String>();
        for (String value : source) if (!other.contains(value)) result.add(value);
        result.sort(null);
        return result;
    }

    private static Map<String, Object> delta(String path, Tally a, Tally b) {
        var result = new LinkedHashMap<String, Object>();
        result.put("path", path);
        result.put("access_units", b.units() - a.units());
        result.put("candidates", b.candidates() - a.candidates());
        result.put("visible_rows", b.visible() - a.visible());
        result.put("heap_fetches", b.heapFetches() - a.heapFetches());
        result.put("recheck_flagged_candidates", b.recheck() - a.recheck());
        return result;
    }

    /** Request, access unit, rejected and projected totals of both sides. */
    private static Map<String, Object> counts(JsonObject a, JsonObject b) {
        var result = new LinkedHashMap<String, Object>();
        result.put("requests_a", nested(a, "requests", "total"));
        result.put("requests_b", nested(b, "requests", "total"));
        result.put("access_units_a", nested(a, "access_units", "total"));
        result.put("access_units_b", nested(b, "access_units", "total"));
        result.put("rejected_units_a", summed(a, "rejected"));
        result.put("rejected_units_b", summed(b, "rejected"));
        result.put("projected_units_a", summed(a, "projected"));
        result.put("projected_units_b", summed(b, "projected"));
        return result;
    }

    private static long nested(JsonObject report, String section, String key) {
        JsonElement element = report.get(section);
        return element != null && element.isJsonObject()
                ? number(element.getAsJsonObject(), key)
                : 0L;
    }

    /** The sum of one projection tally map: rejected reasons or projected shapes. */
    private static long summed(JsonObject report, String key) {
        JsonElement projection = report.get("projection");
        if (projection == null || !projection.isJsonObject()) return 0L;
        JsonElement element = projection.getAsJsonObject().get(key);
        if (element == null || !element.isJsonObject()) return 0L;
        long total = 0;
        for (var entry : element.getAsJsonObject().entrySet())
            total += entry.getValue().getAsLong();
        return total;
    }

    private static long number(JsonObject value, String key) {
        JsonElement element = value.get(key);
        return element == null || element.isJsonNull() ? 0L : element.getAsLong();
    }

    private static String text(JsonObject value, String key) {
        JsonElement element = value.get(key);
        return element == null || element.isJsonNull() ? "" : element.getAsString();
    }
}
