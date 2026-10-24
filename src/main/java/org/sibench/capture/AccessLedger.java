package org.sibench.capture;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.sibench.capture.AccessProjection.Decision;
import org.sibench.capture.AccessUnits.Request;
import org.sibench.capture.AccessUnits.Unit;
import org.sibench.generation.BaselineGenerator;
import org.sibench.schema.SourceSchema;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.*;

/**
 * Partitions raw PG index events into access units. Every event is claimed exactly once, by an
 * access unit, a write-maintenance record, a lifecycle record or an explicit exclusion, so
 * candidates, visible rows and SQL results are attributes of one access and are never summed
 * across layers. Each unit carries the projection decision the trace compiler applies, reported
 * per SQL template (PostgreSQL's query identifier) and per access path within it.
 */
public final class AccessLedger {
    public static final int VERSION = 4;

    private static final class Tally {
        long units, candidates, visible, heapFetches, heapVisible, recheck;
        long descentsMin, descentsMax;

        void add(Unit unit) {
            units++;
            candidates += unit.candidates();
            visible += unit.visible();
            heapFetches += unit.heapFetches;
            heapVisible += unit.heapVisible;
            recheck += unit.recheck;
            descentsMin += unit.descentsMin();
            descentsMax += unit.descentsMax();
        }

        Map<String, Object> describe() {
            var result = new LinkedHashMap<String, Object>();
            result.put("access_units", units);
            result.put("candidates", candidates);
            result.put("visible_rows", visible);
            result.put("heap_fetches", heapFetches);
            result.put("heap_fetch_visible", heapVisible);
            result.put("recheck_flagged_candidates", recheck);
            result.put("tree_descents_min", descentsMin);
            result.put("tree_descents_max", descentsMax);
            return result;
        }
    }

    /** One index access of a template: plan node, index, scan kind, source and decision. */
    private record AccessPath(String source, int planNode, String index, String kind, String decision) {}

    private static final class Template {
        final String sql;
        final Set<String> texts = new TreeSet<>();
        final Map<String, Long> plans = new TreeMap<>();
        final Map<AccessPath, Tally> paths = new TreeMap<>(
                Comparator.comparing(AccessPath::source)
                        .thenComparingInt(AccessPath::planNode)
                        .thenComparing(AccessPath::index)
                        .thenComparing(AccessPath::kind)
                        .thenComparing(AccessPath::decision));
        long requests, withoutIndex, units, candidates, visible, returned, writes, rowChanges;
        long rejected;

        Template(String sql) {
            this.sql = sql;
        }
    }

    private final IndexCatalog catalog;
    private final boolean cCollation;
    private final Map<String, Long> eventTypes = new TreeMap<>();
    private final Map<String, Long> partition = new TreeMap<>();
    private final Map<String, Long> bySource = new TreeMap<>();
    private final Map<String, Long> byKind = new TreeMap<>();
    private final Map<String, Long> projected = new TreeMap<>();
    private final Map<String, Long> rejected = new TreeMap<>();
    private final Map<String, Long> excluded = new TreeMap<>();
    private final Map<String, Long> writes = new TreeMap<>();
    private final Map<String, Long> groups = new TreeMap<>();
    private long searchOperations;
    private final Map<String, Tally> byIndex = new TreeMap<>();
    private final Map<Long, Template> templates = new TreeMap<>();
    private final Tally total = new Tally();
    private final List<Map<String, Object>> inputs = new ArrayList<>();
    private final AccessUnits units;
    private final Map<Request, List<Unit>> requestUnits = new HashMap<>();
    private final Map<Unit, String> decisions = new IdentityHashMap<>();
    private long events, unitEvents, unitCounted, settled, withoutIndex, requests;

    private AccessLedger(SourceSchema schema, boolean cCollation) throws IOException {
        this.catalog = IndexCatalog.fromSchema(schema);
        this.cCollation = cCollation;
        this.units =
                new AccessUnits(
                        new AccessUnits.Listener() {
                            @Override
                            public void unitSettled(Unit unit) throws IOException {
                                settle(unit);
                            }

                            @Override
                            public void groupSettled(AccessUnits.BitmapGroup group)
                                    throws IOException {
                                settleGroup(group);
                            }

                            @Override
                            public void requestFinished(Request request) throws IOException {
                                finish(request);
                            }
                        });
    }

    public static Path defaultReport(Path raw) {
        return (Files.isDirectory(raw) ? raw : raw.toAbsolutePath().getParent())
                .resolve("access-ledger.json");
    }

    /** One backend trace per session file, in name order. */
    public static List<Path> sessionFiles(Path raw) throws IOException {
        if (!Files.isDirectory(raw)) return List.of(raw);
        try (var paths = Files.list(raw)) {
            return paths.filter(
                            p -> {
                                String name = p.getFileName().toString();
                                return name.startsWith("pg-") && name.endsWith(".jsonl");
                            })
                    .sorted()
                    .toList();
        }
    }

    /**
     * Text intervals keep PG order only under the C collation. The schema preflight requires
     * every text column and index to use the database default, so its lc_collate decides.
     */
    static boolean cCollation(JsonObject preflight) {
        if (preflight == null || !preflight.has("environment")) return false;
        JsonObject environment = preflight.getAsJsonObject("environment");
        return environment.has("lc_collate")
                && Set.of("C", "POSIX").contains(environment.get("lc_collate").getAsString());
    }

    /** The schema preflight recorded with the capture, or null when it has none. */
    static JsonObject capturePreflight(Path raw) throws IOException {
        Path directory = Files.isDirectory(raw) ? raw : raw.toAbsolutePath().getParent();
        Path manifest = directory.resolve("capture-run.json");
        if (!Files.exists(manifest)) return null;
        JsonObject run = JsonParser.parseString(Files.readString(manifest)).getAsJsonObject();
        return run.has("schema_preflight") && run.get("schema_preflight").isJsonObject()
                ? run.getAsJsonObject("schema_preflight")
                : null;
    }

    public static Map<String, Object> analyze(SourceSchema schema, Path raw) throws IOException {
        return analyze(schema, raw, cCollation(capturePreflight(raw)));
    }

    static Map<String, Object> analyze(SourceSchema schema, Path raw, boolean cCollation)
            throws IOException {
        List<Path> files = sessionFiles(raw);
        if (files.isEmpty()) throw new IOException("Access ledger requires at least one PG trace");
        var ledger = new AccessLedger(schema, cCollation);
        for (Path file : files) ledger.session(file);
        return ledger.report(schema);
    }

    /** Writes a new report; an existing report path is never overwritten. */
    public static Map<String, Object> audit(SourceSchema schema, Path raw, Path report)
            throws IOException {
        if (Files.exists(report)) throw new IOException("Access ledger report already exists");
        Path partial = report.resolveSibling(report.getFileName() + ".partial");
        if (Files.exists(partial))
            throw new IOException("Unfinished access ledger output exists; use a fresh report");
        var result = analyze(schema, raw);
        Files.writeString(
                partial,
                new GsonBuilder().serializeNulls().setPrettyPrinting().create().toJson(result),
                StandardOpenOption.CREATE_NEW);
        Files.move(partial, report, StandardCopyOption.ATOMIC_MOVE);
        return result;
    }

    private void claim(String category) {
        partition.merge(category, 1L, Long::sum);
    }

    private void session(Path file) throws IOException {
        long sessionEvents = 0, sessionUnits = settled;
        int pid = 0;
        units.beginSession();
        try (var input = Files.newBufferedReader(file)) {
            String line;
            while ((line = input.readLine()) != null) {
                JsonObject e = JsonParser.parseString(line).getAsJsonObject();
                sessionEvents++;
                events++;
                RunTiming.current().advance(1);
                int eventPid = e.get("pid").getAsInt();
                if (pid == 0) pid = eventPid;
                else if (pid != eventPid)
                    throw new IOException("One access ledger input must contain one backend");
                String type = e.get("event").getAsString();
                eventTypes.merge(type, 1L, Long::sum);
                if (!e.has("req") || !e.has("depth"))
                    throw new IOException("Trace lacks request ordinals and executor depth");
                if (type.equals("executor_start")
                        && e.get("depth").getAsInt() <= 1
                        && (!e.has("query_id") || e.get("query_id").getAsLong() == 0))
                    throw new IOException(
                            "Trace lacks PostgreSQL query identifiers; capture with"
                                    + " compute_query_id = on");
                if (type.equals("index_insert_attempt")) {
                    String index = e.get("relation").getAsString();
                    catalog.named(index);
                    writes.merge(index, 1L, Long::sum);
                }
                if (type.equals("scan_rescan")) catalog.named(e.get("relation").getAsString());
                if (!units.accept(e)
                        && !Set.of("transaction_commit", "index_build_start", "index_build_end")
                                .contains(type))
                    throw new IOException("Unclassified raw event: " + type);
                switch (type) {
                    case "executor_start", "executor_run_end", "executor_end" ->
                            claim("executor_lifecycle");
                    case "transaction_commit", "transaction_abort" -> claim("transaction_boundary");
                    case "result_row" -> claim("result_rows");
                    case "row_change" -> claim("row_changes");
                    case "index_build_start", "index_build_end" -> claim("index_build");
                    case "index_insert_attempt" -> claim("index_writes");
                    case "plan_scan", "plan_bitmap_heap" -> claim("plan_bindings");
                    case "bitmap_heap_candidate", "bitmap_heap_lossy" ->
                            claim("bitmap_heap_verdicts");
                    case "scan_end" -> claim("scan_end");
                    default -> unitEvents++;
                }
            }
        }
        units.endSession();
        var input = new LinkedHashMap<String, Object>();
        input.put("file", file.getFileName().toString());
        input.put("sha256", BaselineGenerator.sha256(file));
        input.put("pid", pid);
        input.put("events", sessionEvents);
        input.put("access_units", settled - sessionUnits);
        input.put("identity", "request_ordinal_executor_depth_and_query_id");
        inputs.add(input);
    }

    private void settle(Unit unit) throws IOException {
        settled++;
        unitCounted += unit.events;
        total.add(unit);
        byIndex.computeIfAbsent(unit.scan.index, ignored -> new Tally()).add(unit);
        bySource.merge(unit.source.label, 1L, Long::sum);
        byKind.merge(unit.kind(), 1L, Long::sum);
        if (unit.request != null) requestUnits.computeIfAbsent(unit.request, ignored -> new ArrayList<>()).add(unit);
        Decision decision = AccessProjection.decide(unit, catalog, cCollation);
        // A BitmapAnd input is labelled by its group's decision once the whole group settled.
        if (decision.grouped()) return;
        record(unit, decision);
    }

    private void settleGroup(AccessUnits.BitmapGroup group) throws IOException {
        Decision decision = AccessProjection.decideGroup(group, catalog, cCollation);
        groups.merge(decision.projected() ? "intersection_groups" : "rejected_groups", 1L, Long::sum);
        for (Unit unit : group.members) record(unit, decision);
    }

    private void record(Unit unit, Decision decision) {
        String label =
                decision.projected()
                        ? decision.shape()
                        : (decision.excluded() ? "excluded:" : "rejected:") + decision.reason();
        (decision.projected() ? projected : decision.excluded() ? excluded : rejected)
                .merge(decision.projected() ? decision.shape() : decision.reason(), 1L, Long::sum);
        if (decision.projected()) searchOperations += decision.parts().size();
        if (unit.request != null) decisions.put(unit, label);
    }

    private void finish(Request request) throws IOException {
        requests++;
        String sql = request.sql.strip().replaceAll("\\s+", " ").toLowerCase(Locale.ROOT);
        if (request.processed >= 0
                && (request.returned > 0 || sql.startsWith("select"))
                && request.returned != request.processed)
            throw new IOException(
                    "Result rows differ from executor return for request r"
                            + request.ordinal
                            + ": "
                            + request.returned
                            + " != "
                            + request.processed);
        String text = template(sql);
        Template template = templates.computeIfAbsent(request.queryId, ignored -> new Template(text));
        template.texts.add(text);
        template.requests++;
        template.plans.merge(request.planSignature(), 1L, Long::sum);
        if (request.units == 0) {
            template.withoutIndex++;
            withoutIndex++;
        }
        template.units += request.units;
        template.candidates += request.candidates;
        template.visible += request.visible;
        template.returned += request.returned;
        template.writes += request.writes;
        template.rowChanges += request.rowChanges;
        for (Unit unit : requestUnits.getOrDefault(request, List.of())) {
            String decision = decisions.remove(unit);
            if (decision.startsWith("rejected:")) template.rejected++;
            template.paths
                    .computeIfAbsent(
                            new AccessPath(
                                    unit.source.label,
                                    unit.scan.planNode,
                                    unit.scan.index,
                                    unit.kind(),
                                    decision),
                            ignored -> new Tally())
                    .add(unit);
        }
        requestUnits.remove(request);
    }

    /**
     * Display text only; the template identity is PostgreSQL's query identifier. Quoted strings
     * and bare numbers become '?'; bind markers such as $1 and identifiers such as sbtest1 stay.
     */
    static String template(String sql) {
        return sql.replaceAll("'(?:[^']|'')*'", "?")
                .replaceAll("(?<![\\w$.])-?\\d+(?:\\.\\d+)?(?![\\w])", "?");
    }

    private Map<String, Object> report(SourceSchema schema) throws IOException {
        long claimed = partition.values().stream().mapToLong(Long::longValue).sum();
        if (unitCounted != unitEvents)
            throw new IOException(
                    "Index events were not claimed exactly once: " + unitCounted + " != " + unitEvents);
        if (claimed + unitEvents != events)
            throw new IOException(
                    "Raw events were not partitioned exactly once: "
                            + (claimed + unitEvents)
                            + " != "
                            + events);
        if (!requestUnits.isEmpty() || !decisions.isEmpty())
            throw new IOException("Access units outside any finished request");
        long rejectedUnits = rejected.values().stream().mapToLong(Long::longValue).sum();
        var result = new LinkedHashMap<String, Object>();
        result.put("schema_version", VERSION);
        result.put("status", "passed_event_partition");
        result.put("benchmark", schema.benchmark());
        result.put("inputs", inputs);
        var eventReport = new LinkedHashMap<String, Object>();
        eventReport.put("total", events);
        eventReport.put("by_type", eventTypes);
        result.put("events", eventReport);
        var parts = new LinkedHashMap<String, Object>(partition);
        parts.put("access_unit_events", unitEvents);
        parts.put("sum", events);
        result.put("partition", parts);
        var unitReport = new LinkedHashMap<String, Object>();
        unitReport.put("total", settled);
        unitReport.put("by_source", bySource);
        unitReport.put("by_kind", byKind);
        unitReport.put("open_at_connection_close", units.openAtClose());
        unitReport.put("open_when_statement_aborted", units.abortedOpen());
        unitReport.putAll(total.describe());
        unitReport.remove("access_units");
        result.put("access_units", unitReport);
        var projection = new LinkedHashMap<String, Object>();
        projection.put("status", rejectedUnits == 0 ? "complete" : "has_rejected_units");
        projection.put("text_intervals_ordered", cCollation);
        projection.put("projected", projected);
        projection.put("rejected", rejected);
        projection.put("excluded", excluded);
        projection.put("bitmap_and_groups", groups);
        projection.put("search_operations", searchOperations);
        result.put("projection", projection);
        var indexReports = new ArrayList<Map<String, Object>>();
        // Every source index, accessed or not, so a counter on an untouched index is comparable.
        var names = new TreeSet<String>(byIndex.keySet());
        names.addAll(writes.keySet());
        for (var definition : catalog.definitions()) names.add(definition.name());
        for (String name : names) {
            var item = new LinkedHashMap<String, Object>();
            item.put("index", name);
            var definition = catalog.named(name);
            item.put("table", definition.table());
            item.put("column", definition.column());
            item.put("role", definition.indexId() == 0 ? "primary" : "secondary");
            item.putAll(byIndex.getOrDefault(name, new Tally()).describe());
            item.put("index_insert_attempts", writes.getOrDefault(name, 0L));
            indexReports.add(item);
        }
        result.put("indexes", indexReports);
        var requestReport = new LinkedHashMap<String, Object>();
        requestReport.put("total", requests);
        requestReport.put("without_index_access", withoutIndex);
        requestReport.put("with_index_access", requests - withoutIndex);
        requestReport.put("nested_executors", units.nestedExecutors());
        requestReport.put("nested_executor_result_rows", units.nestedRows());
        requestReport.put("planner_endpoint_probes_assigned_to_next_request", units.probesAssigned());
        requestReport.put("planner_endpoint_probes_without_request", units.probesUnassigned());
        result.put("requests", requestReport);
        var templateReports = new ArrayList<Map<String, Object>>();
        for (var entry : templates.entrySet()) {
            Template t = entry.getValue();
            var item = new LinkedHashMap<String, Object>();
            // A string: int64 identifiers do not survive JSON tools that read numbers as doubles.
            item.put("query_id", Long.toString(entry.getKey()));
            item.put("sql", t.sql);
            if (t.texts.size() > 1) item.put("text_variants", t.texts.size());
            item.put("requests", t.requests);
            item.put("requests_without_index_access", t.withoutIndex);
            item.put("access_units", t.units);
            item.put("rejected_units", t.rejected);
            item.put("candidates", t.candidates);
            item.put("visible_rows", t.visible);
            item.put("returned_rows", t.returned);
            item.put("index_insert_attempts", t.writes);
            item.put("row_changes", t.rowChanges);
            item.put("plan_variants", t.plans.size());
            var plans = new ArrayList<Map<String, Object>>();
            t.plans.forEach(
                    (plan, count) -> {
                        var entryReport = new LinkedHashMap<String, Object>();
                        entryReport.put("plan", plan);
                        entryReport.put("requests", count);
                        plans.add(entryReport);
                    });
            item.put("plans", plans);
            var paths = new ArrayList<Map<String, Object>>();
            for (var path : t.paths.entrySet()) {
                var description = new LinkedHashMap<String, Object>();
                description.put("source", path.getKey().source());
                description.put("plan_node", path.getKey().planNode());
                description.put("index", path.getKey().index());
                description.put("kind", path.getKey().kind());
                description.put("decision", path.getKey().decision());
                description.putAll(path.getValue().describe());
                paths.add(description);
            }
            item.put("access_paths", paths);
            templateReports.add(item);
        }
        result.put("templates", templateReports);
        result.put(
                "counting_rules",
                List.of(
                        "One access unit per index rescan of one live scan; rescans of a nested loop are"
                            + " separate units.",
                        "Candidates, visible rows and returned rows are separate attributes and"
                            + " are never summed.",
                        "Bitmap candidates are TIDs before visibility; their heap verdicts count for"
                            + " every index scan that produced the TID; visible rows of plain and"
                            + " index-only scans are heap-checked or all-visible index rows.",
                        "Templates are PostgreSQL query identifiers; a plan is the bound sequence of"
                            + " plan node, scan kind and index of one request.",
                        "Planner endpoint probes and rescans never fetched from are excluded and"
                            + " reported separately.",
                        "Write maintenance (index_insert_attempt) is not a search.",
                        "heap_fetch_visible counts only rows a plain or index-only scan fetched"
                            + " from the heap and found visible; tree descents are bounded per"
                            + " unit: exact per pass without array keys, 0 to one per element"
                            + " combination per pass with them.",
                        "A BitmapAnd's inputs from one cycle are one group: the heap checks their"
                            + " intersection, so each input carries the group's decision."));
        result.put(
                "limitations",
                List.of(
                        "Residual filters above the index scan are not counted; they do not change the"
                            + " index result.",
                        "Seq scans produce no index event; zero access units is an observed fact, not"
                            + " a plan inference.",
                        "Rejected only where a fact is missing: lossy bitmap pages carry no candidate"
                            + " TIDs, a bitmap heap scan stopped early never checked some candidates,"
                            + " and an all-visible index-only row carries no primary key."));
        return result;
    }
}
