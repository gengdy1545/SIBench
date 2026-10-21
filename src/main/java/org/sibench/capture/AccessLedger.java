package org.sibench.capture;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.sibench.generation.BaselineGenerator;
import org.sibench.schema.SourceSchema;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Partitions raw PG index events into access units. Every event is claimed exactly once, by an
 * access unit, a write-maintenance record, a lifecycle record or an explicit exclusion, so
 * candidates, visible rows and SQL results are attributes of one access and are never summed
 * across layers.
 */
public final class AccessLedger {
    public static final int VERSION = 1;
    private static final int SK_SEARCHARRAY = 0x20, SK_SEARCHNOTNULL = 0x80;
    private static final Set<Integer> INEQUALITY = Set.of(1, 2, 4, 5);
    private static final int MAX_TEMPLATES = 1000;

    private enum Source {
        EXECUTOR("executor"),
        NESTED("nested_executor"),
        PLANNER_PROBE("planner_endpoint_probe");

        final String label;

        Source(String label) {
            this.label = label;
        }
    }

    private static final class Scan {
        final String index;
        String kind = "unbound";
        int planNode = -1;
        Unit current;

        Scan(String index) {
            this.index = index;
        }
    }

    private static final class Unit {
        final Scan scan;
        final Source source;
        final String shape;
        final Request request;
        long calls, found, heapFetches, heapVisible, recheck, bitmapTids;
        boolean bitmapEnded;

        Unit(Scan scan, Source source, String shape, Request request) {
            this.scan = scan;
            this.source = source;
            this.shape = shape;
            this.request = request;
        }

        boolean bitmap() {
            return scan.kind.equals("bitmap") || bitmapTids > 0 || bitmapEnded;
        }

        /** Index tuples returned by the access method, or TIDs placed in a bitmap. */
        long candidates() {
            return bitmap() ? bitmapTids : found;
        }

        /** Only plain index scans report the heap fetch that decides visibility. */
        boolean visibilityObserved() {
            return !bitmap() && !scan.kind.equals("index_only");
        }

        String kind() {
            return bitmap() ? "bitmap" : scan.kind;
        }

        long events() {
            return 1 + calls + heapFetches + bitmapTids + (bitmapEnded ? 1 : 0);
        }
    }

    private static final class Request {
        final String key;
        final String sql;
        long units, candidates, heapVisible, returned, writes, rowChanges;
        long processed = -1;

        Request(String key, String sql) {
            this.key = key;
            this.sql = sql;
        }
    }

    private static final class Tally {
        long units, candidates, heapVisible, heapFetches, recheck, visibilityUnits;

        void add(Unit unit) {
            units++;
            candidates += unit.candidates();
            heapFetches += unit.heapFetches;
            heapVisible += unit.heapVisible;
            recheck += unit.recheck;
            if (unit.visibilityObserved()) visibilityUnits++;
        }

        Map<String, Object> describe() {
            var result = new LinkedHashMap<String, Object>();
            result.put("access_units", units);
            result.put("candidates", candidates);
            result.put("units_with_visibility_observed", visibilityUnits);
            result.put("heap_fetches", heapFetches);
            result.put("heap_visible_rows", heapVisible);
            result.put("recheck_flagged_candidates", recheck);
            return result;
        }
    }

    private static final class Template {
        long requests, withoutIndex, units, candidates, heapVisible, returned, writes, rowChanges;
    }

    private static final class Session {
        final Map<String, Scan> scans = new HashMap<>();
        Request current;
        boolean currentEnded;
        long pendingProbes;
    }

    private final Map<String, SourceSchema.Index> indexes = new HashMap<>();
    private final Map<String, String> tables = new HashMap<>();
    private final Map<String, Long> eventTypes = new TreeMap<>();
    private final Map<String, Long> partition = new TreeMap<>();
    private final Map<String, Long> byShape = new TreeMap<>();
    private final Map<String, Long> bySource = new TreeMap<>();
    private final Map<String, Long> byKind = new TreeMap<>();
    private final Map<String, Long> eligibility = new TreeMap<>();
    private final Map<String, Long> writes = new TreeMap<>();
    private final Map<String, Tally> byIndex = new TreeMap<>();
    private final Map<String, Template> templates = new TreeMap<>();
    private final Tally total = new Tally();
    private final List<Map<String, Object>> inputs = new ArrayList<>();
    private long events, started, settled, fetchEvents, unitCounted, requests, withoutIndex;
    private long openAtClose, abortedOpen, nestedRows, nestedExecutors, probesAssigned, overflow;
    private long probesUnassigned;

    private AccessLedger(SourceSchema schema) {
        if (schema == null) return;
        for (var table : schema.tables())
            for (var index : table.indexes()) {
                indexes.put(index.name(), index);
                tables.put(index.name(), table.name());
            }
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

    /** The optional schema names the accessed indexes; without it every index is accepted. */
    public static Map<String, Object> analyze(SourceSchema schema, Path raw) throws IOException {
        List<Path> files = sessionFiles(raw);
        if (files.isEmpty()) throw new IOException("Access ledger requires at least one PG trace");
        var ledger = new AccessLedger(schema);
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
        var state = new Session();
        long sessionEvents = 0, sessionUnits = started;
        int pid = 0;
        try (var input = Files.newBufferedReader(file)) {
            String line;
            while ((line = input.readLine()) != null) {
                JsonObject e = JsonParser.parseString(line).getAsJsonObject();
                sessionEvents++;
                events++;
                int eventPid = e.get("pid").getAsInt();
                if (pid == 0) pid = eventPid;
                else if (pid != eventPid)
                    throw new IOException("One access ledger input must contain one backend");
                if (!e.has("req") || !e.has("depth"))
                    throw new IOException("Trace lacks request ordinals and executor depth");
                String type = e.get("event").getAsString();
                eventTypes.merge(type, 1L, Long::sum);
                int depth = e.get("depth").getAsInt();
                switch (type) {
                    case "executor_start" -> {
                        claim("executor_lifecycle");
                        if (depth <= 1) {
                            startRequest(state, e, "r" + e.get("req").getAsLong());
                        } else if (state.current == null) {
                            throw new IOException("Nested executor without a parent request");
                        } else {
                            nestedExecutors++;
                        }
                    }
                    case "executor_run_end" -> {
                        claim("executor_lifecycle");
                        if (state.current == null)
                            throw new IOException("Executor return without a started request");
                        if (depth <= 1) state.current.processed = e.get("processed").getAsLong();
                    }
                    case "executor_end" -> {
                        claim("executor_lifecycle");
                        if (state.current == null)
                            throw new IOException("Executor end without a started request");
                        if (depth <= 1) state.currentEnded = true;
                    }
                    case "transaction_commit" -> claim("transaction_boundary");
                    case "transaction_abort" -> {
                        claim("transaction_boundary");
                        abandonFailedScans(state);
                    }
                    case "result_row" -> {
                        claim("result_rows");
                        if (state.current == null)
                            throw new IOException("Result row without a started request");
                        if (depth <= 1) state.current.returned++;
                        else nestedRows++;
                    }
                    case "row_change" -> {
                        claim("row_changes");
                        if (state.current != null) state.current.rowChanges++;
                    }
                    case "index_build_start", "index_build_end" -> claim("index_build");
                    case "index_insert_attempt" -> {
                        claim("index_writes");
                        String index = e.get("relation").getAsString();
                        requireKnown(index);
                        writes.merge(index, 1L, Long::sum);
                        if (state.current != null) state.current.writes++;
                    }
                    case "plan_scan" -> {
                        claim("plan_bindings");
                        String id = e.get("scan").getAsString();
                        if (state.scans.containsKey(id))
                            throw new IOException("Repeated live scan identity: " + id);
                        Scan scan = new Scan(e.get("relation").getAsString());
                        scan.kind = e.get("kind").getAsString();
                        scan.planNode = e.get("plan_node").getAsInt();
                        state.scans.put(id, scan);
                    }
                    case "scan_rescan" -> rescan(state, e, depth);
                    case "scan_next", "heap_visibility", "bitmap_candidate", "bitmap_end" ->
                            fetch(state, e, type);
                    case "scan_end" -> {
                        claim("scan_end");
                        Scan scan = state.scans.remove(e.get("scan").getAsString());
                        if (scan == null)
                            throw new IOException("Scan end without a live scan identity");
                        if (scan.current != null) settle(scan.current);
                    }
                    default -> throw new IOException("Unclassified raw event: " + type);
                }
            }
        }
        // Scans can stay open only while the last request is still executing at connection close.
        for (Scan scan : state.scans.values()) {
            Unit unit = scan.current;
            if (unit == null) continue;
            if (unit.source != Source.PLANNER_PROBE
                    && (unit.request != state.current || state.currentEnded))
                throw new IOException("Index scan left open before its request ended");
            openAtClose++;
            settle(unit);
        }
        finishRequest(state);
        probesUnassigned += state.pendingProbes;
        inputs.add(
                Map.of(
                        "file", file.getFileName().toString(),
                        "sha256", BaselineGenerator.sha256(file),
                        "pid", pid,
                        "events", sessionEvents,
                        "access_units", started - sessionUnits,
                        "identity", "request_ordinal_and_executor_depth"));
    }

    /**
     * A statement that fails never reaches ExecutorEnd, so its scans end only with the abort. Scans of
     * requests that completed normally must already have ended.
     */
    private void abandonFailedScans(Session state) throws IOException {
        for (var iterator = state.scans.values().iterator(); iterator.hasNext(); ) {
            Scan scan = iterator.next();
            Unit unit = scan.current;
            if (unit == null) {
                iterator.remove();
                continue;
            }
            if (unit.source != Source.PLANNER_PROBE
                    && (unit.request != state.current || state.currentEnded))
                throw new IOException("Index scan of a completed request survived the abort");
            abortedOpen++;
            settle(unit);
            iterator.remove();
        }
    }

    private void startRequest(Session state, JsonObject e, String key) throws IOException {
        finishRequest(state);
        state.current = new Request(key, e.has("sql") ? e.get("sql").getAsString() : "");
        probesAssigned += state.pendingProbes;
        state.pendingProbes = 0;
        state.currentEnded = false;
        requests++;
    }

    private void rescan(Session state, JsonObject e, int depth) throws IOException {
        String id = e.get("scan").getAsString(), index = e.get("relation").getAsString();
        requireKnown(index);
        Scan scan = state.scans.get(id);
        if (scan == null) {
            scan = new Scan(index);
            state.scans.put(id, scan);
        }
        String shape = shape(e.getAsJsonArray("keys"));
        Source source =
                depth == 0 && shape.equals("endpoint_probe")
                        ? Source.PLANNER_PROBE
                        : depth <= 1 ? Source.EXECUTOR : Source.NESTED;
        if (source != Source.PLANNER_PROBE) {
            if (depth == 0)
                throw new IOException("Unrecognized index scan outside any executor: " + index);
            if (scan.planNode < 0)
                throw new IOException("Executor index scan without plan binding: " + index);
        }
        if (source != Source.PLANNER_PROBE && state.current == null)
            throw new IOException("Index scan without a started request: " + index);
        if (scan.current != null) settle(scan.current);
        Unit unit = new Unit(scan, source, shape, source == Source.PLANNER_PROBE ? null : state.current);
        scan.current = unit;
        started++;
        if (source == Source.PLANNER_PROBE) state.pendingProbes++;
    }

    private void fetch(Session state, JsonObject e, String type) throws IOException {
        Scan scan = state.scans.get(e.get("scan").getAsString());
        if (scan == null || scan.current == null)
            throw new IOException("Index event without an access unit: " + type);
        Unit unit = scan.current;
        boolean found = e.get("found").getAsBoolean();
        fetchEvents++;
        switch (type) {
            case "scan_next" -> {
                unit.calls++;
                if (found) unit.found++;
                if (found && e.get("recheck").getAsBoolean()) unit.recheck++;
            }
            case "heap_visibility" -> {
                unit.heapFetches++;
                if (found) unit.heapVisible++;
            }
            case "bitmap_candidate" -> unit.bitmapTids++;
            default -> unit.bitmapEnded = true;
        }
    }

    private void requireKnown(String index) throws IOException {
        if (!indexes.isEmpty() && !indexes.containsKey(index))
            throw new IOException("Accessed index is outside the pinned source schema: " + index);
    }

    private static String shape(JsonArray keys) {
        if (keys.size() == 0) return "no_key";
        int equality = 0, inequality = 0;
        var attributes = new HashSet<Integer>();
        for (JsonElement item : keys) {
            JsonObject key = item.getAsJsonObject();
            int strategy = key.get("strategy").getAsInt(), flags = key.get("flags").getAsInt();
            if (key.has("unsupported_key") || (flags & SK_SEARCHARRAY) != 0) return "array_or_row";
            if (keys.size() == 1 && strategy == 0 && (flags & SK_SEARCHNOTNULL) != 0)
                return "endpoint_probe";
            if (flags != 0) return "null_or_flagged";
            attributes.add(key.get("attribute").getAsInt());
            if (strategy == 3) equality++;
            else if (INEQUALITY.contains(strategy)) inequality++;
            else return "other";
        }
        if (keys.size() == 1) return equality == 1 ? "equality" : "range";
        if (equality == 0 && attributes.size() == 1) return "range";
        return inequality == 0 ? "composite_equality" : "composite_mixed";
    }

    /** A unit is settled exactly once: when its scan is rescanned, ended, or left open at close. */
    private void settle(Unit unit) {
        settled++;
        unitCounted += unit.events();
        total.add(unit);
        byIndex.computeIfAbsent(unit.scan.index, ignored -> new Tally()).add(unit);
        bySource.merge(unit.source.label, 1L, Long::sum);
        byShape.merge(unit.shape, 1L, Long::sum);
        byKind.merge(unit.kind(), 1L, Long::sum);
        if (unit.source == Source.PLANNER_PROBE) eligibility.merge("excluded_planner_endpoint_probe", 1L, Long::sum);
        else if (unit.shape.equals("equality")) eligibility.merge("single_column_equality", 1L, Long::sum);
        else eligibility.merge("unsupported_" + unit.shape, 1L, Long::sum);
        Request request = unit.request;
        if (request != null) {
            request.units++;
            request.candidates += unit.candidates();
            request.heapVisible += unit.heapVisible;
        }
    }

    /** Called once all scans of the request have ended: before the next request or at EOF. */
    private void finishRequest(Session state) throws IOException {
        Request request = state.current;
        if (request == null) return;
        String sql = request.sql.strip().replaceAll("\\s+", " ").toLowerCase(Locale.ROOT);
        String shape = template(sql);
        if (request.processed >= 0
                && (request.returned > 0 || sql.startsWith("select"))
                && request.returned != request.processed)
            throw new IOException(
                    "Result rows differ from executor return for request "
                            + request.key
                            + ": "
                            + request.returned
                            + " != "
                            + request.processed);
        String key =
                templates.size() >= MAX_TEMPLATES && !templates.containsKey(shape)
                        ? "<other>"
                        : shape;
        if (key.equals("<other>")) overflow++;
        Template template = templates.computeIfAbsent(key, ignored -> new Template());
        template.requests++;
        if (request.units == 0) {
            template.withoutIndex++;
            withoutIndex++;
        }
        template.units += request.units;
        template.candidates += request.candidates;
        template.heapVisible += request.heapVisible;
        template.returned += request.returned;
        template.writes += request.writes;
        template.rowChanges += request.rowChanges;
        state.current = null;
    }

    /**
     * Statements that embed literals (sysbench oltp_insert on PostgreSQL) would otherwise create
     * one template per request. Quoted strings and bare numbers become '?'; bind markers such as
     * $1 and identifiers such as sbtest1 are left alone.
     */
    static String template(String sql) {
        return sql.replaceAll("'(?:[^']|'')*'", "?")
                .replaceAll("(?<![\\w$.])-?\\d+(?:\\.\\d+)?(?![\\w])", "?");
    }

    private Map<String, Object> report(SourceSchema schema) throws IOException {
        long claimed = partition.values().stream().mapToLong(Long::longValue).sum();
        long unitEvents = started + fetchEvents;
        if (settled != started)
            throw new IOException("Access unit accounting differs: " + settled + " != " + started);
        if (unitCounted != unitEvents)
            throw new IOException(
                    "Index events were not claimed exactly once: " + unitCounted + " != " + unitEvents);
        if (claimed + unitEvents != events)
            throw new IOException(
                    "Raw events were not partitioned exactly once: "
                            + (claimed + unitEvents)
                            + " != "
                            + events);
        var result = new LinkedHashMap<String, Object>();
        result.put("schema_version", VERSION);
        result.put("status", "passed_event_partition");
        if (schema != null) result.put("benchmark", schema.benchmark());
        result.put("inputs", inputs);
        var eventReport = new LinkedHashMap<String, Object>();
        eventReport.put("total", events);
        eventReport.put("by_type", eventTypes);
        result.put("events", eventReport);
        var parts = new LinkedHashMap<String, Object>(partition);
        parts.put("access_unit_events", unitEvents);
        parts.put("sum", events);
        result.put("partition", parts);
        var units = new LinkedHashMap<String, Object>();
        units.put("total", settled);
        units.put("by_source", bySource);
        units.put("by_kind", byKind);
        units.put("by_predicate_shape", byShape);
        units.put("predicate_eligibility", eligibility);
        units.put("open_at_connection_close", openAtClose);
        units.put("open_when_statement_aborted", abortedOpen);
        units.put("candidates", total.candidates);
        units.put("units_with_visibility_observed", total.visibilityUnits);
        units.put("heap_visible_rows", total.heapVisible);
        units.put("heap_fetches", total.heapFetches);
        units.put("recheck_flagged_candidates", total.recheck);
        result.put("access_units", units);
        var indexReports = new ArrayList<Map<String, Object>>();
        var names = new java.util.TreeSet<String>(byIndex.keySet());
        names.addAll(writes.keySet());
        for (String name : names) {
            var item = new LinkedHashMap<String, Object>();
            item.put("index", name);
            var index = indexes.get(name);
            if (index != null) {
                item.put("table", tables.get(name));
                item.put("column", index.column());
                item.put("role", index.primary() ? "primary" : "secondary");
            }
            item.putAll(byIndex.getOrDefault(name, new Tally()).describe());
            item.put("index_insert_attempts", writes.getOrDefault(name, 0L));
            indexReports.add(item);
        }
        result.put("indexes", indexReports);
        var requestReport = new LinkedHashMap<String, Object>();
        requestReport.put("total", requests);
        requestReport.put("without_index_access", withoutIndex);
        requestReport.put("with_index_access", requests - withoutIndex);
        requestReport.put("nested_executors", nestedExecutors);
        requestReport.put("nested_executor_result_rows", nestedRows);
        requestReport.put("planner_endpoint_probes_assigned_to_next_request", probesAssigned);
        requestReport.put("planner_endpoint_probes_without_request", probesUnassigned);
        result.put("requests", requestReport);
        var templateReports = new ArrayList<Map<String, Object>>();
        for (var entry : templates.entrySet()) {
            Template t = entry.getValue();
            var item = new LinkedHashMap<String, Object>();
            item.put("sql", entry.getKey());
            item.put("requests", t.requests);
            item.put("requests_without_index_access", t.withoutIndex);
            item.put("access_units", t.units);
            item.put("candidates", t.candidates);
            item.put("heap_visible_rows", t.heapVisible);
            item.put("returned_rows", t.returned);
            item.put("index_insert_attempts", t.writes);
            item.put("row_changes", t.rowChanges);
            templateReports.add(item);
        }
        result.put("templates", templateReports);
        result.put("template_overflow_requests", overflow);
        result.put(
                "counting_rules",
                List.of(
                        "One access unit per index rescan of one live scan; rescans of a nested loop are"
                            + " separate units.",
                        "Candidates, heap-visible rows and returned rows are separate attributes and"
                            + " are never summed.",
                        "Bitmap candidates are TIDs before visibility; plain index scans report heap"
                            + " visibility; index-only scans report neither.",
                        "Planner endpoint probes are excluded and reported separately.",
                        "Write maintenance (index_insert_attempt) is not a search."));
        result.put(
                "limitations",
                List.of(
                        "Residual filter and recheck removal counts are not captured per plan node.",
                        "Seq scans produce no index event; zero access units is an observed fact, not"
                            + " a plan inference.",
                        "Eligibility describes predicate shape only. The trace compiler still covers"
                            + " the pinned HyBench DDL; YCSB varchar keys and sysbench tables have no"
                            + " canonical projection yet, and range, composite and null predicates are"
                            + " reported but not projected."));
        return result;
    }
}
