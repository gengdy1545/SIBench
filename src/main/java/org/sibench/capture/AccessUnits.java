package org.sibench.capture;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Groups one backend's raw PG index events into access units: one unit per index rescan of one
 * live scan, so nested-loop rescans are separate units. Requests are top-level executions; SPI and
 * trigger queries belong to the request that runs them. Every index event of a scan is attributed
 * to its current unit, which settles exactly once: when the scan is rescanned, ends, is abandoned
 * by an abort, or is still open when the connection closes during its request.
 *
 * <p>The index scans of one BitmapAnd that ran in the same cycle form a {@link BitmapGroup}: the
 * heap checks only their intersection, so the group, not a single scan, carries the visible count.
 *
 * <p>A trace records a unit's facts either per row (every fetch, heap verdict and result row) or
 * as one {@code scan_summary} written right before the event that settles the unit, with bitmap
 * verdicts aggregated by the inputs that produced each TID ({@code bitmap_verdicts}). A capture
 * with {@code detail = rows} carries both, and every summary must equal what its rows add up to.
 */
final class AccessUnits {
    static final int SK_ISNULL = 0x01, SK_SEARCHARRAY = 0x20;
    static final int SK_SEARCHNULL = 0x40, SK_SEARCHNOTNULL = 0x80;

    /** How a backend trace records its index accesses, from its {@code capture_start} line. */
    enum Detail {
        /** Per-row events only: captures written before summaries existed. */
        LEGACY,
        /** Per-row events and summaries; the summaries are checked against the rows. */
        ROWS,
        /** Summaries only. */
        SUMMARY
    }

    static final int CAPTURE_FORMAT = 2;

    enum Source {
        EXECUTOR("executor"),
        NESTED("nested_executor"),
        PLANNER_PROBE("planner_endpoint_probe");

        final String label;

        Source(String label) {
            this.label = label;
        }
    }

    interface Listener {
        default void requestStarted(Request request, JsonObject event) throws IOException {}

        default void requestFinished(Request request) throws IOException {}

        default void unitStarted(Unit unit, JsonObject event) throws IOException {}

        default void unitSettled(Unit unit) throws IOException {}

        /**
         * Units that started before their bitmap heap node bound them now form its first group:
         * PostgreSQL passes constant scan keys to the index while the plan is initialized, before
         * the heap node above it exists.
         */
        default void groupFormed(BitmapGroup group) throws IOException {}

        /** Every member of a BitmapAnd group has settled and no member can join it any more. */
        default void groupSettled(BitmapGroup group) throws IOException {}
    }

    /**
     * How the index scans feeding one bitmap heap node combine: a single scan, a flat BitmapOr, a
     * flat BitmapAnd, or a nesting of the two that has no flat projection.
     */
    enum Combination {
        SINGLE,
        OR,
        AND,
        NESTED
    }

    /** The scans of one BitmapAnd that ran in one cycle, with the heap's verdict on their result. */
    static final class BitmapGroup {
        final List<Unit> members = new ArrayList<>();
        final Set<Scan> scans = new HashSet<>();
        long visible;
        int settled;
        boolean sealed, lossy, verdictOutsideIntersection, reported;
        /** Without per-row candidates, the instrumentation's verdict on the intersection. */
        final boolean perRow;
        Boolean summarizedIntersection;

        BitmapGroup(boolean perRow) {
            this.perRow = perRow;
        }

        /** A candidate every input produced but the heap never checked: the heap stopped early. */
        boolean unresolvedIntersection() throws IOException {
            if (!perRow) {
                if (summarizedIntersection == null)
                    throw new IOException("BitmapAnd group without its intersection verdict");
                return summarizedIntersection;
            }
            return intersectionFromRows();
        }

        private boolean intersectionFromRows() {
            Unit smallest = null;
            for (Unit unit : members)
                if (smallest == null || unit.unresolved.size() < smallest.unresolved.size())
                    smallest = unit;
            if (smallest == null) return false;
            for (String tid : smallest.unresolved) {
                boolean everywhere = true;
                for (Unit unit : members)
                    if (unit != smallest && !unit.unresolved.contains(tid)) {
                        everywhere = false;
                        break;
                    }
                if (everywhere) return true;
            }
            return false;
        }
    }

    /** One run of equal indexed values among the rows a scan made visible, in scan order. */
    record Run(JsonElement value, long rows) {}

    static final class Request {
        final long ordinal;
        final String application;
        final String sql;
        final Long queryId;
        final StringBuilder plan = new StringBuilder();
        long units, candidates, visible, returned, writes, rowChanges;
        long processed = -1;

        Request(long ordinal, String application, String sql, Long queryId) {
            this.ordinal = ordinal;
            this.application = application;
            this.sql = sql;
            this.queryId = queryId;
        }

        /** The plan nodes, scan kinds and indexes the request bound, in binding order. */
        String planSignature() {
            return plan.toString();
        }
    }

    static final class Scan {
        final String id, index;
        String kind = "unbound";
        int planNode = -1;
        Unit current;

        Scan(String id, String index) {
            this.id = id;
            this.index = index;
        }
    }

    static final class Unit {
        final Scan scan;
        final Source source;
        final JsonArray keys;
        final Request request;
        final int depth;
        long events = 1, calls, found, heapFetches, heapVisible, vmVisible, recheck, bitmapTids;
        long bitmapHeapVisible;
        /** Bitmap candidate TIDs still waiting for their heap visibility verdict. */
        final java.util.Set<String> unresolved = new java.util.HashSet<>();
        /** The same count, from a summary that carries no TIDs. */
        long summarizedUnresolved;
        boolean bitmapEnded, bitmapLossy, lastFound, forward, backward;
        /**
         * The MVCC snapshot the scan runs under: PostgreSQL's transaction completion counter as
         * GetSnapshotData() saw it, plus the running XIDs it reported. Absent (-1) for snapshots
         * that are not MVCC, such as the planner's endpoint probes.
         */
        long snapshotCsn = -1, snapshotXmin = -1, snapshotXmax = -1;
        long[] snapshotXip;
        boolean snapshotSuboverflowed;
        /** The bitmap combination this scan feeds, and its BitmapAnd group when it has one. */
        Combination combination = Combination.SINGLE;
        BitmapGroup group;
        /**
         * The identities of the visible rows, in scan order, kept as runs of equal indexed values:
         * completed runs (only for array keys, whose elements each need their own count), the
         * number of rows before the last run, and the last run's value and primary keys. A row
         * made visible without its identity (an all-visible index-only row) leaves them unusable.
         */
        final List<Run> runs = new ArrayList<>();
        long rowsBeforeLastRun;
        JsonElement lastValue;
        final List<JsonElement> lastPks = new ArrayList<>();
        boolean identityMissing;

        Unit(Scan scan, Source source, JsonArray keys, Request request, int depth) {
            this.scan = scan;
            this.source = source;
            this.keys = keys;
            this.request = request;
            this.depth = depth;
        }

        boolean bitmap() {
            return scan.kind.equals("bitmap") || bitmapTids > 0 || bitmapEnded;
        }

        /** Candidate TIDs the heap never checked. */
        long unresolvedCount() {
            return unresolved.size() + summarizedUnresolved;
        }

        String kind() {
            return bitmap() ? "bitmap" : scan.kind;
        }

        /** Index tuples returned by the access method, or TIDs placed in a bitmap. */
        long candidates() {
            return bitmap() ? bitmapTids : found;
        }

        /** Rows the index result made visible: heap-checked, all-visible or bitmap candidates. */
        long visible() {
            return bitmap() ? bitmapHeapVisible : heapVisible + vmVisible;
        }

        /** The access method was asked for a tuple and reported the end of the scan. */
        boolean exhausted() {
            return calls > 0 && !lastFound;
        }

        /**
         * Passes over the index: a bitmap fills once; a tuple scan starts a pass on its first call
         * and on the first call after reporting the end ({@code btgettuple} finds no valid
         * position), so the passes are the end reports plus an unfinished last pass.
         */
        long passes() {
            if (bitmap()) return bitmapEnded ? 1 : 0;
            return calls == 0 ? 0 : calls - found + (lastFound ? 1 : 0);
        }

        /**
         * Tree descents ({@code _bt_first}, PG14's {@code idx_scan}) this unit can have made. A
         * scan without array keys descends exactly once per pass. Array keys descend once per
         * element combination, never for an unsatisfiable array, and fewer when the consumer
         * stops early, so they only bound the count.
         */
        long descentsMin() {
            return arrayKeys() ? 0 : passes();
        }

        long descentsMax() {
            if (!arrayKeys()) return passes();
            long combinations = 1;
            for (JsonElement key : keys) {
                JsonElement array = key.getAsJsonObject().get("array");
                if (array != null && array.isJsonArray())
                    combinations = Math.multiplyExact(combinations, array.getAsJsonArray().size());
            }
            return Math.multiplyExact(passes(), combinations);
        }

        boolean arrayKeys() {
            for (JsonElement key : keys)
                if (key.getAsJsonObject().has("array")) return true;
            return false;
        }

        /** One visible row, in scan order: extends the last run or starts the next. */
        void row(JsonElement value, JsonElement pk) {
            if (lastValue != null && !lastValue.equals(value)) {
                if (arrayKeys()) runs.add(new Run(lastValue, lastPks.size()));
                rowsBeforeLastRun += lastPks.size();
                lastPks.clear();
            }
            lastValue = value;
            lastPks.add(pk);
        }
    }

    /** One bitmap heap node: the scans it checks, how they combine, and its current group. */
    private static final class BitmapHeap {
        final List<String> inputs = new ArrayList<>();
        Combination combination;
        BitmapGroup current;
        /** Per-row verdicts since the last summary, by the input positions that claimed them. */
        final Map<Long, long[]> pending = new HashMap<>();
        long pendingLossy;
    }

    private static final class Session {
        final Map<String, Scan> scans = new HashMap<>();
        final Map<String, BitmapHeap> heaps = new HashMap<>();
        /** Scan identity to the bitmap heap node it feeds, as of the latest plan binding. */
        final Map<String, BitmapHeap> feeds = new HashMap<>();
        Request current;
        boolean currentEnded;
        long pendingProbes;
        Detail detail = Detail.LEGACY;
        boolean seenEvent;
        /** Result rows per executor depth since that depth's last return, checked in ROWS. */
        final Map<Integer, Long> resultRows = new HashMap<>();
    }

    /** Events a summary capture records only in aggregate. */
    static final Set<String> PER_ROW =
            Set.of(
                    "scan_next",
                    "heap_visibility",
                    "vm_visible",
                    "bitmap_candidate",
                    "bitmap_end",
                    "bitmap_heap_candidate",
                    "bitmap_heap_lossy",
                    "result_row");

    private final Listener listener;
    private Session session;
    private long requests, nestedExecutors, nestedRows, probesAssigned, probesUnassigned;
    private long openAtClose, abortedOpen;

    AccessUnits(Listener listener) {
        this.listener = listener;
    }

    void beginSession() {
        session = new Session();
    }

    /**
     * A later transaction-sized part of a backend trace whose first part declared {@code
     * detail}: the concurrent merge settles every part with a fresh tracker.
     */
    void continueSession(Detail detail) {
        session = new Session();
        session.detail = detail;
        session.seenEvent = true;
    }

    Request current() {
        return session.current;
    }

    Detail detail() {
        return session.detail;
    }

    /** Routes one event; returns false for events that are not part of request/scan tracking. */
    boolean accept(JsonObject e) throws IOException {
        if (!e.has("req") || !e.has("depth"))
            throw new IOException("Trace lacks request ordinals and executor depth");
        String type = e.get("event").getAsString();
        int depth = e.get("depth").getAsInt();
        Session state = session;
        boolean first = !state.seenEvent;
        state.seenEvent = true;
        if (state.detail == Detail.SUMMARY && PER_ROW.contains(type))
            throw new IOException("Per-row event in a summary capture: " + type);
        switch (type) {
            case "capture_start" -> {
                if (!first) throw new IOException("capture_start is not the first event of its trace");
                if (e.get("format").getAsInt() != CAPTURE_FORMAT)
                    throw new IOException("Unsupported capture format " + e.get("format"));
                state.detail =
                        switch (e.get("detail").getAsString()) {
                            case "rows" -> Detail.ROWS;
                            case "summary" -> Detail.SUMMARY;
                            default -> throw new IOException("Unknown capture detail " + e.get("detail"));
                        };
            }
            case "scan_summary" -> summary(e);
            case "bitmap_verdicts" -> verdicts(e);
            case "scan_orphan" ->
                    throw new IOException(
                            "Index event without an access unit: " + e.get("orphan_event").getAsString());
            case "bitmap_heap_unbound" ->
                    throw new IOException("Bitmap heap event without a plan binding");
            case "executor_start" -> {
                if (depth <= 1) startRequest(e);
                else if (state.current == null)
                    throw new IOException("Nested executor without a parent request");
                else nestedExecutors++;
            }
            case "executor_run_end" -> {
                if (state.current == null)
                    throw new IOException("Executor return without a started request");
                if (depth <= 1) state.current.processed = e.get("processed").getAsLong();
                if (state.detail != Detail.LEGACY) returned(e, depth);
            }
            case "executor_end" -> {
                if (state.current == null)
                    throw new IOException("Executor end without a started request");
                if (depth <= 1) state.currentEnded = true;
            }
            case "transaction_abort" -> {
                abandonFailedScans();
                // A failed statement's rows never reach a return.
                state.resultRows.clear();
            }
            case "result_row" -> {
                if (state.current == null) throw new IOException("Result row without a started request");
                if (state.detail == Detail.ROWS) state.resultRows.merge(depth, 1L, Long::sum);
                else if (depth <= 1) state.current.returned++;
                else nestedRows++;
            }
            case "row_change" -> {
                if (state.current != null) state.current.rowChanges++;
            }
            case "index_insert_attempt" -> {
                if (state.current != null) state.current.writes++;
            }
            case "plan_scan" -> {
                String id = e.get("scan").getAsString();
                if (state.scans.containsKey(id))
                    throw new IOException("Repeated live scan identity: " + id);
                // A reused scan identity starts unbound until its bitmap heap node binds it again.
                state.feeds.remove(id);
                Scan scan = new Scan(id, e.get("relation").getAsString());
                scan.kind = e.get("kind").getAsString();
                scan.planNode = e.get("plan_node").getAsInt();
                state.scans.put(id, scan);
                if (state.current != null && depth <= 1)
                    state.current
                            .plan
                            .append(state.current.plan.length() == 0 ? "" : ",")
                            .append(scan.planNode)
                            .append(':')
                            .append(scan.kind)
                            .append(':')
                            .append(scan.index);
            }
            case "plan_bitmap_heap" -> {
                if (!e.has("tree"))
                    throw new IOException(
                            "Bitmap heap binding lacks its combination tree; recapture with the"
                                    + " current instrumentation");
                if (e.has("inputs_over_limit"))
                    throw new IOException(
                            "Bitmap heap node with more inputs than the capture attributes");
                var heap = new BitmapHeap();
                for (var scan : e.getAsJsonArray("scans")) heap.inputs.add(scan.getAsString());
                heap.combination = combination(e.get("tree"));
                BitmapHeap previous = state.heaps.put(e.get("heap").getAsString(), heap);
                if (previous != null) requireVerdictsSummarized(previous);
                for (String id : heap.inputs) state.feeds.put(id, heap);
                bindStarted(heap);
            }
            case "bitmap_heap_candidate" -> verdict(e);
            case "bitmap_heap_lossy" -> {
                BitmapHeap heap = heap(e);
                if (heap.combination == Combination.AND && heap.current != null)
                    heap.current.lossy = true;
                for (Unit unit : bitmapUnits(heap)) unit.bitmapLossy = true;
                if (state.detail == Detail.ROWS) heap.pendingLossy++;
            }
            case "scan_rescan" -> rescan(e, depth);
            case "scan_next", "heap_visibility", "vm_visible", "bitmap_candidate", "bitmap_end" ->
                    fetch(e, type);
            case "scan_end" -> {
                String id = e.get("scan").getAsString();
                Scan scan = state.scans.remove(id);
                if (scan == null) throw new IOException("Scan end without a live scan identity");
                // An ended scan never joins another cycle of its BitmapAnd.
                BitmapHeap heap = state.feeds.get(id);
                if (heap != null && heap.current != null) heap.current.sealed = true;
                if (scan.current != null) settle(scan.current);
            }
            default -> {
                return false;
            }
        }
        return true;
    }

    /** Scans can stay open only while the last request is still executing at connection close. */
    void endSession() throws IOException {
        Session state = session;
        for (BitmapHeap heap : state.heaps.values()) requireVerdictsSummarized(heap);
        sealGroups();
        for (Scan scan : state.scans.values()) {
            Unit unit = scan.current;
            if (unit == null) continue;
            if (unit.source != Source.PLANNER_PROBE
                    && (unit.request != state.current || state.currentEnded))
                throw new IOException("Index scan left open before its request ended");
            openAtClose++;
            settle(unit);
        }
        finishRequest();
        probesUnassigned += state.pendingProbes;
        session = null;
    }

    /**
     * A statement that fails never reaches ExecutorEnd, so its scans end only with the abort.
     * Scans of requests that completed normally must already have ended.
     */
    private void abandonFailedScans() throws IOException {
        Session state = session;
        sealGroups();
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

    /** The inputs' units that already started belong to the node's first cycle. */
    private void bindStarted(BitmapHeap heap) throws IOException {
        BitmapGroup group = null;
        for (String id : heap.inputs) {
            Scan scan = session.scans.get(id);
            if (scan == null || scan.current == null || scan.current.source == Source.PLANNER_PROBE)
                continue;
            Unit unit = scan.current;
            if (unit.group != null || unit.events > 1)
                throw new IOException("Bitmap input ran before its heap node bound it: " + id);
            unit.combination = heap.combination;
            if (heap.combination != Combination.AND) continue;
            if (group == null) group = new BitmapGroup(session.detail != Detail.SUMMARY);
            group.members.add(unit);
            group.scans.add(scan);
            unit.group = group;
        }
        if (group == null) return;
        heap.current = group;
        listener.groupFormed(group);
    }

    /** No scan runs again after an abort or at connection close, so no group can grow. */
    private void sealGroups() {
        for (BitmapHeap heap : session.heaps.values())
            if (heap.current != null) heap.current.sealed = true;
    }

    /** The combination a bitmap heap node's tree describes; nesting has no flat projection. */
    static Combination combination(JsonElement tree) throws IOException {
        if (tree.isJsonPrimitive()) return Combination.SINGLE;
        JsonObject node = tree.getAsJsonObject();
        if (node.size() != 1 || !(node.has("and") || node.has("or")))
            throw new IOException("Invalid bitmap combination tree: " + tree);
        boolean and = node.has("and");
        for (JsonElement child : node.getAsJsonArray(and ? "and" : "or"))
            if (!child.isJsonPrimitive()) return Combination.NESTED;
        return and ? Combination.AND : Combination.OR;
    }

    private void startRequest(JsonObject e) throws IOException {
        finishRequest();
        Session state = session;
        state.current =
                new Request(
                        e.get("req").getAsLong(),
                        e.get("application").getAsString(),
                        e.has("sql") ? e.get("sql").getAsString() : "",
                        e.has("query_id") ? e.get("query_id").getAsLong() : null);
        probesAssigned += state.pendingProbes;
        state.pendingProbes = 0;
        state.currentEnded = false;
        requests++;
        listener.requestStarted(state.current, e);
    }

    private void rescan(JsonObject e, int depth) throws IOException {
        Session state = session;
        String id = e.get("scan").getAsString(), index = e.get("relation").getAsString();
        Scan scan = state.scans.get(id);
        if (scan == null) {
            scan = new Scan(id, index);
            state.scans.put(id, scan);
        }
        JsonArray keys = e.getAsJsonArray("keys");
        Source source =
                depth == 0 && endpointProbe(keys)
                        ? Source.PLANNER_PROBE
                        : depth <= 1 ? Source.EXECUTOR : Source.NESTED;
        if (source != Source.PLANNER_PROBE) {
            if (depth == 0)
                throw new IOException("Unrecognized index scan outside any executor: " + index);
            if (scan.planNode < 0)
                throw new IOException("Executor index scan without plan binding: " + index);
            if (state.current == null)
                throw new IOException("Index scan without a started request: " + index);
        }
        // A BitmapAnd input that runs again starts the next cycle; PostgreSQL skips the remaining
        // inputs once the intersection is empty, so a cycle holds the inputs that actually ran.
        BitmapHeap heap = source == Source.PLANNER_PROBE ? null : state.feeds.get(id);
        if (heap != null && heap.combination == Combination.AND
                && heap.current != null && heap.current.scans.contains(scan))
            heap.current.sealed = true;
        if (scan.current != null) settle(scan.current);
        Unit unit =
                new Unit(
                        scan,
                        source,
                        keys,
                        source == Source.PLANNER_PROBE ? null : state.current,
                        depth);
        scan.current = unit;
        if (heap != null) {
            unit.combination = heap.combination;
            if (heap.combination == Combination.AND) {
                if (heap.current == null || heap.current.sealed)
                    heap.current = new BitmapGroup(state.detail != Detail.SUMMARY);
                heap.current.members.add(unit);
                heap.current.scans.add(scan);
                unit.group = heap.current;
            }
        }
        if (source == Source.PLANNER_PROBE) state.pendingProbes++;
        // A copied or imported snapshot leaves the counter at zero, which is not a position.
        if (e.has("csn") && e.get("csn").getAsLong() > 0) {
            unit.snapshotCsn = e.get("csn").getAsLong();
            unit.snapshotXmin = e.get("xmin").getAsLong();
            unit.snapshotXmax = e.get("xmax").getAsLong();
            unit.snapshotSuboverflowed =
                    e.has("suboverflowed") && e.get("suboverflowed").getAsBoolean();
            var running = e.getAsJsonArray("xip");
            unit.snapshotXip = new long[running.size()];
            for (int i = 0; i < running.size(); i++) unit.snapshotXip[i] = running.get(i).getAsLong();
        }
        listener.unitStarted(unit, e);
    }

    /** get_actual_variable_range probes the index endpoints with a lone IS NOT NULL key. */
    static boolean endpointProbe(JsonArray keys) {
        if (keys.size() != 1) return false;
        JsonObject key = keys.get(0).getAsJsonObject();
        return key.get("strategy").getAsInt() == 0
                && (key.get("flags").getAsInt() & SK_SEARCHNOTNULL) != 0;
    }

    private void fetch(JsonObject e, String type) throws IOException {
        Scan scan = session.scans.get(e.get("scan").getAsString());
        if (scan == null || scan.current == null)
            throw new IOException("Index event without an access unit: " + type);
        Unit unit = scan.current;
        boolean found = e.get("found").getAsBoolean();
        unit.events++;
        switch (type) {
            case "scan_next" -> {
                unit.calls++;
                unit.lastFound = found;
                int direction = e.get("direction").getAsInt();
                if (direction > 0) unit.forward = true;
                else if (direction < 0) unit.backward = true;
                if (found) unit.found++;
                if (found && e.get("recheck").getAsBoolean()) unit.recheck++;
            }
            case "heap_visibility" -> {
                unit.heapFetches++;
                if (found) {
                    unit.heapVisible++;
                    identity(unit, e);
                }
            }
            case "vm_visible" -> {
                // An all-visible index-only row is returned without reading the heap: no identity.
                unit.vmVisible++;
                unit.identityMissing = true;
            }
            case "bitmap_candidate" -> {
                unit.bitmapTids++;
                unit.unresolved.add(e.get("tid").getAsString());
            }
            default -> unit.bitmapEnded = true;
        }
    }

    /** The identity of one visible heap row: the scanning index's value and the table's PK. */
    private static void identity(Unit unit, JsonObject e) throws IOException {
        if (!e.has("row"))
            throw new IOException(
                    "Visible heap row without its identity; recapture with the current"
                            + " instrumentation");
        JsonObject row = e.getAsJsonObject("row");
        JsonArray indexed = row.getAsJsonArray("indexed"), pk = row.getAsJsonArray("pk");
        // The source layouts index one column and key one column; anything else has no identity.
        if (indexed.size() != 1 || pk.size() != 1) {
            unit.identityMissing = true;
            return;
        }
        unit.row(indexed.get(0), pk.get(0));
    }

    private BitmapHeap heap(JsonObject e) throws IOException {
        BitmapHeap heap = session.heaps.get(e.get("heap").getAsString());
        if (heap == null) throw new IOException("Bitmap heap event without a plan binding");
        return heap;
    }

    /** Current units of the index scans feeding one bitmap heap node. */
    private List<Unit> bitmapUnits(BitmapHeap heap) {
        var result = new ArrayList<Unit>();
        for (String id : heap.inputs) {
            Scan scan = session.scans.get(id);
            if (scan != null && scan.current != null) result.add(scan.current);
        }
        return result;
    }

    /**
     * PostgreSQL's heap verdict for one candidate TID of an exact bitmap page. Every index scan
     * that produced the TID (both sides of a BitmapOr, say) gets it, so each index counts its own
     * visible matches. A BitmapAnd checks only the intersection of its group's inputs: every
     * input of the cycle must have produced the TID, and the group counts it once.
     */
    private void verdict(JsonObject e) throws IOException {
        BitmapHeap heap = heap(e);
        String tid = e.get("tid").getAsString();
        boolean found = e.get("found").getAsBoolean(), claimed = false;
        long inputs = 0;
        if (heap.combination == Combination.AND) {
            BitmapGroup group = heap.current;
            if (group == null) throw new IOException("BitmapAnd verdict without a cycle: " + tid);
            for (Unit unit : group.members)
                if (unit.unresolved.remove(tid)) {
                    claimed = true;
                    inputs |= 1L << heap.inputs.indexOf(unit.scan.id);
                    if (found) unit.bitmapHeapVisible++;
                } else group.verdictOutsideIntersection = true;
            if (found) group.visible++;
        } else
            for (Unit unit : bitmapUnits(heap))
                if (unit.unresolved.remove(tid)) {
                    claimed = true;
                    inputs |= 1L << heap.inputs.indexOf(unit.scan.id);
                    if (found) unit.bitmapHeapVisible++;
                }
        if (!claimed) throw new IOException("Bitmap heap verdict without its candidate: " + tid);
        if (session.detail == Detail.ROWS)
            heap.pending.computeIfAbsent(inputs, ignored -> new long[2])[found ? 0 : 1]++;
    }

    /**
     * The heap verdicts of one bitmap heap node since its last summary, grouped by the inputs
     * whose current unit produced the TID. Written before any of those units changes, so the
     * attribution is the one each TID's own verdict would make.
     */
    private void verdicts(JsonObject e) throws IOException {
        if (session.detail == Detail.LEGACY)
            throw new IOException("Bitmap verdict summary in a per-row capture");
        BitmapHeap heap = heap(e);
        long lossy = e.get("lossy_pages").getAsLong();
        var sets = new HashMap<Long, long[]>();
        for (JsonElement item : e.getAsJsonArray("verdicts")) {
            JsonObject set = item.getAsJsonObject();
            long inputs = 0;
            for (JsonElement position : set.getAsJsonArray("inputs")) {
                int at = position.getAsInt();
                if (at < 0 || at >= heap.inputs.size())
                    throw new IOException("Bitmap verdict for an unknown input: " + at);
                inputs |= 1L << at;
            }
            long[] counts = sets.computeIfAbsent(inputs, ignored -> new long[2]);
            counts[0] += set.get("visible").getAsLong();
            counts[1] += set.get("invisible").getAsLong();
        }
        if (session.detail == Detail.ROWS) {
            if (lossy != heap.pendingLossy || !sameCounts(sets, heap.pending))
                throw new IOException(
                        "Bitmap verdict summary differs from per-row verdicts of heap "
                                + e.get("heap").getAsString());
            heap.pending.clear();
            heap.pendingLossy = 0;
            return;
        }
        if (lossy > 0) {
            if (heap.combination == Combination.AND && heap.current != null)
                heap.current.lossy = true;
            for (Unit unit : bitmapUnits(heap)) unit.bitmapLossy = true;
        }
        for (var set : sets.entrySet()) {
            long inputs = set.getKey(), visible = set.getValue()[0];
            long verdicts = visible + set.getValue()[1];
            if (verdicts == 0) continue;
            if (inputs == 0) throw new IOException("Bitmap heap verdict without its candidate");
            if (heap.combination == Combination.AND) {
                BitmapGroup group = heap.current;
                if (group == null) throw new IOException("BitmapAnd verdict without a cycle");
                long members = 0;
                for (Unit unit : group.members) {
                    long bit = 1L << heap.inputs.indexOf(unit.scan.id);
                    members |= bit;
                    if ((inputs & bit) != 0) unit.bitmapHeapVisible += visible;
                    else group.verdictOutsideIntersection = true;
                }
                if ((inputs & ~members) != 0)
                    throw new IOException("BitmapAnd verdict outside its cycle's inputs");
                group.visible += visible;
            } else
                for (int at = 0; at < heap.inputs.size(); at++) {
                    if ((inputs & 1L << at) == 0) continue;
                    Scan scan = session.scans.get(heap.inputs.get(at));
                    if (scan == null || scan.current == null)
                        throw new IOException("Bitmap heap verdict without its candidate");
                    scan.current.bitmapHeapVisible += visible;
                }
        }
    }

    private static boolean sameCounts(Map<Long, long[]> a, Map<Long, long[]> b) {
        var left = new HashMap<Long, List<Long>>();
        var right = new HashMap<Long, List<Long>>();
        a.forEach((k, v) -> { if (v[0] + v[1] > 0) left.put(k, List.of(v[0], v[1])); });
        b.forEach((k, v) -> { if (v[0] + v[1] > 0) right.put(k, List.of(v[0], v[1])); });
        return left.equals(right);
    }

    /** Every per-row verdict and lossy page reached a summary before the node was rebound. */
    private void requireVerdictsSummarized(BitmapHeap heap) throws IOException {
        if (session.detail == Detail.ROWS && (!heap.pending.isEmpty() || heap.pendingLossy != 0))
            throw new IOException("Per-row bitmap verdicts without their summary");
    }

    /**
     * The facts of the scan's current unit, written right before the event that settles it. In a
     * ROWS capture every fact must equal what the unit's per-row events added up to.
     */
    private void summary(JsonObject e) throws IOException {
        if (session.detail == Detail.LEGACY)
            throw new IOException("Scan summary in a per-row capture");
        String id = e.get("scan").getAsString();
        Scan scan = session.scans.get(id);
        if (scan == null || scan.current == null)
            throw new IOException("Scan summary without an access unit: " + id);
        Unit unit = scan.current;
        unit.events++;
        if (e.has("fetched_before_binding"))
            throw new IOException("Bitmap input ran before its heap node bound it: " + id);
        if (e.has("candidates_after_verdict"))
            throw new IOException("Bitmap candidates after their heap verdicts: " + id);
        boolean limited = e.has("rows_before_last_run");
        var runs = new ArrayList<Run>();
        var pks = new ArrayList<JsonElement>();
        JsonElement last = null;
        if (limited) {
            for (JsonElement run : e.getAsJsonArray("runs"))
                runs.add(new Run(run.getAsJsonArray().get(0), run.getAsJsonArray().get(1).getAsLong()));
            if (e.has("last_value")) {
                last = e.get("last_value");
                for (JsonElement pk : e.getAsJsonArray("last_pks")) pks.add(pk);
            }
        }
        Boolean intersection =
                e.has("group_unresolved_intersection")
                        ? e.get("group_unresolved_intersection").getAsBoolean()
                        : null;
        if (intersection != null && unit.group == null)
            throw new IOException("Intersection verdict for a unit outside any BitmapAnd: " + id);
        if (session.detail == Detail.ROWS) {
            var differences = new ArrayList<String>();
            same(differences, "calls", e, unit.calls);
            same(differences, "found", e, unit.found);
            same(differences, "last_found", e, unit.lastFound);
            same(differences, "forward", e, unit.forward);
            same(differences, "backward", e, unit.backward);
            same(differences, "recheck", e, unit.recheck);
            same(differences, "heap_fetches", e, unit.heapFetches);
            same(differences, "heap_visible", e, unit.heapVisible);
            same(differences, "vm_visible", e, unit.vmVisible);
            same(differences, "bitmap_tids", e, unit.bitmapTids);
            same(differences, "bitmap_unresolved", e, (long) unit.unresolved.size());
            same(differences, "bitmap_ended", e, unit.bitmapEnded);
            same(differences, "identity_missing", e, unit.identityMissing);
            if (limited) {
                same(differences, "rows_before_last_run", e, unit.rowsBeforeLastRun);
                if (!runs.equals(unit.runs)) differences.add("runs");
                if (!java.util.Objects.equals(last, unit.lastValue)) differences.add("last_value");
                if (!pks.equals(unit.lastPks)) differences.add("last_pks");
            }
            if (intersection != null && intersection != unit.group.intersectionFromRows())
                differences.add("group_unresolved_intersection");
            if (!differences.isEmpty())
                throw new IOException(
                        "Scan summary differs from per-row events of "
                                + scan.index
                                + " request r"
                                + (unit.request == null ? "-" : unit.request.ordinal)
                                + ": "
                                + differences);
            return;
        }
        unit.calls = e.get("calls").getAsLong();
        unit.found = e.get("found").getAsLong();
        unit.lastFound = e.get("last_found").getAsBoolean();
        unit.forward = e.get("forward").getAsBoolean();
        unit.backward = e.get("backward").getAsBoolean();
        unit.recheck = e.get("recheck").getAsLong();
        unit.heapFetches = e.get("heap_fetches").getAsLong();
        unit.heapVisible = e.get("heap_visible").getAsLong();
        unit.vmVisible = e.get("vm_visible").getAsLong();
        unit.bitmapTids = e.get("bitmap_tids").getAsLong();
        unit.summarizedUnresolved = e.get("bitmap_unresolved").getAsLong();
        unit.bitmapEnded = e.get("bitmap_ended").getAsBoolean();
        unit.identityMissing = e.get("identity_missing").getAsBoolean();
        if (limited) {
            unit.rowsBeforeLastRun = e.get("rows_before_last_run").getAsLong();
            unit.runs.addAll(runs);
            unit.lastValue = last;
            unit.lastPks.addAll(pks);
        }
        if (intersection != null) unit.group.summarizedIntersection = intersection;
    }

    private static void same(List<String> differences, String field, JsonObject e, long value) {
        if (e.get(field).getAsLong() != value)
            differences.add(field + " " + e.get(field) + " != " + value);
    }

    private static void same(List<String> differences, String field, JsonObject e, boolean value) {
        if (e.get(field).getAsBoolean() != value)
            differences.add(field + " " + e.get(field) + " != " + value);
    }

    /**
     * The rows one execution returned, from its return. A ROWS capture also has them one by one,
     * and their number must match.
     */
    private void returned(JsonObject e, int depth) throws IOException {
        if (!e.has("returned")) throw new IOException("Executor return without its result rows");
        long returned = e.get("returned").getAsLong();
        if (session.detail == Detail.ROWS) {
            Long rows = session.resultRows.remove(depth);
            if ((rows == null ? 0 : rows) != returned)
                throw new IOException(
                        "Result row count differs from executor return: " + rows + " != " + returned);
        }
        if (depth <= 1) session.current.returned += returned;
        else nestedRows += returned;
    }

    private void settle(Unit unit) throws IOException {
        Request request = unit.request;
        if (request != null) {
            request.units++;
            request.candidates += unit.candidates();
            request.visible += unit.visible();
        }
        unit.scan.current = null;
        listener.unitSettled(unit);
        BitmapGroup group = unit.group;
        if (group != null) {
            group.settled++;
            if (group.sealed && group.settled == group.members.size() && !group.reported) {
                group.reported = true;
                listener.groupSettled(group);
            }
        }
    }

    /** Called once all scans of the request have ended: before the next request or at EOF. */
    private void finishRequest() throws IOException {
        Session state = session;
        Request request = state.current;
        if (request == null) return;
        listener.requestFinished(request);
        state.current = null;
    }

    long requests() {
        return requests;
    }

    long nestedExecutors() {
        return nestedExecutors;
    }

    long nestedRows() {
        return nestedRows;
    }

    long probesAssigned() {
        return probesAssigned;
    }

    long probesUnassigned() {
        return probesUnassigned;
    }

    long openAtClose() {
        return openAtClose;
    }

    long abortedOpen() {
        return abortedOpen;
    }
}
