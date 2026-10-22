package org.sibench.capture;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Groups one backend's raw PG index events into access units: one unit per index rescan of one
 * live scan, so nested-loop rescans are separate units. Requests are top-level executions; SPI and
 * trigger queries belong to the request that runs them. Every index event of a scan is attributed
 * to its current unit, which settles exactly once: when the scan is rescanned, ends, is abandoned
 * by an abort, or is still open when the connection closes during its request.
 */
final class AccessUnits {
    static final int SK_ISNULL = 0x01, SK_SEARCHARRAY = 0x20;
    static final int SK_SEARCHNULL = 0x40, SK_SEARCHNOTNULL = 0x80;

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
    }

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
        final String index;
        String kind = "unbound";
        int planNode = -1;
        Unit current;

        Scan(String index) {
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
        boolean bitmapEnded, bitmapLossy, lastFound, forward, backward;

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
    }

    private static final class Session {
        final Map<String, Scan> scans = new HashMap<>();
        /** Bitmap heap node to the index scans whose candidates it checks. */
        final Map<String, java.util.List<String>> bitmapInputs = new HashMap<>();
        Request current;
        boolean currentEnded;
        long pendingProbes;
    }

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

    Request current() {
        return session.current;
    }

    /** Routes one event; returns false for events that are not part of request/scan tracking. */
    boolean accept(JsonObject e) throws IOException {
        if (!e.has("req") || !e.has("depth"))
            throw new IOException("Trace lacks request ordinals and executor depth");
        String type = e.get("event").getAsString();
        int depth = e.get("depth").getAsInt();
        Session state = session;
        switch (type) {
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
            }
            case "executor_end" -> {
                if (state.current == null)
                    throw new IOException("Executor end without a started request");
                if (depth <= 1) state.currentEnded = true;
            }
            case "transaction_abort" -> abandonFailedScans();
            case "result_row" -> {
                if (state.current == null) throw new IOException("Result row without a started request");
                if (depth <= 1) state.current.returned++;
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
                Scan scan = new Scan(e.get("relation").getAsString());
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
                var inputs = new java.util.ArrayList<String>();
                for (var scan : e.getAsJsonArray("scans")) inputs.add(scan.getAsString());
                state.bitmapInputs.put(e.get("heap").getAsString(), inputs);
            }
            case "bitmap_heap_candidate" -> verdict(e);
            case "bitmap_heap_lossy" -> {
                for (Unit unit : bitmapUnits(e)) unit.bitmapLossy = true;
            }
            case "scan_rescan" -> rescan(e, depth);
            case "scan_next", "heap_visibility", "vm_visible", "bitmap_candidate", "bitmap_end" ->
                    fetch(e, type);
            case "scan_end" -> {
                Scan scan = state.scans.remove(e.get("scan").getAsString());
                if (scan == null) throw new IOException("Scan end without a live scan identity");
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
            scan = new Scan(index);
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
        if (scan.current != null) settle(scan.current);
        Unit unit =
                new Unit(
                        scan,
                        source,
                        keys,
                        source == Source.PLANNER_PROBE ? null : state.current,
                        depth);
        scan.current = unit;
        if (source == Source.PLANNER_PROBE) state.pendingProbes++;
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
                if (found) unit.heapVisible++;
            }
            case "vm_visible" -> unit.vmVisible++;
            case "bitmap_candidate" -> {
                unit.bitmapTids++;
                unit.unresolved.add(e.get("tid").getAsString());
            }
            default -> unit.bitmapEnded = true;
        }
    }

    /** Current units of the index scans feeding one bitmap heap node. */
    private java.util.List<Unit> bitmapUnits(JsonObject e) throws IOException {
        var inputs = session.bitmapInputs.get(e.get("heap").getAsString());
        if (inputs == null) throw new IOException("Bitmap heap event without a plan binding");
        var result = new java.util.ArrayList<Unit>();
        for (String id : inputs) {
            Scan scan = session.scans.get(id);
            if (scan != null && scan.current != null) result.add(scan.current);
        }
        return result;
    }

    /**
     * PostgreSQL's heap verdict for one candidate TID of an exact bitmap page. Every index scan
     * that produced the TID (both sides of a BitmapOr, say) gets it, so each index counts its own
     * visible matches.
     */
    private void verdict(JsonObject e) throws IOException {
        String tid = e.get("tid").getAsString();
        boolean found = e.get("found").getAsBoolean(), claimed = false;
        for (Unit unit : bitmapUnits(e))
            if (unit.unresolved.remove(tid)) {
                claimed = true;
                if (found) unit.bitmapHeapVisible++;
            }
        if (!claimed) throw new IOException("Bitmap heap verdict without its candidate: " + tid);
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
