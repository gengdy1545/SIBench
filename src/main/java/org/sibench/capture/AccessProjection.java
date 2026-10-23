package org.sibench.capture;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import org.sibench.capture.AccessUnits.BitmapGroup;
import org.sibench.capture.AccessUnits.Combination;
import org.sibench.capture.AccessUnits.Run;
import org.sibench.capture.AccessUnits.Source;
import org.sibench.capture.AccessUnits.Unit;
import org.sibench.capture.IndexOperationTrace.Range;
import org.sibench.trace.IndexKey;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Decides, for one settled access unit or BitmapAnd group, the searches that reproduce its native
 * result, or why it has none. The access ledger reports these decisions per template and the trace
 * compiler emits them, so both apply the same rules:
 *
 * <ul>
 *   <li>Scan keys of the single index column intersect to one interval of encoded values; IS
 *       NULL and IS NOT NULL are intervals of the NULLS LAST order; an {@code = ANY} array is one
 *       point per distinct non-null element inside the other keys' interval.
 *   <li>The expected count is the visible rows the index result produced: heap-checked or
 *       all-visible rows of a plain or index-only scan; for a bitmap, the candidate TIDs the heap
 *       found visible, attributed to every index scan that produced them (BitmapOr included).
 *   <li>A scan its consumer stopped early returned the first rows in (value, heap TID) order. A
 *       primary key has no ties, so that is a limit in KV order. A secondary index orders equal
 *       values by heap TID, which has no KV counterpart, so the scan becomes the fully consumed
 *       part before its last value plus one pinned (value, PK) search per row it returned at that
 *       value: every count is still a native fact.
 *   <li>A BitmapAnd is one group: the heap checked only the intersection of its inputs, so the
 *       group's count is the intersection's visible rows, and its members are the inputs' ranges.
 *   <li>Rejections remain only where a fact is missing: lossy bitmap pages carry no TIDs, a heap
 *       scan stopped early never checked some candidates, an all-visible index-only row carries no
 *       primary key. Text intervals keep PG order only under the C collation.
 * </ul>
 */
final class AccessProjection {
    private static final int SUPPORTED_FLAGS =
            AccessUnits.SK_ISNULL | AccessUnits.SK_SEARCHNULL | AccessUnits.SK_SEARCHNOTNULL;

    /** How the parts of a decision combine: independently, or as one group count. */
    enum Mode {
        INDEPENDENT,
        INTERSECTION,
        UNION
    }

    /** One search of a decision; {@code pinned} names one secondary entry by its PK. */
    record Part(int table, int index, Range range, IndexKey pinned, long count) {}

    /** The searches of one access, a rejection reason, or an exclusion from the workload. */
    record Decision(
            List<Part> parts, Mode mode, long count, String shape, String reason, boolean excluded) {
        static Decision reject(String reason) {
            return new Decision(List.of(), Mode.INDEPENDENT, 0, null, reason, false);
        }

        static Decision exclude(String reason) {
            return new Decision(List.of(), Mode.INDEPENDENT, 0, null, reason, true);
        }

        /** A BitmapAnd member: the group, not the unit, decides. */
        static final Decision GROUPED = new Decision(List.of(), Mode.INDEPENDENT, 0, null, null, false);

        boolean projected() {
            return shape != null;
        }

        boolean grouped() {
            return this == GROUPED;
        }
    }

    private AccessProjection() {}

    static Decision decide(Unit unit, IndexCatalog catalog, boolean cCollation)
            throws IOException {
        if (unit.source == Source.PLANNER_PROBE) return Decision.exclude("planner_endpoint_probe");
        IndexCatalog.Definition definition = catalog.named(unit.scan.index);
        String kind = unit.kind();
        if (kind.equals("bitmap")) {
            if (unit.combination == Combination.NESTED)
                return Decision.reject("nested_bitmap_combination");
            if (unit.group != null) return Decision.GROUPED;
            if (!unit.bitmapEnded) return Decision.exclude("rescan_without_fetch");
            if (unit.bitmapLossy) return Decision.reject("bitmap_heap_lossy_page");
            // A heap scan stopped early never checks some candidates.
            if (!unit.unresolved.isEmpty()) return Decision.reject("bitmap_heap_stopped_early");
        } else if (kind.equals("index") || kind.equals("index_only")) {
            if (unit.calls == 0) return Decision.exclude("rescan_without_fetch");
            if (unit.forward && unit.backward) return Decision.reject("scan_direction_changed");
        } else return Decision.reject("unbound_scan_kind");
        Intervals intervals = intervals(unit, definition, cCollation);
        if (intervals.reason != null)
            return intervals.excluded
                    ? Decision.exclude(intervals.reason)
                    : Decision.reject(intervals.reason);
        long visible = unit.visible();
        boolean limited = !kind.equals("bitmap") && !unit.exhausted();
        if (!limited) return whole(definition, intervals.ranges, visible, unit.backward);
        if (visible == 0) return Decision.reject("stopped_without_visible_row");
        // A primary key has no equal values, so the rows consumed are a prefix in KV order.
        if (definition.indexId() == 0 && intervals.ranges.size() == 1) {
            Range range = intervals.ranges.get(0);
            Range limitedRange =
                    new Range(
                            range.lower(),
                            range.lowerInclusive(),
                            range.upper(),
                            range.upperInclusive(),
                            unit.backward,
                            visible);
            return new Decision(
                    List.of(new Part(definition.tableId(), 0, limitedRange, null, visible)),
                    Mode.INDEPENDENT,
                    visible,
                    "limited_" + shape(range),
                    null,
                    false);
        }
        if (unit.identityMissing) return Decision.reject("stopped_scan_without_row_identity");
        return stopped(unit, catalog, definition, intervals.ranges, visible);
    }

    /** The decision of a BitmapAnd group: one intersection of its inputs' ranges. */
    static Decision decideGroup(BitmapGroup group, IndexCatalog catalog, boolean cCollation)
            throws IOException {
        if (group.lossy) return Decision.reject("bitmap_heap_lossy_page");
        if (group.verdictOutsideIntersection)
            return Decision.reject("bitmap_and_verdict_outside_intersection");
        // A candidate every input produced but the heap never checked: the heap stopped early.
        if (unresolvedIntersection(group)) return Decision.reject("bitmap_heap_stopped_early");
        var parts = new ArrayList<Part>();
        for (Unit unit : group.members) {
            if (!unit.bitmapEnded) return Decision.reject("bitmap_and_input_without_bitmap");
            IndexCatalog.Definition definition = catalog.named(unit.scan.index);
            Intervals intervals = intervals(unit, definition, cCollation);
            if (intervals.reason != null) return Decision.reject(intervals.reason);
            if (intervals.ranges.size() != 1)
                return Decision.reject("bitmap_and_input_with_array_key");
            parts.add(
                    new Part(
                            definition.tableId(),
                            definition.indexId(),
                            intervals.ranges.get(0),
                            null,
                            group.visible));
        }
        if (parts.size() == 1)
            return new Decision(
                    List.of(parts.get(0)),
                    Mode.INDEPENDENT,
                    group.visible,
                    shape(parts.get(0).range()),
                    null,
                    false);
        return new Decision(parts, Mode.INTERSECTION, group.visible, "intersection", null, false);
    }

    private static boolean unresolvedIntersection(BitmapGroup group) {
        Unit smallest = null;
        for (Unit unit : group.members)
            if (smallest == null || unit.unresolved.size() < smallest.unresolved.size())
                smallest = unit;
        if (smallest == null) return false;
        for (String tid : smallest.unresolved) {
            boolean everywhere = true;
            for (Unit unit : group.members)
                if (unit != smallest && !unit.unresolved.contains(tid)) {
                    everywhere = false;
                    break;
                }
            if (everywhere) return true;
        }
        return false;
    }

    /** A scan read to its end: one search, or a union of the array's points. */
    private static Decision whole(
            IndexCatalog.Definition definition, List<Range> ranges, long visible, boolean backward) {
        if (ranges.size() == 1) {
            Range range = ranges.get(0);
            Range directed =
                    new Range(
                            range.lower(),
                            range.lowerInclusive(),
                            range.upper(),
                            range.upperInclusive(),
                            backward,
                            0);
            return new Decision(
                    List.of(new Part(definition.tableId(), definition.indexId(), directed, null, visible)),
                    Mode.INDEPENDENT,
                    visible,
                    shape(range),
                    null,
                    false);
        }
        var parts = new ArrayList<Part>();
        for (Range range : ranges)
            parts.add(new Part(definition.tableId(), definition.indexId(), range, null, visible));
        return new Decision(parts, Mode.UNION, visible, "union", null, false);
    }

    /**
     * A scan its consumer stopped early: the intervals it finished, the part of the last interval
     * before its last value, and one pinned search per row it returned at that value.
     */
    private static Decision stopped(
            Unit unit,
            IndexCatalog catalog,
            IndexCatalog.Definition definition,
            List<Range> ranges,
            long visible)
            throws IOException {
        boolean backward = unit.backward;
        IndexKey last = definition.encode(text(unit.lastValue));
        // Earlier runs, with their values encoded once, attributed to the interval holding them.
        var runs = new ArrayList<IndexKey>();
        var counts = new ArrayList<Long>();
        for (Run run : unit.runs) {
            runs.add(definition.encode(text(run.value())));
            counts.add(run.rows());
        }
        List<Range> ordered = new ArrayList<>(ranges);
        if (backward) java.util.Collections.reverse(ordered);
        int boundary = -1;
        for (int i = 0; i < ordered.size(); i++)
            if (ordered.get(i).contains(last)) {
                boundary = i;
                break;
            }
        if (boundary < 0) return Decision.reject("returned_row_outside_scan_interval");
        var parts = new ArrayList<Part>();
        long attributed = 0;
        for (int i = 0; i < boundary; i++) {
            Range range = ordered.get(i);
            long count = 0;
            for (int r = 0; r < runs.size(); r++) if (range.contains(runs.get(r))) count += counts.get(r);
            parts.add(new Part(definition.tableId(), definition.indexId(), directed(range, backward), null, count));
            attributed += count;
        }
        Range range = ordered.get(boundary);
        long before = unit.rowsBeforeLastRun - attributed;
        if (!unit.runs.isEmpty()) {
            // Array keys keep every run: the rows before the boundary interval must all be in it.
            long inside = 0;
            for (int r = 0; r < runs.size(); r++) if (range.contains(runs.get(r))) inside += counts.get(r);
            if (inside != before) return Decision.reject("returned_row_outside_scan_interval");
        }
        Range prefix =
                backward
                        ? new Range(last, false, range.upper(), range.upperInclusive(), true, 0)
                        : new Range(range.lower(), range.lowerInclusive(), last, false, false, 0);
        boolean empty =
                backward
                        ? range.upper() != null && range.upper().compareTo(last) <= 0
                        : range.lower() != null && range.lower().compareTo(last) >= 0;
        if (empty) {
            if (before != 0) return Decision.reject("returned_row_outside_scan_interval");
        } else parts.add(new Part(definition.tableId(), definition.indexId(), prefix, null, before));
        Range point = Range.equal(last);
        if (definition.indexId() == 0) {
            // A primary key value is one row.
            if (unit.lastPks.size() != 1)
                return Decision.reject("returned_row_outside_scan_interval");
            parts.add(new Part(definition.tableId(), 0, point, null, 1));
        } else {
            IndexCatalog.Definition primary = catalog.primary(definition.tableId());
            Set<IndexKey> pinned = new TreeSet<>();
            for (JsonElement pk : unit.lastPks) {
                IndexKey key = primary.encode(text(pk));
                if (!pinned.add(key)) return Decision.reject("row_returned_twice");
                parts.add(new Part(definition.tableId(), definition.indexId(), point, key, 1));
            }
        }
        long total = 0;
        for (Part part : parts) total += part.count();
        if (total != visible) return Decision.reject("returned_rows_differ_from_visible_rows");
        return new Decision(parts, Mode.INDEPENDENT, visible, "stopped_" + shape(range), null, false);
    }

    private static Range directed(Range range, boolean backward) {
        return new Range(
                range.lower(), range.lowerInclusive(), range.upper(), range.upperInclusive(), backward, 0);
    }

    private static String text(JsonElement value) {
        return value == null || value.isJsonNull() ? null : value.getAsString();
    }

    /** "equality", "range" or "full_scan". */
    static String shape(Range range) {
        return range.point()
                ? "equality"
                : range.lower() == null && range.upper() == null ? "full_scan" : "range";
    }

    /** The scan's searched intervals in ascending KV order, or why it has none. */
    private record Intervals(List<Range> ranges, String reason, boolean excluded) {}

    private static Intervals intervals(
            Unit unit, IndexCatalog.Definition definition, boolean cCollation) throws IOException {
        Bounds bounds = new Bounds();
        TreeSet<IndexKey> points = null;
        for (JsonElement item : unit.keys) {
            JsonObject key = item.getAsJsonObject();
            if (key.has("unsupported_key")) return new Intervals(null, "row_comparison", false);
            if (key.get("attribute").getAsInt() != 1)
                return new Intervals(null, "composite_key", false);
            if (key.has("array")) {
                if (key.get("strategy").getAsInt() != 3)
                    return new Intervals(null, "array_inequality", false);
                var elements = new TreeSet<IndexKey>();
                if (key.get("array").isJsonArray())
                    for (JsonElement element : key.getAsJsonArray("array")) {
                        // A null element never equals anything.
                        if (element.isJsonNull()) continue;
                        try {
                            elements.add(
                                    definition.encodeScanKey(
                                            key.get("array_type_oid").getAsInt(),
                                            element.getAsString()));
                        } catch (IOException error) {
                            return new Intervals(null, "unsupported_key_type", false);
                        }
                    }
                if (points == null) points = elements;
                else points.retainAll(elements);
                continue;
            }
            String reason = bounds.add(key, definition);
            if (reason != null) return new Intervals(null, reason, false);
        }
        Range interval =
                new Range(
                        bounds.lower, bounds.lowerInclusive, bounds.upper, bounds.upperInclusive, false, 0);
        if (points == null) {
            if (!interval.point() && definition.textual() && !cCollation)
                return new Intervals(null, "text_interval_requires_c_collation", false);
            return new Intervals(List.of(interval), null, false);
        }
        var ranges = new ArrayList<Range>();
        for (IndexKey point : points) if (interval.contains(point)) ranges.add(Range.equal(point));
        // PostgreSQL proves such keys can never match and does not search at all.
        if (ranges.isEmpty()) return new Intervals(null, "contradictory_scan_keys", true);
        return new Intervals(ranges, null, false);
    }

    /** The intersection of the scan's key intervals over one column. */
    private static final class Bounds {
        IndexKey lower, upper;
        boolean lowerInclusive, upperInclusive;

        String add(JsonObject key, IndexCatalog.Definition definition) throws IOException {
            int flags = key.get("flags").getAsInt(), strategy = key.get("strategy").getAsInt();
            if ((flags & ~SUPPORTED_FLAGS) != 0) return "flagged_scan_key";
            if (flags == (AccessUnits.SK_ISNULL | AccessUnits.SK_SEARCHNULL)) {
                lower(IndexKey.NULL, true);
                upper(IndexKey.NULL, true);
                return null;
            }
            if (flags == (AccessUnits.SK_ISNULL | AccessUnits.SK_SEARCHNOTNULL)) {
                upper(IndexKey.NULL, false);
                return null;
            }
            if (flags != 0) return "null_comparison";
            IndexKey value;
            try {
                value =
                        definition.encodeScanKey(
                                key.get("type_oid").getAsInt(), key.get("value").getAsString());
            } catch (IOException error) {
                return "unsupported_key_type";
            }
            switch (strategy) {
                case 1 -> upper(value, false);
                case 2 -> upper(value, true);
                case 3 -> {
                    lower(value, true);
                    upper(value, true);
                }
                case 4 -> lower(value, true);
                case 5 -> lower(value, false);
                default -> {
                    return "unsupported_strategy";
                }
            }
            // A non-null comparison never matches NULL, which sorts after every value.
            upper(IndexKey.NULL, false);
            return null;
        }

        void lower(IndexKey value, boolean inclusive) {
            int order = lower == null ? 1 : value.compareTo(lower);
            if (order > 0 || order == 0 && !inclusive) {
                lower = value;
                lowerInclusive = inclusive;
            }
        }

        void upper(IndexKey value, boolean inclusive) {
            int order = upper == null ? -1 : value.compareTo(upper);
            if (order < 0 || order == 0 && !inclusive) {
                upper = value;
                upperInclusive = inclusive;
            }
        }
    }
}
