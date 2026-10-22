package org.sibench.capture;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import org.sibench.capture.AccessUnits.Source;
import org.sibench.capture.AccessUnits.Unit;
import org.sibench.capture.IndexOperationTrace.Range;
import org.sibench.trace.IndexKey;

import java.io.IOException;

/**
 * Decides, for one settled access unit, the canonical search it is or why it is not one. The
 * access ledger reports these decisions per template and the trace compiler emits them, so both
 * apply the same rules:
 *
 * <ul>
 *   <li>Scan keys of the single index column intersect to one interval of encoded values; IS
 *       NULL and IS NOT NULL are intervals of the NULLS LAST order. Arrays, row comparisons and
 *       null comparisons are not intervals.
 *   <li>The expected count is the visible rows the index result produced: heap-checked or
 *       all-visible rows of a plain or index-only scan; for a bitmap, the candidate TIDs the heap
 *       found visible, attributed to every index scan that produced them (BitmapOr included).
 *   <li>A plain or index-only scan its consumer stopped before the end has a limit of the rows
 *       consumed, in scan direction. That order is the KV key order only for a primary key: a
 *       non-unique btree orders equal values by heap TID, the KV layout by PK.
 *   <li>A bitmap is a search only when every candidate TID received a heap verdict: lossy pages
 *       carry no TIDs, and a BitmapAnd or an early-stopped heap scan leaves candidates unchecked.
 *   <li>Text intervals keep PG order only under the C collation; equality does not depend on it.
 * </ul>
 */
final class AccessProjection {
    private static final int SUPPORTED_FLAGS = AccessUnits.SK_ISNULL
            | AccessUnits.SK_SEARCHNULL
            | AccessUnits.SK_SEARCHNOTNULL;

    /** Either a search or a reason; excluded units are not workload accesses at all. */
    record Decision(Range range, long count, String reason, boolean excluded) {
        static Decision reject(String reason) {
            return new Decision(null, 0, reason, false);
        }

        static Decision exclude(String reason) {
            return new Decision(null, 0, reason, true);
        }

        boolean projected() {
            return range != null;
        }

        /** "equality", "range" or "full_scan", with a "limited_" prefix when it stops early. */
        String shape() {
            String base =
                    range.point()
                            ? "equality"
                            : range.lower() == null && range.upper() == null ? "full_scan" : "range";
            return range.limit() > 0 ? "limited_" + base : base;
        }
    }

    private AccessProjection() {}

    static Decision decide(Unit unit, IndexCatalog catalog, boolean cCollation)
            throws IOException {
        if (unit.source == Source.PLANNER_PROBE) return Decision.exclude("planner_endpoint_probe");
        IndexCatalog.Definition definition = catalog.named(unit.scan.index);
        String kind = unit.kind();
        if (kind.equals("bitmap")) {
            if (!unit.bitmapEnded) return Decision.exclude("rescan_without_fetch");
            if (unit.bitmapLossy) return Decision.reject("bitmap_heap_lossy_page");
            // A BitmapAnd, or a heap scan stopped early, never checks some candidates.
            if (!unit.unresolved.isEmpty())
                return Decision.reject("bitmap_heap_unresolved_candidates");
        } else if (kind.equals("index") || kind.equals("index_only")) {
            if (unit.calls == 0) return Decision.exclude("rescan_without_fetch");
            if (unit.forward && unit.backward) return Decision.reject("scan_direction_changed");
        } else return Decision.reject("unbound_scan_kind");
        Bounds bounds = new Bounds();
        for (JsonElement item : unit.keys) {
            String reason = bounds.add(item.getAsJsonObject(), definition);
            if (reason != null) return Decision.reject(reason);
        }
        long visible = unit.visible();
        boolean limited = !kind.equals("bitmap") && !unit.exhausted();
        if (limited && visible == 0) return Decision.reject("stopped_without_visible_row");
        if (limited && definition.indexId() > 0)
            return Decision.reject("limited_secondary_scan_tie_order");
        Range range =
                new Range(
                        bounds.lower,
                        bounds.lowerInclusive,
                        bounds.upper,
                        bounds.upperInclusive,
                        unit.backward,
                        limited ? visible : 0);
        if (!range.point() && definition.textual() && !cCollation)
            return Decision.reject("text_interval_requires_c_collation");
        return new Decision(range, visible, null, false);
    }

    /** The intersection of the scan's key intervals over one column. */
    private static final class Bounds {
        IndexKey lower, upper;
        boolean lowerInclusive, upperInclusive;

        String add(JsonObject key, IndexCatalog.Definition definition) throws IOException {
            if (key.has("unsupported_key")
                    || (key.get("flags").getAsInt() & AccessUnits.SK_SEARCHARRAY) != 0)
                return "array_or_row_comparison";
            if (key.get("attribute").getAsInt() != 1) return "composite_key";
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
