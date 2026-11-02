package org.sibench.capture;

import org.sibench.capture.IndexOperationTrace.Operation;
import org.sibench.capture.TidbCapture.Entry;
import org.sibench.capture.TidbCapture.Read;
import org.sibench.trace.IndexKey;

import java.util.*;

/**
 * Independent check of converted TiDB reads: replays the canonical baseline and committed writes
 * into a multi-version index model, evaluates every converted search at its read point, and
 * compares the visible entries, their order and versions with what TiKV or the lock read
 * returned. Shares no evaluation code with the converter.
 */
final class TidbCaptureOracle {
    private TidbCaptureOracle() {}

    /** (indexed value or PK) -> PK -> version -> deleted, per table and index. */
    private static final class Model {
        final Map<Long, TreeMap<IndexKey, TreeMap<IndexKey, TreeMap<Long, Boolean>>>> spaces = new HashMap<>();

        void apply(Operation o) {
            IndexKey value = o.index() == 0 ? o.key() : o.indexedValue();
            spaces.computeIfAbsent(((long) o.table() << 32) | o.index(), k -> new TreeMap<>())
                    .computeIfAbsent(value, k -> new TreeMap<>())
                    .computeIfAbsent(o.key(), k -> new TreeMap<>())
                    .put(o.version(), o.kind() == IndexOperationTrace.LOGICAL_DELETE);
        }

        List<Entry> evaluate(Operation search) {
            var space = spaces.getOrDefault(((long) search.table() << 32) | search.index(), new TreeMap<>());
            var range = search.range();
            NavigableMap<IndexKey, TreeMap<IndexKey, TreeMap<Long, Boolean>>> view = space;
            if (range.lower() != null) view = view.tailMap(range.lower(), range.lowerInclusive());
            if (range.upper() != null) view = view.headMap(range.upper(), range.upperInclusive());
            if (range.descending()) view = view.descendingMap();
            var result = new ArrayList<Entry>();
            for (var value : view.entrySet()) {
                var rows = range.descending() ? value.getValue().descendingMap() : value.getValue();
                for (var row : rows.entrySet()) {
                    var latest = row.getValue().floorEntry(search.readTimestamp());
                    if (latest == null || latest.getValue()) continue;
                    result.add(new Entry(value.getKey(), row.getKey(), latest.getKey()));
                    if (range.limit() > 0 && result.size() == range.limit()) return result;
                }
            }
            return result;
        }
    }

    static Map<String, Object> check(List<Operation> baseline, List<Operation> workload, List<Read> reads) {
        var model = new Model();
        for (var o : baseline) model.apply(o);
        for (var o : workload) if (IndexOperationTrace.isMutation(o)) model.apply(o);
        long checked = 0, mismatches = 0, versionsUnreported = 0;
        var examples = new ArrayList<String>();
        var byOrigin = new TreeMap<String, Long>();
        for (var read : reads) {
            checked++;
            byOrigin.merge(read.origin(), 1L, Long::sum);
            var expected = model.evaluate(read.search());
            boolean same = expected.size() == read.observed().size();
            for (int i = 0; same && i < expected.size(); i++) {
                var e = expected.get(i);
                var o = read.observed().get(i);
                same = e.value().equals(o.value()) && e.pk().equals(o.pk()) && (o.version() < 0 || e.version() == o.version());
                if (o.version() < 0) versionsUnreported++;
            }
            if (!same) {
                mismatches++;
                if (examples.size() < 10)
                    examples.add("event " + read.search().sourceEvent() + " " + read.origin() + " table "
                            + read.search().table() + " index " + read.search().index() + " read point "
                            + read.search().readTimestamp() + ": expected " + expected.size() + " entries, observed "
                            + read.observed().size());
            }
        }
        var report = new LinkedHashMap<String, Object>();
        report.put("searches_checked", checked);
        report.put("mismatches", mismatches);
        report.put("entries_without_reported_version", versionsUnreported);
        report.put("searches_by_origin", byOrigin);
        report.put("mismatch_examples", examples);
        report.put("rule", "visible = latest version <= read point that is not a logical delete; order by"
                + " (component, PK), reversed for descending; limit counts visible entries");
        return report;
    }
}
