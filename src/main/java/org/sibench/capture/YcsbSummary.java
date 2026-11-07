package org.sibench.capture;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

/** Counts reported by the pinned YCSB client. */
final class YcsbSummary {
    private static final Pattern MEASUREMENT = Pattern.compile("^\\[([A-Z][A-Z-]*)\\], ([^,]+), (.+)$");

    private YcsbSummary() {}

    static Map<String, Map<String, Long>> parse(String output) {
        var result = new LinkedHashMap<String, Map<String, Long>>();
        for (String line : output.split("\n")) {
            var matcher = MEASUREMENT.matcher(line.strip());
            if (!matcher.matches()) continue;
            String category = matcher.group(1), metric = matcher.group(2), value = matcher.group(3);
            try {
                result.computeIfAbsent(category, ignored -> new LinkedHashMap<>())
                        .put(metric, Long.parseLong(value.strip()));
            } catch (NumberFormatException ignored) {
                // Latencies and throughput are not counts.
            }
        }
        return result;
    }

    /**
     * The operations the client chose, as the pinned measurement names them. {@code DBWrapper}
     * measures every db.read / db.update, so a READ-MODIFY-WRITE appears once under its own name
     * and again under READ and UPDATE.
     */
    static Map<String, Long> observed(Map<String, Map<String, Long>> measured) {
        long modify = operations(measured, "READ-MODIFY-WRITE");
        long read = operations(measured, "READ") - modify;
        long update = operations(measured, "UPDATE") - modify;
        long insert = operations(measured, "INSERT");
        long scan = operations(measured, "SCAN");
        var result = new LinkedHashMap<String, Long>();
        result.put("read", read);
        result.put("update", update);
        result.put("insert", insert);
        result.put("scan", scan);
        result.put("read_modify_write", modify);
        result.put("total", read + update + insert + scan + modify);
        return result;
    }

    /** DBWrapper's configured default measures failed calls separately, but they still occurred. */
    static long operations(Map<String, Map<String, Long>> measured, String kind) {
        return measured.getOrDefault(kind, Map.of()).getOrDefault("Operations", 0L)
                + measured.getOrDefault(kind + "-FAILED", Map.of()).getOrDefault("Operations", 0L);
    }

}
