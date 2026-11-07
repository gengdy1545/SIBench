package org.sibench.capture;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

/** SQL counters reported by the pinned sysbench client. */
final class SysbenchSummary {
    private SysbenchSummary() {}
    private static final Pattern COUNT = Pattern.compile("^\\s*([a-z ]+?):\\s+(\\d+)(?:\\s.*)?$");

    static Map<String, Long> parse(String output) {
        var result = new LinkedHashMap<String, Long>();
        boolean statistics = false;
        for (String line : output.split("\n")) {
            if (line.startsWith("SQL statistics")) statistics = true;
            else if (line.startsWith("General statistics")) statistics = false;
            if (!statistics) continue;
            var matcher = COUNT.matcher(line);
            if (matcher.matches()) result.putIfAbsent(matcher.group(1).strip(), Long.parseLong(matcher.group(2)));
        }
        return result;
    }

}
