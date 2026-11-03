package org.sibench.capture;

import org.sibench.config.ConfigValues;

import java.util.Map;
import java.nio.file.Path;

/** Final capture products; both strategies use the same source projection. */
public enum TraceOutput {
    PROTO, FULL;

    public static TraceOutput from(Map<String, Object> document) {
        var capture = ConfigValues.object(document, "capture");
        String value = capture.containsKey("output") ? ConfigValues.string(capture, "output") : "proto";
        return switch (value) {
            case "proto" -> PROTO;
            case "full" -> FULL;
            default -> throw new IllegalArgumentException("capture.output must be proto or full");
        };
    }

    public String setting() {
        return name().toLowerCase(java.util.Locale.ROOT);
    }

    static void checkPaths(Map<String, Object> document, Path artifacts, Path raw) {
        if (from(document) != PROTO) return;
        for (String name : new String[] {"trace", "native", "baseline"}) {
            Path retained = artifacts.resolve(name);
            ConfigValues.require(!retained.startsWith(raw) && !raw.startsWith(retained),
                    "Proto capture raw_directory must not overlap " + name);
        }
    }
}
