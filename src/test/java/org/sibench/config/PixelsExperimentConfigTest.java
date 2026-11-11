package org.sibench.config;

import static org.junit.Assert.*;
import static org.sibench.config.ConfigValues.*;
import java.util.*;
import org.junit.Test;

public class PixelsExperimentConfigTest {
    @Test public void queriesAndFreshnessAreIndependentAndDefaultOff() {
        var settings = PixelsExperimentConfig.settings(Map.of());
        assertFalse(bool(settings, "trace"));
        assertFalse(bool(object(settings, "queries"), "enabled"));
        assertFalse(bool(object(settings, "freshness"), "enabled"));
        for (String enabled : List.of("queries", "freshness")) {
            var variant = new LinkedHashMap<>(settings);
            var section = object(variant, enabled);
            section.put("enabled", true);
            if (enabled.equals("queries")) section.put("directory", "sql");
            variant.put(enabled, section);
            PixelsExperimentConfig.settings(Map.of("pixels_experiment", variant));
            variant.put("trace", true);
            assertThrows(IllegalArgumentException.class, () -> PixelsExperimentConfig.settings(Map.of("pixels_experiment", variant)));
        }
    }

    @Test public void rejectsUnboundedInvalidPrefixesStaticFreshnessAndBusinessTimestampRewriting() {
        var settings = new LinkedHashMap<>(PixelsExperimentConfig.defaults());
        settings.put("max_transactions", -1);
        assertThrows(IllegalArgumentException.class, () -> PixelsExperimentConfig.settings(Map.of("pixels_experiment", settings)));
        settings.put("max_transactions", 1);
        settings.put("updates", false);
        assertThrows(IllegalArgumentException.class, () -> PixelsExperimentConfig.settings(Map.of("pixels_experiment", settings)));
        settings.put("max_transactions", 0);
        var freshness = object(settings, "freshness");freshness.put("enabled", true);settings.put("freshness", freshness);
        assertThrows(IllegalArgumentException.class, () -> PixelsExperimentConfig.settings(Map.of("pixels_experiment", settings)));
        settings.put("updates", true);freshness.put("column", "timestamp");settings.put("freshness", freshness);
        assertThrows(IllegalArgumentException.class, () -> PixelsExperimentConfig.settings(Map.of("pixels_experiment", settings)));
    }
}
