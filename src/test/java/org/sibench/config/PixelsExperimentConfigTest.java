package org.sibench.config;

import static org.junit.Assert.*;
import static org.sibench.config.ConfigValues.*;
import java.util.*;
import org.junit.Test;

public class PixelsExperimentConfigTest {
    private static Map<String, Object> published() throws Exception {
        return object(SibenchConfig.read(java.nio.file.Path.of("config/sibench.yaml")), "pixels_experiment");
    }

    @Test public void queriesAndFreshnessAreIndependentAndDefaultOff() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> PixelsExperimentConfig.settings(Map.of()));
        var settings = PixelsExperimentConfig.settings(Map.of("pixels_experiment", published()));
        assertFalse(bool(settings, "trace"));
        assertFalse(bool(object(settings, "queries"), "enabled"));
        assertFalse(bool(object(settings, "freshness"), "enabled"));
        for (String enabled : List.of("queries", "freshness")) {
            var variant = new LinkedHashMap<>(settings);
            var section = object(variant, enabled);
            section.put("enabled", true);
            if (enabled.equals("queries")) section.put("directory", "sql");
            variant.put(enabled, section);
            assertThrows(IllegalArgumentException.class, () -> PixelsExperimentConfig.settings(Map.of("pixels_experiment", variant)));
            var trino = object(variant, "trino");
            trino.put("deployment_directory", "/opt/trino-deployment");
            variant.put("trino", trino);
            PixelsExperimentConfig.settings(Map.of("pixels_experiment", variant));
            variant.put("trace", true);
            assertThrows(IllegalArgumentException.class, () -> PixelsExperimentConfig.settings(Map.of("pixels_experiment", variant)));
        }
    }

    @Test public void queriesAndFreshnessCanUseTheSameExternalTrino() throws Exception {
        for (String enabled : List.of("queries", "freshness")) {
            var settings = published();
            var section = object(settings, enabled);
            section.put("enabled", true);
            if (enabled.equals("queries")) section.put("directory", "sql");
            settings.put(enabled, section);
            var trino = object(settings, "trino");
            trino.put("server", "http://trino.example:8080");
            settings.put("trino", trino);
            PixelsExperimentConfig.settings(Map.of("pixels_experiment", settings));
            trino.put("server", "https://trino.example:8443");
            if (enabled.equals("freshness"))
                assertThrows(IllegalArgumentException.class, () -> PixelsExperimentConfig.settings(Map.of("pixels_experiment", settings)));
            else PixelsExperimentConfig.settings(Map.of("pixels_experiment", settings));
            trino.put("server", "http://trino.example:8080");
            trino.put("deployment_directory", "/opt/trino-deployment");
            assertThrows(IllegalArgumentException.class, () -> PixelsExperimentConfig.settings(Map.of("pixels_experiment", settings)));
            trino.put("deployment_directory", "");
            for (String invalid : List.of("file:///tmp/trino", "http://trino.example/v1/statement", "http://user:password@trino.example")) {
                trino.put("server", invalid);
                assertThrows(IllegalArgumentException.class, () -> PixelsExperimentConfig.settings(Map.of("pixels_experiment", settings)));
            }
        }
    }

    @Test public void rejectsUnboundedInvalidPrefixesStaticFreshnessAndBusinessTimestampRewriting() throws Exception {
        var settings = published();
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
