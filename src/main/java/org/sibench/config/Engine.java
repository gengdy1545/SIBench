package org.sibench.config;

import java.util.Locale;

/** The database owning source rows, transactions and native capture facts. */
public enum Engine {
    DUCKDB, POSTGRES, ROCKSDB, PIXELS;

    /** The lowercase name used in selections and engine configuration files. */
    public String configName() {
        return name().toLowerCase(Locale.ROOT);
    }

    public static Engine of(String name) {
        for (Engine engine : values()) if (engine.configName().equals(name)) return engine;
        throw new IllegalArgumentException("Unknown engine: " + name);
    }
}
