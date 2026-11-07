package org.sibench.config;

/** The database owning source rows, transactions and native capture facts. */
public enum Engine {
    DUCKDB, POSTGRES, ROCKSDB
}
