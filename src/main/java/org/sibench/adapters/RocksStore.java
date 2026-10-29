package org.sibench.adapters;

import java.io.IOException;

/**
 * The ordered byte store under {@link RocksDbBackend}: a local RocksDB, or rocksdb-cloud. It only
 * stores bytes; the index layout, versions and visibility are {@link RocksDbBackend}'s.
 */
interface RocksStore extends AutoCloseable {
    /** Engine and version, as Describe reports it. */
    String name();

    /** Where the database lives: a local directory or an object-storage location. */
    String location();

    /** Where a write and a Flush are durable, as Describe reports it. */
    String durability();

    void put(byte[] key, byte[] value, boolean wal) throws IOException;

    /** The value of the exact key, or null. */
    byte[] get(byte[] key) throws IOException;

    /** A cursor over the keys in {@code [lower, upper)}; a null bound is open. */
    Cursor cursor(byte[] lower, byte[] upper) throws IOException;

    /** Every memtable written to SST files and the WAL synced. */
    void flush() throws IOException;

    /** A RocksDB property such as {@code rocksdb.stats}, or null when the engine has none. */
    String property(String name) throws IOException;

    /**
     * Object-storage requests by type: count, failed, bytes, microseconds, then 32 latency buckets
     * with power-of-two microsecond upper bounds (last bucket overflows); null for a local store.
     */
    java.util.Map<String, long[]> objectRequests() throws IOException;

    @Override
    void close() throws IOException;

    interface Cursor extends AutoCloseable {
        void seek(byte[] key);

        void seekForPrev(byte[] key);

        void seekToLast();

        void next();

        void prev();

        /** Whether it is on an entry; a failed iterator throws instead of ending. */
        boolean valid() throws IOException;

        byte[] key();

        byte[] value();

        @Override
        void close();
    }
}
