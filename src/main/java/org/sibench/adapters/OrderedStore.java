package org.sibench.adapters;

import java.io.IOException;
import java.util.List;

/**
 * The ordered byte store under {@link VersionedIndexBackend}. It only
 * stores bytes; the index layout, versions and visibility are {@link VersionedIndexBackend}'s.
 */
interface OrderedStore extends AutoCloseable {
    /** Engine and version, recorded in deployment metadata. */
    String name();

    /** Where the database lives: a local directory or an object-storage location. */
    String location();

    /** Write acknowledgment and internal persistence levels. */
    String durability();

    /** Whether every write, single or batched, is also recorded in the engine's WAL; fixed when the store opens. */
    boolean wal();

    /** Durability reached by a successful write, separate from query visibility. */
    default Acknowledgement writeAck() {
        return wal() ? Acknowledgement.ACK_WAL : Acknowledgement.ACK_MEMORY;
    }

    void put(byte[] key, byte[] value) throws IOException;

    /** One native batch of ordinary writes; keys and values pair up, with the same WAL setting as individual writes. */
    void putAll(List<byte[]> keys, List<byte[]> values) throws IOException;

    /** The value of the exact key, or null. */
    byte[] get(byte[] key) throws IOException;

    /** A cursor over the keys in {@code [lower, upper)}; a null bound is open. */
    Cursor cursor(byte[] lower, byte[] upper) throws IOException;

    /** Makes prior writes durable to the level declared by the store. */
    void flush() throws IOException;

    /** A RocksDB property such as {@code rocksdb.stats}, or null when the engine has none. */
    String property(String name) throws IOException;

    /**
     * Object-storage requests by type: count, failed, bytes, microseconds, then 32 latency buckets
     * with power-of-two microsecond upper bounds (last bucket overflows); null for a local store.
     */
    java.util.Map<String, long[]> objectRequests() throws IOException;

    /** Parses the native request-statistics text of the cloud JNI libraries: one line per type. */
    static java.util.Map<String, long[]> parseRequests(String text) throws IOException {
        var requests = new java.util.LinkedHashMap<String, long[]>();
        for (String line : text.split("\n")) {
            if (line.isEmpty()) continue;
            String[] fields = line.split(" ");
            if (fields.length != 37)
                throw new IOException("Unexpected request statistics format; rebuild the cloud JNI library for this jar");
            long[] counts = new long[36];
            try {
                for (int i = 0; i < counts.length; i++) {
                    counts[i] = Long.parseLong(fields[i + 1]);
                    if (counts[i] < 0) throw new NumberFormatException("Negative request statistic");
                }
            } catch (NumberFormatException invalid) {
                throw new IOException("Invalid cloud request statistic", invalid);
            }
            requests.put(fields[0], counts);
        }
        return requests;
    }

    @Override
    void close() throws IOException;

    interface Cursor extends AutoCloseable {
        void seek(byte[] key);

        void seekForPrev(byte[] key);

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
