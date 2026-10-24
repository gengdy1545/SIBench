package org.sibench.state;

import org.sibench.trace.IndexKey;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;

/**
 * The current RowID of every primary key, per table, in bounded memory: the baseline is bulk
 * loaded into pinned runs, later inserts, updates and deletes are overlay writes. A deleted key
 * keeps a marker, so it reads as deleted rather than falling back to its baseline row.
 */
public final class RowIdentities implements AutoCloseable {
    /** A deleted key's value; RowIDs start at 1. */
    private static final long DELETED = 0;

    private final SpillingMap map;

    public RowIdentities(Path directory, int memoryEntries, int fanout) throws IOException {
        this.map = new SpillingMap(directory, memoryEntries, fanout);
    }

    private RowIdentities(SpillingMap map) {
        this.map = map;
    }

    public static RowIdentities restore(
            Path directory, int memoryEntries, int fanout, List<SpillingMap.RunFile> files)
            throws IOException {
        return new RowIdentities(SpillingMap.restore(directory, memoryEntries, fanout, files));
    }

    private static byte[] key(int table, IndexKey key) {
        byte[] pk = key.bytes();
        return ByteBuffer.allocate(4 + pk.length).putInt(table).put(pk).array();
    }

    /** Rows of one baseline file in strictly ascending primary-key order. */
    public final class Baseline implements AutoCloseable {
        private final SpillingMap.Bulk bulk;
        private final int table;

        private Baseline(int table) throws IOException {
            this.bulk = map.bulk();
            this.table = table;
        }

        public void add(IndexKey key, long rowId) throws IOException {
            if (rowId < 1) throw new IOException("Invalid RowID");
            try {
                bulk.append(key(table, key), rowId);
            } catch (IllegalStateException error) {
                throw new IOException("Baseline primary keys are not strictly ordered", error);
            }
        }

        public void finish() throws IOException {
            bulk.finish();
        }

        @Override
        public void close() throws IOException {
            bulk.close();
        }
    }

    public Baseline baseline(int table) throws IOException {
        return new Baseline(table);
    }

    /** The key's current RowID, or 0 when the row does not exist (never inserted, or deleted). */
    public long rowId(int table, IndexKey key) throws IOException {
        long value = map.get(key(table, key));
        return value == SpillingMap.ABSENT || value == DELETED ? 0 : value;
    }

    public void put(int table, IndexKey key, long rowId) throws IOException {
        if (rowId < 1) throw new IOException("Invalid RowID");
        map.put(key(table, key), rowId);
    }

    public void delete(int table, IndexKey key) throws IOException {
        map.put(key(table, key), DELETED);
    }

    public SpillingMap map() {
        return map;
    }

    @Override
    public void close() throws IOException {
        map.close();
    }
}
