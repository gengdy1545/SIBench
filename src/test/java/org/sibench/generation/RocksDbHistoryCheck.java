package org.sibench.generation;

import static org.junit.Assert.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.rocksdb.*;

/** Real-engine counterexamples: timestamped reads, RowIDs and concurrent partial updates. */
public class RocksDbHistoryCheck {
    @org.junit.Test public void bridgeAndRocksJavaShareOneNativeRuntime() throws Exception {
        RocksDbTimestamps.load(Path.of(System.getProperty("sibench.rocksdb.library")));
        var libraries = new java.util.HashSet<String>();
        for (String line : java.nio.file.Files.readAllLines(Path.of("/proc/self/maps")))
            if (line.contains("librocksdbjni")) libraries.add(line.split("\\s+", 6)[5]);
        assertEquals(libraries.toString(), 1, libraries.size());
    }

    private static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }

    @Test public void flushedMemtableCacheIsUnneededWithoutTransactionSnapshots() throws Exception {
        RocksDbTimestamps.load(Path.of(System.getProperty("sibench.rocksdb.library")));
        for (long budget : new long[]{0, 1}) {
            var properties = new java.util.Properties();
            properties.setProperty("max_write_buffer_size_to_maintain", Long.toString(budget));
            Path directory = Files.createTempDirectory("rocks-memory-").resolve("data");
            try (var options = new DBOptions().setCreateIfMissing(true);
                 var columns = ColumnFamilyOptions.getColumnFamilyOptionsFromProps(properties).setWriteBufferSize(1L << 20);
                 var transactions = new TransactionDBOptions();
                 var writes = new WriteOptions().setDisableWAL(true)) {
                RocksDbTimestamps.configure(columns);
                var descriptors = List.of(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columns));
                var handles = new ArrayList<ColumnFamilyHandle>();
                try (var db = TransactionDB.open(options, transactions, directory.toString(), descriptors, handles)) {
                    try {
                        write(db, writes, 0, "a", "original");
                        flush(db, handles.get(0));
                        // Native history trimming is scheduled by a write and performed by the next write.
                        write(db, writes, 0, "b", "other");
                        write(db, writes, 0, "c", "other");
                        long retained = db.getLongProperty("rocksdb.num-immutable-mem-table-flushed");
                        if (budget == 0) assertTrue("Default TransactionDB retains flushed memtables", retained > 0);
                        else assertEquals("Explicit budget releases the flushed-memtable cache", 0, retained);
                        try (var transaction = db.beginTransaction(writes); var read = new ReadOptions()) {
                            assertArrayEquals(bytes("original"), transaction.getForUpdate(read, handles.get(0), bytes("a"), true, false));
                            transaction.put(bytes("a"), bytes("changed"));
                            RocksDbTimestamps.commitTimestamp(transaction, 1);
                            transaction.commit();
                        }
                        flush(db, handles.get(0));
                        assertEquals("original", read(db, 0, "a"));
                        assertEquals("changed", read(db, 1, "a"));
                    } finally { handles.forEach(ColumnFamilyHandle::close); }
                }
            }
        }
    }

    @Test public void rowLockPreservesConcurrentPartialUpdates() throws Exception {
        RocksDbTimestamps.load(Path.of(System.getProperty("sibench.rocksdb.library")));
        Path directory = Files.createTempDirectory("rocks-update-").resolve("data");
        var pool = java.util.concurrent.Executors.newSingleThreadExecutor();
        try (var options = new DBOptions().setCreateIfMissing(true);
             var columns = new ColumnFamilyOptions().setCompactionStyle(CompactionStyle.UNIVERSAL);
             var transactions = new TransactionDBOptions().setTransactionLockTimeout(5000);
             var writes = new WriteOptions().setDisableWAL(true);
             var reads = new ReadOptions()) {
            RocksDbTimestamps.configure(columns);
            var descriptors = List.of(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columns));
            var handles = new ArrayList<ColumnFamilyHandle>();
            try (var db = TransactionDB.open(options, transactions, directory.toString(), descriptors, handles)) {
                try {
                    write(db, writes, 0, "a", "00");
                    try (var first = db.beginTransaction(writes)) {
                        assertArrayEquals(bytes("00"), first.getForUpdate(reads, handles.get(0), bytes("a"), true, false));
                        var started = new java.util.concurrent.CountDownLatch(1);
                        var second = pool.submit(() -> {
                            try (var transaction = db.beginTransaction(writes);
                                 var read = new ReadOptions()) {
                                started.countDown();
                                byte[] value = transaction.getForUpdate(read, handles.get(0), bytes("a"), true, false);
                                value[1] = '2';
                                transaction.put(bytes("a"), value);
                                RocksDbTimestamps.commitTimestamp(transaction, 2);
                                transaction.commit();
                            }
                            return null;
                        });
                        assertTrue(started.await(5, java.util.concurrent.TimeUnit.SECONDS));
                        first.put(bytes("a"), bytes("10"));
                        RocksDbTimestamps.commitTimestamp(first, 1);
                        first.commit();
                        second.get(10, java.util.concurrent.TimeUnit.SECONDS);
                    }
                    assertEquals("00", read(db, 0, "a"));
                    assertEquals("10", read(db, 1, "a"));
                    assertEquals("12", read(db, 2, "a"));
                } finally { handles.forEach(ColumnFamilyHandle::close); }
            }
        } finally { pool.shutdownNow(); }
    }

    private static void write(TransactionDB db, WriteOptions options, long timestamp, String key, String value)
            throws Exception {
        try (var transaction = db.beginTransaction(options)) {
            transaction.put(bytes(key), bytes(value));
            RocksDbTimestamps.commitTimestamp(transaction, timestamp);
            transaction.commit();
        }
    }

    private static void flush(RocksDB db, ColumnFamilyHandle handle) throws RocksDBException {
        try (var options = new FlushOptions().setWaitForFlush(true)) { db.flush(options, handle); }
    }

    @Test public void apiRowsKeepRowIdsAcrossUpdatesAndAllocateNewOnesOnReinsert() throws Exception {
        RocksDbTimestamps.load(Path.of(System.getProperty("sibench.rocksdb.library")));
        Path directory = Files.createTempDirectory("rocks-rows-").resolve("data");
        var events = new ArrayList<RocksDbRows.Event>();
        var baseline = new ArrayList<RocksDbRows.Row>();
        byte[] tail = bytes("base".repeat(256));
        try (var rows = new RocksDbRows(directory, 2, new RocksDbRows.Settings(8L << 20, 1L << 20, 2), events::add)) {
            rows.load(List.of(new RocksDbRows.Row(0, "user2", new byte[][]{bytes("two"), tail}),
                    new RocksDbRows.Row(0, "user10", new byte[][]{bytes("ten"), tail})), baseline::add);
            rows.finishLoad();
            try (var files = Files.list(directory)) {
                assertTrue("Baseline values must be flushed before workloads", files.anyMatch(p -> p.toString().endsWith(".sst")));
            }
            assertEquals(List.of("user10", "user2"), rows.read("user1", 3).stream().map(RocksDbRows.Row::key).toList());
            assertEquals(1, rows.read("user10", 1).size());
            assertTrue(rows.read("missing", 0).isEmpty());
            var updated = rows.change("update", "user2", java.util.Map.of(0, bytes("updated")));
            assertEquals(baseline.get(0).id(), updated.id());
            assertArrayEquals(tail, updated.fields()[1]);
            assertNull(rows.change("insert", "user2", java.util.Map.of(0, bytes("duplicate"))));
            assertFalse(events.get(4).committed());
            rows.change("delete", "user2", java.util.Map.of());
            var reinserted = rows.change("insert", "user2", java.util.Map.of(0, bytes("new")));
            assertNotEquals(updated.id(), reinserted.id());
            assertNull(rows.change("update", "missing", java.util.Map.of(0, bytes("x"))));
            assertEquals(8, rows.version());
            // Calls are serialized here, so each reads at the version just before its own.
            assertEquals(List.of(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L), events.stream().map(RocksDbRows.Event::readVersion).toList());
        }
    }

    private static String read(RocksDB db, long version, String key) throws Exception {
        try (var timestamp = new Slice(RocksDbTimestamps.timestamp(version));
             var options = new ReadOptions().setTimestamp(timestamp)) {
            byte[] result = db.get(options, bytes(key));
            return result == null ? null : new String(result, StandardCharsets.UTF_8);
        }
    }
}
