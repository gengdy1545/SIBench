package org.sibench.generation;

import static org.junit.Assert.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.rocksdb.*;

/** Real-engine counterexamples: native history survives close; concurrent partial updates do not vanish. */
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

    @Test public void nativeVersionsSurviveCompactionAndReadOnlyReopen() throws Exception {
        Path library = Path.of(System.getProperty("sibench.rocksdb.library"));
        RocksDbTimestamps.load(library);
        Path directory = Files.createTempDirectory("rocks-history-").resolve("data");
        try (var options = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
             var metadata = new ColumnFamilyOptions();
             var columns = new ColumnFamilyOptions().setCompactionStyle(CompactionStyle.UNIVERSAL)
                     .setEnableBlobFiles(true).setMinBlobSize(0)
                     .setBlobCompressionType(CompressionType.LZ4_COMPRESSION);
             var transactions = new TransactionDBOptions();
             var writes = new WriteOptions().setDisableWAL(true)) {
            RocksDbTimestamps.configure(columns);
            RocksDbTimestamps.fileChecksums(options);
            var descriptors = RocksDbRows.descriptors(metadata, columns);
            var handles = new ArrayList<ColumnFamilyHandle>();
            try (var db = TransactionDB.open(options, transactions, directory.toString(), descriptors, handles)) {
                var rows = handles.get(1);
                try {
                    writeRow(db, rows, writes, 0, 1, "original");
                    flush(db, rows);
                    writeRow(db, rows, writes, 2, 1, "changed");
                    flush(db, rows);
                    try (var transaction = db.beginTransaction(writes)) {
                        transaction.put(rows, bytes("a"), encodedRow(1, "aborted"));
                        RocksDbTimestamps.commitTimestamp(transaction, 3);
                        transaction.rollback();
                    }
                    try (var transaction = db.beginTransaction(writes)) {
                        transaction.delete(rows, bytes("a"));
                        RocksDbTimestamps.commitTimestamp(transaction, 4);
                        transaction.commit();
                    }
                    writeRow(db, rows, writes, 6, 2, "reinserted");
                    flush(db, rows);
                    var before = db.getLiveFilesMetaData().stream().map(SstFileMetaData::fileName).toList();
                    assertTrue("History must span multiple SST inputs", before.size() > 1);
                    db.compactRange(rows);
                    assertNotEquals("Compaction must rewrite actual SST inputs", before,
                            db.getLiveFilesMetaData().stream().map(SstFileMetaData::fileName).toList());
                    db.put(writes, bytes("sealed"), java.nio.ByteBuffer.allocate(20).putInt(1).putLong(6).putLong(2).array());
                    db.put(writes, RocksDbRows.identityKey(1), bytes("a"));
                    db.put(writes, RocksDbRows.identityKey(2), bytes("a"));
                    flush(db, handles.get(0));
                } finally { handles.forEach(ColumnFamilyHandle::close); }
            }
            var written = RocksDbTimestamps.writtenFiles(options);
            try (var files = Files.list(directory)) {
                var data = files.filter(p -> p.toString().endsWith(".sst") || p.toString().endsWith(".blob")).toList();
                assertTrue(data.stream().anyMatch(p -> p.toString().endsWith(".sst")));
                assertTrue(data.stream().anyMatch(p -> p.toString().endsWith(".blob")));
                for (Path file : data) {
                    var identity = written.get(file.getFileName().toString());
                    assertNotNull(identity);
                    assertEquals(Files.size(file), identity.bytes());
                    assertEquals(org.sibench.config.Digests.sha256(file), identity.sha256());
                }
            }
            var inventory = RocksDbData.seal(directory, library, 6, written);
            Path moved = directory.resolveSibling("moved"); Files.move(directory, moved);
            try (var data = RocksDbData.open(moved, inventory, library)) {
                assertArrayEquals(bytes("original"), (byte[]) data.snapshotRow(1, 1, 0).orElseThrow().get("field0"));
                assertArrayEquals(bytes("original"), (byte[]) data.snapshotRow(1, 1, 1).orElseThrow().get("field0"));
                assertArrayEquals(bytes("changed"), (byte[]) data.snapshotRow(1, 1, 2).orElseThrow().get("field0"));
                assertArrayEquals(bytes("changed"), (byte[]) data.snapshotRow(1, 1, 3).orElseThrow().get("field0"));
                assertTrue(data.snapshotRow(1, 1, 4).isEmpty());
                assertTrue(data.snapshotRow(1, 1, 6).isEmpty());
                assertArrayEquals(bytes("reinserted"), (byte[]) data.snapshotRow(1, 2, 6).orElseThrow().get("field0"));
            }
            assertEquals(inventory, RocksDbData.inventory(moved, library, 6));
        }
    }

    private static byte[] encodedRow(long id, String value) {
        byte[] field = bytes(value);
        return java.nio.ByteBuffer.allocate(12 + field.length).putLong(id).putInt(field.length).put(field).array();
    }

    private static void writeRow(TransactionDB db, ColumnFamilyHandle rows, WriteOptions options,
            long timestamp, long id, String value) throws Exception {
        try (var transaction = db.beginTransaction(options)) {
            transaction.put(rows, bytes("a"), encodedRow(id, value));
            RocksDbTimestamps.commitTimestamp(transaction, timestamp);
            transaction.commit();
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

    @Test public void apiRowsRetainIdentityAndReadPointsAfterRelocation() throws Exception {
        Path library = Path.of(System.getProperty("sibench.rocksdb.library"));
        RocksDbTimestamps.load(library);
        Path directory = Files.createTempDirectory("rocks-rows-").resolve("data");
        var events = new ArrayList<RocksDbRows.Event>();
        var baseline = new ArrayList<RocksDbRows.Row>();
        byte[] tail = bytes("base".repeat(256));
        long last;
        var rows = new RocksDbRows(directory, 2, new RocksDbRows.Settings(8L << 20, 1L << 20, 2), events::add);
        try (rows) {
            assertThrows(IllegalStateException.class, rows::writtenFiles);
            rows.load(List.of(new RocksDbRows.Row(0, "user2", new byte[][]{bytes("two"), tail}),
                    new RocksDbRows.Row(0, "user10", new byte[][]{bytes("ten"), tail})), baseline::add);
            rows.finishLoad();
            try (var files = Files.list(directory)) {
                assertTrue("Baseline values must be flushed before workloads", files.anyMatch(p -> p.toString().endsWith(".sst")));
            }
            assertEquals(List.of("user10", "user2"), rows.read("user1", 3, 1, 1).stream().map(RocksDbRows.Row::key).toList());
            assertEquals(1, rows.read("user10", 1, 1, 2).size());
            assertTrue(rows.read("missing", 0, 1, 3).isEmpty());
            var updated = rows.change("update", "user2", java.util.Map.of(0, bytes("updated")), 1, 4);
            assertEquals(baseline.get(0).id(), updated.id());
            assertArrayEquals(tail, updated.fields()[1]);
            assertNull(rows.change("insert", "user2", java.util.Map.of(0, bytes("duplicate")), 1, 5));
            assertFalse(events.get(4).committed());
            rows.change("delete", "user2", java.util.Map.of(), 1, 6);
            var reinserted = rows.change("insert", "user2", java.util.Map.of(0, bytes("new")), 1, 7);
            assertNotEquals(updated.id(), reinserted.id());
            assertNull(rows.change("update", "missing", java.util.Map.of(0, bytes("x")), 1, 8));
            rows.seal(); last = rows.version();
        }
        var inventory = RocksDbData.seal(directory, library, last, rows.writtenFiles());
        assertEquals(RocksDbData.inventory(directory, library, last), inventory);
        assertThrows(java.io.IOException.class, () -> RocksDbData.seal(directory, library, last, java.util.Map.of()));
        String name = inventory.files().keySet().stream()
                .filter(file -> file.endsWith(".sst") || file.endsWith(".blob")).findFirst().orElseThrow();
        var wrong = new java.util.HashMap<>(rows.writtenFiles());
        wrong.put(name, new RocksDbData.File(wrong.get(name).bytes() + 1, wrong.get(name).sha256()));
        assertThrows(java.io.IOException.class, () -> RocksDbData.seal(directory, library, last, wrong));
        wrong.put(name, new RocksDbData.File(rows.writtenFiles().get(name).bytes(), "0".repeat(64)));
        var incorrect = RocksDbData.seal(directory, library, last, wrong);
        assertThrows(java.io.IOException.class, () -> RocksDbData.open(directory, incorrect, library));
        Path moved = directory.resolveSibling("moved"); Files.move(directory, moved);
        try (var data = RocksDbData.open(moved, inventory, library)) {
            long original = baseline.get(0).id();
            assertArrayEquals(bytes("two"), (byte[]) data.snapshotRow(1, original, 0).orElseThrow().get("field0"));
            assertArrayEquals(tail, (byte[]) data.snapshotRow(1, original, 0).orElseThrow().get("field1"));
            assertArrayEquals(bytes("updated"), (byte[]) data.snapshotRow(1, original, 4).orElseThrow().get("field0"));
            assertArrayEquals(bytes("two"), (byte[]) data.rowBeforeStatement(1, original, 1, 4).orElseThrow().get("field0"));
            assertTrue(data.snapshotRow(1, original, 7).isEmpty());
            assertThrows(IllegalArgumentException.class, () -> data.snapshotRow(1, original, last + 1));
        }
        assertEquals(inventory, RocksDbData.inventory(moved, library, last));
    }

    private static String read(RocksDB db, long version, String key) throws Exception {
        try (var timestamp = new Slice(RocksDbTimestamps.timestamp(version));
             var options = new ReadOptions().setTimestamp(timestamp)) {
            byte[] result = db.get(options, bytes(key));
            return result == null ? null : new String(result, StandardCharsets.UTF_8);
        }
    }
}
