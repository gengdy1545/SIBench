package org.sibench.generation;

import java.nio.file.Path;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Transaction;

/** Native timestamp and streaming file-checksum interfaces missing from RocksJava. */
public final class RocksDbTimestamps {
    private static Path loaded;

    private RocksDbTimestamps() {}

    public static synchronized void load(Path library) throws java.io.IOException {
        Path path = library.toRealPath();
        if (loaded != null) {
            if (!loaded.equals(path)) throw new java.io.IOException("RocksDB timestamp bridge already loaded from " + loaded);
            return;
        }
        var runtime = path.getParent().resolve("librocksdbjni-linux64.so");
        try (var embedded = RocksDB.class.getResourceAsStream("/librocksdbjni-linux64.so")) {
            if (embedded == null) throw new java.io.IOException("Missing source-built RocksDB runtime");
            var digest = org.sibench.config.Digests.digest(); byte[] bytes = new byte[1 << 20];
            for (int count; (count = embedded.read(bytes)) != -1;) digest.update(bytes, 0, count);
            if (!java.util.HexFormat.of().formatHex(digest.digest()).equals(org.sibench.config.Digests.sha256(runtime)))
                throw new java.io.IOException("Timestamp bridge runtime differs from the packaged source build");
        }
        RocksDB.loadLibrary();
        System.load(path.toString());
        requireVersion(RocksDB.rocksdbVersion().toString());
        loaded = path;
    }

    public static void configure(ColumnFamilyOptions options) {
        if (loaded == null) throw new IllegalStateException("RocksDB timestamp bridge is not loaded");
        comparator(options.getNativeHandle());
    }

    public static void fileChecksums(DBOptions options) { fileChecksums(options.getNativeHandle()); }
    public static native void releaseMemory();
    public static java.util.Map<String, RocksDbData.File> writtenFiles(DBOptions options) {
        return writtenFiles(options.getNativeHandle());
    }

    public static void commitTimestamp(Transaction transaction, long version) throws RocksDBException {
        if (loaded == null) throw new IllegalStateException("RocksDB timestamp bridge is not loaded");
        if (version < 0) throw new IllegalArgumentException("Negative RocksDB commit timestamp");
        commitTimestamp(transaction.getNativeHandle(), version);
    }

    public static byte[] timestamp(long version) {
        if (version < 0) throw new IllegalArgumentException("Negative RocksDB read timestamp");
        return java.nio.ByteBuffer.allocate(Long.BYTES).order(java.nio.ByteOrder.LITTLE_ENDIAN).putLong(version).array();
    }

    private static native void requireVersion(String version);
    private static native void comparator(long options);
    private static native void fileChecksums(long options);
    private static native java.util.Map<String, RocksDbData.File> writtenFiles(long options);
    private static native void commitTimestamp(long transaction, long version) throws RocksDBException;
}
