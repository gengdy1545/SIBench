package org.sibench.adapters;

import org.sibench.config.ObjectStore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * rocksdb-cloud through the thin JNI binding in src/main/native/index/rocksdb_cloud: SST files and
 * the MANIFEST go to an S3-compatible bucket, a copy of every SST file and the optional WAL stay local.
 * The native library is loaded from {@code sibench.rocksdb_cloud.library}, by default
 * {@code build/index/librocksdb_cloud_jni.so}.
 */
final class CloudRocksStore implements OrderedStore {
    private static boolean loaded;

    private final long handle;
    private final String bucket, objectPath;
    private final boolean wal;
    private boolean closed;

    private CloudRocksStore(long handle, String bucket, String objectPath, boolean wal) {
        this.handle = handle;
        this.bucket = bucket;
        this.objectPath = objectPath;
        this.wal = wal;
    }

    private static synchronized void load() throws IOException {
        if (loaded) return;
        Path library =
                Path.of(
                        System.getProperty(
                                "sibench.rocksdb_cloud.library",
                                "build/index/librocksdb_cloud_jni.so"));
        if (!Files.isRegularFile(library))
            throw new IOException("rocksdb-cloud JNI library not found: " + library);
        System.load(library.toAbsolutePath().toString());
        loaded = true;
    }

    /** A new database in the local directory {@code path} and at the store's object path. */
    static CloudRocksStore create(Path path, ObjectStore store, boolean wal) throws IOException {
        return open(path, store, true, wal);
    }

    /** The database at the store's object path, with or without its local directory. */
    static CloudRocksStore open(Path path, ObjectStore store, boolean wal) throws IOException {
        return open(path, store, false, wal);
    }

    private static CloudRocksStore open(Path path, ObjectStore store, boolean create, boolean wal)
            throws IOException {
        load();
        long handle =
                openStore(
                        path.toString(),
                        store.bucket(),
                        store.objectPath(),
                        store.region(),
                        store.endpoint(),
                        store.virtualHosted(),
                        store.accessKey(),
                        store.secretKey(),
                        store.sessionToken(),
                        create,
                        wal);
        var opened = new CloudRocksStore(handle, store.bucket(), store.objectPath(), wal);
        try {
            opened.objectRequests();
        } catch (IOException error) {
            try {
                opened.close();
            } catch (IOException cleanup) {
                error.addSuppressed(cleanup);
            }
            throw error;
        }
        return opened;
    }

    @Override
    public String name() {
        return version();
    }

    @Override
    public String location() {
        return "s3://" + bucket + "/" + objectPath;
    }

    @Override
    public boolean wal() {
        return wal;
    }

    @Override
    public String durability() {
        return (wal
                        ? "acknowledged writes are in the memtable and the local WAL without fsync"
                        : "acknowledged writes are only in the memtable (WAL disabled)")
                + "; internal persistence flushes memtables and uploads SST files and MANIFEST to bucket "
                + bucket
                + " at "
                + objectPath
                + (wal
                        ? "; the WAL is not uploaded, so writes after the last persistence cycle are only local"
                        : "; a process failure loses writes after the last persistence cycle");
    }

    @Override
    public void put(byte[] key, byte[] value) throws IOException {
        requireOpen();
        write(handle, key, value);
    }

    @Override
    public void putAll(java.util.List<byte[]> keys, java.util.List<byte[]> values) throws IOException {
        requireOpen();
        putAll(handle, keys.toArray(byte[][]::new), values.toArray(byte[][]::new));
    }

    @Override
    public byte[] get(byte[] key) throws IOException {
        requireOpen();
        return get(handle, key);
    }

    @Override
    public Cursor cursor(byte[] lower, byte[] upper) throws IOException {
        requireOpen();
        long cursor = cursor(handle, lower, upper);
        return new Cursor() {
            @Override
            public void seek(byte[] key) {
                CloudRocksStore.seek(cursor, key);
            }

            @Override
            public void seekForPrev(byte[] key) {
                CloudRocksStore.seekForPrev(cursor, key);
            }

            @Override
            public void next() {
                CloudRocksStore.next(cursor);
            }

            @Override
            public void prev() {
                CloudRocksStore.prev(cursor);
            }

            @Override
            public boolean valid() throws IOException {
                return CloudRocksStore.valid(cursor);
            }

            @Override
            public byte[] key() {
                return CloudRocksStore.key(cursor);
            }

            @Override
            public byte[] value() {
                return CloudRocksStore.value(cursor);
            }

            @Override
            public void close() {
                closeCursor(cursor);
            }
        };
    }

    @Override
    public void flush() throws IOException {
        requireOpen();
        flush(handle);
    }

    @Override
    public String property(String name) throws IOException {
        requireOpen();
        return property(handle, name);
    }

    @Override
    public java.util.Map<String, long[]> objectRequests() throws IOException {
        requireOpen();
        return OrderedStore.parseRequests(requests(handle));
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) return;
        closed = true;
        close(handle);
    }

    private void requireOpen() throws IOException {
        if (closed) throw new IOException("rocksdb-cloud store is closed");
    }

    private static native String version();

    // Renamed with the WAL setting, so a library built before it fails to link instead of misreading arguments.
    private static native long openStore(
            String path,
            String bucket,
            String objectPath,
            String region,
            String endpoint,
            boolean virtualHosted,
            String accessKey,
            String secretKey,
            String sessionToken,
            boolean create,
            boolean wal)
            throws IOException;

    private static native void write(long handle, byte[] key, byte[] value) throws IOException;

    private static native void putAll(long handle, byte[][] keys, byte[][] values) throws IOException;

    private static native byte[] get(long handle, byte[] key) throws IOException;

    private static native long cursor(long handle, byte[] lower, byte[] upper);

    private static native void seek(long cursor, byte[] key);

    private static native void seekForPrev(long cursor, byte[] key);

    private static native void next(long cursor);

    private static native void prev(long cursor);

    private static native boolean valid(long cursor) throws IOException;

    private static native byte[] key(long cursor);

    private static native byte[] value(long cursor);

    private static native void closeCursor(long cursor);

    private static native void flush(long handle) throws IOException;

    private static native String property(long handle, String name);

    private static native String requests(long handle);

    private static native void close(long handle) throws IOException;
}
