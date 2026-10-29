package org.sibench.adapters;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * rocksdb-cloud through the thin JNI binding in src/main/native/index/rocksdb_cloud: SST files and
 * the MANIFEST go to an S3-compatible bucket, the WAL and a copy of every SST file stay local.
 * The native library is loaded from {@code sibench.rocksdb_cloud.library}, by default
 * {@code build/index/librocksdb_cloud_jni.so}.
 */
final class CloudRocksStore implements RocksStore {
    private static boolean loaded;

    private final long handle;
    private final String bucket, objectPath;
    private boolean closed;

    private CloudRocksStore(long handle, String bucket, String objectPath) {
        this.handle = handle;
        this.bucket = bucket;
        this.objectPath = objectPath;
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
    static CloudRocksStore create(Path path, ObjectStore store) throws IOException {
        return open(path, store, true);
    }

    /** The database at the store's object path, with or without its local directory. */
    static CloudRocksStore open(Path path, ObjectStore store) throws IOException {
        return open(path, store, false);
    }

    private static CloudRocksStore open(Path path, ObjectStore store, boolean create)
            throws IOException {
        load();
        long handle =
                open(
                        path.toString(),
                        store.bucket(),
                        store.objectPath(),
                        store.region(),
                        store.endpoint(),
                        store.virtualHosted(),
                        store.accessKey(),
                        store.secretKey(),
                        store.sessionToken(),
                        create);
        var opened = new CloudRocksStore(handle, store.bucket(), store.objectPath());
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
    public String durability() {
        return "acknowledged writes are in the memtable and the local WAL without fsync; baseline"
                + " inserts skip the WAL; Flush returns once the SST files and MANIFEST are in"
                + " bucket "
                + bucket
                + " at "
                + objectPath
                + "; the WAL is not uploaded, so writes after the last Flush are only local";
    }

    @Override
    public void put(byte[] key, byte[] value, boolean wal) throws IOException {
        requireOpen();
        put(handle, key, value, wal);
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
            public void seekToLast() {
                CloudRocksStore.seekToLast(cursor);
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
        return parseRequests(requests(handle));
    }

    static java.util.Map<String, long[]> parseRequests(String text) throws IOException {
        var requests = new java.util.LinkedHashMap<String, long[]>();
        for (String line : text.split("\n")) {
            if (line.isEmpty()) continue;
            String[] fields = line.split(" ");
            if (fields.length != 37)
                throw new IOException("Unexpected request statistics format; rebuild the rocksdb-cloud JNI library for this jar");
            long[] counts = new long[36];
            try {
                for (int i = 0; i < counts.length; i++) {
                    counts[i] = Long.parseLong(fields[i + 1]);
                    if (counts[i] < 0) throw new NumberFormatException("Negative request statistic");
                }
            } catch (NumberFormatException invalid) {
                throw new IOException("Invalid rocksdb-cloud request statistic", invalid);
            }
            requests.put(fields[0], counts);
        }
        return requests;
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

    private static native long open(
            String path,
            String bucket,
            String objectPath,
            String region,
            String endpoint,
            boolean virtualHosted,
            String accessKey,
            String secretKey,
            String sessionToken,
            boolean create)
            throws IOException;

    private static native void put(long handle, byte[] key, byte[] value, boolean wal)
            throws IOException;

    private static native byte[] get(long handle, byte[] key) throws IOException;

    private static native long cursor(long handle, byte[] lower, byte[] upper);

    private static native void seek(long cursor, byte[] key);

    private static native void seekForPrev(long cursor, byte[] key);

    private static native void seekToLast(long cursor);

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
