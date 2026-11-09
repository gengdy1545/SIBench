package org.sibench.adapters;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.sibench.adapters.proto.Acknowledgement;

/** Same-instance SlateDB byte storage; Java owns the shared versioned index layout. */
final class SlateDbStore implements OrderedStore {
    private static boolean loaded;
    private final long handle;
    private final String location;
    private volatile boolean closed;

    private SlateDbStore(long handle, ObjectStore store) {
        this.handle = handle;
        location = "s3://" + store.bucket() + "/" + store.objectPath();
    }

    private static synchronized void load() throws IOException {
        if (loaded) return;
        Path library = Path.of(System.getProperty("sibench.slatedb.library", "build/index/libslatedb_jni.so"));
        if (!Files.isRegularFile(library)) throw new IOException("SlateDB JNI library not found: " + library);
        try {
            System.load(library.toAbsolutePath().toString());
            nativeVersion();
        } catch (LinkageError failure) {
            throw new IOException("Cannot load the source-built SlateDB JNI library: " + library, failure);
        }
        loaded = true;
    }

    static SlateDbStore create(Path path, ObjectStore store) throws IOException {
        load();
        Path absolute = path.toAbsolutePath();
        Files.createDirectories(absolute.getParent());
        Files.createDirectory(absolute);
        return open(absolute, store, true);
    }

    static SlateDbStore open(Path path, ObjectStore store) throws IOException {
        load();
        Files.createDirectories(path.toAbsolutePath());
        return open(path.toAbsolutePath(), store, false);
    }

    /** The local path mirrors the compacted SSTs, as rocksdb-cloud keeps its SST files locally. */
    private static SlateDbStore open(Path mirror, ObjectStore store, boolean create) throws IOException {
        return new SlateDbStore(nativeOpen(store.bucket(), store.objectPath(), store.region(),
                store.endpoint(), store.virtualHosted(), store.accessKey(), store.secretKey(),
                store.sessionToken(), mirror.toString(), create), store);
    }

    @Override public String name() { return nativeVersion(); }
    @Override public String location() { return location; }
    @Override public Acknowledgement writeAck() { return Acknowledgement.ACK_MEMORY; }
    @Override public String durability() {
        return "acknowledged writes are visible through the same SlateDB instance's memory read path;"
                + " Flush waits for object-store persistence; compacted SSTs are also mirrored to the local"
                + " path; independent readers may lag";
    }

    @Override public void put(byte[] key, byte[] value, boolean wal) throws IOException {
        requireOpen();
        // SlateDB uses its in-memory WAL for both baseline and workload writes.
        nativePut(handle, key, value);
    }
    /** One SlateDB write batch per baseline request instead of one write per entry. */
    @Override public void putBaseline(java.util.List<byte[]> keys, java.util.List<byte[]> values) throws IOException {
        requireOpen();
        nativePutAll(handle, keys.toArray(byte[][]::new), values.toArray(byte[][]::new));
    }
    @Override public byte[] get(byte[] key) throws IOException { requireOpen(); return nativeGet(handle, key); }
    @Override public void flush() throws IOException { requireOpen(); nativeFlush(handle); }
    @Override public String property(String name) throws IOException { requireOpen(); return null; }
    @Override public Map<String, long[]> objectRequests() throws IOException {
        requireOpen();
        return OrderedStore.parseRequests(nativeRequests(handle));
    }

    @Override public Cursor cursor(byte[] lower, byte[] upper) throws IOException {
        requireOpen();
        long cursor = nativeCursor(handle, lower, upper);
        return new Cursor() {
            @Override public void seek(byte[] key) { nativeMove(cursor, 0, key); }
            @Override public void seekForPrev(byte[] key) { nativeMove(cursor, 1, key); }
            @Override public void seekToLast() { nativeMove(cursor, 2, null); }
            @Override public void next() { nativeMove(cursor, 3, null); }
            @Override public void prev() { nativeMove(cursor, 4, null); }
            @Override public boolean valid() throws IOException { return nativeValid(cursor); }
            @Override public byte[] key() { return nativeEntry(cursor, true); }
            @Override public byte[] value() { return nativeEntry(cursor, false); }
            @Override public void close() { nativeCloseCursor(cursor); }
        };
    }

    @Override public synchronized void close() throws IOException {
        if (closed) return;
        closed = true;
        nativeClose(handle);
    }

    private void requireOpen() throws IOException { if (closed) throw new IOException("SlateDB store is closed"); }
    private static native String nativeVersion();
    private static native long nativeOpen(String bucket, String prefix, String region, String endpoint,
            boolean virtualHosted, String access, String secret, String token, String mirror, boolean create)
            throws IOException;
    private static native String nativeRequests(long handle) throws IOException;
    private static native void nativePut(long handle, byte[] key, byte[] value) throws IOException;
    private static native void nativePutAll(long handle, byte[][] keys, byte[][] values) throws IOException;
    private static native byte[] nativeGet(long handle, byte[] key) throws IOException;
    private static native void nativeFlush(long handle) throws IOException;
    private static native void nativeClose(long handle) throws IOException;
    private static native long nativeCursor(long handle, byte[] lower, byte[] upper) throws IOException;
    private static native void nativeMove(long cursor, int operation, byte[] key);
    private static native boolean nativeValid(long cursor) throws IOException;
    private static native byte[] nativeEntry(long cursor, boolean key);
    private static native void nativeCloseCursor(long cursor);
}
