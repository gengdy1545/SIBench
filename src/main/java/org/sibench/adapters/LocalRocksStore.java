package org.sibench.adapters;

import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.WriteOptions;

import java.io.IOException;
import java.nio.file.Path;

/** A RocksDB instance in a local directory, through RocksJava. */
final class LocalRocksStore implements RocksStore {
    static {
        RocksDB.loadLibrary();
    }

    private final Options options;
    private final RocksDB db;
    private final Path path;
    private final WriteOptions logged = new WriteOptions(),
            unlogged = new WriteOptions().setDisableWAL(true);

    private LocalRocksStore(Options options, RocksDB db, Path path) {
        this.options = options;
        this.db = db;
        this.path = path;
    }

    /** A new database in {@code path}, which must not exist. */
    static LocalRocksStore create(Path path) throws IOException {
        return open(path, new Options().setCreateIfMissing(true).setErrorIfExists(true));
    }

    /** The existing database in {@code path}. */
    static LocalRocksStore open(Path path) throws IOException {
        return open(path, new Options().setCreateIfMissing(false));
    }

    private static LocalRocksStore open(Path path, Options options) throws IOException {
        try {
            return new LocalRocksStore(options, RocksDB.open(options, path.toString()), path);
        } catch (RocksDBException error) {
            options.close();
            throw new IOException(error);
        }
    }

    @Override
    public String name() {
        return "rocksdb-" + RocksDB.rocksdbVersion();
    }

    @Override
    public String location() {
        return path.toAbsolutePath().toString();
    }

    @Override
    public String durability() {
        return "acknowledged writes are in the memtable and WAL without fsync;"
                + " baseline inserts skip the WAL until Flush";
    }

    @Override
    public void put(byte[] key, byte[] value, boolean wal) throws IOException {
        try {
            db.put(wal ? logged : unlogged, key, value);
        } catch (RocksDBException error) {
            throw new IOException(error);
        }
    }

    @Override
    public byte[] get(byte[] key) throws IOException {
        try {
            return db.get(key);
        } catch (RocksDBException error) {
            throw new IOException(error);
        }
    }

    @Override
    public Cursor cursor(byte[] lower, byte[] upper) {
        var read = new ReadOptions();
        Slice lowerSlice = lower == null ? null : new Slice(lower);
        Slice upperSlice = upper == null ? null : new Slice(upper);
        if (lowerSlice != null) read.setIterateLowerBound(lowerSlice);
        if (upperSlice != null) read.setIterateUpperBound(upperSlice);
        RocksIterator iterator = db.newIterator(read);
        return new Cursor() {
            @Override
            public void seek(byte[] key) {
                iterator.seek(key);
            }

            @Override
            public void seekForPrev(byte[] key) {
                iterator.seekForPrev(key);
            }

            @Override
            public void seekToLast() {
                iterator.seekToLast();
            }

            @Override
            public void next() {
                iterator.next();
            }

            @Override
            public void prev() {
                iterator.prev();
            }

            @Override
            public boolean valid() throws IOException {
                if (iterator.isValid()) return true;
                try {
                    iterator.status();
                } catch (RocksDBException error) {
                    throw new IOException(error);
                }
                return false;
            }

            @Override
            public byte[] key() {
                return iterator.key();
            }

            @Override
            public byte[] value() {
                return iterator.value();
            }

            @Override
            public void close() {
                iterator.close();
                read.close();
                if (lowerSlice != null) lowerSlice.close();
                if (upperSlice != null) upperSlice.close();
            }
        };
    }

    @Override
    public void flush() throws IOException {
        try (var flush = new FlushOptions().setWaitForFlush(true)) {
            db.flush(flush);
            db.flushWal(true);
        } catch (RocksDBException error) {
            throw new IOException(error);
        }
    }

    @Override
    public String property(String name) throws IOException {
        try {
            return db.getProperty(name);
        } catch (RocksDBException error) {
            return null;
        }
    }

    @Override
    public java.util.Map<String, long[]> objectRequests() {
        return null;
    }

    @Override
    public void close() {
        logged.close();
        unlogged.close();
        db.close();
        options.close();
    }
}
