package org.sibench.generation;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.SQLException;
import java.util.*;
import org.rocksdb.*;
import org.sibench.config.Digests;

/** Sealed native SST files, read in place without a writable copy or recovery. */
public final class RocksDbData {
    private RocksDbData() {}
    public record File(long bytes, String sha256) {
        public File {
            if (bytes < 0 || sha256 == null || !sha256.matches("[0-9a-f]{64}")) throw new IllegalArgumentException("Invalid RocksDB file identity");
        }
    }
    public record Inventory(String engine, String bridgeSha256, String runtimeSha256, Map<String, File> files, long lastResolvedVersion) {
        public Inventory {
            if (engine == null || bridgeSha256 == null || !bridgeSha256.matches("[0-9a-f]{64}")
                    || runtimeSha256 == null || !runtimeSha256.matches("[0-9a-f]{64}")
                    || files == null || files.isEmpty() || lastResolvedVersion < 0)
                throw new IllegalArgumentException("Invalid RocksDB inventory");
            files = Map.copyOf(files);
            for (String name : files.keySet()) {
                Path path = Path.of(name);
                if (path.isAbsolute() || !path.normalize().equals(path) || path.startsWith(".."))
                    throw new IllegalArgumentException("Invalid RocksDB inventory path");
            }
        }
    }
    public static Inventory inventory(Path data, Path library, long version) throws IOException {
        return inventory(data, library, version, null);
    }
    /** Use write-time data-file identities only for the same, just-closed engine instance. */
    public static Inventory seal(Path data, Path library, long version, Map<String, File> writtenFiles) throws IOException {
        return inventory(data, library, version, Objects.requireNonNull(writtenFiles));
    }
    private static Inventory inventory(Path data, Path library, long version, Map<String, File> writtenFiles) throws IOException {
        if (!Files.isRegularFile(data.resolve("CURRENT"))) throw new IOException("Missing RocksDB CURRENT");
        var files = new TreeMap<String, File>();
        try (var paths = Files.walk(data)) {
            for (Path path : paths.sorted().toList()) {
                if (Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) continue;
                if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) throw new IOException("External RocksDB file dependency: " + path);
                if (path.getFileName().toString().matches("[0-9]+\\.log") && Files.size(path) != 0)
                    throw new IOException("RocksDB generation contains unexpected WAL records: " + path);
                String name = data.relativize(path).toString();
                File identity;
                if (writtenFiles != null && (name.endsWith(".sst") || name.endsWith(".blob"))) {
                    identity = writtenFiles.get(name);
                    if (identity == null || identity.bytes() != Files.size(path))
                        throw new IOException("Missing or mismatched write-time RocksDB file identity: " + path);
                } else identity = new File(Files.size(path), Digests.sha256(path));
                files.put(name, identity);
            }
        }
        return new Inventory(RocksDB.rocksdbVersion().toString(), Digests.sha256(library),
                Digests.sha256(library.getParent().resolve("librocksdbjni-linux64.so")), files, version);
    }
    public static NativeData open(Path data, Inventory expected, Path library) throws IOException, SQLException {
        RocksDbTimestamps.load(library);
        if (!inventory(data, library, expected.lastResolvedVersion()).equals(expected))
            throw new IOException("RocksDB native data/runtime differs from inventory");
        return new Reader(data, expected.lastResolvedVersion());
    }
    private static final class Reader implements NativeData {
        private final DBOptions options = new DBOptions().setCreateIfMissing(false);
        private final ColumnFamilyOptions metadata = new ColumnFamilyOptions(), rows = new ColumnFamilyOptions();
        private final List<ColumnFamilyHandle> handles = new ArrayList<>();
        private final RocksDB database;
        private final long version;
        private final int fields;
        Reader(Path data, long expected) throws SQLException {
            RocksDbTimestamps.configure(rows);
            RocksDB opened = null;
            try {
                // RocksDB creates an empty WAL even with every write unlogged; inventory rejects actual WAL records.
                opened = RocksDB.openReadOnly(options, data.toString(), RocksDbRows.descriptors(metadata, rows), handles, false);
                byte[] marker = opened.get(RocksDbRows.bytes("sealed"));
                if (marker == null || marker.length != 20) throw new SQLException("RocksDB generation is not sealed");
                var buffer = ByteBuffer.wrap(marker); fields = buffer.getInt(); version = buffer.getLong();
                if (fields < 1 || fields > 1024 || version != expected || buffer.getLong() < 0)
                    throw new SQLException("RocksDB sealed metadata differs from inventory");
                database = opened;
            } catch (RocksDBException | SQLException | RuntimeException error) {
                handles.forEach(ColumnFamilyHandle::close); if (opened != null) opened.close();
                rows.close(); metadata.close(); options.close();
                throw new SQLException("Cannot open sealed RocksDB data", error);
            }
        }
        @Override public Optional<Map<String, Object>> snapshotRow(int table, long rowId, long readVersion) throws SQLException {
            if (table != 1 || rowId <= 0 || readVersion < 0 || readVersion > version)
                throw new IllegalArgumentException("Invalid RocksDB row/snapshot");
            try (var timestamp = new Slice(RocksDbTimestamps.timestamp(readVersion));
                 var read = new ReadOptions().setTimestamp(timestamp)) {
                byte[] key = database.get(RocksDbRows.identityKey(rowId));
                if (key == null) return Optional.empty();
                byte[] value = database.get(handles.get(1), read, key);
                if (value == null) return Optional.empty();
                var row = RocksDbRows.decode(new String(key, StandardCharsets.UTF_8), value, fields);
                if (row.id() != rowId) return Optional.empty();
                var result = new LinkedHashMap<String, Object>(); result.put("ycsb_key", row.key());
                for (int i = 0; i < fields; i++) result.put("field" + i, row.fields()[i]);
                return Optional.of(result);
            } catch (RocksDBException | IOException error) { throw new SQLException("Cannot read native RocksDB row", error); }
        }
        @Override public Optional<Map<String, Object>> rowBeforeStatement(int table, long rowId, long connection, long query)
                throws SQLException {
            try {
                byte[] point = database.get(RocksDbRows.pointKey(connection, query));
                if (point == null || point.length != 8) throw new SQLException("Unknown RocksDB source read point");
                return snapshotRow(table, rowId, ByteBuffer.wrap(point).getLong());
            } catch (RocksDBException error) { throw new SQLException(error); }
        }
        @Override public void close() {
            handles.forEach(ColumnFamilyHandle::close); database.close(); rows.close(); metadata.close(); options.close();
        }
    }
}
