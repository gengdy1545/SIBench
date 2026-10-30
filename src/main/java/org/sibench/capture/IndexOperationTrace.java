package org.sibench.capture;

import org.sibench.trace.IndexKey;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HexFormat;
import java.util.zip.CRC32C;

/** Versioned normalized operation blocks, separate from raw PG access facts. */
public final class IndexOperationTrace {
    public static final int INSERT = 1, SEARCH = 2, UPSERT = 3, RELOCATE = 4, LOGICAL_DELETE = 5;
    public static final int TRANSACTION_BEGIN = 6, TRANSACTION_COMMIT = 7;
    public static final int BUSINESS_INSERT = 1, BUSINESS_UPDATE = 2;
    public static final int SNAPSHOT_SEARCH = 1;
    public static final int TRANSACTION_LOCAL_SEARCH = 4;
    public static final int LOWER_INCLUSIVE = 8, UPPER_INCLUSIVE = 16, DESCENDING = 32;
    /**
     * A search group: consecutive searches of one source access whose expected count is the size
     * of the intersection (a BitmapAnd) or union (an = ANY array) of their results by primary key.
     * Every member carries the group's count; a member's own count is not a source fact.
     */
    public static final int INTERSECT_GROUP = 64, UNION_GROUP = 128;
    public static final int SEARCH_FLAGS =
            SNAPSHOT_SEARCH
                    | TRANSACTION_LOCAL_SEARCH
                    | LOWER_INCLUSIVE
                    | UPPER_INCLUSIVE
                    | DESCENDING
                    | INTERSECT_GROUP
                    | UNION_GROUP;
    public static final int RECORD_BYTES = 128, BLOCK_RECORDS = 4096;
    public static final int VERSION = 9;
    public static final int MAX_TABLE = 65535, MAX_INDEX = 1024;
    private static final int MAGIC = 0x53494f50;
    /**
     * A file of zstd-compressed blocks: the same records, each block compressed on its own. Its
     * digest, like that of a derived stream, is the logical one: the SHA256 of the plain file the
     * same operations form ({@link #sha256}), so it does not depend on the compressor.
     */
    private static final int MAGIC_ZSTD = 0x53494f5a;
    static final int ZSTD_LEVEL = 3;

    private IndexOperationTrace() {}

    public static String kindName(int kind) {
        return switch (kind) {
            case INSERT -> "insert";
            case SEARCH -> "search";
            case UPSERT -> "upsert";
            case RELOCATE -> "relocate";
            case LOGICAL_DELETE -> "logical_delete";
            case TRANSACTION_BEGIN -> "transaction_begin";
            case TRANSACTION_COMMIT -> "transaction_commit";
            default -> throw new IllegalArgumentException("Unknown operation kind: " + kind);
        };
    }

    /**
     * The searched interval of one index component: the PK for index 0, the indexed value
     * otherwise. A null bound is unbounded. Matches are visited in ascending (value, PK) order, or
     * descending; a positive limit stops after that many visible matches.
     */
    public record Range(
            IndexKey lower,
            boolean lowerInclusive,
            IndexKey upper,
            boolean upperInclusive,
            boolean descending,
            long limit) {
        public static Range equal(IndexKey value) {
            return new Range(value, true, value, true, false, 0);
        }

        public boolean contains(IndexKey value) {
            if (lower != null) {
                int order = value.compareTo(lower);
                if (order < 0 || order == 0 && !lowerInclusive) return false;
            }
            if (upper != null) {
                int order = value.compareTo(upper);
                if (order > 0 || order == 0 && !upperInclusive) return false;
            }
            return true;
        }

        public boolean point() {
            return lower != null && lower.equals(upper) && lowerInclusive && upperInclusive;
        }

        int flags() {
            return (lowerInclusive ? LOWER_INCLUSIVE : 0)
                    | (upperInclusive ? UPPER_INCLUSIVE : 0)
                    | (descending ? DESCENDING : 0);
        }
    }

    /**
     * Writes carry the encoded PK in {@code key} and, for a secondary index, the encoded indexed
     * value; searches carry a {@link Range} in lower/upper/flags/limit instead. A secondary point
     * search may also carry a PK in {@code key}: it then names exactly one index entry, the row a
     * scan its consumer stopped early returned among equal indexed values.
     */
    /**
     * One normalized operation. For SEARCH, {@code version} is 0 unless the read is
     * transaction-local, when it is the source transaction's CommitVersion; writes use it as the
     * new entry version.
     */
    public record Operation(
            long sourceEvent,
            int kind,
            int table,
            int index,
            int flags,
            IndexKey key,
            IndexKey indexedValue,
            long version,
            long file,
            long position,
            long rowId,
            long expectedCount,
            long transactionId,
            long mutationOrdinal,
            long readTimestamp,
            long workUnitId,
            int expansionPosition,
            int expansionSize,
            IndexKey lower,
            IndexKey upper,
            long limit) {

        public Range range() {
            return new Range(
                    lower,
                    (flags & LOWER_INCLUSIVE) != 0,
                    upper,
                    (flags & UPPER_INCLUSIVE) != 0,
                    (flags & DESCENDING) != 0,
                    limit);
        }

        public Operation withIndex(int id, IndexKey value, int operationKind) {
            return new Operation(
                    sourceEvent,
                    operationKind,
                    table,
                    id,
                    flags,
                    key,
                    value,
                    version,
                    operationKind == LOGICAL_DELETE ? 0 : file,
                    operationKind == LOGICAL_DELETE ? 0 : position,
                    rowId,
                    expectedCount,
                    transactionId,
                    mutationOrdinal,
                    readTimestamp,
                    workUnitId,
                    expansionPosition,
                    expansionSize,
                    lower,
                    upper,
                    limit);
        }

        public Operation withExpansion(int ordinal, int count) {
            return new Operation(
                    sourceEvent,
                    kind,
                    table,
                    index,
                    flags,
                    key,
                    indexedValue,
                    version,
                    file,
                    position,
                    rowId,
                    expectedCount,
                    transactionId,
                    mutationOrdinal,
                    readTimestamp,
                    workUnitId,
                    ordinal,
                    count,
                    lower,
                    upper,
                    limit);
        }

        public Operation withWorkUnitId(long id) {
            return new Operation(
                    sourceEvent,
                    kind,
                    table,
                    index,
                    flags,
                    key,
                    indexedValue,
                    version,
                    file,
                    position,
                    rowId,
                    expectedCount,
                    transactionId,
                    mutationOrdinal,
                    readTimestamp,
                    id,
                    expansionPosition,
                    expansionSize,
                    lower,
                    upper,
                    limit);
        }

        public boolean grouped() {
            return (flags & (INTERSECT_GROUP | UNION_GROUP)) != 0;
        }

        public Operation withTransaction(long id, long ordinal) {
            return new Operation(
                    sourceEvent,
                    kind,
                    table,
                    index,
                    flags,
                    key,
                    indexedValue,
                    version,
                    file,
                    position,
                    rowId,
                    expectedCount,
                    id,
                    ordinal,
                    readTimestamp,
                    workUnitId,
                    expansionPosition,
                    expansionSize,
                    lower,
                    upper,
                    limit);
        }

        /**
         * A search's {@code version} is 0 unless it is transaction-local; then it is the
         * committing transaction's own version. Its {@code readTimestamp} remains the statement
         * snapshot, so a reader takes that snapshot and overlays this transaction's writes up to
         * {@code mutationOrdinal}.
         */
        public Operation withVersion(long searchVersion) {
            return new Operation(
                    sourceEvent,
                    kind,
                    table,
                    index,
                    flags,
                    key,
                    indexedValue,
                    searchVersion,
                    file,
                    position,
                    rowId,
                    expectedCount,
                    transactionId,
                    mutationOrdinal,
                    readTimestamp,
                    workUnitId,
                    expansionPosition,
                    expansionSize,
                    lower,
                    upper,
                    limit);
        }
    }

    /** A primary-key write; {@link IndexMutation} expands it to the secondary indexes. */
    public static Operation write(
            long source,
            int kind,
            int table,
            int flags,
            IndexKey key,
            long version,
            long file,
            long position,
            long rowId) {
        return new Operation(
                source, kind, table, 0, flags, key, null, version, file, position, rowId, 0, 0, 0,
                -1, 0, 0, 0, null, null, 0);
    }

    /**
     * One search. {@code pinned} is the PK of the single secondary entry a pinned point search
     * names, or null; {@code groupFlags} and the group position mark a member of a search group.
     */
    public static Operation search(
            long source,
            int table,
            int index,
            Range range,
            IndexKey pinned,
            long readTimestamp,
            long count,
            int flags,
            int groupPosition,
            int groupSize) {
        return new Operation(
                source,
                SEARCH,
                table,
                index,
                flags | range.flags(),
                pinned,
                null,
                0,
                0,
                0,
                0,
                count,
                0,
                0,
                readTimestamp,
                0,
                groupPosition,
                groupSize,
                range.lower(),
                range.upper(),
                range.limit());
    }

    public static Operation search(
            long source, int table, int index, Range range, long readTimestamp, long count, int flags) {
        return search(source, table, index, range, null, readTimestamp, count, flags, 0, 0);
    }

    public static Operation transaction(long source, int kind, long id, long version, long rows) {
        return new Operation(
                source, kind, 0, 0, 0, null, null, version, 0, 0, 0, rows, id, 0, -1, 0, 0, 0,
                null, null, 0);
    }

    public static boolean isMutation(Operation o) {
        return o.kind() == UPSERT || o.kind() == LOGICAL_DELETE;
    }

    public static boolean isControl(Operation o) {
        return o.kind() == TRANSACTION_BEGIN || o.kind() == TRANSACTION_COMMIT;
    }

    /** Format-level invariants checked on both write and read. */
    static void validate(Operation o) throws IOException {
        boolean control = isControl(o), search = o.kind() == SEARCH;
        boolean valid =
                o.kind() >= INSERT
                        && o.kind() <= TRANSACTION_COMMIT
                        && (control ? o.table() == 0 : o.table() >= 1 && o.table() <= MAX_TABLE)
                        && o.index() >= 0
                        && o.index() <= MAX_INDEX
                        && o.workUnitId() >= 0
                        && (!control || o.workUnitId() == 0 && o.index() == 0)
                        && (search
                                ? validSearch(o)
                                : o.readTimestamp() == -1
                                        && o.lower() == null
                                        && o.upper() == null
                                        && o.limit() == 0
                                        && (control
                                                ? o.key() == null && o.indexedValue() == null
                                                : o.key() != null
                                                        && (o.index() == 0)
                                                                == (o.indexedValue() == null)));
        if (!valid) throw new IOException("Invalid normalized operation");
    }

    private static boolean validSearch(Operation o) {
        int group = o.flags() & (INTERSECT_GROUP | UNION_GROUP);
        boolean pinned = o.key() != null;
        boolean local = (o.flags() & TRANSACTION_LOCAL_SEARCH) != 0;
        return (local
                        ? o.version() > 0
                                && o.transactionId() > 0
                                && o.mutationOrdinal() > 0
                                && (o.flags() & SNAPSHOT_SEARCH) == 0
                        : o.version() == 0)
                && o.readTimestamp() >= 0
                && o.indexedValue() == null
                && (o.flags() & ~SEARCH_FLAGS) == 0
                && (o.lower() != null || (o.flags() & LOWER_INCLUSIVE) == 0)
                && (o.upper() != null || (o.flags() & UPPER_INCLUSIVE) == 0)
                && o.limit() >= 0
                && o.expectedCount() >= 0
                && (o.limit() == 0 || o.expectedCount() == o.limit())
                && group != (INTERSECT_GROUP | UNION_GROUP)
                && (group == 0
                        ? o.expansionPosition() == 0 && o.expansionSize() == 0
                        : o.expansionSize() >= 2
                                && o.expansionPosition() >= 0
                                && o.expansionPosition() < o.expansionSize()
                                && o.limit() == 0
                                && !pinned)
                && (!pinned
                        || o.index() > 0
                                && o.range().point()
                                && o.limit() == 0
                                && o.expectedCount() == 1);
    }

    public interface Consumer {
        void accept(Operation operation) throws IOException;
    }

    /** A stream of operations: a trace file, or one derived when it is read. Returns the count. */
    @FunctionalInterface
    public interface Source {
        long read(Consumer consumer) throws IOException;

        static Source file(Path file) {
            return consumer -> IndexOperationTrace.read(file, consumer);
        }

        static Source file(Path file, String expectedSha256) {
            java.util.Objects.requireNonNull(expectedSha256);
            return consumer -> IndexOperationTrace.read(file, consumer, expectedSha256);
        }
    }

    /**
     * Encodes operations exactly as a trace file holds them. A writer without a file only counts
     * them and takes the SHA256 of the file they would form, for a stream derived when it is read.
     */
    public static final class Writer implements Closeable {
        private final FileChannel channel;
        /** The current block's records, encoded in place; grown when a record does not fit. */
        private ByteBuffer block = ByteBuffer.allocate(BLOCK_RECORDS * (RECORD_BYTES + 32)).order(ByteOrder.LITTLE_ENDIAN);
        /** The file's SHA256, taken from the bytes as they are written instead of a reread. */
        private final java.security.MessageDigest digest;
        private String sha256;
        private long count;

        /** {@code file}: where the trace is written, or null to only digest it. */
        public Writer(Path file) throws IOException {
            try {
                digest = java.security.MessageDigest.getInstance("SHA-256");
            } catch (java.security.NoSuchAlgorithmException error) {
                throw new IllegalStateException(error);
            }
            channel =
                    file == null
                            ? null
                            : FileChannel.open(file, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
            digest.update(header(MAGIC));
            if (channel != null) write(channel, header(MAGIC_ZSTD));
        }

        private static ByteBuffer header(int magic) {
            ByteBuffer header = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
            header.putInt(magic).putInt(VERSION).putInt(RECORD_BYTES).putInt(BLOCK_RECORDS).flip();
            return header;
        }

        /** The logical SHA256 of the closed stream: that of its plain file. */
        public String sha256() {
            if (sha256 == null) throw new IllegalStateException("Trace writer is still open");
            return sha256;
        }

        public void append(Operation o) throws IOException {
            validate(o);
            int size = RECORD_BYTES + length(o.key()) + length(o.indexedValue()) + length(o.lower()) + length(o.upper());
            if (block.remaining() < size) {
                ByteBuffer larger = ByteBuffer.allocate(Math.max(block.capacity() * 2, block.position() + size))
                        .order(ByteOrder.LITTLE_ENDIAN);
                block.flip();
                larger.put(block);
                block = larger;
            }
            ByteBuffer record = block;
            record.putLong(o.sourceEvent())
                    .putInt(o.kind())
                    .putInt(o.table())
                    .putInt(o.index())
                    .putInt(o.flags())
                    .putLong(o.version())
                    .putLong(o.file())
                    .putLong(o.position())
                    .putLong(o.rowId())
                    .putLong(o.expectedCount())
                    .putLong(o.transactionId())
                    .putLong(o.mutationOrdinal())
                    .putLong(o.readTimestamp())
                    .putLong(o.workUnitId())
                    .putLong(o.limit())
                    .putInt(o.expansionPosition())
                    .putInt(o.expansionSize());
            record.putInt(o.key() == null ? -1 : o.key().length())
                    .putInt(o.indexedValue() == null ? -1 : o.indexedValue().length())
                    .putInt(o.lower() == null ? -1 : o.lower().length())
                    .putInt(o.upper() == null ? -1 : o.upper().length());
            if (o.key() != null) o.key().writeTo(record);
            if (o.indexedValue() != null) o.indexedValue().writeTo(record);
            if (o.lower() != null) o.lower().writeTo(record);
            if (o.upper() != null) o.upper().writeTo(record);
            count++;
            if (count % BLOCK_RECORDS == 0) flush();
        }

        private static int length(IndexKey key) {
            return key == null ? 0 : key.length();
        }

        public long count() {
            return count;
        }

        private void flush() throws IOException {
            if (block.position() == 0) return;
            byte[] payload = block.array();
            int length = block.position();
            CRC32C crc = new CRC32C();
            crc.update(payload, 0, length);
            int records = (int) ((count - 1) % BLOCK_RECORDS + 1);
            ByteBuffer header = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN);
            header.putInt(length).putInt(records).putInt((int) crc.getValue()).flip();
            digest.update(header);
            digest.update(payload, 0, length);
            if (channel != null) {
                // The checksum covers the records, so it holds whatever compressed them.
                byte[] compressed = com.github.luben.zstd.Zstd.compress(java.util.Arrays.copyOf(payload, length), ZSTD_LEVEL);
                ByteBuffer stored = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
                stored.putInt(compressed.length).putInt(records).putInt((int) crc.getValue()).putInt(length).flip();
                write(channel, stored);
                write(channel, ByteBuffer.wrap(compressed));
            }
            block.clear();
        }

        @Override
        public void close() throws IOException {
            if (sha256 != null || channel != null && !channel.isOpen()) return;
            try {
                flush();
                sha256 = HexFormat.of().formatHex(digest.digest());
            } finally {
                if (channel != null) channel.close();
            }
        }
    }

    /** One block's records, as the plain format holds them. */
    private interface Blocks {
        void block(int records, int checksum, ByteBuffer payload) throws IOException;
    }

    /** The zstd blocks of a trace, each checked against its checksum. */
    private static void blocks(Path file, Blocks blocks) throws IOException {
        try (FileChannel channel = FileChannel.open(file)) {
            ByteBuffer header = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
            fill(channel, header);
            header.flip();
            int magic = header.getInt();
            if (magic != MAGIC_ZSTD
                    || header.getInt() != VERSION
                    || header.getInt() != RECORD_BYTES
                    || header.getInt() != BLOCK_RECORDS)
                throw new IOException("Unknown normalized trace header");
            while (channel.position() < channel.size()) {
                header.clear().limit(16);
                fill(channel, header);
                header.flip();
                int stored = header.getInt(), records = header.getInt(), checksum = header.getInt();
                int bytes = header.getInt();
                // Records carry variable-length keys, so only a lower bound is fixed.
                if (bytes <= 0
                        || stored <= 0
                        || records < 1
                        || bytes > (long) records * (RECORD_BYTES + 4L * IndexKey.MAX_BYTES)
                        || records > BLOCK_RECORDS
                        || bytes < (long) records * RECORD_BYTES)
                    throw new IOException("Invalid normalized block length");
                ByteBuffer block = ByteBuffer.allocate(stored).order(ByteOrder.LITTLE_ENDIAN);
                fill(channel, block);
                byte[] payload;
                try {
                    payload = com.github.luben.zstd.Zstd.decompress(block.array(), bytes);
                } catch (RuntimeException corrupt) {
                    throw new IOException("Normalized block does not decompress", corrupt);
                }
                if (payload.length != bytes) throw new IOException("Invalid normalized block length");
                CRC32C crc = new CRC32C();
                crc.update(payload, 0, bytes);
                if ((int) crc.getValue() != checksum)
                    throw new IOException("Normalized block checksum mismatch");
                blocks.block(records, checksum, ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN));
            }
        }
    }

    /**
     * The logical SHA256 of a trace: that of the plain file its operations form.
     */
    public static String sha256(Path file) throws IOException {
        var digest = logicalDigest();
        var header = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN);
        blocks(file, (records, checksum, payload) -> digestBlock(digest, header, records, checksum, payload));
        return HexFormat.of().formatHex(digest.digest());
    }

    private static java.security.MessageDigest logicalDigest() {
        try {
            var digest = java.security.MessageDigest.getInstance("SHA-256");
            digest.update(Writer.header(MAGIC));
            return digest;
        } catch (java.security.NoSuchAlgorithmException error) {
            throw new IllegalStateException(error);
        }
    }

    private static void digestBlock(java.security.MessageDigest digest, ByteBuffer header,
            int records, int checksum, ByteBuffer payload) {
        header.clear();
        header.putInt(payload.remaining()).putInt(records).putInt(checksum).flip();
        digest.update(header);
        digest.update(payload.duplicate());
    }

    /** Reads a trace of the current record format, in zstd blocks. */
    public static long read(Path file, Consumer consumer) throws IOException {
        return read(file, consumer, null);
    }

    /** Checks the logical identity in the same block traversal as decoding and CRC verification. */
    private static long read(Path file, Consumer consumer, String expectedSha256) throws IOException {
        long[] count = {0};
        var digest = expectedSha256 == null ? null : logicalDigest();
        var header = expectedSha256 == null ? null : ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN);
        blocks(file, (records, checksum, block) -> {
                if (digest != null) digestBlock(digest, header, records, checksum, block);
                for (int i = 0; i < records; i++) {
                    if (block.remaining() < RECORD_BYTES)
                        throw new IOException("Truncated normalized operation prefix");
                    long sourceEvent = block.getLong();
                    int kind = block.getInt(), table = block.getInt(), index = block.getInt();
                    int flags = block.getInt();
                    long version = block.getLong(), file1 = block.getLong();
                    long position = block.getLong(), rowId = block.getLong();
                    long expectedCount = block.getLong(), transactionId = block.getLong();
                    long mutationOrdinal = block.getLong(), readTimestamp = block.getLong();
                    long workUnitId = block.getLong(), limit = block.getLong();
                    int expansionPosition = block.getInt(), expansionSize = block.getInt();
                    int[] lengths = new int[4];
                    long variable = 0;
                    for (int part = 0; part < 4; part++) {
                        lengths[part] = block.getInt();
                        if (lengths[part] < -1 || lengths[part] > IndexKey.MAX_BYTES)
                            throw new IOException("Invalid key length");
                        variable += Math.max(0, lengths[part]);
                    }
                    if (variable > block.remaining() - (long) (records - i - 1) * RECORD_BYTES)
                        throw new IOException("Invalid key length");
                    IndexKey[] parts = new IndexKey[4];
                    for (int part = 0; part < 4; part++) {
                        if (lengths[part] < 0) continue;
                        byte[] value = new byte[lengths[part]];
                        block.get(value);
                        parts[part] = IndexKey.decode(value);
                    }
                    Operation o =
                            new Operation(
                                    sourceEvent,
                                    kind,
                                    table,
                                    index,
                                    flags,
                                    parts[0],
                                    parts[1],
                                    version,
                                    file1,
                                    position,
                                    rowId,
                                    expectedCount,
                                    transactionId,
                                    mutationOrdinal,
                                    readTimestamp,
                                    workUnitId,
                                    expansionPosition,
                                    expansionSize,
                                    parts[2],
                                    parts[3],
                                    limit);
                    validate(o);
                    consumer.accept(o);
                    count[0]++;
                }
                if (block.hasRemaining()) throw new IOException("Trailing operation block bytes");
        });
        if (digest != null && !HexFormat.of().formatHex(digest.digest()).equals(expectedSha256))
            throw new IOException("Trace checksum mismatch: " + file.getFileName());
        return count[0];
    }

    private static void fill(FileChannel channel, ByteBuffer target) throws IOException {
        while (target.hasRemaining())
            if (channel.read(target) < 0) throw new EOFException("Truncated operation trace");
    }

    private static void write(FileChannel channel, ByteBuffer source) throws IOException {
        while (source.hasRemaining()) channel.write(source);
    }
}
