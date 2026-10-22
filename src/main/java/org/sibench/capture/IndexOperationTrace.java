package org.sibench.capture;

import org.sibench.trace.IndexKey;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32C;

/** Versioned normalized operation blocks, separate from raw PG access facts. */
public final class IndexOperationTrace {
    public static final int INSERT = 1, SEARCH = 2, UPSERT = 3, RELOCATE = 4, LOGICAL_DELETE = 5;
    public static final int TRANSACTION_BEGIN = 6, TRANSACTION_COMMIT = 7;
    public static final int BUSINESS_INSERT = 1, BUSINESS_UPDATE = 2;
    public static final int SNAPSHOT_SEARCH = 1, DERIVED_SEARCH = 2;
    public static final int TRANSACTION_LOCAL_SEARCH = 4;
    public static final int LOWER_INCLUSIVE = 8, UPPER_INCLUSIVE = 16, DESCENDING = 32;
    public static final int SEARCH_FLAGS =
            SNAPSHOT_SEARCH
                    | DERIVED_SEARCH
                    | TRANSACTION_LOCAL_SEARCH
                    | LOWER_INCLUSIVE
                    | UPPER_INCLUSIVE
                    | DESCENDING;
    public static final int RECORD_BYTES = 128, BLOCK_RECORDS = 4096;
    public static final int VERSION = 8;
    public static final int MAX_TABLE = 65535, MAX_INDEX = 1024;
    private static final int MAGIC = 0x53494f50;

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
     * value; searches carry a {@link Range} in lower/upper/flags/limit instead.
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

    public static Operation search(
            long source,
            int table,
            int index,
            Range range,
            long readTimestamp,
            long count,
            int flags) {
        return new Operation(
                source,
                SEARCH,
                table,
                index,
                flags | range.flags(),
                null,
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
                0,
                0,
                range.lower(),
                range.upper(),
                range.limit());
    }

    public static Operation transactionSearch(
            long source,
            int table,
            int index,
            Range range,
            long readTimestamp,
            long count,
            long transactionId,
            long precedingMutations,
            boolean local) {
        return search(
                        source,
                        table,
                        index,
                        range,
                        readTimestamp,
                        count,
                        local ? TRANSACTION_LOCAL_SEARCH : 0)
                .withTransaction(transactionId, precedingMutations);
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
                                ? o.version() == 0
                                        && o.readTimestamp() >= 0
                                        && o.key() == null
                                        && o.indexedValue() == null
                                        && (o.flags() & ~SEARCH_FLAGS) == 0
                                        && (o.lower() != null || (o.flags() & LOWER_INCLUSIVE) == 0)
                                        && (o.upper() != null || (o.flags() & UPPER_INCLUSIVE) == 0)
                                        && o.limit() >= 0
                                        && o.expectedCount() >= 0
                                        && (o.limit() == 0 || o.expectedCount() == o.limit())
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

    public interface Consumer {
        void accept(Operation operation) throws IOException;
    }

    public static final class Writer implements Closeable {
        private final FileChannel channel;
        private final ByteArrayOutputStream block = new ByteArrayOutputStream();
        private long count;

        public Writer(Path file) throws IOException {
            channel =
                    FileChannel.open(file, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
            ByteBuffer header = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
            header.putInt(MAGIC).putInt(VERSION).putInt(RECORD_BYTES).putInt(BLOCK_RECORDS).flip();
            write(channel, header);
        }

        public void append(Operation o) throws IOException {
            validate(o);
            ByteBuffer record =
                    ByteBuffer.allocate(
                                    RECORD_BYTES
                                            + length(o.key())
                                            + length(o.indexedValue())
                                            + length(o.lower())
                                            + length(o.upper()))
                            .order(ByteOrder.LITTLE_ENDIAN);
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
            for (IndexKey part : new IndexKey[] {o.key(), o.indexedValue(), o.lower(), o.upper()})
                record.putInt(part == null ? -1 : part.length());
            for (IndexKey part : new IndexKey[] {o.key(), o.indexedValue(), o.lower(), o.upper()})
                if (part != null) part.writeTo(record);
            block.writeBytes(record.array());
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
            if (block.size() == 0) return;
            byte[] payload = block.toByteArray();
            int length = payload.length;
            CRC32C crc = new CRC32C();
            crc.update(payload, 0, length);
            ByteBuffer header = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN);
            header.putInt(length)
                    .putInt((int) ((count - 1) % BLOCK_RECORDS + 1))
                    .putInt((int) crc.getValue())
                    .flip();
            write(channel, header);
            write(channel, ByteBuffer.wrap(payload));
            block.reset();
        }

        @Override
        public void close() throws IOException {
            try {
                flush();
            } finally {
                channel.close();
            }
        }
    }

    /** Reads a current-format trace; every other version is rejected. */
    public static long read(Path file, Consumer consumer) throws IOException {
        try (FileChannel channel = FileChannel.open(file)) {
            ByteBuffer header = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
            fill(channel, header);
            header.flip();
            if (header.getInt() != MAGIC
                    || header.getInt() != VERSION
                    || header.getInt() != RECORD_BYTES
                    || header.getInt() != BLOCK_RECORDS)
                throw new IOException("Unknown normalized trace header");
            long count = 0;
            while (channel.position() < channel.size()) {
                header.clear().limit(12);
                fill(channel, header);
                header.flip();
                int bytes = header.getInt(), records = header.getInt(), checksum = header.getInt();
                // Records carry variable-length keys, so only a lower bound is fixed.
                if (bytes <= 0
                        || records < 1
                        || bytes > (long) records * (RECORD_BYTES + 4L * IndexKey.MAX_BYTES)
                        || records > BLOCK_RECORDS
                        || bytes < (long) records * RECORD_BYTES)
                    throw new IOException("Invalid normalized block length");
                ByteBuffer block = ByteBuffer.allocate(bytes).order(ByteOrder.LITTLE_ENDIAN);
                fill(channel, block);
                block.flip();
                CRC32C crc = new CRC32C();
                crc.update(block.array(), 0, bytes);
                if ((int) crc.getValue() != checksum)
                    throw new IOException("Normalized block checksum mismatch");
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
                    count++;
                }
                if (block.hasRemaining()) throw new IOException("Trailing operation block bytes");
            }
            return count;
        }
    }

    private static void fill(FileChannel channel, ByteBuffer target) throws IOException {
        while (target.hasRemaining())
            if (channel.read(target) < 0) throw new EOFException("Truncated operation trace");
    }

    private static void write(FileChannel channel, ByteBuffer source) throws IOException {
        while (source.hasRemaining()) channel.write(source);
    }
}
