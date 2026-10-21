package org.sibench.capture;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
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
    public static final int RECORD_BYTES = 132, BLOCK_RECORDS = 4096;
    public static final int VERSION = 7;
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

    public record Operation(
            long sourceEvent,
            int kind,
            int table,
            int index,
            int flags,
            long key,
            long version,
            long file,
            long position,
            long rowId,
            long secondarySource,
            long secondaryTarget,
            long expectedCount,
            long transactionId,
            long mutationOrdinal,
            long readTimestamp,
            long workUnitId,
            String indexedValue,
            int expansionPosition,
            int expansionSize) {
        public Operation(
                long sourceEvent,
                int kind,
                int table,
                int index,
                int flags,
                long key,
                long version,
                long file,
                long position,
                long rowId,
                long secondarySource,
                long secondaryTarget,
                long expectedCount,
                long transactionId,
                long mutationOrdinal,
                long readTimestamp,
                long workUnitId) {
            this(
                    sourceEvent,
                    kind,
                    table,
                    index,
                    flags,
                    key,
                    version,
                    file,
                    position,
                    rowId,
                    secondarySource,
                    secondaryTarget,
                    expectedCount,
                    transactionId,
                    mutationOrdinal,
                    readTimestamp,
                    workUnitId,
                    index == 0
                            ? null
                            : Long.toString(
                                    kind == SEARCH
                                            ? key
                                            : index == 1 ? secondarySource : secondaryTarget),
                    0,
                    0);
        }

        public Operation withIndex(int id, String value, int operationKind) {
            return new Operation(
                    sourceEvent,
                    operationKind,
                    table,
                    id,
                    flags,
                    key,
                    version,
                    operationKind == LOGICAL_DELETE ? 0 : file,
                    operationKind == LOGICAL_DELETE ? 0 : position,
                    rowId,
                    secondarySource,
                    secondaryTarget,
                    expectedCount,
                    transactionId,
                    mutationOrdinal,
                    readTimestamp,
                    workUnitId,
                    value,
                    expansionPosition,
                    expansionSize);
        }

        public Operation withExpansion(int ordinal, int count) {
            return new Operation(
                    sourceEvent,
                    kind,
                    table,
                    index,
                    flags,
                    key,
                    version,
                    file,
                    position,
                    rowId,
                    secondarySource,
                    secondaryTarget,
                    expectedCount,
                    transactionId,
                    mutationOrdinal,
                    readTimestamp,
                    workUnitId,
                    indexedValue,
                    ordinal,
                    count);
        }

        public Operation withIndexedValue(String value) {
            return new Operation(
                    sourceEvent,
                    kind,
                    table,
                    index,
                    flags,
                    key,
                    version,
                    file,
                    position,
                    rowId,
                    secondarySource,
                    secondaryTarget,
                    expectedCount,
                    transactionId,
                    mutationOrdinal,
                    readTimestamp,
                    workUnitId,
                    value,
                    expansionPosition,
                    expansionSize);
        }

        public Operation(
                long sourceEvent,
                int kind,
                int table,
                int index,
                int flags,
                long key,
                long version,
                long file,
                long position,
                long rowId,
                long secondarySource,
                long secondaryTarget,
                long expectedCount,
                long transactionId,
                long mutationOrdinal,
                long readTimestamp) {
            this(
                    sourceEvent,
                    kind,
                    table,
                    index,
                    flags,
                    key,
                    version,
                    file,
                    position,
                    rowId,
                    secondarySource,
                    secondaryTarget,
                    expectedCount,
                    transactionId,
                    mutationOrdinal,
                    readTimestamp,
                    0);
        }

        public Operation withWorkUnitId(long id) {
            return new Operation(
                    sourceEvent,
                    kind,
                    table,
                    index,
                    flags,
                    key,
                    version,
                    file,
                    position,
                    rowId,
                    secondarySource,
                    secondaryTarget,
                    expectedCount,
                    transactionId,
                    mutationOrdinal,
                    readTimestamp,
                    id,
                    indexedValue,
                    expansionPosition,
                    expansionSize);
        }

        public Operation(
                long sourceEvent,
                int kind,
                int table,
                int index,
                int flags,
                long key,
                long version,
                long file,
                long position,
                long rowId,
                long secondarySource,
                long secondaryTarget,
                long expectedCount,
                long transactionId,
                long mutationOrdinal) {
            this(
                    sourceEvent,
                    kind,
                    table,
                    index,
                    flags,
                    key,
                    kind == SEARCH ? 0 : version,
                    file,
                    position,
                    rowId,
                    secondarySource,
                    secondaryTarget,
                    expectedCount,
                    transactionId,
                    mutationOrdinal,
                    kind == SEARCH ? version : -1);
        }

        public Operation(
                long sourceEvent,
                int kind,
                int table,
                int index,
                int flags,
                long key,
                long version,
                long file,
                long position,
                long rowId,
                long secondarySource,
                long secondaryTarget,
                long expectedCount) {
            this(
                    sourceEvent,
                    kind,
                    table,
                    index,
                    flags,
                    key,
                    version,
                    file,
                    position,
                    rowId,
                    secondarySource,
                    secondaryTarget,
                    expectedCount,
                    0,
                    0);
        }
    }

    public static Operation search(
            long source,
            int table,
            int index,
            long key,
            long readTimestamp,
            long count,
            int flags) {
        return new Operation(
                source,
                SEARCH,
                table,
                index,
                flags,
                key,
                0,
                0,
                0,
                0,
                0,
                0,
                count,
                0,
                0,
                readTimestamp);
    }

    public static boolean isMutation(Operation o) {
        return o.kind() == UPSERT || o.kind() == LOGICAL_DELETE;
    }

    public static Operation transactionSearch(
            long source,
            int table,
            int index,
            long key,
            long readTimestamp,
            long count,
            long transactionId,
            long precedingMutations,
            boolean local) {
        return new Operation(
                source,
                SEARCH,
                table,
                index,
                local ? TRANSACTION_LOCAL_SEARCH : 0,
                key,
                0,
                0,
                0,
                0,
                0,
                0,
                count,
                transactionId,
                precedingMutations,
                readTimestamp);
    }

    public static boolean isControl(Operation o) {
        return o.kind() == TRANSACTION_BEGIN || o.kind() == TRANSACTION_COMMIT;
    }

    public static Operation transaction(long source, int kind, long id, long version, long rows) {
        return new Operation(source, kind, 0, 0, 0, 0, version, 0, 0, 0, 0, 0, rows, id, 0);
    }

    private static void expandLegacyRow(Operation row, Consumer consumer) throws IOException {
        consumer.accept(row);
        if (row.table() == 5 && row.index() == 0 && row.kind() != SEARCH) {
            for (int index = 1; index <= 2; index++)
                consumer.accept(
                        new Operation(
                                        row.sourceEvent(),
                                        row.kind(),
                                        row.table(),
                                        index,
                                        row.flags(),
                                        row.key(),
                                        row.version(),
                                        row.file(),
                                        row.position(),
                                        row.rowId(),
                                        row.secondarySource(),
                                        row.secondaryTarget(),
                                        row.expectedCount(),
                                        row.transactionId(),
                                        row.mutationOrdinal())
                                .withWorkUnitId(row.workUnitId()));
        }
    }

    @FunctionalInterface
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
            if ((o.kind() == SEARCH && (o.version() != 0 || o.readTimestamp() < 0))
                    || (o.kind() != SEARCH && o.readTimestamp() != -1)
                    || o.workUnitId() < 0
                    || isControl(o) && o.workUnitId() != 0)
                throw new IOException("Invalid CommitVersion/ReadTimestamp fields");
            byte[] value =
                    o.indexedValue == null ? null : o.indexedValue.getBytes(StandardCharsets.UTF_8);
            if (value != null && value.length > 32768)
                throw new IOException("Indexed value exceeds supported encoded key width");
            ByteBuffer record =
                    ByteBuffer.allocate(RECORD_BYTES + (value == null ? 0 : value.length))
                            .order(ByteOrder.LITTLE_ENDIAN);
            record.putLong(o.sourceEvent)
                    .putInt(o.kind)
                    .putInt(o.table)
                    .putInt(o.index)
                    .putInt(o.flags)
                    .putLong(o.key)
                    .putLong(o.version)
                    .putLong(o.file)
                    .putLong(o.position)
                    .putLong(o.rowId)
                    .putLong(o.secondarySource)
                    .putLong(o.secondaryTarget)
                    .putLong(o.expectedCount)
                    .putLong(o.transactionId)
                    .putLong(o.mutationOrdinal)
                    .putLong(o.readTimestamp)
                    .putLong(o.workUnitId)
                    .putInt(o.expansionPosition)
                    .putInt(o.expansionSize)
                    .putInt(value == null ? -1 : value.length);
            if (value != null) record.put(value);
            block.writeBytes(record.array());
            count++;
            if (count % BLOCK_RECORDS == 0) flush();
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

    public static long read(Path file, Consumer consumer) throws IOException {
        return read(file, consumer, false);
    }

    /** Legacy reads normalize SEARCH.version into ReadTimestamp without rewriting source files. */
    public static long readExpanded(Path file, Consumer consumer) throws IOException {
        return read(file, consumer, true);
    }

    private static long read(Path file, Consumer consumer, boolean expandLegacy)
            throws IOException {
        try (FileChannel channel = FileChannel.open(file)) {
            ByteBuffer header = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
            fill(channel, header);
            header.flip();
            int magic = header.getInt(), version = header.getInt();
            int recordBytes = recordBytes(version);
            if (magic != MAGIC
                    || version < 1
                    || version > VERSION
                    || header.getInt() != recordBytes
                    || header.getInt() != BLOCK_RECORDS)
                throw new IOException("Unknown normalized trace header");
            long count = 0;
            while (channel.position() < channel.size()) {
                header.clear().limit(12);
                fill(channel, header);
                header.flip();
                int bytes = header.getInt(), records = header.getInt(), checksum = header.getInt();
                if (bytes <= 0
                        || bytes > 256 * 1024 * 1024
                        || records < 1
                        || records > BLOCK_RECORDS
                        || (version < 7
                                && (bytes % recordBytes != 0 || records != bytes / recordBytes))
                        || bytes < (long) records * recordBytes)
                    throw new IOException("Invalid normalized block length");
                ByteBuffer block = ByteBuffer.allocate(bytes).order(ByteOrder.LITTLE_ENDIAN);
                fill(channel, block);
                block.flip();
                CRC32C crc = new CRC32C();
                crc.update(block.array(), 0, bytes);
                if ((int) crc.getValue() != checksum)
                    throw new IOException("Normalized block checksum mismatch");
                for (int i = 0; i < records; i++) {
                    if (block.remaining() < recordBytes)
                        throw new IOException("Truncated normalized operation prefix");
                    Operation decoded =
                            new Operation(
                                    block.getLong(),
                                    block.getInt(),
                                    block.getInt(),
                                    block.getInt(),
                                    block.getInt(),
                                    block.getLong(),
                                    block.getLong(),
                                    block.getLong(),
                                    block.getLong(),
                                    block.getLong(),
                                    block.getLong(),
                                    block.getLong(),
                                    block.getLong(),
                                    version >= 3 ? block.getLong() : 0,
                                    version >= 3 ? block.getLong() : 0,
                                    version >= 4 ? block.getLong() : -1,
                                    version >= 6 ? block.getLong() : 0);
                    Operation o =
                            version >= 4
                                    ? decoded
                                    : new Operation(
                                            decoded.sourceEvent(),
                                            decoded.kind(),
                                            decoded.table(),
                                            decoded.index(),
                                            decoded.flags(),
                                            decoded.key(),
                                            decoded.version(),
                                            decoded.file(),
                                            decoded.position(),
                                            decoded.rowId(),
                                            decoded.secondarySource(),
                                            decoded.secondaryTarget(),
                                            decoded.expectedCount(),
                                            decoded.transactionId(),
                                            decoded.mutationOrdinal());
                    if (version >= 7) {
                        int expansionPosition = block.getInt(), expansionSize = block.getInt();
                        int length = block.getInt();
                        if (length < -1
                                || length > 32768
                                || length > block.remaining() - (records - i - 1) * recordBytes)
                            throw new IOException("Invalid indexed value length");
                        String indexedValue = null;
                        if (length >= 0) {
                            byte[] value = new byte[length];
                            block.get(value);
                            indexedValue =
                                    StandardCharsets.UTF_8
                                            .newDecoder()
                                            .onMalformedInput(CodingErrorAction.REPORT)
                                            .onUnmappableCharacter(CodingErrorAction.REPORT)
                                            .decode(ByteBuffer.wrap(value))
                                            .toString();
                        }
                        o =
                                new Operation(
                                        o.sourceEvent(),
                                        o.kind(),
                                        o.table(),
                                        o.index(),
                                        o.flags(),
                                        o.key(),
                                        o.version(),
                                        o.file(),
                                        o.position(),
                                        o.rowId(),
                                        o.secondarySource(),
                                        o.secondaryTarget(),
                                        o.expectedCount(),
                                        o.transactionId(),
                                        o.mutationOrdinal(),
                                        o.readTimestamp(),
                                        o.workUnitId(),
                                        indexedValue,
                                        expansionPosition,
                                        expansionSize);
                    }
                    if (o.kind < INSERT
                            || o.kind
                                    > (version == 1
                                            ? RELOCATE
                                            : version == 2 ? LOGICAL_DELETE : TRANSACTION_COMMIT)
                            || o.table < (isControl(o) ? 0 : 1)
                            || o.table > (isControl(o) ? 0 : 8)
                            || o.index < 0
                            || o.index > (version >= 7 ? 1024 : 2)
                            || o.workUnitId() < 0
                            || isControl(o) && o.workUnitId() != 0)
                        throw new IOException("Invalid normalized operation");
                    if (expandLegacy && version == 1) expandLegacyRow(o, consumer);
                    else consumer.accept(o);
                    count++;
                }
                if (block.hasRemaining()) throw new IOException("Trailing operation block bytes");
            }
            return count;
        }
    }

    public static int formatVersion(Path file) throws IOException {
        try (var channel = FileChannel.open(file)) {
            var header = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
            fill(channel, header);
            header.flip();
            int magic = header.getInt(), version = header.getInt();
            if (magic != MAGIC
                    || version < 1
                    || version > VERSION
                    || header.getInt() != recordBytes(version)
                    || header.getInt() != BLOCK_RECORDS)
                throw new IOException("Unknown normalized trace header");
            return version;
        }
    }

    private static int recordBytes(int version) {
        return version < 3
                ? 88
                : version == 3 ? 104 : version < 6 ? 112 : version == 6 ? 120 : RECORD_BYTES;
    }

    private static void fill(FileChannel channel, ByteBuffer target) throws IOException {
        while (target.hasRemaining())
            if (channel.read(target) < 0) throw new EOFException("Truncated operation trace");
    }

    private static void write(FileChannel channel, ByteBuffer source) throws IOException {
        while (source.hasRemaining()) channel.write(source);
    }
}
