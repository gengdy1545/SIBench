package org.sibench.capture;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32C;

/** PoC normalized operation blocks, separate from the raw PG access projection. */
public final class IndexOperationTrace {
    public static final int INSERT = 1, SEARCH = 2, UPSERT = 3, RELOCATE = 4, LOGICAL_DELETE = 5;
    public static final int TRANSACTION_BEGIN = 6, TRANSACTION_COMMIT = 7;
    public static final int BUSINESS_INSERT = 1, BUSINESS_UPDATE = 2;
    public static final int SNAPSHOT_SEARCH = 1, DERIVED_SEARCH = 2;
    public static final int RECORD_BYTES = 112, BLOCK_RECORDS = 4096;
    public static final int VERSION = 4;
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
            long readTimestamp) {
        public Operation(
                long sourceEvent, int kind, int table, int index, int flags, long key,
                long version, long file, long position, long rowId, long secondarySource,
                long secondaryTarget, long expectedCount, long transactionId, long mutationOrdinal) {
            this(sourceEvent, kind, table, index, flags, key, kind == SEARCH ? 0 : version,
                    file, position, rowId, secondarySource, secondaryTarget, expectedCount,
                    transactionId, mutationOrdinal, kind == SEARCH ? version : -1);
        }

        public Operation(
                long sourceEvent, int kind, int table, int index, int flags, long key,
                long version, long file, long position, long rowId, long secondarySource,
                long secondaryTarget, long expectedCount) {
            this(sourceEvent, kind, table, index, flags, key, version, file, position, rowId,
                    secondarySource, secondaryTarget, expectedCount, 0, 0);
        }
    }

    public static Operation search(
            long source, int table, int index, long key, long readTimestamp, long count, int flags) {
        return new Operation(source, SEARCH, table, index, flags, key, 0,
                0, 0, 0, 0, 0, count, 0, 0, readTimestamp);
    }

    public static boolean isMutation(Operation o) {
        return o.kind() == UPSERT || o.kind() == LOGICAL_DELETE;
    }

    public static boolean isControl(Operation o) {
        return o.kind() == TRANSACTION_BEGIN || o.kind() == TRANSACTION_COMMIT;
    }

    public static Operation transaction(long source, int kind, long id, long version, long rows) {
        return new Operation(source, kind, 0, 0, 0, 0, version, 0, 0, 0, 0, 0, rows, id, 0);
    }

    public static void expandRow(Operation row, Consumer consumer) throws IOException {
        consumer.accept(row);
        if (row.table() == 5 && row.index() == 0 && row.kind() != SEARCH) {
            for (int index = 1; index <= 2; index++)
                consumer.accept(
                        new Operation(
                                row.sourceEvent(), row.kind(), row.table(), index, row.flags(),
                                row.key(), row.version(), row.file(), row.position(), row.rowId(),
                                row.secondarySource(), row.secondaryTarget(), row.expectedCount(),
                                row.transactionId(), row.mutationOrdinal()));
        }
    }

    @FunctionalInterface
    public interface Consumer {
        void accept(Operation operation) throws IOException;
    }

    public static final class Writer implements Closeable {
        private final FileChannel channel;
        private final ByteBuffer block =
                ByteBuffer.allocate(RECORD_BYTES * BLOCK_RECORDS).order(ByteOrder.LITTLE_ENDIAN);
        private long count;

        public Writer(Path file) throws IOException {
            channel =
                    FileChannel.open(file, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
            ByteBuffer header = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
            header.putInt(MAGIC).putInt(VERSION).putInt(RECORD_BYTES).putInt(BLOCK_RECORDS).flip();
            write(channel, header);
        }

        public void appendRow(Operation row) throws IOException {
            if (row.index() != 0 || row.kind() == SEARCH)
                throw new IllegalArgumentException("Expected primary row mutation");
            expandRow(row, this::append);
        }

        public void append(Operation o) throws IOException {
            if ((o.kind() == SEARCH && (o.version() != 0 || o.readTimestamp() < 0))
                    || (o.kind() != SEARCH && o.readTimestamp() != -1))
                throw new IOException("Invalid CommitVersion/ReadTimestamp fields");
            block.putLong(o.sourceEvent)
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
                    .putLong(o.readTimestamp);
            count++;
            if (!block.hasRemaining()) flush();
        }

        public long count() {
            return count;
        }

        private void flush() throws IOException {
            if (block.position() == 0) return;
            int length = block.position();
            CRC32C crc = new CRC32C();
            crc.update(block.array(), 0, length);
            ByteBuffer header = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN);
            header.putInt(length).putInt(length / RECORD_BYTES).putInt((int) crc.getValue()).flip();
            write(channel, header);
            block.flip();
            write(channel, block);
            block.clear();
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

    private static long read(Path file, Consumer consumer, boolean expandLegacy) throws IOException {
        try (FileChannel channel = FileChannel.open(file)) {
            ByteBuffer header = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
            fill(channel, header);
            header.flip();
            int magic = header.getInt(), version = header.getInt();
            int recordBytes = recordBytes(version);
            if (magic != MAGIC || version < 1 || version > VERSION
                    || header.getInt() != recordBytes || header.getInt() != BLOCK_RECORDS)
                throw new IOException("Unknown normalized trace header");
            ByteBuffer block =
                    ByteBuffer.allocate(recordBytes * BLOCK_RECORDS)
                            .order(ByteOrder.LITTLE_ENDIAN);
            long count = 0;
            while (channel.position() < channel.size()) {
                header.clear().limit(12);
                fill(channel, header);
                header.flip();
                int bytes = header.getInt(), records = header.getInt(), checksum = header.getInt();
                if (bytes <= 0
                        || bytes > block.capacity()
                        || bytes % recordBytes != 0
                        || records != bytes / recordBytes)
                    throw new IOException("Invalid normalized block length");
                block.clear().limit(bytes);
                fill(channel, block);
                block.flip();
                CRC32C crc = new CRC32C();
                crc.update(block.array(), 0, bytes);
                if ((int) crc.getValue() != checksum)
                    throw new IOException("Normalized block checksum mismatch");
                for (int i = 0; i < records; i++) {
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
                                    version >= 4 ? block.getLong() : -1);
                    Operation o = version >= 4 ? decoded : new Operation(
                            decoded.sourceEvent(), decoded.kind(), decoded.table(), decoded.index(),
                            decoded.flags(), decoded.key(), decoded.version(), decoded.file(),
                            decoded.position(), decoded.rowId(), decoded.secondarySource(),
                            decoded.secondaryTarget(), decoded.expectedCount(), decoded.transactionId(),
                            decoded.mutationOrdinal());
                    if (o.kind < INSERT
                            || o.kind > (version == 1 ? RELOCATE : version == 2 ? LOGICAL_DELETE : TRANSACTION_COMMIT)
                            || o.table < (isControl(o) ? 0 : 1)
                            || o.table > (isControl(o) ? 0 : 8)
                            || o.index < 0
                            || o.index > 2) throw new IOException("Invalid normalized operation");
                    if (expandLegacy && version == 1) expandRow(o, consumer);
                    else consumer.accept(o);
                    count++;
                }
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
            if (magic != MAGIC || version < 1 || version > VERSION
                    || header.getInt() != recordBytes(version)
                    || header.getInt() != BLOCK_RECORDS)
                throw new IOException("Unknown normalized trace header");
            return version;
        }
    }

    private static int recordBytes(int version) {
        return version < 3 ? 88 : version == 3 ? 104 : RECORD_BYTES;
    }

    private static void fill(FileChannel channel, ByteBuffer target) throws IOException {
        while (target.hasRemaining())
            if (channel.read(target) < 0) throw new EOFException("Truncated operation trace");
    }

    private static void write(FileChannel channel, ByteBuffer source) throws IOException {
        while (source.hasRemaining()) channel.write(source);
    }
}
