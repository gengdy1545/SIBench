package org.sibench.capture;

import static org.junit.Assert.*;

import org.junit.Test;
import org.sibench.capture.IndexOperationTrace.Operation;
import org.sibench.capture.IndexOperationTrace.Range;
import org.sibench.trace.IndexKey;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;

public class IndexOperationTraceTest {
    @Test
    public void decodingChecksHandEncodedLogicalIdentityAndRejectsAValidDifferentTrace() throws Exception {
        Path file = Files.createTempDirectory("sibench-stream-digest-").resolve("trace.ops");
        try {
            var operation = IndexOperationTrace.write(1, IndexOperationTrace.INSERT, 1, 0,
                    IndexKey.ofInt(42), 0, 7, 0, 11);
            try (var writer = new IndexOperationTrace.Writer(file)) { writer.append(operation); }
            var payload = java.nio.ByteBuffer.allocate(137).order(java.nio.ByteOrder.LITTLE_ENDIAN);
            payload.putLong(1).putInt(1).putInt(1).putInt(0).putInt(0)
                    .putLong(0).putLong(7).putLong(0).putLong(11).putLong(0)
                    .putLong(0).putLong(0).putLong(-1).putLong(0).putLong(0)
                    .putInt(0).putInt(0).putInt(9).putInt(-1).putInt(-1).putInt(-1)
                    .put(IndexKey.ofInt(42).bytes());
            var crc = new java.util.zip.CRC32C(); crc.update(payload.array());
            var plain = java.nio.ByteBuffer.allocate(12 + 12 + 137).order(java.nio.ByteOrder.LITTLE_ENDIAN);
            plain.putInt(0x53494f50).putInt(128).putInt(4096)
                    .putInt(137).putInt(1).putInt((int) crc.getValue()).put(payload.array());
            String expected = HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(plain.array()));
            var decoded = new ArrayList<Operation>();
            assertEquals(1, IndexOperationTrace.Source.file(file, expected).read(decoded::add));
            assertEquals(List.of(operation), decoded);
            assertEquals(expected, IndexOperationTrace.sha256(file));
            Files.delete(file);
            try (var writer = new IndexOperationTrace.Writer(file)) {
                writer.append(operation.withWorkUnitId(1));
            }
            var mismatch = assertThrows(IOException.class,
                    () -> IndexOperationTrace.Source.file(file, expected).read(ignored -> {}));
            assertTrue(mismatch.getMessage().contains("checksum mismatch"));
        } finally {
            Files.deleteIfExists(file); Files.delete(file.getParent());
        }
    }

    private static IndexKey text(String value) throws IOException {
        return IndexKey.ofText(value);
    }

    @Test
    public void encodedKeysAndRangesRoundTripAndDamagedKeysFail() throws Exception {
        Path directory = Files.createTempDirectory("sibench-variable-values-");
        Path file = directory.resolve("trace.ops");
        try {
            var written = new ArrayList<Operation>();
            written.add(
                    IndexOperationTrace.write(
                                    1,
                                    IndexOperationTrace.INSERT,
                                    5,
                                    0,
                                    text("user1"),
                                    0,
                                    1,
                                    0,
                                    1)
                            .withIndex(3, IndexKey.NULL, IndexOperationTrace.INSERT));
            for (String value : new String[] {"", "ordinary", "中文"})
                written.add(
                        IndexOperationTrace.search(
                                2, 5, 3, Range.equal(text(value)), 0, 0, 0));
            written.add(
                    IndexOperationTrace.search(
                            3,
                            5,
                            0,
                            new Range(IndexKey.ofInt(-5), false, null, false, true, 2),
                            4,
                            2,
                            0));
            written.add(
                    IndexOperationTrace.search(
                            4, 5, 0, new Range(null, false, null, false, false, 0), 4, 9, 0));
            try (var writer = new IndexOperationTrace.Writer(file)) {
                for (Operation operation : written) writer.append(operation);
            }
            var decoded = new ArrayList<Operation>();
            IndexOperationTrace.read(file, decoded::add);
            assertEquals(written, decoded);
            assertTrue(decoded.get(0).indexedValue().isNull());
            assertTrue(decoded.get(1).range().point());
            Range range = decoded.get(4).range();
            assertEquals(IndexKey.ofInt(-5), range.lower());
            assertFalse(range.lowerInclusive());
            assertTrue(range.descending());
            assertEquals(2, range.limit());
            assertNull(decoded.get(5).range().lower());
            byte[] bytes = Files.readAllBytes(file);
            // Remove the terminator of the last text key so it no longer self-delimits.
            byte[] text = text("中文").bytes();
            int at = -1;
            for (int i = 0; i + text.length <= bytes.length; i++)
                if (Arrays.equals(bytes, i, i + text.length, text, 0, text.length)) at = i;
            bytes[at + text.length - 1] = (byte) 'x';
            var crc = new java.util.zip.CRC32C();
            crc.update(bytes, 28, bytes.length - 28);
            java.nio.ByteBuffer.wrap(bytes)
                    .order(java.nio.ByteOrder.LITTLE_ENDIAN)
                    .putInt(24, (int) crc.getValue());
            Files.write(file, bytes);
            assertThrows(IOException.class, () -> IndexOperationTrace.read(file, ignored -> {}));
        } finally {
            Files.deleteIfExists(file);
            Files.delete(directory);
        }
    }

    @Test
    public void roundTripsBlockBoundariesAndRejectsDamage() throws Exception {
        Path directory = Files.createTempDirectory("sibench-ops-");
        Path file = directory.resolve("trace.bin");
        try {
            try (var writer = new IndexOperationTrace.Writer(file)) {
                for (int i = 0; i < 4097; i++)
                    writer.append(
                            IndexOperationTrace.write(
                                    i,
                                    IndexOperationTrace.INSERT,
                                    3,
                                    0,
                                    IndexKey.ofInt(i + 1),
                                    0,
                                    3,
                                    i,
                                    602001 + i));
            }
            long[] next = {0};
            assertEquals(
                    4097,
                    IndexOperationTrace.read(
                            file,
                            o -> {
                                assertEquals(next[0]++, o.sourceEvent());
                                assertEquals(o.key().intValue() - 1, o.position());
                            }));
            byte[] bytes = Files.readAllBytes(file);
            bytes[50] ^= 1;
            Files.write(file, bytes);
            assertThrows(IOException.class, () -> IndexOperationTrace.read(file, o -> {}));
            Files.write(file, Arrays.copyOf(bytes, 25));
            assertThrows(IOException.class, () -> IndexOperationTrace.read(file, o -> {}));
        } finally {
            Files.deleteIfExists(file);
            Files.delete(directory);
        }
    }

    @Test
    public void rejectsInvalidIdentitiesAndAmbiguousFields() throws Exception {
        Path file = Files.createTempFile("sibench-unit-", ".ops");
        Files.delete(file);
        Range point = Range.equal(IndexKey.ofInt(1));
        try (var writer = new IndexOperationTrace.Writer(file)) {
            for (Operation invalid :
                    List.of(
                            IndexOperationTrace.search(1, 3, 0, point, 0, 0, 0).withWorkUnitId(-1),
                            IndexOperationTrace.transaction(1, 6, 1, 1, 0).withWorkUnitId(1),
                            // A limited search returns exactly its limit.
                            IndexOperationTrace.search(
                                    1, 3, 0, new Range(null, false, null, false, false, 3), 0, 2, 0),
                            // Inclusive flags need a bound.
                            IndexOperationTrace.search(
                                    1, 3, 0, new Range(null, true, null, false, false, 0), 0, 0, 0),
                            // A secondary write carries its indexed value; a primary one does not.
                            IndexOperationTrace.write(
                                            1, IndexOperationTrace.INSERT, 3, 0, IndexKey.ofInt(1), 0,
                                            1, 0, 1)
                                    .withIndex(1, null, IndexOperationTrace.INSERT),
                            IndexOperationTrace.write(
                                    1, IndexOperationTrace.INSERT, 3, 0, null, 0, 1, 0, 1),
                            IndexOperationTrace.write(
                                    1, IndexOperationTrace.INSERT, 70000, 0, IndexKey.ofInt(1), 0,
                                    1, 0, 1)))
                assertThrows(IOException.class, () -> writer.append(invalid));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    @Test
    public void searchesCarryAReadTimestampNotAVersion() throws Exception {
        Path file = Files.createTempFile("sibench-read-time-", ".ops");
        Files.delete(file);
        try {
            Operation search =
                    IndexOperationTrace.search(
                            1,
                            3,
                            0,
                            Range.equal(IndexKey.ofInt(1)),
                            105,
                            1,
                            IndexOperationTrace.SNAPSHOT_SEARCH);
            try (var writer = new IndexOperationTrace.Writer(file)) {
                writer.append(search);
                // A search that also carries a version is ambiguous and must not be written.
                assertThrows(
                        IOException.class,
                        () ->
                                writer.append(
                                        new Operation(
                                                1, 2, 3, 0, 0, null, null, 105, 0, 0, 0, 1, 0, 0,
                                                105, 0, 0, 0, IndexKey.ofInt(1), IndexKey.ofInt(1),
                                                0)));
            }
            List<Operation> actual = new ArrayList<>();
            IndexOperationTrace.read(file, actual::add);
            assertEquals(List.of(search), actual);
            assertEquals(0, actual.get(0).version());
            assertEquals(105, actual.get(0).readTimestamp());
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /** Pinned searches and group members round-trip; malformed combinations never get written. */
    @Test
    public void pinnedSearchesAndGroupsRoundTripAndMalformedOnesFail() throws Exception {
        Path file = Files.createTempFile("sibench-groups-", ".ops");
        Files.delete(file);
        Range point = Range.equal(IndexKey.ofInt(6));
        Range interval = new Range(IndexKey.ofInt(1), true, IndexKey.ofInt(4), false, false, 0);
        int intersect = IndexOperationTrace.INTERSECT_GROUP, union = IndexOperationTrace.UNION_GROUP;
        try {
            var written =
                    List.of(
                            IndexOperationTrace.search(1, 5, 1, point, IndexKey.ofInt(9), 3, 1, 0, 0, 0),
                            IndexOperationTrace.search(2, 5, 1, interval, null, 3, 4, intersect, 0, 2),
                            IndexOperationTrace.search(2, 5, 2, point, null, 3, 4, intersect, 1, 2),
                            IndexOperationTrace.search(3, 5, 1, point, null, 3, 2, union, 0, 2),
                            IndexOperationTrace.search(3, 5, 1, Range.equal(IndexKey.ofInt(8)), null, 3, 2, union, 1, 2));
            try (var writer = new IndexOperationTrace.Writer(file)) {
                for (Operation operation : written) writer.append(operation);
            }
            var decoded = new ArrayList<Operation>();
            IndexOperationTrace.read(file, decoded::add);
            assertEquals(written, decoded);
            assertTrue(decoded.get(1).grouped());
            assertFalse(decoded.get(0).grouped());
        } finally {
            Files.deleteIfExists(file);
        }
        for (Operation invalid :
                List.of(
                        // A pinned search names one secondary entry: a point, count 1, no PK index.
                        IndexOperationTrace.search(1, 5, 1, interval, IndexKey.ofInt(9), 3, 1, 0, 0, 0),
                        IndexOperationTrace.search(1, 5, 1, point, IndexKey.ofInt(9), 3, 2, 0, 0, 0),
                        IndexOperationTrace.search(1, 5, 0, point, IndexKey.ofInt(9), 3, 1, 0, 0, 0),
                        // A group has at least two members, a valid position, no limit or pin.
                        IndexOperationTrace.search(2, 5, 1, interval, null, 3, 4, intersect, 0, 1),
                        IndexOperationTrace.search(2, 5, 1, interval, null, 3, 4, intersect, 2, 2),
                        IndexOperationTrace.search(2, 5, 1, interval, null, 3, 4, intersect | union, 0, 2),
                        IndexOperationTrace.search(2, 5, 1, point, IndexKey.ofInt(9), 3, 1, union, 0, 2),
                        IndexOperationTrace.search(
                                2, 5, 1, new Range(null, false, null, false, false, 4), null, 3, 4, union, 0, 2),
                        // Only group members carry positions.
                        IndexOperationTrace.search(2, 5, 1, interval, null, 3, 4, 0, 0, 2))) {
            Path target = Files.createTempFile("sibench-invalid-", ".ops");
            Files.delete(target);
            try (var writer = new IndexOperationTrace.Writer(target)) {
                assertThrows(IOException.class, () -> writer.append(invalid));
            } finally {
                Files.deleteIfExists(target);
            }
        }
    }

    @Test
    public void rejectsUnknownFileTypesAndRecordLayouts() throws Exception {
        Path file = Files.createTempFile("sibench-format-", ".ops");
        Files.delete(file);
        try {
            try (var writer = new IndexOperationTrace.Writer(file)) {
                writer.append(
                        IndexOperationTrace.search(
                                1, 3, 0, Range.equal(IndexKey.ofInt(1)), 5, 1, 0));
            }
            byte[] bytes = Files.readAllBytes(file);
            var header = java.nio.ByteBuffer.wrap(bytes).order(java.nio.ByteOrder.LITTLE_ENDIAN);
            assertEquals(0x53494f5a, header.getInt());
            assertEquals(128, header.getInt());
            assertEquals(4096, header.getInt());
            for (int offset : new int[] {0, 4, 8}) {
                byte[] invalid = bytes.clone();
                java.nio.ByteBuffer.wrap(invalid)
                        .order(java.nio.ByteOrder.LITTLE_ENDIAN)
                        .putInt(offset, 0);
                Files.write(file, invalid);
                assertThrows(IOException.class, () -> IndexOperationTrace.read(file, o -> {}));
            }
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /**
     * A trace is stored in zstd blocks; its digest is that of the plain stream, so it equals the
     * digest of the same operations never written, and a damaged block fails to read.
     */
    @Test
    public void compressedBlocksReadBackAndKeepThePlainDigest() throws Exception {
        Path directory = Files.createTempDirectory("ops");
        Path file = directory.resolve("workload.ops");
        var operations = new ArrayList<Operation>();
        int count = IndexOperationTrace.BLOCK_RECORDS * 2 + 17;
        for (int i = 0; i < count; i++)
            operations.add(
                    IndexOperationTrace.write(i + 1, IndexOperationTrace.INSERT, 1, 0, IndexKey.ofInt(i), 0, 1, i, i + 1)
                            .withWorkUnitId(1 + i / IndexOperationTrace.BLOCK_RECORDS));
        var stored = new IndexOperationTrace.Writer(file);
        var digest = new IndexOperationTrace.Writer(null);
        try (stored; digest) {
            for (Operation operation : operations) {
                stored.append(operation);
                digest.append(operation);
            }
        }
        assertEquals(digest.sha256(), stored.sha256());
        assertEquals(stored.sha256(), IndexOperationTrace.sha256(file));
        var read = new ArrayList<Operation>();
        assertEquals(count, IndexOperationTrace.read(file, read::add));
        assertEquals(operations, read);
        // Records repeat little, so blocks compress well below the plain record size.
        assertTrue(Files.size(file) < (long) count * IndexOperationTrace.RECORD_BYTES / 2);
        byte[] bytes = Files.readAllBytes(file);
        bytes[bytes.length - 3] ^= 0x5a;
        Files.write(file, bytes);
        assertThrows(IOException.class, () -> IndexOperationTrace.read(file, operation -> {}));
    }
}
