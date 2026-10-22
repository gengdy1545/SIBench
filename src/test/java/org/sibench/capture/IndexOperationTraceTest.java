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

    @Test
    public void rejectsEveryFormatVersionExceptTheCurrentOne() throws Exception {
        Path file = Files.createTempFile("sibench-format-", ".ops");
        Files.delete(file);
        try {
            try (var writer = new IndexOperationTrace.Writer(file)) {
                writer.append(
                        IndexOperationTrace.search(
                                1, 3, 0, Range.equal(IndexKey.ofInt(1)), 5, 1, 0));
            }
            byte[] bytes = Files.readAllBytes(file);
            for (int version : new int[] {1, 3, 6, 7, 9}) {
                java.nio.ByteBuffer.wrap(bytes)
                        .order(java.nio.ByteOrder.LITTLE_ENDIAN)
                        .putInt(4, version);
                Files.write(file, bytes);
                assertThrows(IOException.class, () -> IndexOperationTrace.read(file, o -> {}));
            }
        } finally {
            Files.deleteIfExists(file);
        }
    }
}
