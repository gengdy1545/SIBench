package org.sibench.capture;

import static org.junit.Assert.*;

import org.junit.Test;
import org.sibench.capture.IndexOperationTrace.Operation;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;

public class IndexOperationTraceTest {
    @Test
    public void variableValuesRoundTripAndInvalidUtf8OrWidthsFail() throws Exception {
        Path directory = Files.createTempDirectory("sibench-variable-values-");
        Path file = directory.resolve("trace.ops");
        try {
            try (var writer = new IndexOperationTrace.Writer(file)) {
                for (String value : new String[] {null, "", "ordinary", "\u4e2d\u6587"})
                    writer.append(
                            IndexOperationTrace.search(1, 5, 3, 0, 0, 0, 0)
                                    .withIndexedValue(value));
                assertThrows(
                        IOException.class,
                        () ->
                                writer.append(
                                        IndexOperationTrace.search(1, 5, 3, 0, 0, 0, 0)
                                                .withIndexedValue("x".repeat(32769))));
            }
            var decoded = new ArrayList<Operation>();
            IndexOperationTrace.read(file, decoded::add);
            assertNull(decoded.get(0).indexedValue());
            assertEquals("", decoded.get(1).indexedValue());
            assertEquals("\u4e2d\u6587", decoded.get(3).indexedValue());
            byte[] bytes = Files.readAllBytes(file);
            bytes[bytes.length - 1] = (byte) 0xff;
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
                            new Operation(i, 1, 3, 0, 0, i + 1, 0, 3, i, 602001 + i, 0, 0, 0));
            }
            long[] next = {0};
            assertEquals(
                    4097,
                    IndexOperationTrace.read(
                            file,
                            o -> {
                                assertEquals(next[0]++, o.sourceEvent());
                                assertEquals(o.key() - 1, o.position());
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
    public void rejectsInvalidWorkUnitIdentities() throws Exception {
        Path file = Files.createTempFile("sibench-unit-", ".ops");
        Files.delete(file);
        try (var writer = new IndexOperationTrace.Writer(file)) {
            assertThrows(
                    IOException.class,
                    () ->
                            writer.append(
                                    IndexOperationTrace.search(1, 3, 0, 1, 0, 0, 0)
                                            .withWorkUnitId(-1)));
            assertThrows(
                    IOException.class,
                    () ->
                            writer.append(
                                    IndexOperationTrace.transaction(1, 6, 1, 1, 0)
                                            .withWorkUnitId(1)));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    @Test
    public void replayPreservesRepeatedWritesAndRowIdRelocations() throws Exception {
        for (boolean rowId : new boolean[] {true, false}) {
            var model = new IndexReferenceReplay(rowId);
            model.accept(new Operation(0, 1, 3, 0, 0, 1, 0, 3, 0, 302001, 0, 0, 0));
            model.accept(new Operation(1, 3, 3, 0, 0, 1, 1, 9, 0, 302001, 0, 0, 0));
            model.accept(new Operation(2, 3, 3, 0, 0, 1, 2, 9, 1, 302001, 0, 0, 0));
            model.accept(new Operation(3, 4, 3, 0, 0, 1, 2, 11, 0, 302001, 0, 0, 0));
            assertEquals(rowId ? 0 : 1, model.relocationWrites());
            assertEquals(rowId ? 302001 : IndexReferenceReplay.locator(11, 0), model.value(3, 1));
            model.accept(new Operation(4, 4, 3, 0, 0, 1, 1, 11, 1, 302001, 0, 0, 0));
            assertEquals(
                    rowId ? 302001 : IndexReferenceReplay.locator(11, 1),
                    model.lookupPrimary(3, 1, 1).value());
        }
    }

    @Test
    public void recordsExplicitIndexesAndLegacyExpansionIsReadOnly() throws Exception {
        Path directory = Files.createTempDirectory("sibench-codec-");
        Path explicit = directory.resolve("v6.ops"), legacy = directory.resolve("v1.ops");
        var row = new Operation(12, 1, 5, 0, 0, 4, 0, 5, 6, 9, 2, 3, 0).withWorkUnitId(42);
        try {
            try (var writer = new IndexOperationTrace.Writer(explicit)) {
                org.sibench.capture.LegacyIndexFixtures.appendRow(writer, row);
            }
            List<Operation> ops = new ArrayList<>();
            assertEquals(3, IndexOperationTrace.readExpanded(explicit, ops::add));
            assertEquals(List.of(0, 1, 2), ops.stream().map(Operation::index).toList());
            assertTrue(ops.stream().allMatch(o -> o.workUnitId() == 42));
            assertTrue(
                    ops.stream()
                            .allMatch(o -> o.key() == 4 && o.position() == 6 && o.rowId() == 9));
            byte[] bytes = legacyBytes(row, 1);
            Files.write(legacy, bytes);
            ops.clear();
            assertEquals(1, IndexOperationTrace.readExpanded(legacy, ops::add));
            assertEquals(3, ops.size());
            assertArrayEquals(bytes, Files.readAllBytes(legacy));
            bytes[4] = 7;
            Files.write(legacy, bytes);
            assertThrows(
                    IOException.class, () -> IndexOperationTrace.readExpanded(legacy, o -> {}));
            Files.write(legacy, legacyBytes(row, 2));
            ops.clear();
            assertEquals(1, IndexOperationTrace.readExpanded(legacy, ops::add));
            assertEquals(1, ops.size());
        } finally {
            Files.deleteIfExists(explicit);
            Files.deleteIfExists(legacy);
            Files.delete(directory);
        }
    }

    private static byte[] legacyBytes(Operation o, int version) {
        var record = java.nio.ByteBuffer.allocate(88).order(java.nio.ByteOrder.LITTLE_ENDIAN);
        record.putLong(o.sourceEvent())
                .putInt(o.kind())
                .putInt(o.table())
                .putInt(o.index())
                .putInt(o.flags())
                .putLong(o.key())
                .putLong(o.version())
                .putLong(o.file())
                .putLong(o.position())
                .putLong(o.rowId())
                .putLong(o.secondarySource())
                .putLong(o.secondaryTarget())
                .putLong(o.expectedCount());
        var crc = new java.util.zip.CRC32C();
        crc.update(record.array(), 0, 88);
        return java.nio.ByteBuffer.allocate(16 + 12 + 88)
                .order(java.nio.ByteOrder.LITTLE_ENDIAN)
                .putInt(0x53494f50)
                .putInt(version)
                .putInt(88)
                .putInt(4096)
                .putInt(88)
                .putInt(1)
                .putInt((int) crc.getValue())
                .put(record.array())
                .array();
    }

    @Test
    public void preservesTransactionIdentityAndOrdinalAcrossIndexExpansion() throws Exception {
        Path file = Files.createTempDirectory("sibench-v6-").resolve("transaction.ops");
        try {
            try (var writer = new IndexOperationTrace.Writer(file)) {
                writer.append(IndexOperationTrace.transaction(20, 6, 20, 5, 0));
                org.sibench.capture.LegacyIndexFixtures.appendRow(
                        writer, new Operation(10, 3, 5, 0, 1, 1, 5, 9, 0, 50, 10, 20, 0, 20, 1));
                writer.append(IndexOperationTrace.transaction(20, 7, 20, 5, 1));
            }
            List<Operation> operations = new ArrayList<>();
            assertEquals(IndexOperationTrace.VERSION, IndexOperationTrace.formatVersion(file));
            assertEquals(5, IndexOperationTrace.readExpanded(file, operations::add));
            assertTrue(
                    operations.stream().allMatch(o -> o.transactionId() == 20 && o.version() == 5));
            assertTrue(operations.subList(1, 4).stream().allMatch(o -> o.mutationOrdinal() == 1));
        } finally {
            Files.deleteIfExists(file);
            Files.delete(file.getParent());
        }
    }

    @Test
    public void separatesReadTimestampAndReadsV3WithoutChangingHistoricalBytes() throws Exception {
        Path directory = Files.createTempDirectory("sibench-read-time-");
        try {
            Path current = directory.resolve("v6.ops"), old = directory.resolve("v3.ops");
            Operation search =
                    IndexOperationTrace.search(
                            1, 3, 0, 1, 105, 1, IndexOperationTrace.SNAPSHOT_SEARCH);
            try (var writer = new IndexOperationTrace.Writer(current)) {
                writer.append(search);
                assertThrows(
                        IOException.class,
                        () ->
                                writer.append(
                                        new Operation(
                                                1, 2, 3, 0, 0, 1, 105, 0, 0, 0, 0, 0, 1, 0, 0,
                                                105)));
            }
            List<Operation> actual = new ArrayList<>();
            IndexOperationTrace.read(current, actual::add);
            assertEquals(List.of(search), actual);
            assertEquals(0, actual.get(0).version());
            assertEquals(105, actual.get(0).readTimestamp());
            for (int version : new int[] {4, 5}) {
                byte[] recordBytes = Arrays.copyOfRange(Files.readAllBytes(current), 28, 140);
                var checksum = new java.util.zip.CRC32C();
                checksum.update(recordBytes, 0, recordBytes.length);
                byte[] previousBytes =
                        java.nio.ByteBuffer.allocate(140)
                                .order(java.nio.ByteOrder.LITTLE_ENDIAN)
                                .putInt(0x53494f50)
                                .putInt(version)
                                .putInt(112)
                                .putInt(4096)
                                .putInt(112)
                                .putInt(1)
                                .putInt((int) checksum.getValue())
                                .put(recordBytes)
                                .array();
                Path previous = directory.resolve("v" + version + ".ops");
                Files.write(previous, previousBytes);
                actual.clear();
                IndexOperationTrace.read(previous, actual::add);
                assertEquals(List.of(search), actual);
                assertArrayEquals(previousBytes, Files.readAllBytes(previous));
            }
            var record = java.nio.ByteBuffer.allocate(104).order(java.nio.ByteOrder.LITTLE_ENDIAN);
            record.putLong(1)
                    .putInt(2)
                    .putInt(3)
                    .putInt(0)
                    .putInt(0)
                    .putLong(1)
                    .putLong(105)
                    .putLong(0)
                    .putLong(0)
                    .putLong(0)
                    .putLong(0)
                    .putLong(0)
                    .putLong(1)
                    .putLong(0)
                    .putLong(0);
            var crc = new java.util.zip.CRC32C();
            crc.update(record.array(), 0, 104);
            byte[] bytes =
                    java.nio.ByteBuffer.allocate(16 + 12 + 104)
                            .order(java.nio.ByteOrder.LITTLE_ENDIAN)
                            .putInt(0x53494f50)
                            .putInt(3)
                            .putInt(104)
                            .putInt(4096)
                            .putInt(104)
                            .putInt(1)
                            .putInt((int) crc.getValue())
                            .put(record.array())
                            .array();
            Files.write(old, bytes);
            actual.clear();
            IndexOperationTrace.read(old, actual::add);
            assertEquals(3, IndexOperationTrace.formatVersion(old));
            assertEquals(0, actual.get(0).version());
            assertEquals(105, actual.get(0).readTimestamp());
            assertArrayEquals(bytes, Files.readAllBytes(old));
        } finally {
            try (var files = Files.walk(directory)) {
                for (Path file : files.sorted(Comparator.reverseOrder()).toList())
                    Files.delete(file);
            }
        }
    }

    @Test
    public void roundTripsTransactionLocalSearchPosition() throws Exception {
        Path file = Files.createTempFile("sibench-local-read-", ".ops");
        Files.delete(file);
        try {
            Operation search =
                    IndexOperationTrace.transactionSearch(11, 3, 0, 1, 5, 1, 20, 2, true)
                            .withWorkUnitId(128);
            try (var writer = new IndexOperationTrace.Writer(file)) {
                writer.append(search);
            }
            List<Operation> actual = new ArrayList<>();
            IndexOperationTrace.read(file, actual::add);
            assertEquals(List.of(search), actual);
            assertEquals(0, search.version());
            assertEquals(5, search.readTimestamp());
            assertEquals(IndexOperationTrace.TRANSACTION_LOCAL_SEARCH, search.flags());
            assertEquals(20, search.transactionId());
            assertEquals(2, search.mutationOrdinal());
            assertEquals(128, search.workUnitId());
        } finally {
            Files.deleteIfExists(file);
        }
    }
}
