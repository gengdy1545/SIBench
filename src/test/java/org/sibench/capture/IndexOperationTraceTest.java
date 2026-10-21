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
    public void searchesCarryAReadTimestampNotAVersion() throws Exception {
        Path file = Files.createTempFile("sibench-read-time-", ".ops");
        Files.delete(file);
        try {
            Operation search =
                    IndexOperationTrace.search(
                            1, 3, 0, 1, 105, 1, IndexOperationTrace.SNAPSHOT_SEARCH);
            try (var writer = new IndexOperationTrace.Writer(file)) {
                writer.append(search);
                // A search that also carries a version is ambiguous and must not be written.
                assertThrows(
                        IOException.class,
                        () ->
                                writer.append(
                                        new Operation(
                                                1, 2, 3, 0, 0, 1, 105, 0, 0, 0, 0, 0, 1, 0, 0,
                                                105)));
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
                writer.append(IndexOperationTrace.search(1, 3, 0, 1, 5, 1, 0));
            }
            byte[] bytes = Files.readAllBytes(file);
            for (int version : new int[] {1, 3, 6, 8}) {
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
