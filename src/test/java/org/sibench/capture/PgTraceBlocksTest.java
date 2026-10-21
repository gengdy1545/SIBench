package org.sibench.capture;

import static org.junit.Assert.*;

import org.junit.Test;

import java.io.IOException;
import java.nio.*;
import java.nio.file.*;
import java.util.zip.CRC32C;

public class PgTraceBlocksTest {
    @Test
    public void packsPlanBindingsWithoutChangingExistingEventCodes() throws Exception {
        Path directory = Files.createTempDirectory("sibench-plan-test-");
        Path raw = Files.createDirectory(directory.resolve("raw"));
        String event =
                "{\"event\":\"plan_scan\",\"pid\":42,\"req\":1,\"depth\":1,\"relation\":\"t_pkey\","
                        + "\"scan\":\"0x1\",\"plan_node\":0,\"kind\":\"index\"}\n";
        try {
            Files.writeString(raw.resolve("pg-42.jsonl"), event);
            PgTraceBlocks.compile(raw, directory.resolve("trace"));
            assertEquals(1, PgTraceBlocks.decode(directory.resolve("trace/access.bin")));
            assertEquals(event, Files.readString(directory.resolve("trace/payload.jsonl")));
        } finally {
            try (var paths = Files.walk(directory)) {
                for (Path path : paths.sorted(java.util.Comparator.reverseOrder()).toList())
                    Files.delete(path);
            }
        }
    }

    @Test
    public void decodesAndRejectsDamage() throws Exception {
        Path file = Files.createTempFile("sibench-block-test-", ".bin");
        try {
            byte[] record = new byte[64];
            ByteBuffer.wrap(record).order(ByteOrder.LITTLE_ENDIAN).putLong(1).putInt(3);
            CRC32C crc = new CRC32C();
            crc.update(record, 0, record.length);
            ByteBuffer bytes = ByteBuffer.allocate(92).order(ByteOrder.LITTLE_ENDIAN);
            bytes.putInt(0x53494254)
                    .putInt(1)
                    .putInt(64)
                    .putInt(4096)
                    .putInt(64)
                    .putInt(1)
                    .putInt((int) crc.getValue())
                    .put(record);
            Files.write(file, bytes.array());
            assertEquals(1, PgTraceBlocks.decode(file));
            byte[] corrupt = bytes.array().clone();
            corrupt[40] ^= 1;
            Files.write(file, corrupt);
            assertThrows(IOException.class, () -> PgTraceBlocks.decode(file));
            Files.write(file, java.util.Arrays.copyOf(bytes.array(), 91));
            assertThrows(IOException.class, () -> PgTraceBlocks.decode(file));
            corrupt = bytes.array().clone();
            corrupt[4] = 2;
            Files.write(file, corrupt);
            assertThrows(IOException.class, () -> PgTraceBlocks.decode(file));
        } finally {
            Files.deleteIfExists(file);
        }
    }
}
