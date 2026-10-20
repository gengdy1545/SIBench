package org.sibench.capture;

import static org.junit.Assert.*;

import org.junit.Test;

import java.io.IOException;
import java.nio.*;
import java.nio.file.*;
import java.util.zip.CRC32C;

public class PgTraceBlocksTest {
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
