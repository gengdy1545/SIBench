package org.sibench.capture;

import static org.junit.Assert.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.junit.Test;

public class PgTextCopyRowsTest {
    private static byte[] fixture() throws IOException {
        var bytes = new ByteArrayOutputStream();
        var out = new DataOutputStream(bytes);
        out.write(new byte[] {80, 71, 67, 79, 80, 89, 10, (byte) 255, 13, 10, 0});
        out.writeInt(0); out.writeInt(0);
        for (String[] row : new String[][] {{"user0", null, ""}, {"user1", "中文🙂", "\t\n\\"}}) {
            out.writeShort(row.length);
            for (String value : row) {
                byte[] text = value == null ? null : value.getBytes(StandardCharsets.UTF_8);
                out.writeInt(text == null ? -1 : text.length);
                if (text != null) out.write(text);
            }
        }
        out.writeShort(-1);
        return bytes.toByteArray();
    }

    private static void checkRows(InputStream stream) throws IOException {
        var rows = new PgTextCopyRows(stream, 3);
        assertArrayEquals(new String[] {"user0", null, ""}, rows.next());
        assertArrayEquals(new String[] {"user1", "中文🙂", "\t\n\\"}, rows.next());
        assertNull(rows.next()); assertNull(rows.next());
    }

    @Test public void preservesNullEmptyUnicodeAndControlCharacters() throws Exception {
        checkRows(new ByteArrayInputStream(fixture()));
    }

    @Test public void acceptsFieldsSplitAcrossTransportReads() throws Exception {
        checkRows(new ByteArrayInputStream(fixture()) {
            @Override public synchronized int read(byte[] bytes, int offset, int length) {
                return super.read(bytes, offset, Math.min(1, length));
            }
        });
    }

    private static void consume(byte[] data) throws IOException {
        var rows = new PgTextCopyRows(new ByteArrayInputStream(data), 3);
        while (rows.next() != null) {}
    }

    @Test public void rejectsEveryTruncatedPrefixIncludingMissingTrailer() throws Exception {
        byte[] data = fixture();
        for (int end = 0; end < data.length; end++) {
            byte[] prefix = Arrays.copyOf(data, end);
            assertThrows("prefix " + end, IOException.class, () -> consume(prefix));
        }
    }

    @Test public void rejectsUnsupportedHeadersWrongShapeAndInvalidLengths() throws Exception {
        for (int offset : new int[] {0, 14, 18, 20, 21}) {
            byte[] bad = fixture();
            // Signature, flags, extension, field count, negative length respectively.
            bad[offset] = (byte) (offset == 21 ? 255 : bad[offset] + 1);
            assertThrows("offset " + offset, IOException.class, () -> consume(bad));
        }
        byte[] trailing = Arrays.copyOf(fixture(), fixture().length + 1);
        assertThrows(IOException.class, () -> consume(trailing));
    }
}
