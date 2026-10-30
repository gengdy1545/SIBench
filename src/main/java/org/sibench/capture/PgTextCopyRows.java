package org.sibench.capture;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/** PG 14 binary COPY for the preflight-checked YCSB text columns; the caller owns the stream. */
final class PgTextCopyRows implements PgBaselineExport.Rows {
    private static final byte[] SIGNATURE = {80, 71, 67, 79, 80, 89, 10, (byte) 255, 13, 10, 0};
    private final DataInputStream input;
    private final int columns;
    private boolean ended;

    PgTextCopyRows(InputStream input, int columns) throws IOException {
        this.input = new DataInputStream(input);
        this.columns = columns;
        if (!Arrays.equals(this.input.readNBytes(11), SIGNATURE)
                || this.input.readInt() != 0 || this.input.readInt() != 0)
            throw new IOException("Unsupported PG binary COPY header");
    }

    @Override
    public String[] next() throws IOException {
        if (ended) return null;
        int count = input.readShort();
        if (count == -1) {
            if (input.read() != -1) throw new IOException("Trailing PG binary COPY data");
            ended = true;
            return null;
        }
        if (count != columns) throw new IOException("PG binary COPY column count changed");
        String[] row = new String[columns];
        for (int column = 0; column < columns; column++) {
            int length = input.readInt();
            if (length == -1) continue;
            if (length < 0 || length >= 1 << 30)
                throw new IOException("Invalid PG binary COPY text length");
            byte[] bytes = input.readNBytes(length);
            if (bytes.length != length) throw new EOFException("Truncated PG binary COPY value");
            row[column] = new String(bytes, StandardCharsets.UTF_8);
        }
        return row;
    }
}
