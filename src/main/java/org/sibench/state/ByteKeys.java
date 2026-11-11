package org.sibench.state;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/** Order-preserving, terminated byte strings, including empty and prefix-related keys. */
public final class ByteKeys {
    private ByteKeys() {}

    public static byte[] encode(byte[] bytes) {
        var out = new ByteArrayOutputStream(bytes.length + 2);
        for (byte b : bytes) {
            out.write(b);
            if (b == 0) out.write(255);
        }
        out.write(0); out.write(0);
        return out.toByteArray();
    }

    public static byte[] decode(ByteBuffer input) throws IOException {
        var out = new ByteArrayOutputStream();
        while (input.hasRemaining()) {
            byte b = input.get();
            if (b != 0) out.write(b);
            else {
                if (!input.hasRemaining()) break;
                int next = input.get() & 255;
                if (next == 0) return out.toByteArray();
                if (next != 255) throw new IOException("Invalid byte-key escape");
                out.write(0);
            }
        }
        throw new IOException("Unterminated byte key");
    }

    /** Exclusive upper bound of a prefix; null when the prefix has no successor. */
    public static byte[] successor(byte[] bytes) {
        byte[] result = bytes.clone();
        for (int i = result.length - 1; i >= 0; i--) {
            if (result[i] != (byte) 255) {
                result[i]++;
                return Arrays.copyOf(result, i + 1);
            }
        }
        return null;
    }

    public static byte[] concat(byte[] a, byte[] b) {
        byte[] result = Arrays.copyOf(a, a.length + b.length);
        System.arraycopy(b, 0, result, a.length, b.length);
        return result;
    }
}
