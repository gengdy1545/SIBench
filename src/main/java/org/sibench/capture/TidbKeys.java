package org.sibench.capture;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;

/**
 * Decodes TiDB 8.5 table keys: {@code t{table}_r{handle}} record keys and {@code
 * t{table}_i{index}{values}{handle}} index keys, whose components use TiDB's memcomparable
 * codec. Only NULL, bytes, signed and unsigned integer components are supported; others fail.
 */
final class TidbKeys {
    static final int NIL = 0x00, BYTES = 0x01, INT = 0x03, UINT = 0x04, MAX_FLAG = 0xFA;
    private static final int GROUP = 8, MARKER = 0xFF;

    private TidbKeys() {}

    /** A decoded key prefix: the table, the index (0 for a record key) and complete components. */
    record Parsed(long table, long index, List<Object> components, int rest) {
        /** Bytes after the last complete component; 0 for a key that ends on a component. */
        boolean exact() {
            return rest == 0;
        }
    }

    static byte[] hex(String text) {
        return HexFormat.of().parseHex(text);
    }

    /**
     * Parses a key of one table. {@code intHandle} selects the record-key layout of an integer
     * handle, which has no type flag. Returns null if the key is outside any table's record or
     * index space, e.g. a range bound past the last table key.
     */
    static Parsed parse(byte[] key, boolean intHandle) throws IOException {
        if (key.length < 11 || key[0] != 't' || key[9] != '_') return null;
        long table = decodeInt(key, 1);
        int position = 11;
        long index;
        if (key[10] == 'r') {
            index = 0;
        } else if (key[10] == 'i') {
            if (key.length < 19) return new Parsed(table, -1, List.of(), key.length - 11);
            index = decodeInt(key, 11);
            position = 19;
        } else {
            return null;
        }
        var components = new ArrayList<Object>();
        if (index == 0 && intHandle) {
            if (key.length >= position + 8) {
                components.add(decodeInt(key, position));
                position += 8;
            }
            return new Parsed(table, 0, components, key.length - position);
        }
        while (position < key.length) {
            int next = component(key, position, components);
            if (next < 0) break;
            position = next;
        }
        return new Parsed(table, index, components, key.length - position);
    }

    /** Decodes one flagged component at {@code at}; returns the next position, or -1 if incomplete. */
    private static int component(byte[] key, int at, List<Object> out) throws IOException {
        int flag = key[at] & 0xFF;
        if (flag >= MAX_FLAG) return -1; // a bound past every component value
        switch (flag) {
            case NIL -> {
                out.add(null);
                return at + 1;
            }
            case INT, UINT -> {
                if (key.length < at + 9) return -1;
                long raw = decodeInt(key, at + 1);
                if (flag == UINT) {
                    raw ^= Long.MIN_VALUE;
                    if (raw < 0) throw new IOException("Unsigned key component exceeds int64");
                }
                out.add(raw);
                return at + 9;
            }
            case BYTES -> {
                var value = new java.io.ByteArrayOutputStream();
                int position = at + 1;
                while (true) {
                    if (key.length < position + GROUP + 1) return -1;
                    int marker = key[position + GROUP] & 0xFF;
                    int pad = MARKER - marker;
                    if (pad < 0 || pad > GROUP) throw new IOException("Invalid bytes group marker");
                    value.write(key, position, GROUP - pad);
                    for (int i = GROUP - pad; i < GROUP; i++)
                        if (key[position + i] != 0) throw new IOException("Invalid bytes group padding");
                    position += GROUP + 1;
                    if (pad != 0) break;
                }
                out.add(value.toByteArray());
                return position;
            }
            default -> throw new IOException("Unsupported TiDB key component flag " + flag);
        }
    }

    /** TiDB's comparable int64: big-endian with the sign bit flipped. */
    static long decodeInt(byte[] key, int at) {
        long value = 0;
        for (int i = 0; i < 8; i++) value = value << 8 | (key[at + i] & 0xFF);
        return value ^ Long.MIN_VALUE;
    }

    static String text(Object component) {
        return component instanceof byte[] bytes ? new String(bytes, StandardCharsets.UTF_8) : String.valueOf(component);
    }

    static boolean same(Object a, Object b) {
        return a instanceof byte[] x && b instanceof byte[] y ? Arrays.equals(x, y) : java.util.Objects.equals(a, b);
    }
}
