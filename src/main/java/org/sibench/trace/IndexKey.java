package org.sibench.trace;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.HexFormat;

/**
 * One order-preserving key component. Unsigned lexicographic order of the bytes is the PostgreSQL
 * btree order of the source value under the default ascending, NULLS LAST operator class (text
 * only under the C collation), and every component is self-delimiting, so concatenated components
 * compare as the tuple they encode.
 *
 * <pre>
 * NULL       0x02
 * integer    0x01, int64 big-endian with the sign bit flipped (int2/int4/int8 alike)
 * timestamp  0x01, microseconds since 1970-01-01 as above; -infinity/infinity are the extremes
 * text       0x01, UTF-8 bytes, 0x00 (PostgreSQL text never contains NUL); CHAR(n) drops
 *            trailing spaces first, as bpchar comparison does
 * </pre>
 */
public final class IndexKey implements Comparable<IndexKey> {
    public static final int MAX_BYTES = 32768;
    private static final byte VALUE = 0x01, NULL_TAG = 0x02;
    public static final IndexKey NULL = new IndexKey(new byte[] {NULL_TAG});

    private final byte[] bytes;

    private IndexKey(byte[] bytes) {
        this.bytes = bytes;
    }

    public static IndexKey ofInt(long value) {
        return fixed(value ^ Long.MIN_VALUE);
    }

    public static IndexKey ofTimestamp(LocalDateTime value) throws IOException {
        if (value.getNano() % 1000 != 0)
            throw new IOException("Timestamp is finer than PostgreSQL microseconds: " + value);
        try {
            long micros =
                    Math.addExact(
                            Math.multiplyExact(value.toEpochSecond(ZoneOffset.UTC), 1_000_000L),
                            value.getNano() / 1000);
            // PostgreSQL reserves the int64 extremes for -infinity and infinity.
            if (micros == Long.MIN_VALUE || micros == Long.MAX_VALUE)
                throw new IOException("Timestamp collides with an infinity: " + value);
            return fixed(micros ^ Long.MIN_VALUE);
        } catch (ArithmeticException error) {
            throw new IOException("Timestamp outside int64 microseconds: " + value, error);
        }
    }

    public static IndexKey ofText(String value) throws IOException {
        if (value.indexOf('\0') >= 0) throw new IOException("Text key contains NUL");
        byte[] text = value.getBytes(StandardCharsets.UTF_8);
        byte[] bytes = new byte[text.length + 2];
        bytes[0] = VALUE;
        System.arraycopy(text, 0, bytes, 1, text.length);
        return checked(bytes);
    }

    private static IndexKey fixed(long sortable) {
        return new IndexKey(ByteBuffer.allocate(9).put(VALUE).putLong(sortable).array());
    }

    /**
     * Encodes PostgreSQL text output (scan keys, row images and baseline values all use it) of a
     * value of {@code type}: int, timestamp, date (compared as midnight with a timestamp column),
     * char(n)/bpchar, varchar or text. A null value is {@link #NULL}.
     */
    private static final java.util.regex.Pattern CHAR = java.util.regex.Pattern.compile("char\\([0-9]+\\)");

    public static IndexKey parse(String type, String text) throws IOException {
        if (text == null) return NULL;
        try {
            if (type.equals("int")) return ofInt(Long.parseLong(text));
            if (type.equals("timestamp")) {
                if (text.equals("infinity")) return fixed(Long.MAX_VALUE ^ Long.MIN_VALUE);
                if (text.equals("-infinity")) return fixed(0);
                return ofTimestamp(LocalDateTime.parse(text.replace(' ', 'T')));
            }
            if (type.equals("date")) return ofTimestamp(LocalDate.parse(text).atStartOfDay());
            if (type.equals("bpchar") || CHAR.matcher(type).matches()) {
                int end = text.length();
                while (end > 0 && text.charAt(end - 1) == ' ') end--;
                return ofText(text.substring(0, end));
            }
            if (type.equals("varchar") || type.equals("text")) return ofText(text);
        } catch (RuntimeException error) {
            throw new IOException("Invalid " + type + " key value: " + text, error);
        }
        throw new IOException("Unsupported key type: " + type);
    }

    /** Reads bytes back from a trace; malformed components are rejected. */
    public static IndexKey decode(byte[] bytes) throws IOException {
        if (bytes.length == 0) throw new IOException("Empty key component");
        return checked(bytes.clone());
    }

    private static IndexKey checked(byte[] bytes) throws IOException {
        if (bytes.length > MAX_BYTES) throw new IOException("Key exceeds " + MAX_BYTES + " bytes");
        boolean valid =
                bytes[0] == NULL_TAG
                        ? bytes.length == 1
                        : bytes[0] == VALUE && (bytes.length == 9 || terminatedText(bytes));
        if (!valid) throw new IOException("Malformed key component");
        return new IndexKey(bytes);
    }

    private static boolean terminatedText(byte[] bytes) {
        if (bytes.length < 2 || bytes[bytes.length - 1] != 0) return false;
        for (int i = 1; i < bytes.length - 1; i++) if (bytes[i] == 0) return false;
        try {
            StandardCharsets.UTF_8
                    .newDecoder()
                    .decode(ByteBuffer.wrap(bytes, 1, bytes.length - 2));
            return true;
        } catch (java.nio.charset.CharacterCodingException error) {
            return false;
        }
    }

    public boolean isNull() {
        return bytes.length == 1 && bytes[0] == NULL_TAG;
    }

    public int length() {
        return bytes.length;
    }

    public byte[] bytes() {
        return bytes.clone();
    }

    public void writeTo(ByteBuffer target) {
        target.put(bytes);
    }

    /** The integer this component encodes; only valid for an int column. */
    public long intValue() throws IOException {
        if (bytes.length != 9 || bytes[0] != VALUE) throw new IOException("Not an integer key");
        return ByteBuffer.wrap(bytes, 1, 8).getLong() ^ Long.MIN_VALUE;
    }

    /**
     * The smallest byte string greater than every string that starts with these bytes. A KV
     * adapter uses it to turn an exclusive lower or inclusive upper bound into a byte range over
     * keys that continue with further components (PK, version).
     */
    public byte[] prefixSuccessor() {
        byte[] result = bytes.clone();
        int i = result.length - 1;
        while (i >= 0 && result[i] == (byte) 0xff) i--;
        if (i < 0) throw new IllegalStateException("Key has no prefix successor");
        result[i]++;
        return Arrays.copyOf(result, i + 1);
    }

    @Override
    public int compareTo(IndexKey other) {
        return Arrays.compareUnsigned(bytes, other.bytes);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof IndexKey key && Arrays.equals(bytes, key.bytes);
    }

    /**
     * Hashed with {@link KeyHash}: {@code Arrays.hashCode} collides heavily on the big-endian
     * integer keys this encoding gives, which turned hash maps of keys into trees.
     */
    @Override
    public int hashCode() {
        return KeyHash.of(bytes);
    }

    /** The base-31 polynomial of the bytes, which the replay reports' checksums are defined with. */
    public int checksumWord() {
        return Arrays.hashCode(bytes);
    }

    @Override
    public String toString() {
        return HexFormat.of().formatHex(bytes);
    }
}
