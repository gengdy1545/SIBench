package org.sibench.capture;

import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * The hash one result row is recorded as, on the server by the PG instrumentation and on the
 * client by {@link CaptureDriver}: 64-bit FNV-1a over each column as {@code N} (null) or {@code V}
 * plus its canonical text, followed by 0x1f. Canonical text removes only the differences between a
 * server's text output and what a JDBC client reads back: float bits, numeric without trailing
 * fraction zeros, timestamps with six fraction digits, char without its padding. Anything else is
 * compared as text, so a representation this does not know fails the audit instead of passing.
 */
final class ResultRowHash {
    private static final long OFFSET = 0xcbf29ce484222325L, PRIME = 0x100000001b3L;
    private static final DateTimeFormatter TIMESTAMP =
            DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSSSS");

    private ResultRowHash() {}

    /** The hash of one client row: column text as read by getString, with its JDBC types. */
    static String of(List<String> values, List<Integer> jdbcTypes) {
        long hash = OFFSET;
        for (int i = 0; i < values.size(); i++) {
            String value = values.get(i);
            if (value == null) hash = fnv(hash, "N");
            else {
                hash = fnv(hash, "V");
                hash = fnv(hash, canonical(value, jdbcTypes.get(i)));
            }
            hash = fnv(hash, "\u001f");
        }
        return String.format("%016x", hash);
    }

    static String canonical(String value, int jdbcType) {
        try {
            return switch (jdbcType) {
                case Types.REAL -> String.format("%08x", Float.floatToIntBits(Float.parseFloat(value)));
                case Types.FLOAT, Types.DOUBLE ->
                        String.format("%016x", Double.doubleToLongBits(Double.parseDouble(value)));
                case Types.NUMERIC, Types.DECIMAL -> numeric(value);
                case Types.TIMESTAMP -> timestamp(value);
                case Types.CHAR -> value.stripTrailing();
                default -> value;
            };
        } catch (RuntimeException unparsed) {
            // The server falls back to its text output for the same values (NaN numeric, BC
            // timestamps), which is also what the client reads.
            return value;
        }
    }

    private static String numeric(String value) {
        if (!value.contains(".")) return value;
        String stripped = value.replaceAll("0+$", "");
        if (stripped.endsWith(".")) stripped = stripped.substring(0, stripped.length() - 1);
        return stripped.equals("-0") ? "0" : stripped;
    }

    private static String timestamp(String value) {
        LocalDateTime time = LocalDateTime.parse(value.replace(' ', 'T'));
        if (time.getYear() < 1 || time.getYear() > 9999) return value;
        return time.format(TIMESTAMP);
    }

    private static long fnv(long hash, String text) {
        for (byte b : text.getBytes(StandardCharsets.UTF_8)) {
            hash ^= b & 0xff;
            hash *= PRIME;
        }
        return hash;
    }
}
