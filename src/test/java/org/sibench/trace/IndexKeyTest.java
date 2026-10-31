package org.sibench.trace;

import static org.junit.Assert.*;

import org.junit.Test;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.*;

public class IndexKeyTest {
    private static int sign(int value) {
        return Integer.signum(value);
    }

    private static int compareTexts(String a, String b) {
        // PostgreSQL C collation: memcmp of the UTF-8 bytes, shorter prefix first.
        return Arrays.compareUnsigned(
                a.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                b.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    private static String randomText(Random random) {
        int length = random.nextInt(6);
        var text = new StringBuilder();
        String alphabet = "abé中 \u0001￿";
        for (int i = 0; i < length; i++) {
            if (random.nextInt(20) == 0) text.appendCodePoint(0x1f600);
            else text.append(alphabet.charAt(random.nextInt(alphabet.length())));
        }
        return text.toString();
    }

    @Test
    public void integersTimestampsAndTextKeepSourceOrder() throws Exception {
        var random = new Random(20261022);
        long[] edges = {Long.MIN_VALUE, -1, 0, 1, Long.MAX_VALUE, Integer.MIN_VALUE};
        for (int i = 0; i < 20000; i++) {
            long a = i < 36 ? edges[i % 6] : random.nextLong();
            long b = i < 36 ? edges[i / 6] : random.nextInt(3) == 0 ? a : random.nextLong();
            assertEquals(
                    sign(Long.compare(a, b)),
                    sign(IndexKey.ofInt(a).compareTo(IndexKey.ofInt(b))));
            assertEquals(a, IndexKey.ofInt(a).intValue());
            String x = randomText(random), y = random.nextInt(4) == 0 ? x : randomText(random);
            assertEquals(
                    sign(compareTexts(x, y)),
                    sign(IndexKey.ofText(x).compareTo(IndexKey.ofText(y))));
            var t = LocalDateTime.of(1 + random.nextInt(9998), 1, 1, 0, 0)
                    .plusSeconds(random.nextInt(366 * 86400))
                    .plusNanos(1000L * random.nextInt(1_000_000));
            var u = random.nextBoolean() ? t : t.plusNanos(1000L * (random.nextInt(2001) - 1000));
            assertEquals(
                    sign(t.compareTo(u)),
                    sign(IndexKey.ofTimestamp(t).compareTo(IndexKey.ofTimestamp(u))));
        }
    }

    @Test
    public void postgresTextFormsAndNullsLast() throws Exception {
        assertEquals(
                IndexKey.parse("timestamp", "2024-01-02 03:04:05.5"),
                IndexKey.parse("timestamp", "2024-01-02T03:04:05.500000"));
        assertEquals(
                IndexKey.parse("timestamp", "2024-01-02 00:00:00"),
                IndexKey.parse("date", "2024-01-02"));
        assertTrue(
                IndexKey.parse("timestamp", "-infinity")
                                .compareTo(IndexKey.parse("timestamp", "0001-01-01 00:00:00"))
                        < 0);
        assertTrue(
                IndexKey.parse("timestamp", "infinity")
                                .compareTo(IndexKey.parse("timestamp", "9999-12-31 23:59:59"))
                        > 0);
        // bpchar compares without trailing spaces; varchar keeps them.
        assertEquals(IndexKey.parse("char(5)", "ab   "), IndexKey.parse("bpchar", "ab"));
        assertNotEquals(IndexKey.parse("varchar", "ab "), IndexKey.parse("varchar", "ab"));
        for (String type : List.of("int", "timestamp", "text"))
            assertTrue(IndexKey.NULL.compareTo(IndexKey.parse(type, maximum(type))) > 0);
        assertTrue(IndexKey.parse("int", null).isNull());
        assertThrows(IOException.class, () -> IndexKey.parse("text", "a\0b"));
        assertThrows(IOException.class, () -> IndexKey.parse("timestamp", "0044-03-15 00:00:00 BC"));
        assertThrows(IOException.class, () -> IndexKey.parse("timestamp", "2024-01-02T03:04:05.0000001"));
        assertThrows(IOException.class, () -> IndexKey.parse("numeric", "1"));
        assertThrows(IOException.class, () -> IndexKey.parse("int", "1.0"));
    }

    private static String maximum(String type) {
        return switch (type) {
            case "int" -> Long.toString(Long.MAX_VALUE);
            case "timestamp" -> "infinity";
            default -> "￿￿";
        };
    }

    @Test
    public void concatenatedComponentsCompareAsTuplesAndRangesMapToBytes() throws Exception {
        var random = new Random(7);
        var tuples = new ArrayList<IndexKey[]>();
        for (int i = 0; i < 3000; i++) {
            IndexKey value =
                    random.nextInt(10) == 0 ? IndexKey.NULL : IndexKey.ofText(randomText(random));
            tuples.add(new IndexKey[] {value, IndexKey.ofInt(random.nextInt(50) - 25)});
        }
        Comparator<IndexKey[]> tuple =
                Comparator.<IndexKey[], IndexKey>comparing(t -> t[0]).thenComparing(t -> t[1]);
        for (int i = 0; i + 1 < tuples.size(); i++) {
            IndexKey[] a = tuples.get(i), b = tuples.get(i + 1);
            assertEquals(
                    sign(tuple.compare(a, b)),
                    sign(Arrays.compareUnsigned(composite(a, 0), composite(b, 0))));
        }
        // An adapter scans [v, successor(v)) for equality and [successor(v), ...) for "> v",
        // whatever PK and version bytes follow.
        for (int i = 0; i < 2000; i++) {
            IndexKey[] probe = tuples.get(random.nextInt(tuples.size()));
            IndexKey[] entry = tuples.get(random.nextInt(tuples.size()));
            byte[] key = composite(entry, random.nextLong());
            byte[] start = probe[0].bytes(), end = probe[0].prefixSuccessor();
            boolean inRange =
                    Arrays.compareUnsigned(key, start) >= 0 && Arrays.compareUnsigned(key, end) < 0;
            assertEquals(entry[0].equals(probe[0]), inRange);
            assertEquals(
                    entry[0].compareTo(probe[0]) > 0, Arrays.compareUnsigned(key, end) >= 0);
        }
    }

    private static byte[] composite(IndexKey[] tuple, long version) {
        var buffer = java.nio.ByteBuffer.allocate(tuple[0].length() + tuple[1].length() + 8);
        tuple[0].writeTo(buffer);
        tuple[1].writeTo(buffer);
        return buffer.putLong(~version).array();
    }

    @Test
    public void decodingRejectsMalformedComponents() throws Exception {
        IndexKey text = IndexKey.ofText("中");
        assertEquals(text, IndexKey.decode(text.bytes()));
        assertEquals(IndexKey.NULL, IndexKey.decode(new byte[] {2}));
        for (byte[] bad :
                new byte[][] {
                    {},
                    {2, 0},
                    {1, 'a'},
                    {1, 'a', 0, 'b', 0},
                    {1, (byte) 0xc3, 0},
                    {3, 0},
                    {1, 0, 0, 0, 0, 0, 0, 0, 0, 0}
                })
            assertThrows(IOException.class, () -> IndexKey.decode(bad));
    }
}
