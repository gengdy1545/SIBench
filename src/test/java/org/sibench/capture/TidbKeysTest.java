package org.sibench.capture;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import org.junit.Test;

public class TidbKeysTest {
    static String table(long id) {
        return "74" + intHex(id);
    }

    /** TiDB's comparable int64 (sign bit flipped, big-endian), written by hand. */
    static String intHex(long value) {
        return String.format("%016x", value ^ Long.MIN_VALUE);
    }

    static String record(long table, long handle) {
        return table(table) + "5f72" + intHex(handle);
    }

    static String index(long table, long index, long value, long handle) {
        return table(table) + "5f69" + intHex(index) + "03" + intHex(value) + "03" + intHex(handle);
    }

    @Test
    public void decodesAKeyCapturedFromTiKV() throws IOException {
        // Captured from TiKV: table 149 (0x95), index 1, k = 7, handle 7.
        var p = TidbKeys.parse(TidbKeys.hex("7480000000000000955f698000000000000001038000000000000007038000000000000007"), true);
        assertEquals(149, p.table());
        assertEquals(1, p.index());
        assertEquals(List.of(7L, 7L), p.components());
        assertTrue(p.exact());
    }

    @Test
    public void decodesIntegerRecordKeysAndNegativeHandles() throws IOException {
        var p = TidbKeys.parse(TidbKeys.hex(record(100, -3)), true);
        assertEquals(0, p.index());
        assertEquals(List.of(-3L), p.components());
        var tail = TidbKeys.parse(TidbKeys.hex(record(100, 5) + "00"), true);
        assertEquals(List.of(5L), tail.components());
        assertEquals(1, tail.rest());
    }

    @Test
    public void decodesBytesGroupsOfEveryLength() throws IOException {
        // "user1": 5 bytes, 3 pad zeros, marker 0xFF - 3.
        String user1 = "01" + "7573657231000000" + "fc";
        // "abcdefgh": one full group (marker 0xFF) then an empty group (marker 0xFF - 8).
        String eight = "01" + "6162636465666768" + "ff" + "0000000000000000" + "f7";
        var p = TidbKeys.parse(TidbKeys.hex(table(7) + "5f72" + user1), false);
        assertEquals("user1", TidbKeys.text(p.components().get(0)));
        var q = TidbKeys.parse(TidbKeys.hex(table(7) + "5f72" + eight), false);
        assertArrayEquals("abcdefgh".getBytes(), (byte[]) q.components().get(0));
        assertThrows(IOException.class, () -> TidbKeys.parse(TidbKeys.hex(table(7) + "5f72" + "01" + "7573657231000001" + "fc"), false));
    }

    @Test
    public void stopsAtTheMaximumFlagAndRejectsUnsupportedComponents() throws IOException {
        var p = TidbKeys.parse(TidbKeys.hex(table(9) + "5f69" + intHex(1) + "fb"), true);
        assertEquals(1, p.index());
        assertTrue(p.components().isEmpty());
        assertEquals(1, p.rest());
        // 0x05 is TiDB's float flag.
        assertThrows(IOException.class, () -> TidbKeys.parse(TidbKeys.hex(table(9) + "5f69" + intHex(1) + "05" + intHex(0)), true));
        assertNull(TidbKeys.parse(TidbKeys.hex(table(9) + "5f73"), true));
    }

    @Test
    public void handBuiltHexMatchesTheDecoder() {
        assertEquals("8000000000000007", intHex(7));
        assertTrue(Arrays.equals(HexFormat.of().parseHex("7480000000000000955f728000000000000007"), TidbKeys.hex(record(149, 7))));
    }
}
