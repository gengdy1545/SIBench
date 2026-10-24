package org.sibench.state;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.stream.Stream;

public class SpillingMapTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    private static byte[] key(long value) {
        return java.nio.ByteBuffer.allocate(8).putLong(value ^ Long.MIN_VALUE).array();
    }

    private static byte[] text(String value) {
        return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private long files(Path directory) throws IOException {
        try (Stream<Path> paths = Files.list(directory)) {
            return paths.count();
        }
    }

    @Test
    public void behavesLikeAMapWhateverTheMemoryBudget() throws Exception {
        for (int memory : new int[] {1, 2, 7, 100, 100_000}) {
            Path directory = temporary.newFolder().toPath();
            var expected = new TreeMap<Long, Long>();
            try (var map = new SpillingMap(directory, memory, 3)) {
                var random = new Random(42);
                for (int i = 0; i < 20_000; i++) {
                    long k = random.nextInt(3_000) - 1_000L;
                    if (random.nextInt(4) == 0) {
                        long found = map.get(key(k));
                        assertEquals(expected.getOrDefault(k, SpillingMap.ABSENT), (Long) found);
                    } else {
                        long v = random.nextInt(1_000_000) - 500_000L;
                        map.put(key(k), v);
                        expected.put(k, v);
                    }
                }
                for (long k = -1_100; k < 2_200; k++)
                    assertEquals(
                            "key " + k + " memory " + memory,
                            (long) expected.getOrDefault(k, SpillingMap.ABSENT),
                            map.get(key(k)));
                if (memory < 100) {
                    assertTrue(map.flushes() > 0);
                    assertTrue(map.merges() > 0);
                    // Logarithmic merging keeps the run count small however long the stream is.
                    assertTrue("runs " + map.runCount(), map.runCount() <= 3 * 8);
                }
                if (memory == 100_000) assertEquals(0, map.runCount());
            }
        }
    }

    @Test
    public void aValueMayBeZeroNegativeOrExtreme() throws Exception {
        try (var map = new SpillingMap(temporary.newFolder().toPath(), 2, 2)) {
            long[] values = {0, -1, 1, Long.MAX_VALUE, Long.MIN_VALUE + 1};
            for (int i = 0; i < values.length; i++) map.put(key(i), values[i]);
            for (int i = 0; i < values.length; i++) assertEquals(values[i], map.get(key(i)));
            assertEquals(SpillingMap.ABSENT, map.get(key(99)));
            assertThrows(IllegalArgumentException.class, () -> map.put(key(1), SpillingMap.ABSENT));
        }
    }

    @Test
    public void keysOfAnyLengthOrderAsUnsignedBytes() throws Exception {
        try (var map = new SpillingMap(temporary.newFolder().toPath(), 3, 2)) {
            var expected = new TreeMap<String, Long>();
            var random = new Random(7);
            var keys = new ArrayList<String>();
            keys.add("");
            for (int i = 0; i < 400; i++) {
                var builder = new StringBuilder();
                for (int n = random.nextInt(40); n > 0; n--)
                    builder.append((char) ('a' + random.nextInt(3)));
                keys.add(builder.toString());
            }
            // A key beyond one read buffer, and byte values above 0x7f.
            keys.add("x".repeat(20_000));
            long next = 1;
            for (String k : keys) {
                map.put(text(k), next);
                expected.put(k, next++);
            }
            for (String k : keys) assertEquals((long) expected.get(k), map.get(text(k)));
            assertEquals(SpillingMap.ABSENT, map.get(text("zzz")));
            byte[] high = {(byte) 0xff, (byte) 0x80, 0x01};
            byte[] low = {0x7f};
            map.put(high, 1);
            map.put(low, 2);
            for (int i = 0; i < 20; i++) map.put(key(1000 + i), i);
            assertEquals(1, map.get(high));
            assertEquals(2, map.get(low));
        }
    }

    @Test
    public void pinnedBaselineRunsAreReadFromDiskAndOverwrittenByLaterWrites() throws Exception {
        try (var map = new SpillingMap(temporary.newFolder().toPath(), 10, 2)) {
            try (var bulk = map.bulk()) {
                for (long k = 0; k < 10_000; k++) bulk.append(key(k * 2), k + 1);
                bulk.finish();
            }
            assertEquals(1, map.runCount());
            assertEquals(0, map.memoryEntries());
            assertEquals(1, map.get(key(0)));
            assertEquals(5_000, map.get(key(9_998)));
            assertEquals(10_000, map.get(key(19_998)));
            assertEquals(SpillingMap.ABSENT, map.get(key(1)));
            assertEquals(SpillingMap.ABSENT, map.get(key(-5)));
            assertEquals(SpillingMap.ABSENT, map.get(key(20_000)));
            // Overlay writes win over the baseline and never touch it.
            for (long k = 0; k < 100; k++) map.put(key(k * 2), 0);
            map.put(key(1), 77);
            for (long k = 0; k < 100; k++) assertEquals(0, map.get(key(k * 2)));
            assertEquals(77, map.get(key(1)));
            assertEquals(101, map.get(key(200)));
            // Merges never fold the pinned run in.
            for (long k = 0; k < 500; k++) map.put(key(30_000 + k), k);
            assertEquals(5_000, map.get(key(9_998)));
            assertTrue(map.merges() > 0);
            assertThrows(IllegalStateException.class, map::bulk);
        }
    }

    @Test
    public void bulkRunsMustAscendStrictly() throws Exception {
        try (var map = new SpillingMap(temporary.newFolder().toPath(), 10, 2)) {
            try (var bulk = map.bulk()) {
                bulk.append(key(5), 1);
                assertThrows(IllegalStateException.class, () -> bulk.append(key(5), 2));
                assertThrows(IllegalStateException.class, () -> bulk.append(key(4), 2));
            }
            assertEquals(0, map.runCount());
        }
    }

    @Test
    public void pinnedRunsOfSeveralTablesKeepTheirOwnRanges() throws Exception {
        try (var map = new SpillingMap(temporary.newFolder().toPath(), 10, 2)) {
            for (int table = 1; table <= 3; table++) {
                try (var bulk = map.bulk()) {
                    for (long k = 0; k < 200; k++)
                        bulk.append(java.nio.ByteBuffer.allocate(12).putInt(table).putLong(k).array(),
                                table * 1_000 + k);
                    bulk.finish();
                }
            }
            assertEquals(3, map.runCount());
            for (int table = 1; table <= 3; table++)
                assertEquals(
                        table * 1_000 + 150L,
                        map.get(java.nio.ByteBuffer.allocate(12).putInt(table).putLong(150).array()));
            assertEquals(
                    SpillingMap.ABSENT,
                    map.get(java.nio.ByteBuffer.allocate(12).putInt(4).putLong(0).array()));
        }
    }

    @Test
    public void aCheckpointRestoresExactlyThatStateAndKeepsReplacedFilesUntilReleased()
            throws Exception {
        Path directory = temporary.newFolder().toPath();
        var atCheckpoint = new TreeMap<Long, Long>();
        List<SpillingMap.RunFile> checkpoint;
        try (var map = new SpillingMap(directory, 5, 2)) {
            try (var bulk = map.bulk()) {
                for (long k = 0; k < 300; k++) {
                    bulk.append(key(k), k + 1);
                    atCheckpoint.put(k, k + 1);
                }
                bulk.finish();
            }
            var random = new Random(3);
            for (int i = 0; i < 300; i++) {
                long k = random.nextInt(600), v = random.nextInt(1000);
                map.put(key(k), v);
                atCheckpoint.put(k, v);
            }
            checkpoint = map.checkpoint();
            assertEquals(0, map.memoryEntries());
            // Writes after the checkpoint merge runs the checkpoint names; their files stay.
            for (int i = 0; i < 400; i++) map.put(key(10_000 + i), i);
            assertTrue(map.merges() > 0);
            for (var file : checkpoint) assertTrue(Files.exists(directory.resolve(file.name())));
            // A copy taken from those files is the state at the checkpoint, nothing later.
            Path copy = temporary.newFolder().toPath();
            for (var file : checkpoint)
                Files.copy(directory.resolve(file.name()), copy.resolve(file.name()));
            try (var restored = SpillingMap.restore(copy, 5, 2, checkpoint)) {
                for (long k = -5; k < 700; k++)
                    assertEquals(
                            (long) atCheckpoint.getOrDefault(k, SpillingMap.ABSENT),
                            restored.get(key(k)));
                assertEquals(SpillingMap.ABSENT, restored.get(key(10_000)));
                // The restored map continues like the original would.
                restored.put(key(10_000), 5);
                assertEquals(5, restored.get(key(10_000)));
            }
            map.released();
            for (long k = 0; k < 400; k++) assertEquals(k, map.get(key(10_000 + k)));
        }
    }

    @Test
    public void releasedDeletesOnlyReplacedFiles() throws Exception {
        Path directory = temporary.newFolder().toPath();
        try (var map = new SpillingMap(directory, 2, 2)) {
            for (long k = 0; k < 40; k++) map.put(key(k), k);
            long files = files(directory);
            assertTrue(map.merges() > 0);
            map.released();
            assertTrue(files(directory) < files);
            assertEquals(map.runCount(), files(directory));
            for (long k = 0; k < 40; k++) assertEquals(k, map.get(key(k)));
        }
    }

    @Test
    public void aTruncatedRunIsRejectedOnRestore() throws Exception {
        Path directory = temporary.newFolder().toPath();
        List<SpillingMap.RunFile> files;
        try (var map = new SpillingMap(directory, 4, 2)) {
            for (long k = 0; k < 10; k++) map.put(key(k), k);
            files = map.checkpoint();
        }
        Path run = directory.resolve(files.get(0).name());
        byte[] content = Files.readAllBytes(run);
        Files.write(run, Arrays.copyOf(content, content.length - 5));
        assertThrows(IOException.class, () -> SpillingMap.restore(directory, 4, 2, files));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(java.nio.charset.StandardCharsets.ISO_8859_1);
    }

    private static List<String> scan(SpillingMap map, byte[] from, byte[] to, boolean descending)
            throws Exception {
        var result = new ArrayList<String>();
        try (var entries = map.scan(from, to, descending)) {
            while (entries.next())
                result.add(
                        new String(entries.key(), java.nio.charset.StandardCharsets.ISO_8859_1)
                                + "="
                                + new String(entries.value(), java.nio.charset.StandardCharsets.ISO_8859_1));
        }
        return result;
    }

    private static List<String> expectedScan(
            TreeMap<String, String> expected, String from, String to, boolean descending) {
        var view =
                new TreeMap<String, String>(
                        (a, b) ->
                                Arrays.compareUnsigned(bytes(a), bytes(b)));
        view.putAll(expected);
        var result = new ArrayList<String>();
        for (var entry : (descending ? view.descendingMap() : view).entrySet()) {
            byte[] key = bytes(entry.getKey());
            if (from != null && Arrays.compareUnsigned(key, bytes(from)) < 0) continue;
            if (to != null && Arrays.compareUnsigned(key, bytes(to)) >= 0) continue;
            result.add(entry.getKey() + "=" + entry.getValue());
        }
        return result;
    }

    @Test
    public void scansAndFloorsAgreeWithAMapAcrossMemoryRunsAndPinnedBaseline() throws Exception {
        for (int memory : new int[] {1, 5, 1_000_000}) {
            var expected = new TreeMap<String, String>();
            try (var map = new SpillingMap(temporary.newFolder().toPath(), memory, 3)) {
                // A pinned baseline of even keys, then overlay writes that shadow and interleave.
                try (var bulk = map.bulk()) {
                    for (int i = 0; i < 300; i += 2) {
                        String k = "k%04d".formatted(i), v = "base" + i;
                        bulk.append(bytes(k), bytes(v));
                        expected.put(k, v);
                    }
                    bulk.finish();
                }
                var random = new Random(21);
                for (int i = 0; i < 600; i++) {
                    String k = "k%04d".formatted(random.nextInt(340));
                    String v = "w" + i;
                    map.putBytes(bytes(k), bytes(v));
                    expected.put(k, v);
                }
                for (int trial = 0; trial < 120; trial++) {
                    String from = random.nextInt(6) == 0 ? null : "k%04d".formatted(random.nextInt(360));
                    String to = random.nextInt(6) == 0 ? null : "k%04d".formatted(random.nextInt(360));
                    for (boolean descending : new boolean[] {false, true})
                        assertEquals(
                                "memory %d scan [%s,%s) desc=%s".formatted(memory, from, to, descending),
                                expectedScan(expected, from, to, descending),
                                scan(
                                        map,
                                        from == null ? null : bytes(from),
                                        to == null ? null : bytes(to),
                                        descending));
                }
                var all = new TreeMap<String, String>(expected);
                for (int trial = 0; trial < 200; trial++) {
                    String probe = "k%04d".formatted(random.nextInt(400));
                    var floor = map.floor(bytes(probe));
                    var want = all.floorEntry(probe);
                    if (want == null) assertNull(floor);
                    else {
                        assertEquals(want.getKey(), new String(floor.getKey(), java.nio.charset.StandardCharsets.ISO_8859_1));
                        assertEquals(want.getValue(), new String(floor.getValue(), java.nio.charset.StandardCharsets.ISO_8859_1));
                    }
                }
                assertNull(map.floor(bytes("a")));
                assertEquals(
                        all.lastKey(),
                        new String(map.floor(bytes("zzz")).getKey(), java.nio.charset.StandardCharsets.ISO_8859_1));
            }
        }
    }

    @Test
    public void aNewerValueShadowsOlderOnesInEveryScanDirection() throws Exception {
        try (var map = new SpillingMap(temporary.newFolder().toPath(), 2, 2)) {
            for (int generation = 0; generation < 6; generation++) {
                for (int k = 0; k < 4; k++) map.putBytes(bytes("key" + k), bytes("g" + generation));
            }
            map.putBytes(bytes("key9"), bytes("last"));
            assertEquals(
                    List.of("key0=g5", "key1=g5", "key2=g5", "key3=g5", "key9=last"),
                    scan(map, null, null, false));
            assertEquals(
                    List.of("key9=last", "key3=g5", "key2=g5", "key1=g5", "key0=g5"),
                    scan(map, null, null, true));
            assertEquals(List.of("key1=g5", "key2=g5"), scan(map, bytes("key1"), bytes("key3"), false));
            assertEquals(List.of("key2=g5", "key1=g5"), scan(map, bytes("key1"), bytes("key3"), true));
            assertEquals(List.of(), scan(map, bytes("key3"), bytes("key3"), false));
            assertEquals(List.of(), scan(map, bytes("key5"), bytes("key1"), true));
        }
    }

    @Test
    public void emptyAndByteValuesOfAnyLengthSurviveRunsAndMerges() throws Exception {
        try (var map = new SpillingMap(temporary.newFolder().toPath(), 3, 2)) {
            var expected = new TreeMap<Integer, byte[]>();
            var random = new Random(4);
            for (int i = 0; i < 200; i++) {
                byte[] value = new byte[i % 7 == 0 ? 0 : random.nextInt(300)];
                random.nextBytes(value);
                map.putBytes(key(i), value);
                expected.put(i, value);
            }
            assertTrue(map.merges() > 0);
            for (var entry : expected.entrySet())
                assertArrayEquals(entry.getValue(), map.getBytes(key(entry.getKey())));
            assertNull(map.getBytes(key(5_000)));
        }
    }

    @Test
    public void theDirectoryIsCreatedOnlyWhenARunIsWritten() throws Exception {
        Path directory = temporary.getRoot().toPath().resolve("lazy");
        try (var map = new SpillingMap(directory, 10, 2)) {
            for (long k = 0; k < 5; k++) map.put(key(k), k);
            assertEquals(2, map.get(key(2)));
            assertFalse(Files.exists(directory));
            for (long k = 5; k < 20; k++) map.put(key(k), k);
            assertTrue(Files.isDirectory(directory));
        }
    }
}
