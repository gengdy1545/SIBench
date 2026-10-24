package org.sibench.state;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

public class ExternalSortTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    private record Pair(byte[] key, byte[] value) {}

    private static List<Pair> records(int count, long seed) {
        var random = new Random(seed);
        var result = new ArrayList<Pair>();
        for (int i = 0; i < count; i++) {
            // Few distinct keys, so equal keys are common and their order matters.
            byte[] key = new byte[random.nextInt(4)];
            for (int b = 0; b < key.length; b++) key[b] = (byte) (random.nextInt(3) * 100);
            byte[] value = new byte[random.nextInt(30)];
            random.nextBytes(value);
            result.add(new Pair(key, value));
        }
        return result;
    }

    private static List<Pair> drain(ExternalSort.Sorted sorted) throws Exception {
        var result = new ArrayList<Pair>();
        while (sorted.hasNext()) {
            result.add(new Pair(sorted.peekKey(), sorted.peekValue()));
            sorted.advance();
        }
        return result;
    }

    private static List<Pair> expected(List<Pair> input) {
        var copy = new ArrayList<>(input);
        // List.sort is stable, so equal keys keep insertion order.
        copy.sort((a, b) -> Arrays.compareUnsigned(a.key(), b.key()));
        return copy;
    }

    private static void assertSame(List<Pair> want, List<Pair> got) {
        assertEquals(want.size(), got.size());
        for (int i = 0; i < want.size(); i++) {
            assertArrayEquals("key " + i, want.get(i).key(), got.get(i).key());
            assertArrayEquals("value " + i, want.get(i).value(), got.get(i).value());
        }
    }

    @Test
    public void theResultIsTheStableSortWhateverTheBudget() throws Exception {
        var input = records(5_000, 11);
        for (long budget : new long[] {1, 500, 20_000, 1L << 30}) {
            try (var sort = new ExternalSort(temporary.newFolder().toPath(), budget)) {
                for (Pair p : input) sort.add(p.key(), p.value());
                assertEquals(5_000, sort.count());
                if (budget < 20_000) assertTrue(sort.runs() > 0);
                else if (budget > 1L << 20) assertEquals(0, sort.runs());
                try (var sorted = sort.sorted()) {
                    assertSame(expected(input), drain(sorted));
                }
            }
        }
    }

    @Test
    public void manyRunsAreMergedInPassesWithBoundedOpenFiles() throws Exception {
        Path directory = temporary.newFolder().toPath();
        var input = records(2_000, 5);
        try (var sort = new ExternalSort(directory, 1)) {
            for (Pair p : input) sort.add(p.key(), p.value());
            // One run per record before reduction.
            assertEquals(2_000, sort.runs());
            try (var sorted = sort.sorted()) {
                assertTrue("runs " + sort.runs(), sort.runs() <= 64);
                assertSame(expected(input), drain(sorted));
            }
        }
    }

    @Test
    public void emptyInputsAndOneSortedCallOnly() throws Exception {
        try (var sort = new ExternalSort(temporary.newFolder().toPath(), 100)) {
            try (var sorted = sort.sorted()) {
                assertFalse(sorted.hasNext());
                assertThrows(IllegalStateException.class, sorted::advance);
            }
            assertThrows(IllegalStateException.class, sort::sorted);
            assertThrows(IllegalStateException.class, () -> sort.add(new byte[0], new byte[0]));
        }
    }

    @Test
    public void closeRemovesEveryRunFile() throws Exception {
        Path directory = temporary.newFolder().toPath();
        try (var sort = new ExternalSort(directory, 1)) {
            for (Pair p : records(100, 2)) sort.add(p.key(), p.value());
            try (var sorted = sort.sorted()) {
                drain(sorted);
            }
        }
        try (var files = Files.list(directory)) {
            assertEquals(0, files.count());
        }
    }

    @Test
    public void keysCompareAsUnsignedBytes() throws Exception {
        try (var sort = new ExternalSort(temporary.newFolder().toPath(), 1)) {
            byte[][] keys = {{(byte) 0xff}, {0x01}, {(byte) 0x80}, {0x7f}, {}};
            for (byte[] key : keys) sort.add(key, new byte[] {1});
            try (var sorted = sort.sorted()) {
                var got = drain(sorted);
                assertArrayEquals(new byte[] {}, got.get(0).key());
                assertArrayEquals(new byte[] {0x01}, got.get(1).key());
                assertArrayEquals(new byte[] {0x7f}, got.get(2).key());
                assertArrayEquals(new byte[] {(byte) 0x80}, got.get(3).key());
                assertArrayEquals(new byte[] {(byte) 0xff}, got.get(4).key());
            }
        }
    }
}
