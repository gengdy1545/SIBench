package org.sibench.state;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Sorts (key, value) records by the unsigned bytes of the key in memory bounded by a byte budget:
 * records collect in memory, a full buffer is written out as a sorted run, and {@link #sorted}
 * merges the runs and the remaining buffer. Records with equal keys come out in insertion order,
 * so the result depends only on the records and never on how they were spilled.
 */
public final class ExternalSort implements AutoCloseable {
    /** Per-record bookkeeping charged to the budget on top of the key and value bytes. */
    private static final int OVERHEAD = 64;

    private static final int MERGE_FANIN = 64;

    private record Record(byte[] key, byte[] value, long order) {}

    private static final Comparator<Record> ORDER =
            (a, b) -> {
                int order = Arrays.compareUnsigned(a.key(), b.key());
                return order != 0 ? order : Long.compare(a.order(), b.order());
            };

    private final java.util.function.Supplier<Path> directory;
    private final long budget;
    private final List<Record> buffer = new ArrayList<>();
    private final List<Path> runs = new ArrayList<>();
    private Path resolved;
    private boolean createdDirectory;
    private long buffered, added, nextRun;
    private boolean sorted;

    public ExternalSort(Path directory, long memoryBytes) {
        this(() -> directory, memoryBytes);
    }

    /** The directory is asked for when the first run is written, not before. */
    public ExternalSort(java.util.function.Supplier<Path> directory, long memoryBytes) {
        if (memoryBytes < 1) throw new IllegalArgumentException("Sort needs a positive budget");
        this.directory = directory;
        this.budget = memoryBytes;
    }

    public void add(byte[] key, byte[] value) throws IOException {
        if (sorted) throw new IllegalStateException("Records are added before sorting");
        buffer.add(new Record(key, value, added++));
        buffered += OVERHEAD + key.length + value.length;
        if (buffered >= budget) spill();
    }

    public long count() {
        return added;
    }

    public int runs() {
        return runs.size();
    }

    private Path nextPath() throws IOException {
        if (resolved == null) {
            resolved = directory.get();
            createdDirectory = !Files.exists(resolved);
            Files.createDirectories(resolved);
        }
        return resolved.resolve("sort-%08d.run".formatted(nextRun++));
    }

    private void spill() throws IOException {
        if (buffer.isEmpty()) return;
        buffer.sort(ORDER);
        Path path = nextPath();
        try (var out = output(path)) {
            for (Record record : buffer) write(out, record);
        }
        runs.add(path);
        buffer.clear();
        buffered = 0;
    }

    /** The insertion order is stored with each record so ties sort the same wherever they spill. */
    private static void write(DataOutputStream out, Record record) throws IOException {
        out.writeInt(record.key().length);
        out.writeInt(record.value().length);
        out.writeLong(record.order());
        out.write(record.key());
        out.write(record.value());
    }

    private static DataOutputStream output(Path path) throws IOException {
        return new DataOutputStream(
                new BufferedOutputStream(
                        Files.newOutputStream(
                                path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE),
                        1 << 16));
    }

    private static final class RunReader implements Closeable {
        private final DataInputStream in;
        Record current;

        RunReader(Path path) throws IOException {
            in = new DataInputStream(new BufferedInputStream(Files.newInputStream(path), 1 << 16));
        }

        boolean advance() throws IOException {
            int keyLength;
            try {
                keyLength = in.readInt();
            } catch (EOFException end) {
                current = null;
                return false;
            }
            int valueLength = in.readInt();
            long order = in.readLong();
            byte[] key = new byte[keyLength], value = new byte[valueLength];
            in.readFully(key);
            in.readFully(value);
            current = new Record(key, value, order);
            return true;
        }

        @Override
        public void close() throws IOException {
            in.close();
        }
    }

    /** Merges runs into one until at most the fan-in remain, so open files stay bounded. */
    private void reduce() throws IOException {
        while (runs.size() > MERGE_FANIN) {
            var group = new ArrayList<>(runs.subList(0, MERGE_FANIN));
            Path merged = nextPath();
            try (var out = output(merged);
                    var merge = new Merge(group)) {
                Record record;
                while ((record = merge.next()) != null) write(out, record);
            }
            for (Path path : group) Files.delete(path);
            runs.subList(0, MERGE_FANIN).clear();
            // The merged run holds the oldest records, so it stays first.
            runs.add(0, merged);
        }
    }

    /** The records in key order; call once, after the last {@link #add}. */
    public Sorted sorted() throws IOException {
        if (sorted) throw new IllegalStateException("Already sorted");
        sorted = true;
        reduce();
        buffer.sort(ORDER);
        return new Sorted(new Merge(runs), new ArrayList<>(buffer));
    }

    /** A k-way merge of run files. */
    private static final class Merge implements Closeable {
        private final List<RunReader> readers = new ArrayList<>();
        private final PriorityQueue<RunReader> queue =
                new PriorityQueue<>((a, b) -> ORDER.compare(a.current, b.current));

        Merge(List<Path> runs) throws IOException {
            try {
                for (Path run : runs) {
                    var reader = new RunReader(run);
                    readers.add(reader);
                    if (reader.advance()) queue.add(reader);
                }
            } catch (IOException | RuntimeException error) {
                close();
                throw error;
            }
        }

        Record next() throws IOException {
            RunReader reader = queue.poll();
            if (reader == null) return null;
            Record result = reader.current;
            if (reader.advance()) queue.add(reader);
            return result;
        }

        @Override
        public void close() throws IOException {
            IOException failure = null;
            for (RunReader reader : readers) {
                try {
                    reader.close();
                } catch (IOException error) {
                    if (failure == null) failure = error;
                    else failure.addSuppressed(error);
                }
            }
            if (failure != null) throw failure;
        }
    }

    /** The sorted records, one at a time, with one record of look-ahead. */
    public static final class Sorted implements Closeable {
        private final Merge runs;
        private final List<Record> memory;
        private int memoryAt;
        private Record fromRuns, fromMemory, head;

        private Sorted(Merge runs, List<Record> memory) throws IOException {
            this.runs = runs;
            this.memory = memory;
            fromRuns = runs.next();
            fromMemory = memory.isEmpty() ? null : memory.get(memoryAt++);
            choose();
        }

        private void choose() throws IOException {
            if (fromRuns == null && fromMemory == null) {
                head = null;
            } else if (fromMemory == null || fromRuns != null && ORDER.compare(fromRuns, fromMemory) <= 0) {
                head = fromRuns;
                fromRuns = runs.next();
            } else {
                head = fromMemory;
                fromMemory = memoryAt < memory.size() ? memory.get(memoryAt++) : null;
            }
        }

        public boolean hasNext() {
            return head != null;
        }

        public byte[] peekKey() {
            return head.key();
        }

        public byte[] peekValue() {
            return head.value();
        }

        public void advance() throws IOException {
            if (head == null) throw new IllegalStateException("Sorted records are exhausted");
            choose();
        }

        @Override
        public void close() throws IOException {
            runs.close();
        }
    }

    /**
     * Removes the run files, and the directory when this sort created it; the sorted view must be
     * closed first.
     */
    @Override
    public void close() throws IOException {
        for (Path run : runs) Files.deleteIfExists(run);
        runs.clear();
        buffer.clear();
        if (createdDirectory) {
            try {
                Files.deleteIfExists(resolved);
            } catch (java.nio.file.DirectoryNotEmptyException foreign) {
                // Something else was put there; it is not ours to delete.
            }
        }
    }
}
