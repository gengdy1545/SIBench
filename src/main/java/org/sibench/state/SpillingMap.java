package org.sibench.state;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.PriorityQueue;
import java.util.TreeMap;

/**
 * An ordered map from byte-string keys to byte-string values whose memory is bounded: a sorted
 * in-memory table that is written out as an immutable sorted run when it holds {@code
 * memoryEntries} entries, and runs that are merged logarithmically (the newest {@code fanout}
 * runs of one level become one run of the next). A later write of a key replaces an earlier one,
 * so the answer to any lookup or scan is independent of how entries happen to be laid out in
 * memory and on disk.
 *
 * <p>Pinned runs are bulk-loaded, already sorted, below everything else and never merged; they
 * hold an immutable baseline whose size is not bounded by memory. Runs are files in one
 * directory, created when the first run is written; {@link #checkpoint()} makes the current state
 * a list of files that {@link #restore} reopens, and files a merge replaced stay on disk until
 * {@link #released()}, because a checkpoint may still name them.
 *
 * <p>Scans and lookups see writes made before they started; a write while a scan is open
 * invalidates the scan.
 */
public final class SpillingMap implements AutoCloseable {
    /** What {@link #get} returns for a key that was never written; not a valid value. */
    public static final long ABSENT = Long.MIN_VALUE;

    static final int INDEX_INTERVAL = 64;
    private static final int MAGIC = 0x53504d53;
    private static final int FORMAT = 2;
    private static final int BLOOM_BITS_PER_KEY = 10;
    private static final long BLOOM_LIMIT = 1L << 25;
    /** A lookup reads one sparse-index span; a merge or scan streams a run in larger reads. */
    private static final int LOOKUP_BYTES = 1 << 13, STREAM_BYTES = 1 << 16;

    /** One run as a checkpoint names it. */
    public record RunFile(String name, int level, boolean pinned) {}

    /** Entries of a scan, one at a time. */
    public interface Entries extends Closeable {
        boolean next() throws IOException;

        byte[] key();

        byte[] value();

        /**
         * Continues the scan from {@code key}: ascending, at the first key at or above it;
         * descending, at the first key below it. Positions within the original bounds only.
         */
        void seek(byte[] key) throws IOException;
    }

    private final java.util.function.Supplier<Path> directory;
    private final int memoryEntries, fanout;
    private final TreeMap<byte[], byte[]> table = new TreeMap<>(Arrays::compareUnsigned);
    private final List<Run> runs = new ArrayList<>();
    private final List<Path> retired = new ArrayList<>();
    private ByteBuffer scratch = ByteBuffer.allocate(LOOKUP_BYTES).order(ByteOrder.LITTLE_ENDIAN);
    private boolean directoryCreated;
    private Path restored;
    private Path resolved;
    private long nextRun, merges, flushes, lookups, runReads;

    public SpillingMap(Path directory, int memoryEntries, int fanout) {
        this(() -> directory, memoryEntries, fanout);
    }

    /** The directory is asked for when the first file is written, not before. */
    public SpillingMap(java.util.function.Supplier<Path> directory, int memoryEntries, int fanout) {
        if (memoryEntries < 1 || fanout < 2)
            throw new IllegalArgumentException("Spilling map needs memory for an entry and fanout >= 2");
        this.directory = directory;
        this.memoryEntries = memoryEntries;
        this.fanout = fanout;
    }

    /** Reopens the runs a checkpoint named, oldest first; the in-memory table starts empty. */
    public static SpillingMap restore(
            Path directory, int memoryEntries, int fanout, List<RunFile> files) throws IOException {
        var map = new SpillingMap(directory, memoryEntries, fanout);
        map.directoryCreated = Files.isDirectory(directory);
        map.restored = directory;
        for (RunFile file : files) {
            Run run = Run.open(directory.resolve(file.name()), file.level(), file.pinned());
            map.runs.add(run);
            map.nextRun = Math.max(map.nextRun, sequence(file.name()) + 1);
        }
        return map;
    }

    // ---- long values ----

    public void put(byte[] key, long value) throws IOException {
        if (value == ABSENT) throw new IllegalArgumentException("Reserved value");
        putBytes(key, ByteBuffer.allocate(8).putLong(value).array());
    }

    /** The last value written for the key, or {@link #ABSENT}. */
    public long get(byte[] key) throws IOException {
        byte[] value = getBytes(key);
        return value == null ? ABSENT : ByteBuffer.wrap(value).getLong();
    }

    // ---- byte values ----

    public void putBytes(byte[] key, byte[] value) throws IOException {
        table.put(key, value);
        if (table.size() >= memoryEntries) flush();
    }

    /** The last value written for the key, or null. */
    public byte[] getBytes(byte[] key) throws IOException {
        lookups++;
        byte[] value = table.get(key);
        if (value != null) return value;
        for (int i = runs.size() - 1; i >= 0; i--) {
            byte[] found = runs.get(i).find(key, this);
            if (found != null) return found;
        }
        return null;
    }

    /** The entry with the greatest key at or below {@code key}, or null. */
    public Map.Entry<byte[], byte[]> floor(byte[] key) throws IOException {
        lookups++;
        byte[] bestKey = null, bestValue = null;
        // Oldest first, so a newer source wins a tie on the same key.
        for (Run run : runs) {
            var found = run.floor(key, this);
            if (found != null
                    && (bestKey == null || Arrays.compareUnsigned(found.getKey(), bestKey) >= 0)) {
                bestKey = found.getKey();
                bestValue = found.getValue();
            }
        }
        var memory = table.floorEntry(key);
        if (memory != null
                && (bestKey == null || Arrays.compareUnsigned(memory.getKey(), bestKey) >= 0)) {
            bestKey = memory.getKey();
            bestValue = memory.getValue();
        }
        return bestKey == null ? null : Map.entry(bestKey, bestValue);
    }

    /**
     * The entries with {@code from <= key < to} (a null bound is open) in key order, or in reverse
     * order when {@code descending}. Where a key was written more than once the newest value is
     * the only one returned.
     */
    public Entries scan(byte[] from, byte[] to, boolean descending) throws IOException {
        return new Scan(from, to, descending);
    }

    /**
     * Starts a pinned run of strictly ascending keys. Pinned runs sit below every later write, so
     * they can only be added before the first {@link #putBytes}.
     */
    public Bulk bulk() throws IOException {
        if (!table.isEmpty() || runs.stream().anyMatch(run -> !run.pinned))
            throw new IllegalStateException("Pinned runs are loaded before any write");
        return new Bulk(nextPath());
    }

    public final class Bulk implements Closeable {
        private final RunWriter writer;
        private boolean finished;

        private Bulk(Path path) throws IOException {
            writer = new RunWriter(path, 0, true, 0);
        }

        public void append(byte[] key, long value) throws IOException {
            if (value == ABSENT) throw new IllegalArgumentException("Reserved value");
            writer.append(key, ByteBuffer.allocate(8).putLong(value).array());
        }

        public void append(byte[] key, byte[] value) throws IOException {
            writer.append(key, value);
        }

        /** Registers the run; an empty one leaves no file. */
        public void finish() throws IOException {
            finished = true;
            Run run = writer.finish();
            if (run != null) runs.add(run);
        }

        @Override
        public void close() throws IOException {
            if (!finished) writer.abort();
        }
    }

    /** Writes the in-memory table out as a run and merges as far as the level rule asks. */
    public void flush() throws IOException {
        if (table.isEmpty()) return;
        var writer = new RunWriter(nextPath(), 0, false, table.size());
        for (var entry : table.entrySet()) writer.append(entry.getKey(), entry.getValue());
        runs.add(writer.finish());
        table.clear();
        flushes++;
        compact();
    }

    private void compact() throws IOException {
        while (true) {
            int last = runs.size() - 1, first = last - fanout + 1;
            if (first < 0) return;
            int level = runs.get(last).level;
            for (int i = first; i <= last; i++)
                if (runs.get(i).pinned || runs.get(i).level != level) return;
            merge(first, last, level + 1);
        }
    }

    /** Merges runs {@code first..last}, a contiguous span, into one; the newest value wins. */
    private void merge(int first, int last, int level) throws IOException {
        var sources = new ArrayList<RunCursor>();
        long upper = 0;
        for (int i = first; i <= last; i++) {
            sources.add(new RunCursor(runs.get(i), i));
            upper += runs.get(i).entries;
        }
        // Equal keys come out newest first; the older ones are dropped.
        var queue =
                new PriorityQueue<RunCursor>(
                        (a, b) -> {
                            int order = Arrays.compareUnsigned(a.key, b.key);
                            return order != 0 ? order : Integer.compare(b.order, a.order);
                        });
        for (RunCursor cursor : sources) if (cursor.advance()) queue.add(cursor);
        var writer = new RunWriter(nextPath(), level, false, upper);
        byte[] previous = null;
        while (!queue.isEmpty()) {
            RunCursor cursor = queue.poll();
            if (previous == null || Arrays.compareUnsigned(previous, cursor.key) != 0) {
                writer.append(cursor.key, cursor.value);
                previous = cursor.key;
            }
            if (cursor.advance()) queue.add(cursor);
        }
        for (RunCursor cursor : sources) cursor.close();
        Run merged = writer.finish();
        for (int i = last; i >= first; i--) {
            Run old = runs.remove(i);
            old.close();
            retired.add(old.path);
        }
        runs.add(first, merged);
        merges++;
    }

    /** The state as files: flushes the table, so a restore starts from exactly this content. */
    public List<RunFile> checkpoint() throws IOException {
        flush();
        var files = new ArrayList<RunFile>();
        for (Run run : runs)
            files.add(new RunFile(run.path.getFileName().toString(), run.level, run.pinned));
        return files;
    }

    /** Deletes the files merges replaced; call once no durable checkpoint names them. */
    public void released() throws IOException {
        for (Path path : retired) Files.deleteIfExists(path);
        retired.clear();
    }

    public int runCount() {
        return runs.size();
    }

    public long merges() {
        return merges;
    }

    public long flushes() {
        return flushes;
    }

    /** Disk reads that were needed to answer lookups, for the report of a run. */
    public long runReads() {
        return runReads;
    }

    public long lookups() {
        return lookups;
    }

    public int memoryEntries() {
        return table.size();
    }

    /** Entries of the pinned and spilled runs, not counting what the in-memory table holds. */
    public long runEntries() {
        long total = 0;
        for (Run run : runs) total += run.entries;
        return total;
    }

    @Override
    public void close() throws IOException {
        IOException failure = null;
        for (Run run : runs) {
            try {
                run.close();
            } catch (IOException error) {
                if (failure == null) failure = error;
                else failure.addSuppressed(error);
            }
        }
        if (failure != null) throw failure;
    }

    private Path nextPath() throws IOException {
        if (resolved == null) resolved = restored != null ? restored : directory.get();
        if (!directoryCreated) {
            Files.createDirectories(resolved);
            directoryCreated = true;
        }
        return resolved.resolve("run-%08d.spill".formatted(nextRun++));
    }

    private static long sequence(String name) {
        return Long.parseLong(name.substring("run-".length(), name.length() - ".spill".length()));
    }

    // ---- scans ----

    /** One source of a scan, positioned on an entry after {@link #advance}. */
    private interface Source extends Closeable {
        boolean advance() throws IOException;

        byte[] key();

        byte[] value();
    }

    private final class Scan implements Entries {
        private final List<Source> sources = new ArrayList<>();
        private final PriorityQueue<Ranked> queue;
        private final byte[] from, to;
        private final boolean descending;
        private byte[] key, value;

        private record Ranked(Source source, int age) {}

        Scan(byte[] from, byte[] to, boolean descending) throws IOException {
            this.from = from;
            this.to = to;
            this.descending = descending;
            queue =
                    new PriorityQueue<>(
                            (a, b) -> {
                                int order = Arrays.compareUnsigned(a.source().key(), b.source().key());
                                if (descending) order = -order;
                                return order != 0 ? order : Integer.compare(b.age(), a.age());
                            });
            open(from, to);
        }

        private void open(byte[] lower, byte[] upper) throws IOException {
            try {
                int age = 0;
                for (Run run : runs) add(run.source(SpillingMap.this, lower, upper, descending), age++);
                add(new MemorySource(lower, upper, descending), age);
            } catch (IOException | RuntimeException error) {
                close();
                throw error;
            }
        }

        @Override
        public void seek(byte[] key) throws IOException {
            close();
            sources.clear();
            queue.clear();
            if (descending) open(from, to != null && Arrays.compareUnsigned(to, key) < 0 ? to : key);
            else open(from != null && Arrays.compareUnsigned(from, key) > 0 ? from : key, to);
        }

        private void add(Source source, int age) throws IOException {
            sources.add(source);
            if (source.advance()) queue.add(new Ranked(source, age));
        }

        @Override
        public boolean next() throws IOException {
            Ranked top = queue.poll();
            if (top == null) return false;
            key = top.source().key();
            value = top.source().value();
            // Older sources holding the same key are shadowed by the one just taken.
            while (!queue.isEmpty()
                    && Arrays.equals(queue.peek().source().key(), key)) {
                Ranked older = queue.poll();
                if (older.source().advance()) queue.add(older);
            }
            if (top.source().advance()) queue.add(top);
            return true;
        }

        @Override
        public byte[] key() {
            return key;
        }

        @Override
        public byte[] value() {
            return value;
        }

        @Override
        public void close() throws IOException {
            IOException failure = null;
            for (Source source : sources) {
                try {
                    source.close();
                } catch (IOException error) {
                    if (failure == null) failure = error;
                    else failure.addSuppressed(error);
                }
            }
            if (failure != null) throw failure;
        }
    }

    private final class MemorySource implements Source {
        private final java.util.Iterator<Map.Entry<byte[], byte[]>> iterator;
        private Map.Entry<byte[], byte[]> current;

        MemorySource(byte[] from, byte[] to, boolean descending) {
            NavigableMap<byte[], byte[]> view;
            if (from == null && to == null) view = table;
            else if (from == null) view = table.headMap(to, false);
            else if (to == null) view = table.tailMap(from, true);
            else if (Arrays.compareUnsigned(from, to) >= 0) view = new TreeMap<>(table.comparator());
            else view = table.subMap(from, true, to, false);
            iterator = (descending ? view.descendingMap() : view).entrySet().iterator();
        }

        @Override
        public boolean advance() {
            if (!iterator.hasNext()) return false;
            current = iterator.next();
            return true;
        }

        @Override
        public byte[] key() {
            return current.getKey();
        }

        @Override
        public byte[] value() {
            return current.getValue();
        }

        @Override
        public void close() {}
    }

    // ---- run files ----

    private static final class Run implements Closeable {
        final Path path;
        final long entries, dataEnd;
        final int level;
        final boolean pinned;
        final byte[] min, max;
        final byte[][] indexKeys;
        final long[] indexOffsets, bloom;
        private FileChannel channel;

        Run(
                Path path,
                int level,
                boolean pinned,
                long entries,
                long dataEnd,
                byte[] min,
                byte[] max,
                byte[][] indexKeys,
                long[] indexOffsets,
                long[] bloom) {
            this.path = path;
            this.level = level;
            this.pinned = pinned;
            this.entries = entries;
            this.dataEnd = dataEnd;
            this.min = min;
            this.max = max;
            this.indexKeys = indexKeys;
            this.indexOffsets = indexOffsets;
            this.bloom = bloom;
        }

        FileChannel channel() throws IOException {
            if (channel == null) channel = FileChannel.open(path, StandardOpenOption.READ);
            return channel;
        }

        /** The sparse-index block that may hold the key: the last whose first key is not above. */
        private int blockAtOrBelow(byte[] key) {
            int low = 0, high = indexKeys.length - 1, at = 0;
            while (low <= high) {
                int middle = (low + high) >>> 1;
                if (Arrays.compareUnsigned(indexKeys[middle], key) <= 0) {
                    at = middle;
                    low = middle + 1;
                } else high = middle - 1;
            }
            return at;
        }

        private long blockEnd(int block) {
            return block + 1 < indexOffsets.length ? indexOffsets[block + 1] : dataEnd;
        }

        byte[] find(byte[] key, SpillingMap owner) throws IOException {
            if (Arrays.compareUnsigned(key, min) < 0 || Arrays.compareUnsigned(key, max) > 0)
                return null;
            if (bloom != null && !mayContain(bloom, key)) return null;
            int at = blockAtOrBelow(key);
            owner.runReads++;
            var reader = new EntryReader(channel(), indexOffsets[at], blockEnd(at), owner.scratch);
            try {
                while (reader.next()) {
                    int order = Arrays.compareUnsigned(reader.key, key);
                    if (order == 0) return reader.value;
                    if (order > 0) return null;
                }
                return null;
            } finally {
                owner.scratch = reader.buffer;
            }
        }

        /** The greatest entry at or below the key; it lies in the key's own block. */
        Map.Entry<byte[], byte[]> floor(byte[] key, SpillingMap owner) throws IOException {
            if (Arrays.compareUnsigned(key, min) < 0) return null;
            int at = blockAtOrBelow(key);
            owner.runReads++;
            var reader = new EntryReader(channel(), indexOffsets[at], blockEnd(at), owner.scratch);
            Map.Entry<byte[], byte[]> best = null;
            try {
                while (reader.next()) {
                    if (Arrays.compareUnsigned(reader.key, key) > 0) break;
                    best = Map.entry(reader.key, reader.value);
                }
                return best;
            } finally {
                owner.scratch = reader.buffer;
            }
        }

        Source source(SpillingMap owner, byte[] from, byte[] to, boolean descending)
                throws IOException {
            return descending ? new ReverseSource(this, from, to) : new ForwardSource(this, from, to);
        }

        static Run open(Path path, int level, boolean pinned) throws IOException {
            try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
                long size = channel.size();
                if (size < 24) throw new IOException("Truncated spill run: " + path);
                ByteBuffer trailer = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
                readFully(channel, trailer, size - 16);
                long footer = trailer.getLong();
                if (trailer.getInt() != MAGIC) throw new IOException("Not a spill run: " + path);
                if (footer < 8 || footer > size - 16) throw new IOException("Corrupt spill run: " + path);
                ByteBuffer body =
                        ByteBuffer.allocate((int) (size - 16 - footer)).order(ByteOrder.LITTLE_ENDIAN);
                readFully(channel, body, footer);
                long entries = body.getLong(), dataEnd = body.getLong();
                byte[] min = bytes(body), max = bytes(body);
                int count = body.getInt();
                byte[][] keys = new byte[count][];
                long[] offsets = new long[count];
                for (int i = 0; i < count; i++) {
                    keys[i] = bytes(body);
                    offsets[i] = body.getLong();
                }
                int words = body.getInt();
                long[] bloom = null;
                if (words > 0) {
                    bloom = new long[words];
                    for (int i = 0; i < words; i++) bloom[i] = body.getLong();
                }
                return new Run(path, level, pinned, entries, dataEnd, min, max, keys, offsets, bloom);
            }
        }

        @Override
        public void close() throws IOException {
            if (channel != null) {
                channel.close();
                channel = null;
            }
        }
    }

    private static byte[] bytes(ByteBuffer from) {
        byte[] result = new byte[from.getInt()];
        from.get(result);
        return result;
    }

    private static void readFully(FileChannel channel, ByteBuffer target, long position)
            throws IOException {
        while (target.hasRemaining()) {
            int read = channel.read(target, position + target.position());
            if (read < 0) throw new IOException("Truncated spill run");
        }
        target.flip();
    }

    /** Sequential entries of a run span; each {@link #next} replaces key and value. */
    private static final class EntryReader {
        private final FileChannel channel;
        private final long end;
        private ByteBuffer buffer;
        private long position;
        byte[] key, value;

        EntryReader(FileChannel channel, long start, long end, ByteBuffer buffer) {
            this.channel = channel;
            this.position = start;
            this.end = end;
            this.buffer = buffer;
            buffer.clear().limit(0);
        }

        boolean next() throws IOException {
            if (position >= end) return false;
            ensure(8);
            int keyLength = buffer.getInt(buffer.position());
            int valueLength = buffer.getInt(buffer.position() + 4);
            ensure(8 + keyLength + valueLength);
            buffer.getInt();
            buffer.getInt();
            key = new byte[keyLength];
            buffer.get(key);
            value = new byte[valueLength];
            buffer.get(value);
            position += 8L + keyLength + valueLength;
            return true;
        }

        /** Makes at least {@code bytes} bytes readable, refilling from the file when needed. */
        private void ensure(int bytes) throws IOException {
            if (buffer.remaining() >= bytes) return;
            if (buffer.capacity() < bytes)
                buffer = ByteBuffer.allocate(bytes + LOOKUP_BYTES).order(ByteOrder.LITTLE_ENDIAN);
            long from = position;
            buffer.clear();
            while (buffer.position() < bytes) {
                int read = channel.read(buffer, from + buffer.position());
                if (read < 0) throw new IOException("Truncated spill run entry");
            }
            buffer.flip();
        }
    }

    /** A scan source reading a run forward from the block holding {@code from}. */
    private static final class ForwardSource implements Source {
        private final FileChannel channel;
        private final EntryReader reader;
        private final byte[] from, to;

        ForwardSource(Run run, byte[] from, byte[] to) throws IOException {
            this.channel = FileChannel.open(run.path, StandardOpenOption.READ);
            this.from = from;
            this.to = to;
            long start = from == null ? 8 : run.indexOffsets[run.blockAtOrBelow(from)];
            reader =
                    new EntryReader(
                            channel,
                            start,
                            run.dataEnd,
                            ByteBuffer.allocate(STREAM_BYTES).order(ByteOrder.LITTLE_ENDIAN));
        }

        @Override
        public boolean advance() throws IOException {
            while (reader.next()) {
                if (from != null && Arrays.compareUnsigned(reader.key, from) < 0) continue;
                return to == null || Arrays.compareUnsigned(reader.key, to) < 0;
            }
            return false;
        }

        @Override
        public byte[] key() {
            return reader.key;
        }

        @Override
        public byte[] value() {
            return reader.value;
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }
    }

    /** A scan source reading a run backwards one sparse-index block at a time. */
    private static final class ReverseSource implements Source {
        private final Run run;
        private final FileChannel channel;
        private final byte[] from, to;
        private final ByteBuffer buffer =
                ByteBuffer.allocate(STREAM_BYTES).order(ByteOrder.LITTLE_ENDIAN);
        private List<byte[][]> block = List.of();
        private int blockIndex, at = -1;
        private byte[][] current;

        ReverseSource(Run run, byte[] from, byte[] to) throws IOException {
            this.run = run;
            this.channel = FileChannel.open(run.path, StandardOpenOption.READ);
            this.from = from;
            this.to = to;
            // The last block whose first key is below the bound holds the last entry below it.
            int last = run.indexKeys.length - 1;
            if (to != null) {
                int low = 0, high = last, found = -1;
                while (low <= high) {
                    int middle = (low + high) >>> 1;
                    if (Arrays.compareUnsigned(run.indexKeys[middle], to) < 0) {
                        found = middle;
                        low = middle + 1;
                    } else high = middle - 1;
                }
                last = found;
            }
            blockIndex = last;
            if (blockIndex >= 0) load();
        }

        private void load() throws IOException {
            var reader =
                    new EntryReader(
                            channel, run.indexOffsets[blockIndex], run.blockEnd(blockIndex), buffer);
            block = new ArrayList<>();
            while (reader.next()) block.add(new byte[][] {reader.key, reader.value});
            at = block.size();
        }

        @Override
        public boolean advance() throws IOException {
            while (blockIndex >= 0) {
                while (--at >= 0) {
                    byte[][] entry = block.get(at);
                    if (to != null && Arrays.compareUnsigned(entry[0], to) >= 0) continue;
                    if (from != null && Arrays.compareUnsigned(entry[0], from) < 0) return false;
                    current = entry;
                    return true;
                }
                if (--blockIndex >= 0) load();
            }
            return false;
        }

        @Override
        public byte[] key() {
            return current[0];
        }

        @Override
        public byte[] value() {
            return current[1];
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }
    }

    /** A merge input: the run's entries in order, tagged with its age. */
    private static final class RunCursor implements Closeable {
        final int order;
        private final FileChannel channel;
        private final EntryReader reader;
        byte[] key, value;

        RunCursor(Run run, int order) throws IOException {
            this.order = order;
            this.channel = FileChannel.open(run.path, StandardOpenOption.READ);
            this.reader =
                    new EntryReader(
                            channel,
                            8,
                            run.dataEnd,
                            ByteBuffer.allocate(STREAM_BYTES).order(ByteOrder.LITTLE_ENDIAN));
        }

        boolean advance() throws IOException {
            if (!reader.next()) return false;
            key = reader.key;
            value = reader.value;
            return true;
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }
    }

    /** Writes strictly ascending entries, then the sparse index, the bounds and the filter. */
    private static final class RunWriter {
        private final Path path;
        private final int level;
        private final boolean pinned;
        private final DataOutputStream out;
        private final List<byte[]> indexKeys = new ArrayList<>();
        private final List<Long> indexOffsets = new ArrayList<>();
        private final long[] bloom;
        private final long bloomBits, expected;
        private long entries, offset = 8;
        private byte[] first, last;

        RunWriter(Path path, int level, boolean pinned, long expected) throws IOException {
            this.path = path;
            this.level = level;
            this.pinned = pinned;
            long bits = expected > 0 && expected <= BLOOM_LIMIT ? expected * BLOOM_BITS_PER_KEY : 0;
            this.expected = expected;
            this.bloom = bits == 0 ? null : new long[(int) ((bits + 63) / 64)];
            // Insertion and lookup must size the filter alike: the whole words.
            this.bloomBits = bloom == null ? 0 : bloom.length * 64L;
            out =
                    new DataOutputStream(
                            new BufferedOutputStream(
                                    Files.newOutputStream(
                                            path,
                                            StandardOpenOption.CREATE_NEW,
                                            StandardOpenOption.WRITE),
                                    1 << 16));
            out.write(littleInt(MAGIC));
            out.write(littleInt(FORMAT));
        }

        void append(byte[] key, byte[] value) throws IOException {
            if (last != null && Arrays.compareUnsigned(last, key) >= 0)
                throw new IllegalStateException("Run keys must be strictly ascending");
            if (entries % INDEX_INTERVAL == 0) {
                indexKeys.add(key);
                indexOffsets.add(offset);
            }
            out.write(littleInt(key.length));
            out.write(littleInt(value.length));
            out.write(key);
            out.write(value);
            offset += 8L + key.length + value.length;
            if (first == null) first = key;
            last = key;
            entries++;
            if (bloom != null) {
                if (entries > expected)
                    throw new IllegalStateException("More entries than the run was sized for");
                addToBloom(bloom, bloomBits, key);
            }
        }

        Run finish() throws IOException {
            if (entries == 0) {
                abort();
                return null;
            }
            long dataEnd = offset;
            var footer = new java.io.ByteArrayOutputStream();
            var body = new DataOutputStream(footer);
            body.write(littleLong(entries));
            body.write(littleLong(dataEnd));
            body.write(littleInt(first.length));
            body.write(first);
            body.write(littleInt(last.length));
            body.write(last);
            body.write(littleInt(indexKeys.size()));
            for (int i = 0; i < indexKeys.size(); i++) {
                body.write(littleInt(indexKeys.get(i).length));
                body.write(indexKeys.get(i));
                body.write(littleLong(indexOffsets.get(i)));
            }
            body.write(littleInt(bloom == null ? 0 : bloom.length));
            if (bloom != null) for (long word : bloom) body.write(littleLong(word));
            out.write(footer.toByteArray());
            // The footer starts where the entries end.
            out.write(littleLong(dataEnd));
            out.write(littleInt(MAGIC));
            out.write(littleInt(0));
            out.close();
            return new Run(
                    path,
                    level,
                    pinned,
                    entries,
                    dataEnd,
                    first,
                    last,
                    indexKeys.toArray(new byte[0][]),
                    indexOffsets.stream().mapToLong(Long::longValue).toArray(),
                    bloom);
        }

        void abort() throws IOException {
            out.close();
            Files.deleteIfExists(path);
        }
    }

    private static byte[] littleInt(int value) {
        return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array();
    }

    private static byte[] littleLong(long value) {
        return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array();
    }

    // ---- bloom filter ----

    private static long hash(byte[] key) {
        long h = 0xcbf29ce484222325L;
        for (byte b : key) {
            h ^= b & 0xff;
            h *= 0x100000001b3L;
        }
        h ^= h >>> 33;
        h *= 0xff51afd7ed558ccdL;
        h ^= h >>> 33;
        return h;
    }

    private static void addToBloom(long[] bloom, long bits, byte[] key) {
        long h = hash(key), step = (h >>> 32) | 1;
        for (int i = 0; i < 7; i++) {
            long bit = Long.remainderUnsigned(h + i * step, bits);
            bloom[(int) (bit >>> 6)] |= 1L << (bit & 63);
        }
    }

    private static boolean mayContain(long[] bloom, byte[] key) {
        long bits = (long) bloom.length * 64, h = hash(key), step = (h >>> 32) | 1;
        for (int i = 0; i < 7; i++) {
            long bit = Long.remainderUnsigned(h + i * step, bits);
            if ((bloom[(int) (bit >>> 6)] & (1L << (bit & 63))) == 0) return false;
        }
        return true;
    }
}
