package org.sibench.capture;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;

/** Pending source transactions in reclaimable, per-backend compressed segments. */
final class CaptureTransactions implements AutoCloseable {
    @FunctionalInterface interface Events { void accept(Event event) throws IOException; }
    @FunctionalInterface interface Watermark { long read() throws Exception; }
    private static final int MEMORY_BYTES = 256 << 10;
    private static final long PENDING_MEMORY_BYTES = 64L << 20;
    private static final long SEGMENT_BYTES = 8L << 20;
    private static final int DIAGNOSTIC_TRANSACTIONS = 65_536;
    private final Path directory;
    private final long limit;
    private final int transactionLimit;
    private final PriorityQueue<Chunk> ready = new PriorityQueue<>(Comparator
            .comparingLong((Chunk c) -> c.csn).thenComparing(c -> !c.writer)
            .thenComparingLong(c -> c.sequence));
    private final Set<Chunk> pending = new HashSet<>();
    private final Map<Integer, Segment> tails = new HashMap<>();
    private long segmentSequence;
    private long bytes, memoryBytes, peakBytes, peakMemoryBytes, sequence, safe = -1;
    private int peakTransactions;
    private Throwable failure;
    private boolean finished;

    CaptureTransactions(Path directory, long limit, int transactionLimit) throws IOException {
        if (limit < 1 || transactionLimit < 1) throw new IllegalArgumentException("Positive transaction budgets required");
        this.directory = Files.createDirectory(directory);
        this.limit = limit;
        this.transactionLimit = transactionLimit;
    }

    synchronized Chunk begin(int pid) throws IOException {
        check();
        if (finished || pending.size() >= transactionLimit)
            throw new IOException("Pending source transaction limit reached");
        var chunk = new Chunk(pid, ++sequence);
        pending.add(chunk);
        peakTransactions = Math.max(peakTransactions, pending.size());
        return chunk;
    }

    private synchronized void reserve(long additional) throws IOException {
        check();
        if (additional > limit - bytes) throw new IOException("Pending source transaction byte budget reached");
        bytes += additional;
        peakBytes = Math.max(peakBytes, bytes);
    }

    private synchronized boolean keepInMemory(Chunk chunk, int cost) {
        if (chunk.bytes > MEMORY_BYTES || cost > PENDING_MEMORY_BYTES - memoryBytes) return false;
        memoryBytes += cost;
        chunk.memoryBytes += cost;
        peakMemoryBytes = Math.max(peakMemoryBytes, memoryBytes);
        return true;
    }

    private synchronized void releaseMemory(Chunk chunk) {
        memoryBytes -= chunk.memoryBytes;
        chunk.memoryBytes = 0;
    }

    synchronized Map<String, Object> metrics() {
        return Map.of("encoded_byte_limit", limit, "transaction_limit", transactionLimit,
                "memory_encoded_byte_limit", PENDING_MEMORY_BYTES,
                "spool_segment_target_bytes", SEGMENT_BYTES,
                "peak_encoded_bytes", peakBytes, "peak_memory_encoded_bytes", peakMemoryBytes,
                "peak_transactions", peakTransactions, "pending_encoded_bytes", bytes,
                "pending_memory_encoded_bytes", memoryBytes, "pending_transactions", pending.size());
    }

    void complete(Chunk chunk, long csn, boolean writer, boolean committed) throws IOException {
        chunk.seal();
        synchronized (this) {
            check();
            chunk.sealed = true;
            chunk.csn = csn;
            chunk.writer = writer;
            chunk.committed = committed;
            ready.add(chunk);
            chunk.queued = true;
            notifyAll();
        }
    }

    /** A watermark is read only after source completion facts have reached this process. */
    Chunk next(Watermark watermark) throws Exception {
        while (true) {
            synchronized (this) {
                check();
                if (finished) return ready.isEmpty() ? null : takeReady();
                if (!ready.isEmpty() && ready.peek().csn <= safe) return takeReady();
            }
            long observed = watermark.read();
            synchronized (this) {
                check();
                // Later source completions cannot invalidate a position already certified safe.
                safe = Math.max(safe, observed);
                if (!ready.isEmpty() && (finished || ready.peek().csn <= safe)) return takeReady();
                wait(20);
            }
        }
    }

    private Chunk takeReady() {
        var chunk = ready.remove();
        chunk.queued = false;
        return chunk;
    }

    void finish() throws IOException {
        List<Integer> pids;
        synchronized (this) {
            check();
            if (pending.stream().anyMatch(chunk -> !chunk.sealed)) throw new IOException("Capture ends with unfinished transactions");
            finished = true;
            pids = List.copyOf(tails.keySet());
            notifyAll();
        }
        for (int pid : pids) endStream(pid);
    }

    /** One producer owns a backend's frames; consumers can read completed frames concurrently. */
    private Segment segment(int pid) throws IOException {
        Segment result;
        synchronized (this) { result = tails.get(pid); }
        if (result != null && result.channel.position() >= SEGMENT_BYTES) {
            endStream(pid);
            result = null;
        }
        if (result == null) {
            long id;
            synchronized (this) { id = ++segmentSequence; }
            result = new Segment(directory.resolve("segment-" + id + ".events.zst"));
            synchronized (this) { tails.put(pid, result); }
        }
        synchronized (this) {
            if (result.writing) throw new IOException("Overlapping spool writers for one source backend");
            result.writing = true;
            result.references++;
        }
        return result;
    }

    void endStream(int pid) throws IOException {
        Segment segment;
        synchronized (this) {
            segment = tails.get(pid);
            if (segment == null) return;
            if (segment.writing) throw new IOException("Source spool ends inside a transaction");
            tails.remove(pid);
        }
        segment.output.close();
        boolean remove;
        synchronized (this) {
            segment.retired = true;
            remove = segment.references == 0 && !segment.deleted;
            if (remove) segment.deleted = true;
        }
        if (remove) Files.delete(segment.file);
    }

    private final class Segment {
        final Path file;
        final FileChannel channel;
        final BufferedWriter output;
        int references;
        boolean writing, retired, deleted;

        Segment(Path file) throws IOException {
            this.file = file;
            channel = FileChannel.open(file, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
            try {
                var compressed = new com.github.luben.zstd.ZstdOutputStream(new BufferedOutputStream(
                        Channels.newOutputStream(channel)), com.github.luben.zstd.RecyclingBufferPool.INSTANCE, 1)
                        .setChecksum(true).setCloseFrameOnFlush(true);
                output = new BufferedWriter(new OutputStreamWriter(compressed, StandardCharsets.UTF_8));
            } catch (IOException | RuntimeException | Error error) { channel.close(); throw error; }
        }

        void release() throws IOException {
            boolean remove;
            synchronized (CaptureTransactions.this) {
                references--;
                remove = retired && references == 0 && !deleted;
                if (remove) deleted = true;
            }
            if (remove) Files.delete(file);
        }
    }

    private static final class FrameInput extends FilterInputStream {
        private long remaining;
        FrameInput(InputStream input, long length) { super(input); remaining = length; }
        @Override public int read() throws IOException {
            if (remaining == 0) return -1;
            int value = in.read();
            if (value < 0) throw new EOFException("Truncated transaction frame");
            remaining--;
            return value;
        }
        @Override public int read(byte[] bytes, int offset, int length) throws IOException {
            if (length == 0) return 0;
            if (remaining == 0) return -1;
            int count = in.read(bytes, offset, (int) Math.min(remaining, length));
            if (count < 0) throw new EOFException("Truncated transaction frame");
            remaining -= count;
            return count;
        }
    }

    synchronized void fail(Throwable error) {
        if (failure == null) failure = error;
        notifyAll();
    }

    private void check() throws IOException {
        if (failure != null) throw new IOException("Direct capture failed", failure);
    }

    final class Chunk implements AutoCloseable {
        final int pid;
        final long sequence;
        long csn, bytes, memoryBytes, lines;
        boolean writer, committed;
        private List<Event> events = new ArrayList<>();
        private Segment segment;
        private long offset, length;
        private BufferedWriter output;
        private boolean sealed, closed, queued;

        Chunk(int pid, long sequence) { this.pid = pid; this.sequence = sequence; }

        /** Reuses the receiver's UTF-8 byte count, including the newline. */
        void append(String line, com.google.gson.JsonObject event, int cost) throws IOException {
            if (sealed) throw new IOException("Source transaction is already complete");
            reserve(cost);
            bytes += cost;
            lines++;
            if (output == null && !keepInMemory(this, cost)) {
                segment = segment(pid);
                offset = segment.channel.position();
                output = segment.output;
                for (Event kept : events) {
                    output.write(kept.json().toString());
                    output.newLine();
                }
                events = null;
                releaseMemory(this);
            }
            if (output == null) events.add(Event.of(event));
            else { output.write(line); output.newLine(); }
        }

        private void seal() throws IOException {
            if (sealed) throw new IOException("Repeated source transaction completion");
            if (output != null) {
                output.flush();
                length = segment.channel.position() - offset;
                output = null;
                synchronized (CaptureTransactions.this) { segment.writing = false; }
            }
        }

        void read(Events consumer) throws IOException {
            if (!sealed) throw new IOException("Source outcome is not known");
            if (segment == null) { for (Event event : events) consumer.accept(event); }
            else try (var channel = FileChannel.open(segment.file, StandardOpenOption.READ);
                    var input = new BufferedReader(new InputStreamReader(
                            new com.github.luben.zstd.ZstdInputStream(new FrameInput(
                                    Channels.newInputStream(channel.position(offset)), length),
                                    com.github.luben.zstd.RecyclingBufferPool.INSTANCE), StandardCharsets.UTF_8))) {
                for (String line; (line = input.readLine()) != null; ) consumer.accept(Event.parse(line));
            }
        }

        @Override public void close() throws IOException {
            synchronized (CaptureTransactions.this) {
                if (closed) return;
                closed = true;
                // Detach before deleting so a cancelled queued chunk cannot be claimed.
                if (queued) { ready.remove(this); queued = false; }
            }
            if (output != null) seal();
            if (segment != null) segment.release();
            if (events != null) events.clear();
            synchronized (CaptureTransactions.this) {
                releaseMemory(this);
                CaptureTransactions.this.bytes -= bytes;
                pending.remove(this);
                CaptureTransactions.this.notifyAll();
            }
        }
    }

    synchronized void retainFailure() throws IOException {
        for (Chunk chunk : pending) {
            if (chunk.output != null) chunk.seal();
        }
        for (int pid : List.copyOf(tails.keySet())) endStream(pid);
        try (var output = new com.google.gson.stream.JsonWriter(Files.newBufferedWriter(
                directory.resolve("failure.json"), StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW))) {
            output.beginObject().name("error").value(String.valueOf(failure)).name("budgets");
            new com.google.gson.Gson().toJson(metrics(), Map.class, output);
            output.name("pending_transactions_truncated").value(pending.size() > DIAGNOSTIC_TRANSACTIONS);
            output.name("pending_transactions").beginArray();
            int details = 0;
            for (Chunk chunk : pending) {
                if (details++ == DIAGNOSTIC_TRANSACTIONS) break;
                output.beginObject().name("pid").value(chunk.pid).name("bytes").value(chunk.bytes)
                        .name("lines").value(chunk.lines).name("complete").value(chunk.sealed)
                        .name("file").value(chunk.segment == null ? "in_memory" : chunk.segment.file.getFileName().toString())
                        .name("offset").value(chunk.offset).name("length").value(chunk.length).endObject();
            }
            output.endArray().endObject();
        }
    }

    @Override public synchronized void close() throws IOException {
        ready.clear();
        for (Chunk chunk : List.copyOf(pending)) { chunk.queued = false; chunk.close(); }
        for (int pid : List.copyOf(tails.keySet())) endStream(pid);
        Files.delete(directory);
    }
}
