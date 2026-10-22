package org.sibench.capture;

import com.google.gson.*;

import java.io.*;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.zip.CRC32C;

/**
 * Lossless execution-event container plus fixed-width access projection. Not a SQL replay engine.
 */
public final class PgTraceBlocks {
    private static final int MAGIC = 0x53494254,
            VERSION = 1,
            RECORD_BYTES = 64,
            BLOCK_EVENTS = 4096;
    private static final Map<String, Integer> TYPES =
            Map.ofEntries(
                    Map.entry("index_insert_attempt", 1),
                    Map.entry("scan_rescan", 2),
                    Map.entry("scan_next", 3),
                    Map.entry("heap_visibility", 4),
                    Map.entry("bitmap_candidate", 5),
                    Map.entry("bitmap_end", 6),
                    Map.entry("scan_end", 7),
                    Map.entry("index_build_start", 8),
                    Map.entry("index_build_end", 9),
                    Map.entry("transaction_commit", 10),
                    Map.entry("transaction_abort", 11),
                    Map.entry("row_change", 12),
                    Map.entry("executor_start", 13),
                    Map.entry("executor_end", 14),
                    Map.entry("result_row", 15),
                    Map.entry("executor_run_end", 16),
                    Map.entry("plan_scan", 17),
                    Map.entry("vm_visible", 18),
                    Map.entry("bitmap_heap_candidate", 19),
                    Map.entry("bitmap_heap_lossy", 20),
                    Map.entry("plan_bitmap_heap", 21));

    private static final int MAX_TYPE =
            TYPES.values().stream().mapToInt(Integer::intValue).max().orElseThrow();

    private PgTraceBlocks() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2)
            throw new IllegalArgumentException("PgTraceBlocks RAW_DIRECTORY OUTPUT_DIRECTORY");
        compile(Path.of(args[0]), Path.of(args[1]));
    }

    public static void compile(Path raw, Path output) throws Exception {
        Files.createDirectory(output);
        Files.writeString(
                output.resolve("INCOMPLETE"),
                "Raw trace compilation in progress\n",
                StandardOpenOption.CREATE_NEW);
        Map<String, Long> counts = new TreeMap<>();
        List<Map<String, Object>> inputs = new ArrayList<>();
        long sequence = 0;
        int blockCount = 0;
        ByteBuffer block =
                ByteBuffer.allocate(BLOCK_EVENTS * RECORD_BYTES).order(ByteOrder.LITTLE_ENDIAN);
        try (var channel =
                        FileChannel.open(
                                output.resolve("access.bin"),
                                StandardOpenOption.CREATE_NEW,
                                StandardOpenOption.WRITE);
                var payload =
                        new BufferedOutputStream(
                                Files.newOutputStream(
                                        output.resolve("payload.jsonl"),
                                        StandardOpenOption.CREATE_NEW))) {
            ByteBuffer header = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
            header.putInt(MAGIC).putInt(VERSION).putInt(RECORD_BYTES).putInt(BLOCK_EVENTS).flip();
            write(channel, header);
            long offset = 0;
            List<Path> paths;
            try (var files = Files.list(raw)) {
                paths =
                        files.filter(
                                        p ->
                                                p.getFileName().toString().startsWith("pg-")
                                                        && p.toString().endsWith(".jsonl"))
                                .sorted()
                                .toList();
            }
            for (Path path : paths) {
                long lines = 0;
                inputs.add(
                        Map.of(
                                "file",
                                path.getFileName().toString(),
                                "sha256",
                                org.sibench.generation.BaselineGenerator.sha256(path)));
                try (var reader = Files.newBufferedReader(path)) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        lines++;
                        JsonObject event = JsonParser.parseString(line).getAsJsonObject();
                        String type = event.get("event").getAsString();
                        Integer code = TYPES.get(type);
                        if (code == null) throw new IOException("Unknown event type: " + type);
                        byte[] bytes =
                                (line + "\n").getBytes(java.nio.charset.StandardCharsets.UTF_8);
                        int tidBlock = -1, tidOffset = -1;
                        if (event.has("tid")) {
                            String[] tid =
                                    event.get("tid")
                                            .getAsString()
                                            .replace("(", "")
                                            .replace(")", "")
                                            .split(",");
                            tidBlock = Integer.parseInt(tid[0]);
                            tidOffset = Integer.parseInt(tid[1]);
                        }
                        block.putLong(++sequence)
                                .putInt(code)
                                .putInt((int) number(event, "pid"))
                                .putLong(number(event, "xid"))
                                .putInt((int) number(event, "relation_oid"))
                                .putInt((int) number(event, "heap_oid"))
                                .putInt(tidBlock)
                                .putInt(tidOffset)
                                .putInt(
                                        event.has("found") && event.get("found").getAsBoolean()
                                                ? 1
                                                : 0)
                                .putInt((int) number(event, "direction"))
                                .putLong(offset)
                                .putInt(bytes.length)
                                .putInt((int) number(event, "cid"));
                        payload.write(bytes);
                        offset += bytes.length;
                        counts.merge(type, 1L, Long::sum);
                        if (!block.hasRemaining()) {
                            flush(channel, block);
                            blockCount++;
                        }
                    }
                }
                System.out.println(path.getFileName() + " events=" + lines);
            }
            if (block.position() > 0) {
                flush(channel, block);
                blockCount++;
            }
        }
        Map<String, Object> manifest = new LinkedHashMap<>();
        manifest.put("version", VERSION);
        manifest.put("events", sequence);
        manifest.put("blocks", blockCount);
        manifest.put("counts", counts);
        manifest.put("inputs", inputs);
        manifest.put("scope", "raw_access_projection_with_lossless_payload");
        manifest.put(
                "ordering",
                "input files sorted by filename, preserving per-file order; not a global"
                    + " transaction order");
        manifest.put("full_index_service_replay", false);
        manifest.put(
                "unsupported",
                List.of(
                        "PG visibility to service version mapping",
                        "transaction-local index effects",
                        "full secondary index result equivalence",
                        "bitmap lossy and residual filter attribution"));
        manifest.put(
                "access_sha256",
                org.sibench.generation.BaselineGenerator.sha256(output.resolve("access.bin")));
        manifest.put(
                "payload_sha256",
                org.sibench.generation.BaselineGenerator.sha256(output.resolve("payload.jsonl")));
        manifest.put("decode_benchmark", benchmark(output.resolve("access.bin"), sequence));
        Files.writeString(
                output.resolve("manifest.json"),
                new GsonBuilder().setPrettyPrinting().create().toJson(manifest));
        Files.delete(output.resolve("INCOMPLETE"));
    }

    private static long number(JsonObject object, String key) {
        return object.has(key) ? object.get(key).getAsLong() : 0;
    }

    private static void write(FileChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) channel.write(buffer);
    }

    private static void flush(FileChannel channel, ByteBuffer block) throws IOException {
        int length = block.position();
        CRC32C checksum = new CRC32C();
        checksum.update(block.array(), 0, length);
        ByteBuffer header = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN);
        header.putInt(length)
                .putInt(length / RECORD_BYTES)
                .putInt((int) checksum.getValue())
                .flip();
        write(channel, header);
        block.flip();
        write(channel, block);
        block.clear();
    }

    static long decode(Path file) throws IOException {
        long count = 0;
        try (var channel = FileChannel.open(file, StandardOpenOption.READ)) {
            ByteBuffer header = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
            read(channel, header);
            header.flip();
            if (header.getInt() != MAGIC
                    || header.getInt() != VERSION
                    || header.getInt() != RECORD_BYTES
                    || header.getInt() != BLOCK_EVENTS)
                throw new IOException("Unsupported trace header");
            ByteBuffer block =
                    ByteBuffer.allocate(BLOCK_EVENTS * RECORD_BYTES).order(ByteOrder.LITTLE_ENDIAN);
            while (channel.position() < channel.size()) {
                header.clear().limit(12);
                read(channel, header);
                header.flip();
                int bytes = header.getInt(), events = header.getInt(), expected = header.getInt();
                if (bytes <= 0
                        || bytes > block.capacity()
                        || bytes % RECORD_BYTES != 0
                        || events != bytes / RECORD_BYTES)
                    throw new IOException("Invalid block size");
                block.clear().limit(bytes);
                read(channel, block);
                block.flip();
                CRC32C crc = new CRC32C();
                crc.update(block.array(), 0, bytes);
                if ((int) crc.getValue() != expected)
                    throw new IOException("Block checksum mismatch");
                for (int i = 0; i < events; i++) {
                    long seq = block.getLong();
                    int type = block.getInt();
                    if (seq != ++count || type < 1 || type > MAX_TYPE)
                        throw new IOException("Invalid sequence or event");
                    block.position(block.position() + RECORD_BYTES - 12);
                }
            }
        }
        return count;
    }

    private static void read(FileChannel channel, ByteBuffer target) throws IOException {
        while (target.hasRemaining())
            if (channel.read(target) < 0) throw new EOFException("Truncated trace");
    }

    private static Map<String, Object> benchmark(Path file, long expected) throws IOException {
        if (decode(file) != expected) throw new IOException("Event count mismatch");
        long started = System.nanoTime(), decoded = 0;
        int passes = 0;
        do {
            decoded += decode(file);
            passes++;
        } while (System.nanoTime() - started < 2_000_000_000L);
        double seconds = (System.nanoTime() - started) / 1e9;
        return Map.of(
                "events_per_second",
                decoded / seconds,
                "seconds",
                seconds,
                "passes",
                passes,
                "scope",
                "warm sequential read + CRC + fixed-header decode; no payload parsing, dependency"
                    + " scheduling or backend RPC");
    }
}
