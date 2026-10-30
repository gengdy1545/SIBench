package org.sibench.capture;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.DigestInputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * The source work-unit catalog of a trace, one JSON line per source SQL request, stored as a zstd
 * stream. Its digest ({@code work_units_sha256}) is that of the plain lines, so it does not depend
 * on the compressor.
 *
 * <p>The catalog is read once, in order: identities are contiguous from 1 and source events
 * increase. Only each unit's first source event is kept, because checking an operation's work unit
 * must not retain every JSON line or a boxed map of all units.
 */
public final class WorkUnits {
    static final String FILE = "work-units.jsonl.zst";

    private WorkUnits() {}

    /** The catalog being written and the digest of its plain lines, final once it is closed. */
    record Output(BufferedWriter lines, MessageDigest digest) {}

    /** The checked catalog: its plain-line digest and the first source event of each unit. */
    public record Catalog(String sha256, long[] firstEvents, long count) {
        /** The first source event of a unit, or -1 when the identity is outside the catalog. */
        public long firstEvent(long workUnitId) {
            return workUnitId < 1 || workUnitId > count ? -1 : firstEvents[(int) workUnitId];
        }
    }

    static Output create(Path trace) throws IOException {
        MessageDigest digest = sha256();
        var file = Files.newOutputStream(trace.resolve(FILE), StandardOpenOption.CREATE_NEW);
        var compressed = new ZstdOutputStream(new BufferedOutputStream(file, 1 << 20), IndexOperationTrace.ZSTD_LEVEL);
        var lines = new BufferedWriter(
                new OutputStreamWriter(new DigestOutputStream(compressed, digest), StandardCharsets.UTF_8), 1 << 16);
        return new Output(lines, digest);
    }

    private static InputStream open(Path trace) throws IOException {
        return new ZstdInputStream(
                new BufferedInputStream(Files.newInputStream(trace.resolve(FILE)), 1 << 20));
    }

    /**
     * Reads and checks the catalog once. Unit identities must be {@code 1..count}, source events
     * strictly increasing, and each unit must name a process and application.
     */
    public static Catalog read(Path trace) throws IOException {
        MessageDigest digest = sha256();
        long[] events = new long[1024];
        long count = 0, lastEvent = 0;
        try (var input = new DigestInputStream(open(trace), digest);
                var reader =
                        new BufferedReader(
                                new InputStreamReader(input, StandardCharsets.UTF_8), 1 << 16)) {
            for (String line; (line = reader.readLine()) != null; ) {
                long id = 0, event = 0;
                int pid = 0, fields = 0;
                String application = "";
                // Hash all original bytes, but avoid a JSON tree for fields the catalog never retains.
                try (var unit = new JsonReader(new java.io.StringReader(line))) {
                    unit.beginObject();
                    while (unit.hasNext()) {
                        switch (unit.nextName()) {
                            case "work_unit_id" -> { id = unit.nextLong(); fields |= 1; }
                            case "source_event" -> { event = unit.nextLong(); fields |= 2; }
                            case "pid" -> { pid = unit.nextInt(); fields |= 4; }
                            case "application" -> { application = unit.nextString(); fields |= 8; }
                            default -> unit.skipValue();
                        }
                    }
                    unit.endObject();
                    if (unit.peek() != JsonToken.END_DOCUMENT) throw new IOException("Extra work-unit JSON value");
                } catch (IllegalStateException | NumberFormatException invalid) {
                    throw new IOException("Invalid source work-unit JSON", invalid);
                }
                if (id != count + 1
                        || event <= lastEvent
                        || fields != 15 || pid < 1 || application.isEmpty())
                    throw new IOException("Invalid source work-unit catalog order/identity");
                if (id > Integer.MAX_VALUE - 8)
                    throw new IOException("Source work-unit catalog exceeds the in-memory index range");
                if (id >= events.length)
                    events = java.util.Arrays.copyOf(events, Math.toIntExact(Math.max(id + 1, events.length * 2)));
                events[(int) id] = event;
                count = id;
                lastEvent = event;
            }
        }
        return new Catalog(HexFormat.of().formatHex(digest.digest()), events, count);
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }
}
