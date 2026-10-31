package org.sibench.capture;

import com.github.luben.zstd.ZstdInputStream;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * The backend trace files of a capture: {@code pg-<pid>.jsonl.zst}. A compressed file whose
 * backend did not end its frame fails to read. Client traces ({@code jdbc-*.jsonl.zst}) open the
 * same way.
 */
public final class ServerTrace {
    private static final String PREFIX = "pg-", ZSTD = ".jsonl.zst";

    private ServerTrace() {}

    static String detail(com.google.gson.JsonObject header) throws IOException {
        if (header == null || !header.has("event")
                || !header.get("event").getAsString().equals("capture_start"))
            throw new IOException("Trace does not start with capture_start");
        if (!header.has("pid") || header.get("pid").getAsInt() < 1)
            throw new IOException("Capture header lacks its backend identity");
        if (!header.has("detail") || !java.util.Set.of("rows", "summary")
                .contains(header.get("detail").getAsString()))
            throw new IOException("Unknown capture detail");
        return header.get("detail").getAsString();
    }

    public static boolean isServerTrace(String name) {
        return name.startsWith(PREFIX) && name.endsWith(ZSTD);
    }

    /** The backend pid a trace file is named after. */
    public static int pid(String name) {
        if (!isServerTrace(name)) throw new IllegalArgumentException("Not a backend trace: " + name);
        return Integer.parseInt(name.substring(PREFIX.length(), name.length() - ZSTD.length()));
    }

    /** The name a backend's trace has, whether or not it exists yet. */
    static Path path(Path directory, int pid) {
        return directory.resolve(PREFIX + pid + ZSTD);
    }

    /** The trace file of one backend in a raw directory. */
    public static Path locate(Path directory, int pid) throws IOException {
        Path file = path(directory, pid);
        if (!Files.isRegularFile(file)) throw new IOException("Backend trace not found: " + file);
        return file;
    }

    public static BufferedReader open(Path file) throws IOException {
        if (!file.getFileName().toString().endsWith(ZSTD))
            throw new IOException("Not a zstd backend trace: " + file);
        var input = new ZstdInputStream(new BufferedInputStream(Files.newInputStream(file), 1 << 20));
        return new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8), 1 << 16);
    }
}
