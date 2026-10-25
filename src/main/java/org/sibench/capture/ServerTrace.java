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
 * The backend trace files of a capture: {@code pg-<pid>.jsonl}, or {@code pg-<pid>.jsonl.zst}
 * when the instrumentation wrote a zstd stream ({@code capture.compression: zstd}). Readers see the
 * same lines either way; a compressed file whose backend did not end its frame fails to read.
 */
public final class ServerTrace {
    private static final String PREFIX = "pg-", PLAIN = ".jsonl", ZSTD = ".jsonl.zst";

    private ServerTrace() {}

    public static boolean isServerTrace(String name) {
        return name.startsWith(PREFIX) && (name.endsWith(PLAIN) || name.endsWith(ZSTD));
    }

    /** The backend pid a trace file is named after. */
    public static int pid(String name) {
        if (!isServerTrace(name)) throw new IllegalArgumentException("Not a backend trace: " + name);
        String suffix = name.endsWith(ZSTD) ? ZSTD : PLAIN;
        return Integer.parseInt(name.substring(PREFIX.length(), name.length() - suffix.length()));
    }

    /** The trace file of one backend in a raw directory, whichever encoding it was written in. */
    public static Path locate(Path directory, int pid) throws IOException {
        Path plain = directory.resolve(PREFIX + pid + PLAIN), zstd = directory.resolve(PREFIX + pid + ZSTD);
        boolean hasPlain = Files.exists(plain), hasZstd = Files.exists(zstd);
        if (hasPlain && hasZstd) throw new IOException("Backend " + pid + " has both a plain and a zstd trace");
        return hasZstd ? zstd : plain;
    }

    public static BufferedReader open(Path file) throws IOException {
        if (!file.getFileName().toString().endsWith(ZSTD)) return Files.newBufferedReader(file);
        var input = new ZstdInputStream(new BufferedInputStream(Files.newInputStream(file), 1 << 20));
        return new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8), 1 << 16);
    }
}
