package org.sibench.capture;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;

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
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

/**
 * The source work-unit catalog of a trace, one JSON line per source SQL request, stored as a zstd
 * stream. Its digest ({@code work_units_sha256}) is that of the plain lines, so it does not depend
 * on the compressor.
 */
public final class WorkUnits {
    static final String FILE = "work-units.jsonl.zst";

    private WorkUnits() {}

    /** The catalog being written and the digest of its plain lines, final once it is closed. */
    record Output(BufferedWriter lines, MessageDigest digest) {}

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

    /** The catalog's lines, in order. */
    public static List<String> lines(Path trace) throws IOException {
        var result = new ArrayList<String>();
        try (var reader = new BufferedReader(new InputStreamReader(open(trace), StandardCharsets.UTF_8), 1 << 16)) {
            for (String line; (line = reader.readLine()) != null; ) result.add(line);
        }
        return result;
    }

    /** The SHA256 of the catalog's plain lines. */
    public static String sha256(Path trace) throws IOException {
        MessageDigest digest = sha256();
        try (var input = new DigestInputStream(open(trace), digest)) {
            input.transferTo(java.io.OutputStream.nullOutputStream());
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }
}
