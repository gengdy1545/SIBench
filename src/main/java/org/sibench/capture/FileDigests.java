package org.sibench.capture;

import org.sibench.generation.BaselineGenerator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.ConcurrentHashMap;

/**
 * SHA256 of capture and baseline files, computed once per process. One stage checks the same
 * immutable inputs several times (the run manifest, the row audit binding, the per-session
 * report); a file whose identity, size and modification time are unchanged since its digest was
 * taken is not read again. A check that must observe a change during its own run (a source
 * that changed while it was being read) uses {@link BaselineGenerator#sha256} directly.
 */
public final class FileDigests {
    private record Identity(Path path, Object key, long size, long modified) {}

    private static final ConcurrentHashMap<Identity, String> DIGESTS = new ConcurrentHashMap<>();

    private FileDigests() {}

    public static String sha256(Path file) throws IOException {
        Path path = file.toAbsolutePath().normalize();
        var attributes = Files.readAttributes(path, BasicFileAttributes.class);
        var modified = attributes.lastModifiedTime().toInstant();
        var identity =
                new Identity(
                        path,
                        attributes.fileKey(),
                        attributes.size(),
                        modified.getEpochSecond() * 1_000_000_000L + modified.getNano());
        String known = DIGESTS.get(identity);
        if (known != null) return known;
        String digest = BaselineGenerator.sha256(path);
        DIGESTS.put(identity, digest);
        return digest;
    }
}
