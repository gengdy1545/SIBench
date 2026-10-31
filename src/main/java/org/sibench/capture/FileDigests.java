package org.sibench.capture;

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
 * that changed while it was being read) uses {@link #read} directly.
 *
 * <p>Capture code digests files here rather than through the baseline generator, so the generator
 * and its Parquet writer stay outside the capture code a certification covers ({@link CaptureCode}).
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
        String digest = read(path);
        DIGESTS.put(identity, digest);
        return digest;
    }

    /**
     * Digests files concurrently into the cache: digesting is read-bound, and one reader at a time
     * leaves a disk far below its throughput.
     */
    static void prefetch(java.util.List<Path> files) throws IOException {
        var pool = java.util.concurrent.Executors.newFixedThreadPool(Math.min(8, Math.max(1, files.size())));
        try {
            var futures = new java.util.ArrayList<java.util.concurrent.Future<String>>();
            for (Path file : files) futures.add(pool.submit(() -> sha256(file)));
            for (var future : futures)
                try {
                    future.get();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IOException(interrupted);
                } catch (java.util.concurrent.ExecutionException failed) {
                    if (failed.getCause() instanceof IOException io) throw io;
                    throw new IOException(failed.getCause());
                }
        } finally {
            pool.shutdownNow();
        }
    }

    /** The file's SHA256, read now. */
    public static String read(Path path) throws IOException {
        return org.sibench.config.Digests.sha256(path);
    }
}
