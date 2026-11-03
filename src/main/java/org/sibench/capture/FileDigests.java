package org.sibench.capture;

import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Reads actual bytes at integrity boundaries; file metadata cannot certify unchanged contents. */
public final class FileDigests {
    private FileDigests() {}

    public static String sha256(Path file) throws IOException {
        return org.sibench.config.Digests.sha256(file);
    }

    /** Hash each input once in this verification pass, retaining only the resulting digests. */
    static Map<Path, String> hashFiles(List<Path> files) throws IOException {
        var pool = java.util.concurrent.Executors.newFixedThreadPool(Math.min(8, Math.max(1, files.size())));
        try {
            var futures = new LinkedHashMap<Path, java.util.concurrent.Future<String>>();
            for (Path file : files) futures.computeIfAbsent(file, path -> pool.submit(() -> sha256(path)));
            var result = new LinkedHashMap<Path, String>();
            for (var entry : futures.entrySet())
                try {
                    result.put(entry.getKey(), entry.getValue().get());
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IOException(interrupted);
                } catch (java.util.concurrent.ExecutionException failed) {
                    if (failed.getCause() instanceof IOException io) throw io;
                    throw new IOException(failed.getCause());
                }
            return result;
        } finally {
            pool.shutdownNow();
        }
    }
}
