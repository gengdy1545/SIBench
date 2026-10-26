package org.sibench.capture;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;
import java.util.TreeMap;

/**
 * SHA256 of baseline data files kept beside the baseline directory ({@code <baseline>.digests.json})
 * across processes, by the identity {@link FileDigests} trusts within one: the file (device and
 * inode), its size and its modification time. A baseline is written once and then only read and
 * hard-linked, which keeps all three, so every stage of a capture and every capture linking the
 * same still baseline no longer reads tens of GB again to check it. A file that changed has
 * another size or time and is read; an entry for it is replaced.
 */
final class BaselineDigests {
    private final Path file;
    private final Map<String, String> known = new TreeMap<>();
    private boolean changed;

    private BaselineDigests(Path file) {
        this.file = file;
    }

    static Path location(Path baseline) {
        Path absolute = baseline.toAbsolutePath().normalize();
        return absolute.resolveSibling(absolute.getFileName() + ".digests.json");
    }

    /** The digests recorded for a baseline directory; none when there is no readable record. */
    static BaselineDigests of(Path baseline) {
        var digests = new BaselineDigests(location(baseline));
        try {
            if (Files.exists(digests.file)) {
                JsonObject value = JsonParser.parseString(Files.readString(digests.file)).getAsJsonObject();
                for (var entry : value.entrySet()) digests.known.put(entry.getKey(), entry.getValue().getAsString());
            }
        } catch (IOException | RuntimeException unreadable) {
            // Only a cache: every file is read again.
            digests.known.clear();
        }
        return digests;
    }

    /**
     * Adds what one baseline directory recorded to another's record: entries name files, not
     * paths, so they hold for hard links of the same files.
     */
    static void merge(Path from, Path into) {
        var target = of(into);
        for (var entry : of(from).known.entrySet())
            if (!entry.getValue().equals(target.known.put(entry.getKey(), entry.getValue()))) target.changed = true;
        target.save();
    }

    private static String identity(Path path) throws IOException {
        var attributes = Files.readAttributes(path, BasicFileAttributes.class);
        var modified = attributes.lastModifiedTime().toInstant();
        Object key = attributes.fileKey();
        if (key == null) return null;
        return key + "|" + attributes.size() + "|" + modified.getEpochSecond() + "." + modified.getNano();
    }

    /** The recorded digest of the file as it is now, or null when it must be read. */
    String get(Path path) throws IOException {
        String identity = identity(path);
        return identity == null ? null : known.get(identity);
    }

    void put(Path path, String digest) throws IOException {
        String identity = identity(path);
        if (identity == null || digest.equals(known.get(identity))) return;
        known.put(identity, digest);
        changed = true;
    }

    /** Writes the record when it gained entries; a record that cannot be written is skipped. */
    void save() {
        if (!changed) return;
        var value = new JsonObject();
        known.forEach(value::addProperty);
        Path partial = file.resolveSibling(file.getFileName() + ".partial-" + ProcessHandle.current().pid());
        try {
            Files.writeString(partial, value.toString());
            Files.move(partial, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            changed = false;
        } catch (IOException | RuntimeException unwritable) {
            try {
                Files.deleteIfExists(partial);
            } catch (IOException ignored) {
                // Nothing more to clean.
            }
        }
    }
}
