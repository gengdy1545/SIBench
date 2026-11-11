package org.sibench.capture;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.HexFormat;
import org.sibench.config.Digests;

/** File identities and trust-boundary checks shared by published input artifacts. */
final class InputFiles {
    static final int MAX_MANIFEST_BYTES = 16 << 20;
    private InputFiles() {}

    record Manifest(JsonObject document, String sha256) {}

    static Manifest manifest(Path directory) throws IOException {
        Path path = resolve(directory, "manifest.json");
        require(Files.size(path) <= MAX_MANIFEST_BYTES, "Input manifest exceeds 16 MiB");
        byte[] bytes;
        try (var input = Files.newInputStream(path)) { bytes = input.readNBytes(MAX_MANIFEST_BYTES + 1); }
        require(bytes.length <= MAX_MANIFEST_BYTES, "Input manifest grew beyond 16 MiB");
        try {
            var document = JsonParser.parseString(new String(bytes, StandardCharsets.UTF_8)).getAsJsonObject();
            return new Manifest(document, HexFormat.of().formatHex(Digests.digest().digest(bytes)));
        } catch (RuntimeException malformed) { throw new IOException("Invalid input manifest: " + path, malformed); }
    }

    static JsonObject file(Path root, Path path) throws IOException {
        var result = new JsonObject();
        result.addProperty("path", root.relativize(path).toString());
        result.addProperty("bytes", Files.size(path));
        result.addProperty("sha256", Digests.sha256(path));
        return result;
    }

    static Path verified(Path directory, JsonObject file) throws IOException {
        Path path = resolve(directory, file.get("path").getAsString());
        require(Files.size(path) == number(file, "bytes")
                && Digests.sha256(path).equals(file.get("sha256").getAsString()), "Input file changed: " + path);
        return path;
    }

    static Path resolve(Path directory, String name) throws IOException {
        Path relative = Path.of(name);
        require(!name.isBlank() && !relative.isAbsolute() && relative.normalize().toString().equals(name), "Noncanonical input path");
        Path path = directory;
        for (Path part : relative) {
            require(!part.toString().equals("..") && !part.toString().equals("."), "Input path traversal");
            path = path.resolve(part);
            require(!Files.isSymbolicLink(path), "Symbolic input path");
        }
        require(Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS), "Missing input file: " + path);
        return path;
    }

    static long number(JsonObject object, String key) throws IOException {
        try {
            var value = object.get(key);
            require(value != null && value.isJsonPrimitive() && value.getAsJsonPrimitive().isNumber(), "Missing numeric input field: " + key);
            long result = value.getAsBigDecimal().longValueExact();
            require(result >= 0, "Negative input field: " + key);
            return result;
        } catch (ArithmeticException | NumberFormatException invalid) {
            throw new IOException("Nonintegral or overflowing input field: " + key, invalid);
        }
    }

    private static void require(boolean valid, String message) throws IOException {
        if (!valid) throw new IOException(message);
    }
}
