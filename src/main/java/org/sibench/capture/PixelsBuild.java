package org.sibench.capture;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.sibench.config.Digests;

/**
 * Identity of a Pixels CDC build (scripts/build-pixels.sh): it must come from the current Pixels and
 * pixels-sink gitlinks, sink patch and tracer sources, with both build phases and their files intact.
 */
final class PixelsBuild {
    private PixelsBuild() {}

    static Map<String, Object> verify(Path root, Path build) throws IOException {
        var java = read(build.resolve("java-build.json"));
        var nativeBuild = read(build.resolve("native-build.json"));
        String pixels = gitlink(root, "third_party/pixels"), sink = gitlink(root, "third_party/pixels-sink");
        require(java.get("pixels_commit").getAsString().equals(pixels) && nativeBuild.get("pixels_commit").getAsString().equals(pixels)
                && java.get("pixels_sink_commit").getAsString().equals(sink), "Pixels build differs from the repository gitlinks");
        require(nativeBuild.get("postgresql_commit").getAsString().equals(gitlink(root, "third_party/postgresql")),
                "Source PostgreSQL build differs from its gitlink");
        require(java.get("pixels_patch_sha256").getAsString().equals(
                Digests.sha256(root.resolve("src/main/native/pixels/pixels-0.2.0-SNAPSHOT.patch"))),
                "Pixels LOAD patch changed after the build");
        require(java.get("pixels_sink_patch_sha256").getAsString().equals(
                Digests.sha256(root.resolve("src/main/native/pixels-sink/pixels-sink-0.2.0-SNAPSHOT.patch"))),
                "pixels-sink patch changed after the build");
        require(java.get("tracer_sources_sha256").getAsString().equals(treeSha256(root.resolve("src/main/native/pixels"))),
                "Pixels tracer sources changed after the build");
        require(java.has("build_script_sha256") && java.get("build_script_sha256").getAsString()
                .equals(Digests.sha256(root.resolve("scripts/build-pixels.sh"))), "Pixels build recipe changed after the Java build");
        verifyFiles(build);
        var identity = new LinkedHashMap<String, Object>();
        identity.put("java", new com.google.gson.Gson().fromJson(java, Map.class));
        identity.put("native", new com.google.gson.Gson().fromJson(nativeBuild, Map.class));
        return identity;
    }

    /** The built jars and native files still match the identities both build phases recorded. */
    static void verifyFiles(Path build) throws IOException {
        var java = read(build.resolve("java-build.json"));
        var nativeBuild = read(build.resolve("native-build.json"));
        require(java.get("pixels_commit").getAsString().equals(nativeBuild.get("pixels_commit").getAsString()),
                "Pixels Java and native builds come from different sources");
        require(java.getAsJsonObject("jars").keySet().equals(Set.of("pixels-cli.jar", "pixels-daemon.jar", "pixels-sink.jar", "sibench-pixels.jar")),
                "Incomplete Pixels Java artifacts");
        for (var jar : java.getAsJsonObject("jars").entrySet())
            require(Digests.sha256(build.resolve("jars").resolve(jar.getKey())).equals(jar.getValue().getAsString()),
                    "Pixels build file changed: " + jar.getKey());
        var files = nativeBuild.getAsJsonObject("files");
        for (var file : Map.of("libpixels-retina.so", "home/lib/libpixels-retina.so", "libjemalloc.so", "home/lib/libjemalloc.so",
                "postgres", "postgres/runtime/bin/postgres", "pg_ctl", "postgres/runtime/bin/pg_ctl", "initdb", "postgres/runtime/bin/initdb",
                "etcd", "etcd/etcd").entrySet())
            require(files.has(file.getKey()) && Digests.sha256(build.resolve(file.getValue())).equals(files.get(file.getKey()).getAsString()),
                    "Pixels native file changed: " + file.getKey());
        var configs = nativeBuild.getAsJsonObject("runtime_configs");
        require(configs != null && configs.keySet().equals(Set.of("defaults/pixels.properties", "home/etc/pixels.properties",
                "source/pixels/scripts/etc/coordinator-jvm.config", "source/pixels/scripts/etc/retina-jvm.config")), "Missing Pixels runtime input identities");
        for (var config : configs.entrySet())
            require(Digests.sha256(build.resolve(config.getKey())).equals(config.getValue().getAsString()), "Pixels runtime input changed: " + config.getKey());
        require(nativeBuild.has("native_sources_sha256") && treeSha256(build.resolve("source/pixels/cpp/pixels-retina"))
                .equals(nativeBuild.get("native_sources_sha256").getAsString()), "Pixels native sources changed after the build");
        require(Long.parseLong(org.sibench.config.PixelsConfig.properties(build.resolve("defaults/pixels.properties"))
                .get("retina.tile.visibility.capacity")) == nativeBuild.get("retina_capacity").getAsLong(), "Pixels runtime/native capacities differ");
    }

    static JsonObject identity(Path build) throws IOException {
        verifyFiles(build);
        var identity = new JsonObject();
        identity.add("java", read(build.resolve("java-build.json")));
        identity.add("native", read(build.resolve("native-build.json")));
        return identity;
    }

    /** Native binaries may target another OS; source pins and compiled semantics must agree. */
    static void requireHost(JsonObject expected, JsonObject actual) throws IOException {
        require(actual != null && actual.has("java") && actual.has("native"), "Missing Pixels host build identity");
        require(expected.get("java").equals(actual.get("java")), "Pixels host Java build differs from the generator");
        for (String field : java.util.List.of("pixels_commit", "postgresql_commit", "retina_capacity", "native_sources_sha256"))
            require(expected.getAsJsonObject("native").get(field).equals(actual.getAsJsonObject("native").get(field)),
                    "Pixels host native build differs: " + field);
    }

    /** Same digest as the build script: sha256sum lines of every file, sorted by ./relative path. */
    static String treeSha256(Path directory) throws IOException {
        var lines = new StringBuilder();
        try (var files = Files.walk(directory)) {
            for (Path file : files.filter(Files::isRegularFile).sorted(java.util.Comparator.comparing(
                    path -> ("./" + directory.relativize(path)).getBytes(java.nio.charset.StandardCharsets.UTF_8),
                    java.util.Arrays::compareUnsigned)).toList())
                lines.append(Digests.sha256(file)).append("  ./").append(directory.relativize(file)).append('\n');
        }
        return java.util.HexFormat.of().formatHex(Digests.digest().digest(lines.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8)));
    }

    private static JsonObject read(Path file) throws IOException {
        require(Files.isRegularFile(file), "Missing Pixels build identity " + file);
        return JsonParser.parseString(Files.readString(file)).getAsJsonObject();
    }

    private static String gitlink(Path root, String path) throws IOException {
        var process = new ProcessBuilder("git", "-C", root.toString(), "ls-files", "--stage", "--", path)
                .redirectError(ProcessBuilder.Redirect.DISCARD).start();
        String line = new String(process.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8).strip();
        try {
            if (!process.waitFor(30, java.util.concurrent.TimeUnit.SECONDS) || process.exitValue() != 0 || !line.startsWith("160000 "))
                throw new IOException("Missing gitlink " + path);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IOException(error);
        }
        return line.split("\\s+")[1];
    }

    private static void require(boolean condition, String message) throws IOException {
        if (!condition) throw new IOException(message);
    }
}
