package org.sibench.capture;

import static org.junit.Assert.*;

import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.config.Digests;

public class RocksDbBuildTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();
    private Path root, source, build, bridge;
    private Map<String, Object> inputs;
    private final Map<String, String> artifacts = new TreeMap<>();

    private static String git(Path directory, String... args) throws Exception {
        var command = new ArrayList<>(List.of("git", "-C", directory.toString()));
        command.addAll(List.of(args));
        var process = new ProcessBuilder(command).redirectErrorStream(true).start();
        String output = new String(process.getInputStream().readAllBytes()).strip();
        assertTrue(process.waitFor(10, java.util.concurrent.TimeUnit.SECONDS));
        assertEquals(output, 0, process.exitValue());
        return output;
    }

    @Before public void prepare() throws Exception {
        root = temporary.getRoot().toPath();
        source = root.resolve("third_party/rocksdb");
        build = root.resolve("build/services/rocksdb");
        Files.createDirectories(source);
        Files.createDirectories(build.resolve("cmake"));
        Files.writeString(source.resolve("upstream.cc"), "source\n");
        git(source, "init", "-q");
        git(source, "add", ".");
        git(source, "-c", "user.name=Test", "-c", "user.email=test@example.invalid",
                "-c", "commit.gpgsign=false", "commit", "-qm", "source");
        git(root, "init", "-q");
        String pin = git(source, "rev-parse", "HEAD");
        git(root, "update-index", "--add", "--cacheinfo", "160000," + pin + ",third_party/rocksdb");
        bridge = root.resolve("src/main/native/rocksdb/timestamps_jni.cc");
        Files.createDirectories(bridge.getParent());
        Files.writeString(bridge, "bridge\n");
        Path script = root.resolve("scripts/build-rocksdb.sh");
        Files.createDirectories(script.getParent());
        Files.writeString(script, "recipe\n");
        inputs = Map.of("source_commit", pin, "native_sources",
                Map.of("timestamps_jni.cc", Digests.sha256(bridge)), "build_script_sha256", Digests.sha256(script));
        for (String name : List.of("rocksdbjni.jar", "libsibench_rocksdb.so", "source-files.json",
                "compiler-version.txt", "cmake/CMakeCache.txt", "cmake/compile_commands.json"))
            Files.writeString(build.resolve(name), "fixture " + name);
        try (var runtime = org.rocksdb.RocksDB.class.getResourceAsStream("/librocksdbjni-linux64.so")) {
            assertNotNull(runtime);
            Files.copy(runtime, build.resolve("librocksdbjni-linux64.so"));
        }
        for (String name : List.of("rocksdbjni.jar", "libsibench_rocksdb.so", "librocksdbjni-linux64.so",
                "source-files.json", "compiler-version.txt", "cmake/CMakeCache.txt", "cmake/compile_commands.json"))
            artifacts.put(name, Digests.sha256(build.resolve(name)));
    }

    private void receipt() throws IOException {
        Files.writeString(build.resolve("engine-build.json"), new GsonBuilder().create().toJson(
                Map.of("inputs", inputs, "artifacts", artifacts)));
    }

    @Test public void requiresCompleteReceiptAndRejectsChangedArtifacts() throws Exception {
        assertThrows(IOException.class, () -> NativeBuildCheck.verifyRocks(root, build));
        receipt();
        var original = NativeBuildCheck.verifyRocks(root, build);
        for (String name : artifacts.keySet()) {
            Path file = build.resolve(name);
            byte[] bytes = Files.readAllBytes(file);
            Files.writeString(file, "changed");
            assertThrows(name, IOException.class, () -> NativeBuildCheck.verifyRocks(root, build));
            Files.write(file, bytes);
        }
        assertEquals(original, NativeBuildCheck.verifyRocks(root, build));
        artifacts.remove("rocksdbjni.jar");
        receipt();
        assertThrows(IOException.class, () -> NativeBuildCheck.verifyRocks(root, build));
    }

    @Test public void bindsPinProjectSourcesAndCleanUpstream() throws Exception {
        receipt();
        NativeBuildCheck.verifyRocks(root, build);
        for (Path file : List.of(bridge, root.resolve("scripts/build-rocksdb.sh"), source.resolve("upstream.cc"))) {
            byte[] bytes = Files.readAllBytes(file);
            Files.writeString(file, "changed");
            assertThrows(IOException.class, () -> NativeBuildCheck.verifyRocks(root, build));
            Files.write(file, bytes);
        }
        git(root, "update-index", "--cacheinfo", "160000," + "0".repeat(39) + "1,third_party/rocksdb");
        assertThrows(IOException.class, () -> NativeBuildCheck.verifyRocks(root, build));
        git(root, "update-index", "--cacheinfo", "160000," + inputs.get("source_commit") + ",third_party/rocksdb");
        Files.writeString(source.resolve("unexpected.h"), "untracked");
        assertThrows(IOException.class, () -> NativeBuildCheck.verifyRocks(root, build));
    }

    @Test public void matchingReceiptCannotAuthorizeAnotherPackagedRuntime() throws Exception {
        Files.writeString(build.resolve("librocksdbjni-linux64.so"), "another build");
        artifacts.put("librocksdbjni-linux64.so", Digests.sha256(build.resolve("librocksdbjni-linux64.so")));
        receipt();
        var error = assertThrows(IOException.class, () -> NativeBuildCheck.verifyRocks(root, build));
        assertTrue(error.getMessage(), error.getMessage().contains("packaged source build"));
    }

    @Test public void runtimeComesFromThePackagedSourceBuild() throws Exception {
        Path output = root.resolve("loaded-libraries.txt");
        var process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin/java").toString(),
                "-Djava.library.path=" + build, "-cp", System.getProperty("java.class.path"),
                getClass().getName()).redirectErrorStream(true).redirectOutput(output.toFile()).start();
        assertTrue(process.waitFor(15, java.util.concurrent.TimeUnit.SECONDS));
        String maps = Files.readString(output);
        assertEquals(maps, 0, process.exitValue());
        assertFalse("Ambient library path bypassed the packaged runtime: " + maps, maps.contains(build.toString()));
        assertTrue(maps, maps.contains("librocksdbjni"));
    }

    public static void main(String[] args) throws Exception {
        org.rocksdb.RocksDB.loadLibrary();
        for (String line : Files.readAllLines(Path.of("/proc/self/maps")))
            if (line.contains("librocksdbjni")) System.out.println(line);
    }
}
