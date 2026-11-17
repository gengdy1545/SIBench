package org.sibench.capture;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class DuckDbBuildTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();
    private Path root, source, build, header;

    private static String git(Path root, String... arguments) throws Exception {
        var command = new ArrayList<>(List.of("git", "-C", root.toString()));
        command.addAll(List.of(arguments));
        var process = new ProcessBuilder(command).redirectErrorStream(true).start();
        String output = new String(process.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8).strip();
        assertTrue(process.waitFor(10, java.util.concurrent.TimeUnit.SECONDS));
        assertEquals(output, 0, process.exitValue()); return output;
    }

    @Before public void prepareBuildInputsAndArtifacts() throws Exception {
        root = temporary.getRoot().toPath(); source = root.resolve("third_party/duckdb");
        build = root.resolve("build/services/duckdb");
        Files.createDirectories(source); Files.createDirectories(build);
        Files.writeString(source.resolve("upstream.cpp"), "upstream source\n");
        git(source,"init","-q"); git(source,"add",".");
        git(source,"-c","user.name=Test","-c","user.email=test@example.invalid","-c","commit.gpgsign=false","commit","-qm","source");
        git(root,"init","-q");
        git(root,"update-index","--add","--cacheinfo","160000,"+git(source,"rev-parse","HEAD")+",third_party/duckdb");
        Files.createDirectories(root.resolve("scripts"));
        Files.writeString(root.resolve("scripts/build-duckdb.sh"), "build recipe\n");
        header = root.resolve("src/main/native/duckdb/capture.hpp"); Files.createDirectories(header.getParent());
        Files.writeString(header,"instrumentation\n");
        Files.writeString(header.resolveSibling("duckdb_host.cpp"),"host source\n");
        Files.createDirectories(build.resolve("src"));
        for (String name : List.of("duckdb_host","src/libduckdb.so","duckdb","CMakeCache.txt","compile_commands.json","compiler-version.txt",
                "source-files.json"))
            Files.writeString(build.resolve(name),"test artifact "+name+"\n");
        assertTrue(build.resolve("duckdb_host").toFile().setExecutable(true));
    }

    private void recordFixture() throws Exception {
        var nativeFiles = new java.util.TreeMap<String, String>();
        try (var files = Files.walk(root.resolve("src/main/native/duckdb"))) {
            for (Path file : files.filter(Files::isRegularFile).toList())
                nativeFiles.put(file.getFileName().toString(), org.sibench.config.Digests.sha256(file));
        }
        var inputs = java.util.Map.of("source_commit", git(source, "rev-parse", "HEAD"),
                "native_sources", nativeFiles,
                "build_script_sha256", org.sibench.config.Digests.sha256(root.resolve("scripts/build-duckdb.sh")));
        var artifacts = new java.util.TreeMap<String, String>();
        for (String name : List.of("duckdb_host", "src/libduckdb.so", "duckdb", "CMakeCache.txt",
                "compile_commands.json", "compiler-version.txt", "source-files.json"))
            artifacts.put(name, org.sibench.config.Digests.sha256(build.resolve(name)));
        Files.writeString(build.resolve("engine-build.json"), new com.google.gson.Gson().toJson(
                java.util.Map.of("inputs", inputs, "artifacts", artifacts)));
    }

    @Test public void generationRequiresReceiptAndRejectsChangedExecutableOrBuildSettings() throws Exception {
        assertThrows(IOException.class,()->NativeBuildCheck.verifyEngine(root,build));
        recordFixture(); var original=NativeBuildCheck.verifyEngine(root,build);
        for (String name : List.of("duckdb_host","src/libduckdb.so","CMakeCache.txt","compile_commands.json","source-files.json")) {
            Path file=build.resolve(name);byte[] before=Files.readAllBytes(file);
            Files.writeString(file,"changed\n");
            assertThrows(IOException.class,()->NativeBuildCheck.verifyEngine(root,build));
            Files.write(file,before);
        }
        assertEquals(original,NativeBuildCheck.verifyEngine(root,build));
    }

    @Test public void generationRejectsChangedProjectInputsAndDirtyUpstream() throws Exception {
        recordFixture();
        for(Path file:List.of(header,root.resolve("scripts/build-duckdb.sh"),source.resolve("upstream.cpp"))) {
            byte[] before=Files.readAllBytes(file);Files.writeString(file,"changed\n");
            assertThrows(IOException.class,()->NativeBuildCheck.verifyEngine(root,build));
            Files.write(file,before);
        }
        Files.writeString(source.resolve("unexpected.hpp"),"untracked source\n");
        assertThrows(IOException.class,()->NativeBuildCheck.verifyEngine(root,build));
    }
}
