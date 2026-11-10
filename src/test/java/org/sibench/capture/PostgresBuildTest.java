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

public class PostgresBuildTest {
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
        root = temporary.getRoot().toPath(); source = root.resolve("third_party/postgresql");
        build = root.resolve("build/services/postgres");
        Files.createDirectories(source); Files.createDirectories(build.resolve("source"));
        Files.writeString(source.resolve("upstream.cpp"), "upstream source\n");
        git(source,"init","-q"); git(source,"add",".");
        git(source,"-c","user.name=Test","-c","user.email=test@example.invalid","-c","commit.gpgsign=false","commit","-qm","source");
        git(root,"init","-q");
        git(root,"update-index","--add","--cacheinfo","160000,"+git(source,"rev-parse","HEAD")+",third_party/postgresql");
        Files.createDirectories(root.resolve("scripts"));
        Files.writeString(root.resolve("scripts/build-postgres.sh"), "build recipe\n");
        header = root.resolve("src/main/native/postgres/sibench_capture.c"); Files.createDirectories(header.getParent());
        Files.writeString(header,"instrumentation\n");
        Files.writeString(header.resolveSibling("row_capture.c"),"host source\n");
        Files.writeString(root.resolve("postgres.yaml"),"schema_version: 1\npurpose: engine\nengine: postgres\n"
                +"build_directory: build/services/postgres\nsettings: {}\n");
        Files.writeString(root.resolve("runtime.yaml"),"schema_version: 1\npurpose: sibench\nengines:\n  postgres: postgres.yaml\n");
        action("postgres-inputs");
        Files.copy(source.resolve("upstream.cpp"),build.resolve("source/upstream.cpp"));
        Files.copy(header,build.resolve("source/capture.hpp"));
        Files.copy(header.resolveSibling("row_capture.c"),build.resolve("row_capture.c"));
        action("postgres-prepared");
        Files.createDirectories(build.resolve("src"));
        for (String name : List.of("runtime/bin/postgres","runtime/bin/pg_ctl","runtime/bin/initdb","runtime/lib/row_capture.so","source/config.status","compiler-version.txt"))
            {Files.createDirectories(build.resolve(name).getParent());Files.writeString(build.resolve(name),"test artifact "+name+"\n");}
        
    }

    private void action(String action) throws Exception {
        NativeBuildCheck.main(new String[]{action,root.toString(),"runtime.yaml"});
    }

    @Test public void generationRequiresReceiptAndRejectsChangedExecutableOrBuildSettings() throws Exception {
        assertThrows(IOException.class,()->NativeBuildCheck.verifyPostgres(root,build));
        action("postgres-record"); var original=NativeBuildCheck.verifyPostgres(root,build);
        for (String name : List.of("runtime/bin/postgres","runtime/lib/row_capture.so","source/config.status","source-files.json")) {
            Path file=build.resolve(name);byte[] before=Files.readAllBytes(file);
            Files.writeString(file,"changed\n");
            assertThrows(IOException.class,()->NativeBuildCheck.verifyPostgres(root,build));
            Files.write(file,before); assertEquals(original,NativeBuildCheck.verifyPostgres(root,build));
        }
        assertThrows(java.nio.file.FileAlreadyExistsException.class,()->action("postgres-record"));
    }

    @Test public void buildCannotBeCertifiedAfterSourcesChange() throws Exception {
        Path copied=build.resolve("source/upstream.cpp");Files.writeString(copied,"different source\n");
        assertThrows(IOException.class,()->action("postgres-record"));
        assertFalse(Files.exists(build.resolve("engine-build.json")));
        Files.writeString(copied,"upstream source\n");
        Files.writeString(header,"different instrumentation\n");
        assertThrows(IOException.class,()->action("postgres-record"));
        assertFalse(Files.exists(build.resolve("engine-build.json")));
    }

    @Test public void generationRejectsChangedProjectInputsAndDirtyUpstream() throws Exception {
        action("postgres-record");
        for(Path file:List.of(header,root.resolve("scripts/build-postgres.sh"),source.resolve("upstream.cpp"))) {
            byte[] before=Files.readAllBytes(file);Files.writeString(file,"changed\n");
            assertThrows(IOException.class,()->NativeBuildCheck.verifyPostgres(root,build));
            Files.write(file,before);
        }
        Files.writeString(source.resolve("unexpected.hpp"),"untracked source\n");
        assertThrows(IOException.class,()->NativeBuildCheck.verifyPostgres(root,build));
    }
}
