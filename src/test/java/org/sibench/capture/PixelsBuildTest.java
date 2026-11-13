package org.sibench.capture;

import static org.junit.Assert.*;
import com.google.gson.Gson;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.config.Digests;

public class PixelsBuildTest {
    @Rule public TemporaryFolder temporary=new TemporaryFolder();

    @Test public void cdcSourceNeedsOnlyItsOwnArtifactsAndRejectsChangedInputs() throws Exception {
        Path root=Path.of("").toRealPath(),build=temporary.getRoot().toPath();
        var jars=new LinkedHashMap<String,String>();
        for(String name:java.util.List.of("pixels-sink.jar","sibench-pixels.jar"))
            jars.put(name,file(build,"jars/"+name,name));
        var binaries=new LinkedHashMap<String,String>();
        for(String name:java.util.List.of("postgres","pg_ctl","initdb"))
            binaries.put(name,file(build,"postgres/runtime/bin/"+name,name));
        var javaIdentity=Map.of("pixels_commit",pin("pixels"),"pixels_sink_commit",pin("pixels-sink"),
                "pixels_patch_sha256",Digests.sha256(root.resolve("src/main/native/pixels/pixels-0.2.0-SNAPSHOT.patch")),
                "pixels_sink_patch_sha256",Digests.sha256(root.resolve("src/main/native/pixels-sink/pixels-sink-0.2.0-SNAPSHOT.patch")),
                "tracer_sources_sha256",PixelsBuild.treeSha256(root.resolve("src/main/native/pixels")),
                "build_script_sha256",Digests.sha256(root.resolve("scripts/build-pixels.sh")),"jars",jars);
        Files.writeString(build.resolve("java-build.json"),new Gson().toJson(javaIdentity));
        Files.writeString(build.resolve("native-build.json"),new Gson().toJson(Map.of("postgresql_commit",pin("postgresql"),"files",binaries)));
        PixelsBuild.verifySource(root,build);
        assertFalse(Files.exists(build.resolve("home")));
        for(String name:java.util.List.of("jars/pixels-sink.jar","jars/sibench-pixels.jar","postgres/runtime/bin/postgres",
                "postgres/runtime/bin/pg_ctl","postgres/runtime/bin/initdb")) {
            Path path=build.resolve(name);String original=Files.readString(path);
            Files.writeString(path,"changed");
            assertThrows(name,IOException.class,()->PixelsBuild.verifySource(root,build));
            Files.writeString(path,original);
        }
    }

    private static String pin(String component)throws Exception {
        var process=new ProcessBuilder("git","ls-files","--stage","--","third_party/"+component).start();
        String result=new String(process.getInputStream().readAllBytes(),java.nio.charset.StandardCharsets.UTF_8);
        assertEquals(0,process.waitFor());return result.split("\\s+")[1];
    }

    @Test public void everyLoadedNativeComponentAndRuntimeInputMustStillMatchItsBuild() throws Exception {
        Path build=temporary.getRoot().toPath();
        var jars=new LinkedHashMap<String,String>();
        for (String name:java.util.List.of("pixels-cli.jar","pixels-daemon.jar","pixels-sink.jar","sibench-pixels.jar"))
            jars.put(name,file(build,"jars/"+name,name));
        var nativeFiles=new LinkedHashMap<String,String>();
        var paths=Map.of("libpixels-retina.so","home/lib/libpixels-retina.so", "libjemalloc.so","home/lib/libjemalloc.so",
                "postgres","postgres/runtime/bin/postgres", "initdb","postgres/runtime/bin/initdb", "pg_ctl","postgres/runtime/bin/pg_ctl",
                "etcd","etcd/etcd");
        for (var entry:paths.entrySet()) nativeFiles.put(entry.getKey(),file(build,entry.getValue(),entry.getKey()));
        var configs=new LinkedHashMap<String,String>();
        for (String name:java.util.List.of("defaults/pixels.properties","home/etc/pixels.properties",
                "source/pixels/scripts/etc/coordinator-jvm.config","source/pixels/scripts/etc/retina-jvm.config"))
            configs.put(name,file(build,name,name.endsWith(".properties")?"retina.tile.visibility.capacity=64\n":"-ea\n"));
        file(build,"source/pixels/cpp/pixels-retina/lib.cpp","source");
        var identity=new LinkedHashMap<String,Object>(Map.of("pixels_commit","pixels", "retina_capacity",64,
                "files",nativeFiles,"runtime_configs",configs,
                "native_sources_sha256",PixelsBuild.treeSha256(build.resolve("source/pixels/cpp/pixels-retina"))));
        Files.writeString(build.resolve("java-build.json"),new Gson().toJson(Map.of("pixels_commit","pixels","jars",jars)));
        Files.writeString(build.resolve("native-build.json"),new Gson().toJson(identity));
        PixelsBuild.verifyFiles(build);
        for (String name:java.util.List.of("home/lib/libjemalloc.so","postgres/runtime/bin/pg_ctl","postgres/runtime/bin/initdb",
                "source/pixels/scripts/etc/retina-jvm.config","defaults/pixels.properties","source/pixels/cpp/pixels-retina/lib.cpp")) {
            Path target=build.resolve(name);
            String original=Files.readString(target);
            Files.writeString(target,"changed");
            assertThrows(name,IOException.class,()->PixelsBuild.verifyFiles(build));
            Files.writeString(target,original);
        }
        identity.put("retina_capacity",128);
        Files.writeString(build.resolve("native-build.json"),new Gson().toJson(identity));
        assertTrue(assertThrows(IOException.class,()->PixelsBuild.verifyFiles(build)).getMessage().contains("capacities differ"));
    }

    private static String file(Path root,String name,String text) throws IOException {
        Path path=root.resolve(name);
        Files.createDirectories(path.getParent());
        Files.writeString(path,text);
        return Digests.sha256(path);
    }
}
