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
