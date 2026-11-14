package org.sibench.capture;

import static org.junit.Assert.*;
import java.io.IOException;
import java.nio.file.Files;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TrinoHostTest {
    @Rule public TemporaryFolder temporary=new TemporaryFolder();

    @Test public void deploymentReceiptCannotAuthorizeFilesOutsideItsDirectory()throws Exception {
        var root=temporary.newFolder("deployment").toPath();
        Files.createDirectories(root.resolve("server/lib"));Files.createDirectory(root.resolve("server/plugin"));
        Files.createDirectory(root.resolve("jdk"));
        var jar=Files.writeString(root.resolve("server/lib/connector.jar"),"connector");
        var receipt=new com.google.gson.JsonObject();receipt.addProperty("pixels_commit","pin");
        receipt.addProperty("server_directory","server");receipt.addProperty("java_home","jdk");
        var files=new com.google.gson.JsonObject();files.addProperty("server/lib/connector.jar",org.sibench.config.Digests.sha256(jar));
        receipt.add("files",files);Files.writeString(root.resolve("deployment.json"),receipt.toString());
        TrinoHost.verify(root,"pin");
        var outside=Files.writeString(temporary.getRoot().toPath().resolve("outside.jar"),"connector");
        Files.delete(jar);Files.createSymbolicLink(jar,outside);
        assertThrows(IOException.class,()->TrinoHost.verify(root,"pin"));
        Files.delete(jar);Files.writeString(jar,"connector");
        var externalPlugin=temporary.newFolder("plugin").toPath();
        Files.writeString(externalPlugin.resolve("unlisted.jar"),"plugin");
        var pluginLink=Files.createSymbolicLink(root.resolve("server/plugin/extra"),externalPlugin);
        assertThrows(IOException.class,()->TrinoHost.verify(root,"pin"));
        Files.delete(pluginLink);
        Files.delete(root.resolve("jdk"));Files.createSymbolicLink(root.resolve("jdk"),outside.getParent());
        assertThrows(IOException.class,()->TrinoHost.verify(root,"pin"));
    }
}
