package org.sibench.capture;

import static org.junit.Assert.*;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.sibench.config.ConfigValues;
import org.sibench.config.Engine;
import org.sibench.config.PixelsConfig;
import org.sibench.config.SibenchConfig;

public class PixelsHostTest {
    @org.junit.Rule public org.junit.rules.TemporaryFolder temporary = new org.junit.rules.TemporaryFolder();

    @Test public void fixedPortsSurviveAcrossSessionsAndAutomaticPortsRemainDistinct() throws Exception {
        int metadata, retina, etcd;
        try (var first = new ServerSocket(0); var second = new ServerSocket(0); var third = new ServerSocket(0)) {
            metadata = first.getLocalPort(); retina = second.getLocalPort(); etcd = third.getLocalPort();
        }
        var overrides = Map.of("metadata.server.port", Integer.toString(metadata), "retina.server.port", Integer.toString(retina), "etcd.port", Integer.toString(etcd));
        for (int run = 0; run < 2; run++) {
            var ports = PixelsHost.ports("127.0.0.1", overrides);
            assertEquals(Integer.valueOf(metadata), ports.get("metadata"));
            assertEquals(Integer.valueOf(retina), ports.get("retina"));
            assertEquals(Integer.valueOf(etcd), ports.get("etcd"));
            assertEquals(10, ports.size());
            assertEquals(10, new java.util.HashSet<>(ports.values()).size());
        }
        assertEquals(10, new java.util.HashSet<>(PixelsHost.ports("127.0.0.1", Map.of()).values()).size());
        assertEquals("metadata.server.port=18801\n", PixelsHost.properties("metadata.server.port=1\n",
                Map.of("metadata.server.port", "18801"), Map.of("metadata.server.port", "18801")));
    }

    @Test public void sinkOverridesCannotFilterOrRewriteSourceEventsOrHideTypos() {
        var accepted=new java.util.ArrayList<String>();
        for (String key:java.util.List.of("debezium.skipped.operations","debezium.column.exclude.list", "debezium.unknown",
                "sink.unknown", "debezium.column.truncate.to.1.chars")) {
            try { PixelsConfig.sinkProperties(Map.of(key,"x")); accepted.add(key); }
            catch (IllegalArgumentException expected) { }
        }
        assertEquals(java.util.List.of(),accepted);
    }

    @Test public void pixelsPropertiesReplaceUpstreamValuesAndRejectUnknownOrOwnedKeys() {
        String defaults = "a.x=1\n#b.x=2\nc.x=3\n";
        assertEquals("a.x=5\n#b.x=2\nc.x=9\nd.x=7\n",
                PixelsHost.properties(defaults, Map.of("c.x", "9"), new java.util.LinkedHashMap<>(Map.of("a.x", "5", "d.x", "7"))));
        assertThrows(IllegalArgumentException.class, () -> PixelsHost.properties(defaults, Map.of("z.x", "1"), Map.of()));
        for (String owned : new String[] {"retina.enable", "index.rocksdb.data.path", "metadata.server.port", "etcd.hosts",
                "retina.buffer.object.storage.folder", "retina.tile.visibility.capacity"})
            assertThrows(owned, IllegalArgumentException.class, () -> PixelsConfig.pixelsProperties(Map.of(owned, "x")));
        for (String owned : new String[] {"debezium.snapshot.mode", "debezium.database.port", "sink.query.url", "sink.trans.mode",
                "debezium.offset.storage.file.filename", "bootstrap.servers"})
            assertThrows(owned, IllegalArgumentException.class, () -> PixelsConfig.sinkProperties(Map.of(owned, "x")));
        assertEquals(Map.of("sink.retina.client", "8"), PixelsConfig.sinkProperties(Map.of("sink.retina.client", "8")));
        assertThrows(IllegalArgumentException.class,
                () -> PixelsConfig.sinkProperties(Map.of("sink.retina.client", "8\ndebezium.skipped.operations=u")));
        assertThrows(IllegalArgumentException.class,
                () -> PixelsConfig.pixelsProperties(Map.of("node.virtual.num", "1\nretina.storage.gc.enabled=true")));
    }

    @Test public void publicEngineConfigInlinesItsOverridesForTheRunSnapshot() throws Exception {
        java.nio.file.Path root = java.nio.file.Path.of("").toAbsolutePath();
        var settings = SibenchConfig.engine(root, SibenchConfig.read(root.resolve("config/sibench.yaml")), Engine.PIXELS);
        assertEquals("local", ConfigValues.string(settings, "pixels_host"));
        assertEquals("false", ConfigValues.object(settings, "properties").get("index.rocksdb.multicf"));
        assertEquals("none", ConfigValues.object(settings, "sink_properties").get("sink.monitor.freshness.level"));
        assertTrue(SibenchConfig.postgresSettings(ConfigValues.object(ConfigValues.object(settings, "source"), "settings")).containsKey("fsync"));
        // Every public override names an upstream Pixels key, so none is silently ignored.
        String upstream = java.nio.file.Files.readString(root.resolve("third_party/pixels/pixels-common/src/main/resources/pixels.properties"));
        PixelsHost.properties(upstream, PixelsConfig.properties(root.resolve("config/engine/pixels/pixels.properties")), Map.of());
    }

    @Test public void fileApplicationGetsNoPostgresSourceSettings() {
        var root = java.nio.file.Path.of("/run/sink");
        String apply = PixelsSink.properties(Map.of("sink.retina.client", "8"), root, null);
        assertTrue(apply.contains("sink.mode=retina\n") && apply.contains("sink.retina.client=8\n"));
        assertFalse(apply.contains("debezium.") || apply.contains("sink.datasource"));
        String capture = PixelsSink.properties(Map.of(), root, 15432);
        assertTrue(capture.contains("sink.mode=proto\n") && capture.contains("debezium.database.port=15432\n")
                && capture.contains("debezium.offset.storage.file.filename=/run/sink/offsets.dat\n"));
    }

    @Test public void occupiedFixedPortsFailBeforeAnyServiceStarts() throws Exception {
        try (var occupied = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            assertThrows(IOException.class, () -> PixelsHost.ports("127.0.0.1",
                    Map.of("metadata.server.port", Integer.toString(occupied.getLocalPort()))));
            assertFalse(occupied.isClosed());
        }
    }

    private Object session(java.nio.file.Path root) throws Exception {
        var type = Class.forName("org.sibench.capture.PixelsHost$Session");
        var constructor = type.getDeclaredConstructors()[0];
        constructor.setAccessible(true);
        try (var listener = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
             var client = new java.net.Socket("127.0.0.1", listener.getLocalPort())) {
            return constructor.newInstance(root, root, "test-token", listener.accept(), null);
        }
    }

    private static void handle(Object session, String request) throws Exception {
        var method = session.getClass().getDeclaredMethod("handle", JsonObject.class);
        method.setAccessible(true);
        try { method.invoke(session, JsonParser.parseString(request).getAsJsonObject()); }
        catch (java.lang.reflect.InvocationTargetException error) {
            if (error.getCause() instanceof Exception cause) throw cause;
            throw error;
        }
    }

    @Test public void failedStartNeverOwnsOrDeletesAnExistingDirectory() throws Exception {
        var root = temporary.getRoot().toPath();
        var existing = java.nio.file.Files.createDirectory(root.resolve("existing"));
        var sentinel = java.nio.file.Files.writeString(existing.resolve("keep"), "existing state");
        var session = session(root);
        try {
            assertThrows(IOException.class, () -> handle(session,
                    "{\"op\":\"start\",\"run\":\"existing\",\"timeout_ms\":1000}"));
            assertThrows(IOException.class, () -> handle(session, "{\"op\":\"delete\",\"timeout_ms\":1000}"));
            assertEquals("existing state", java.nio.file.Files.readString(sentinel));
        } finally { ((java.net.Socket) field(session, "socket")).close(); }
    }

    private static Object field(Object session, String name) throws Exception {
        var field = session.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(session);
    }

    private static void setField(Object session, String name, Object value) throws Exception {
        var field = session.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(session, value);
    }

    @Test public void uploadsRejectEscapesOversizeAndSymlinksAndRemoveOnlyTheirOwnPartialFile() throws Exception {
        var root = temporary.getRoot().toPath();
        var run = java.nio.file.Files.createDirectory(root.resolve("run"));
        var outside = java.nio.file.Files.createDirectory(root.resolve("outside"));
        var session = session(root);
        setField(session, "run", run);
        try {
            for (String invalid : new String[] {"-1", "4294967297"})
                assertThrows(IOException.class, () -> handle(session,
                        "{\"op\":\"upload\",\"table\":\"t\",\"name\":\"part.tbl\",\"bytes\":"+invalid+",\"timeout_ms\":1000}"));
            java.nio.file.Files.createSymbolicLink(run.resolve("load"), outside);
            assertThrows(IOException.class, () -> handle(session,
                    "{\"op\":\"upload\",\"table\":\"t\",\"name\":\"part.tbl\",\"bytes\":1,\"timeout_ms\":1000}"));
            assertFalse(java.nio.file.Files.exists(outside.resolve("t")));
            java.nio.file.Files.delete(run.resolve("load"));
            assertThrows(IOException.class, () -> handle(session,
                    "{\"op\":\"upload\",\"table\":\"t\",\"name\":\"part.tbl\",\"bytes\":1,\"timeout_ms\":1000}"));
            var partial = run.resolve("load/t/part.tbl");
            assertFalse(java.nio.file.Files.exists(partial));
            java.nio.file.Files.writeString(partial, "keep");
            assertThrows(IOException.class, () -> handle(session,
                    "{\"op\":\"upload\",\"table\":\"t\",\"name\":\"part.tbl\",\"bytes\":1,\"timeout_ms\":1000}"));
            assertEquals("keep", java.nio.file.Files.readString(partial));
        } finally { ((java.net.Socket) field(session, "socket")).close(); }
    }

    @Test public void commandRecordsRedactTheTracerToken() throws Exception {
        var root = temporary.getRoot().toPath();
        var session = session(root);
        setField(session, "run", root);
        setField(session, "environment", Map.of());
        var method = session.getClass().getDeclaredMethod("launch", String.class, java.util.List.class, Map.class);
        method.setAccessible(true);
        try {
            method.invoke(session, "retina", java.util.List.of(java.nio.file.Path.of(System.getProperty("java.home"),"bin/java").toString(),
                    "-Dsibench.trace.token=test-secret", "-version"), Map.of());
            var text = java.nio.file.Files.readString(root.resolve("retina.command"));
            assertFalse(text.contains("test-secret"));
            assertTrue(text.contains("-Dsibench.trace.token=<redacted>"));
        } finally {
            for (Object process : ((Map<?,?>) field(session,"processes")).values()) {
                ((Process) process).destroyForcibly();
                assertTrue(((Process) process).waitFor(3,TimeUnit.SECONDS));
            }
            ((java.net.Socket) field(session, "socket")).close();
        }
    }

    private static JsonObject identity() {
        return JsonParser.parseString("""
                {"java":{"jars":{"tracer":"a"},"pixels_commit":"p"},
                 "native":{"pixels_commit":"p","postgresql_commit":"pg","retina_capacity":1024,"native_sources_sha256":"cpp","files":{"retina":"a"}}}
                """).getAsJsonObject();
    }

    @Test public void hostIdentityRejectsDifferentJavaAndNativeSemanticsButRecordsPlatformBinaries() throws Exception {
        var expected = identity();
        var actual = identity();
        actual.getAsJsonObject("native").getAsJsonObject("files").addProperty("retina", "other-platform");
        PixelsBuild.requireHost(expected, actual);
        actual.getAsJsonObject("java").getAsJsonObject("jars").addProperty("tracer", "old");
        assertThrows(IOException.class, () -> PixelsBuild.requireHost(expected, actual));
        var wrongCapacity = identity();
        wrongCapacity.getAsJsonObject("native").addProperty("retina_capacity", 512);
        assertThrows(IOException.class, () -> PixelsBuild.requireHost(expected, wrongCapacity));
        assertThrows(IOException.class, () -> PixelsBuild.requireHost(expected, null));
    }

    @Test public void silentHandshakeAndSilentRequestTimeOutAndCloseTheConnection() throws Exception {
        for (boolean handshake : new boolean[] {true, false}) {
            try (var server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
                var worker = Executors.newSingleThreadExecutor();
                try {
                    var peer = worker.submit(() -> {
                        try (var socket = server.accept()) {
                            socket.setSoTimeout(handshake ? 35_000 : 5000);
                            var input = new DataInputStream(socket.getInputStream());
                            input.readNBytes(input.readInt());
                            if (!handshake) {
                                byte[] reply = ("{\"build\":" + identity() + "}").getBytes(StandardCharsets.UTF_8);
                                var output = new DataOutputStream(socket.getOutputStream());
                                output.writeInt(reply.length); output.write(reply); output.flush();
                                input.readNBytes(input.readInt());
                            }
                            assertEquals(-1, input.read());
                        }
                        return null;
                    });
                    long start = System.nanoTime();
                    IOException failure = assertThrows(IOException.class, () -> {
                        try (var client = PixelsHost.Client.connect("127.0.0.1:" + server.getLocalPort(), "token", identity())) {
                            assertEquals(identity(), client.identity());
                            client.call(Map.of("op", "stop"), Duration.ofMillis(200));
                        }
                    });
                    assertTrue(failure.getMessage(), failure.getMessage().contains("timed out"));
                    // The handshake has the production 30-second bound; the request its own 200 ms.
                    long elapsed = System.nanoTime() - start;
                    assertTrue(elapsed < TimeUnit.SECONDS.toNanos(handshake ? 34 : 4));
                    if (handshake) assertTrue(elapsed >= TimeUnit.SECONDS.toNanos(29));
                    peer.get(handshake ? 35 : 5, TimeUnit.SECONDS);
                } finally { worker.shutdownNow(); }
            }
        }
    }

    @Test public void uploadDeadlineAlsoInterruptsABlockedSocketWrite() throws Exception {
        var file = java.nio.file.Files.createTempFile("pixels-upload-", ".tbl");
        try (var server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            try (var data = java.nio.channels.FileChannel.open(file, java.nio.file.StandardOpenOption.WRITE)) {
                data.position(32L * 1024 * 1024 - 1); data.write(java.nio.ByteBuffer.wrap(new byte[] {0}));
            }
            var worker = Executors.newSingleThreadExecutor();
            try {
                var peer = worker.submit(() -> {
                    try (var socket = server.accept()) {
                        socket.setSoTimeout(5000);
                        var input = new DataInputStream(socket.getInputStream());
                        input.readNBytes(input.readInt());
                        var output = new DataOutputStream(socket.getOutputStream());
                        byte[] reply = ("{\"build\":" + identity() + "}").getBytes(StandardCharsets.UTF_8);
                        output.writeInt(reply.length); output.write(reply); output.flush();
                        input.readNBytes(input.readInt());
                        Thread.sleep(600);
                        assertTrue(input.transferTo(java.io.OutputStream.nullOutputStream()) < java.nio.file.Files.size(file));
                    }
                    return null;
                });
                try (var client = PixelsHost.Client.connect("127.0.0.1:" + server.getLocalPort(), "token", identity())) {
                    var failure = assertThrows(IOException.class, () -> client.upload("t", "t.tbl", file, Duration.ofMillis(200)));
                    assertTrue(failure.getMessage(), failure.getMessage().contains("timed out"));
                }
                peer.get(5, TimeUnit.SECONDS);
            } finally { worker.shutdownNow(); }
        } finally { java.nio.file.Files.delete(file); }
    }
}
