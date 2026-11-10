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

public class PixelsHostTest {
    @org.junit.Rule public org.junit.rules.TemporaryFolder temporary = new org.junit.rules.TemporaryFolder();

    private Object session(java.nio.file.Path root) throws Exception {
        var type = Class.forName("org.sibench.capture.PixelsHost$Session");
        var constructor = type.getDeclaredConstructors()[0];
        constructor.setAccessible(true);
        try (var listener = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
             var client = new java.net.Socket("127.0.0.1", listener.getLocalPort())) {
            return constructor.newInstance(root, root, "test-token", listener.accept());
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
                            socket.setSoTimeout(5000);
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
                        try (var client = PixelsHost.Client.connect("127.0.0.1:" + server.getLocalPort(), "token", identity(),
                                Duration.ofMillis(handshake ? 200 : 2000))) {
                            assertEquals(identity(), client.identity());
                            client.call(Map.of("op", "check"), Duration.ofMillis(200));
                        }
                    });
                    assertTrue(failure.getMessage(), failure.getMessage().contains("timed out"));
                    assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(4));
                    peer.get(5, TimeUnit.SECONDS);
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
