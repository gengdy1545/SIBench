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
    private static JsonObject identity() {
        return JsonParser.parseString("""
                {"java":{"jars":{"tracer":"a"},"pixels_commit":"p"},
                 "native":{"pixels_commit":"p","postgresql_commit":"pg","retina_capacity":1024,"files":{"retina":"a"}}}
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
