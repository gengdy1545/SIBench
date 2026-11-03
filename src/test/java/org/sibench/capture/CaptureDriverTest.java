package org.sibench.capture;

import static org.junit.Assert.*;

import java.io.*;
import java.net.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.concurrent.atomic.*;
import org.junit.Test;

public class CaptureDriverTest {
    @Test(timeout = 10000)
    public void phaseInterruptionPreservesEverySourceByteAndTheInterruptFlag() throws Exception {
        byte[] payload = new byte[4 << 20];
        for (int i = 0; i < payload.length; i++) payload[i] = (byte) i;
        for (boolean blockingAdapter : new boolean[] {true, false}) {
            Path directory = Files.createTempDirectory("capture-interrupt-");
            Path endpoint = directory.resolve("source.sock");
            try (var listener = ServerSocketChannel.open(StandardProtocolFamily.UNIX);
                    var sender = SocketChannel.open(StandardProtocolFamily.UNIX)) {
                listener.bind(UnixDomainSocketAddress.of(endpoint));
                sender.setOption(StandardSocketOptions.SO_SNDBUF, 1024);
                sender.connect(UnixDomainSocketAddress.of(endpoint));
                try (var receiver = listener.accept()) {
                    var error = new AtomicReference<Throwable>();
                    var interrupted = new AtomicBoolean();
                    OutputStream output = blockingAdapter ? Channels.newOutputStream(sender) : CaptureDriver.directOutput(sender);
                    var worker = new Thread(() -> {
                        try (output) { output.write(payload); }
                        catch (Throwable failure) { error.set(failure); }
                        finally { interrupted.set(Thread.currentThread().isInterrupted()); }
                    });
                    worker.start();
                    var received = new ByteArrayOutputStream();
                    var first = ByteBuffer.allocate(1);
                    assertEquals(1, receiver.read(first));
                    received.write(first.array());
                    // The small send buffer cannot hold the payload: interrupt an active write.
                    worker.interrupt();
                    var bytes = ByteBuffer.allocate(65536);
                    while (receiver.read(bytes) >= 0) {
                        received.write(bytes.array(), 0, bytes.position());
                        bytes.clear();
                    }
                    worker.join(2000);
                    assertFalse(worker.isAlive());
                    assertTrue(interrupted.get());
                    if (blockingAdapter) {
                        assertTrue(String.valueOf(error.get()), error.get() instanceof ClosedByInterruptException);
                        assertTrue(received.size() < payload.length);
                    } else {
                        assertNull(error.get());
                        assertArrayEquals(payload, received.toByteArray());
                    }
                }
            } finally {
                Files.deleteIfExists(endpoint);
                Files.delete(directory);
            }
        }
    }
}
