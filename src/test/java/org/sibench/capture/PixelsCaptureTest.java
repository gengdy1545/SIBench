package org.sibench.capture;

import static org.junit.Assert.*;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.schema.SourceSchema;

/** Drives PixelsCapture through a real tracer socket with scripted facts and file-sink reports. */
public class PixelsCaptureTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    private static void fact(DataOutputStream out, int kind, long timestamp, int key, long rowId) throws IOException {
        out.writeByte(kind);
        out.writeLong(kind == 10 ? 0 : 7);
        out.writeLong(kind == 10 ? 0 : 9);
        out.writeLong(timestamp);
        byte[] bytes = kind == 10 ? new byte[0] : ByteBuffer.allocate(4).putInt(key).array();
        out.writeInt(bytes.length);
        out.write(bytes);
        out.writeLong(rowId);
        out.writeLong(1);
        out.writeInt(0);
        out.writeInt(0);
        out.flush();
    }

    @Test(timeout = 10000) public void invalidKeyLengthFailsBeforeAllocatingOrWaitingForItsPayload() throws Exception {
        var source = SourceSchema.of("sysbench", Path.of("benchmark/sysbench"), Map.of("tables", 1L));
        var loopback = InetAddress.getLoopbackAddress();
        for (int length : new int[] {-1, 32 * 1024 * 1024 + 1}) {
            var trace = new PixelsTrace(temporary.newFolder().toPath().resolve("trace"), source);
            var listener = new ServerSocket(0, 1, loopback);
            try (var capture = new PixelsCapture(trace, listener, "token")) {
                capture.acceptTracer(Duration.ofSeconds(2));
                try (var socket = new Socket(loopback, listener.getLocalPort())) {
                    var out = new DataOutputStream(socket.getOutputStream());
                    out.writeInt(0x53495854); out.writeUTF("token");
                    out.writeByte(PixelsTrace.PUT);
                    out.writeLong(1); out.writeLong(1); out.writeLong(1); out.writeInt(length); out.flush();
                    IOException failure = assertThrows(IOException.class,
                            () -> capture.call(() -> {}, true, Duration.ofSeconds(2)));
                    var causes = new java.io.StringWriter();
                    failure.printStackTrace(new java.io.PrintWriter(causes));
                    assertTrue(causes.toString(), causes.toString().contains("Invalid Pixels tracer key size"));
                }
            }
        }
    }

    @Test public void barrierAcknowledgementRunsSyncedStepsAndExportsCommittedTransactions() throws Exception {
        var source = SourceSchema.of("sysbench", Path.of("benchmark/sysbench"), Map.of("tables", 1L));
        var trace = new PixelsTrace(temporary.getRoot().toPath().resolve("trace"), source);
        trace.register(Map.of(7L, new PixelsTrace.Table("sbtest1", 1, 9, "int"), 8L, new PixelsTrace.Table("sibench_marker", 0, 10, "bigint")));
        var loopback = InetAddress.getLoopbackAddress();
        var traceListener = new ServerSocket(0, 1, loopback);
        try (var capture = new PixelsCapture(trace, traceListener, "trace-token")) {
            capture.acceptTracer(Duration.ofSeconds(10));
            try (var tracer = new Socket(loopback, traceListener.getLocalPort())) {
                var out = new DataOutputStream(tracer.getOutputStream());
                var barriers = new DataInputStream(tracer.getInputStream());
                out.writeInt(0x53495854);
                out.writeUTF("trace-token");
                fact(out, PixelsTrace.PUT, 2, 1, 100);
                // A scripted tracer answers each barrier only after the facts it has already sent.
                var answered = new AtomicBoolean();
                var responder = new Thread(() -> {
                    try {
                        while (true) {
                            long barrier = barriers.readLong();
                            synchronized (out) { fact(out, 10, 0, 0, barrier); }
                            answered.set(true);
                        }
                    } catch (IOException closed) {
                        // The test closes the connection.
                    }
                });
                responder.setDaemon(true);
                responder.start();
                capture.synced(() -> trace.finishBaseline(Map.of("sbtest1", 1L)), Duration.ofSeconds(10));
                assertTrue(answered.get());

                // The file sink reports BEGIN in source order and commits possibly out of order.
                capture.begin(5);
                capture.begin(6);
                synchronized (out) {
                    fact(out, PixelsTrace.LOOKUP, 5, 1, 100);
                    fact(out, PixelsTrace.UPDATE, 5, 1, 101);
                    out.writeByte(PixelsTrace.PUT);
                    out.writeLong(8);
                    out.writeLong(10);
                    out.writeLong(6);
                    out.writeInt(8);
                    out.write(ByteBuffer.allocate(8).putLong(1).array());
                    out.writeLong(1);
                    out.writeLong(1);
                    out.writeInt(0);
                    out.writeInt(0);
                    out.flush();
                }
                capture.commit(6);
                capture.commit(5);
                capture.synced(trace::finish, Duration.ofSeconds(10));
                // The marker-only transaction closes a phase and is not a Trace version.
                assertEquals(1, trace.version());
                // A lost tracer connection surfaces at the next check.
                tracer.close();
                long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
                IOException failure = null;
                while (failure == null && System.nanoTime() < deadline) {
                    try {
                        capture.check();
                        Thread.sleep(10);
                    } catch (IOException error) {
                        failure = error;
                    }
                }
                assertNotNull(failure);
                // A step submitted after a failure reports it instead of waiting for the timeout.
                long start = System.nanoTime();
                assertThrows(IOException.class, () -> capture.call(() -> {}, false, Duration.ofMinutes(5)));
                assertTrue(System.nanoTime() - start < Duration.ofSeconds(5).toNanos());
            }
        }
    }
}
