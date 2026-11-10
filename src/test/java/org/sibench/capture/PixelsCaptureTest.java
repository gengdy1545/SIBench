package org.sibench.capture;

import static org.junit.Assert.*;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.schema.SourceSchema;

/** Drives PixelsCapture through real sockets with a scripted tracer and sink. */
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
        out.writeLong(-1);
        out.flush();
    }

    @Test public void barrierAcknowledgementRunsSyncedStepsAndExportsCommittedTransactions() throws Exception {
        var source = SourceSchema.of("sysbench", Path.of("benchmark/sysbench"), Map.of("tables", 1L));
        var trace = new PixelsTrace(temporary.getRoot().toPath().resolve("trace"), source, Map.of(
                7L, new PixelsTrace.Table("sbtest1", 1, 9, "int"), 8L, new PixelsTrace.Table("sibench_marker", 0, 10, "bigint")), null);
        var loopback = InetAddress.getLoopbackAddress();
        var traceListener = new ServerSocket(0, 1, loopback);
        var reportListener = new ServerSocket(0, 1, loopback);
        try (var capture = new PixelsCapture(trace, traceListener, "trace-token", reportListener, "sink-token")) {
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

                try (var sink = new Socket(loopback, reportListener.getLocalPort())) {
                    var reports = sink.getOutputStream();
                    reports.write("T sink-token\nB 5\nB 6\n".getBytes(StandardCharsets.UTF_8));
                    reports.flush();
                    capture.acceptSink(Duration.ofSeconds(10));
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
                        out.writeLong(-1);
                        out.flush();
                    }
                    reports.write("C 6\nC 5\n".getBytes(StandardCharsets.UTF_8));
                    reports.flush();
                    capture.awaitMarker(1, Duration.ofSeconds(10));
                    capture.call(trace::finish, false, Duration.ofSeconds(10));
                    assertEquals(1, trace.version());
                    // Stopping the sink after its last marker is not a failure.
                    capture.sinkStopping();
                    // A reported failure surfaces at the next check.
                    reports.write("F Retina write failed\n".getBytes(StandardCharsets.UTF_8));
                    reports.flush();
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
}
