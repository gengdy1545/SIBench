package org.sibench.capture;

import static org.junit.Assert.*;
import java.io.IOException;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Test;

public class PostgresCaptureTest {
    @Test public void truncatedLiveSourcePreservesTheFirstErrorAndDiagnostics()throws Exception {
        Path root=Files.createTempDirectory("pg-capture-test-"),endpoint=root.resolve("capture.sock");
        try(var capture=new PostgresCapture(endpoint,root.resolve("pending"),1<<20,8)) {
            try(var socket=SocketChannel.open(StandardProtocolFamily.UNIX)) {
                socket.connect(UnixDomainSocketAddress.of(endpoint));
                var bytes=ByteBuffer.wrap(("pg-123.jsonl.zst\n"
                        +"{\"event\":\"capture_start\",\"pid\":123,\"sequence\":0}\n").getBytes(StandardCharsets.UTF_8));
                while(bytes.hasRemaining())socket.write(bytes);
            }
            long deadline=System.nanoTime()+java.util.concurrent.TimeUnit.SECONDS.toNanos(3);
            IOException failure=null;
            while(failure==null&&System.nanoTime()<deadline) {
                try{capture.check();Thread.sleep(10);}catch(IOException error){failure=error;}
            }
            assertNotNull("Truncated source must fail without endSources",failure);
            assertTrue(failure.getCause().getMessage().contains("completion marker"));
            assertThrows(IOException.class,()->capture.transactions.next(()->Long.MAX_VALUE));
        }
        Path report=root.resolve("pending/failure.json");
        assertTrue(Files.readString(report).contains("completion marker"));
        Files.delete(report);Files.delete(root.resolve("pending"));Files.delete(root);
    }
}
