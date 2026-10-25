package org.sibench.capture;

import static org.junit.Assert.*;

import com.github.luben.zstd.ZstdOutputStream;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;

/** A zstd backend trace reads as the same lines as a plain one; an unended one does not read. */
public class ServerTraceTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void namesAndBothEncodingsRead() throws Exception {
        var directory = temporary.getRoot().toPath();
        assertTrue(ServerTrace.isServerTrace("pg-42.jsonl"));
        assertTrue(ServerTrace.isServerTrace("pg-42.jsonl.zst"));
        assertFalse(ServerTrace.isServerTrace("jdbc-1-2.jsonl"));
        assertEquals(42, ServerTrace.pid("pg-42.jsonl.zst"));
        String text = "{\"event\":\"a\"}\n{\"event\":\"b\"}\n";
        Files.writeString(directory.resolve("pg-1.jsonl"), text);
        // The instrumentation flushes at every transaction end and ends the frame at exit; a
        // later write starts another frame of the same stream.
        try (var out = new ZstdOutputStream(Files.newOutputStream(directory.resolve("pg-2.jsonl.zst")))) {
            out.write("{\"event\":\"a\"}\n".getBytes(StandardCharsets.UTF_8));
            out.flush();
            out.write("{\"event\":\"b\"}\n".getBytes(StandardCharsets.UTF_8));
        }
        for (int pid : new int[] {1, 2}) {
            var lines = new ArrayList<String>();
            try (var reader = ServerTrace.open(ServerTrace.locate(directory, pid))) {
                for (String line; (line = reader.readLine()) != null; ) lines.add(line);
            }
            assertEquals(java.util.List.of("{\"event\":\"a\"}", "{\"event\":\"b\"}"), lines);
        }
        assertEquals("pg-2.jsonl.zst", ServerTrace.locate(directory, 2).getFileName().toString());
        Files.writeString(directory.resolve("pg-2.jsonl"), text);
        assertThrows(IOException.class, () -> ServerTrace.locate(directory, 2));
    }

    @Test
    public void aFrameWithoutItsEndFailsToRead() throws Exception {
        var file = temporary.getRoot().toPath().resolve("pg-3.jsonl.zst");
        var out = new ZstdOutputStream(Files.newOutputStream(file));
        out.write("{\"event\":\"a\"}\n{\"event\":\"b\"}\n".getBytes(StandardCharsets.UTF_8));
        out.flush();
        // The backend never exited: the frame has no end.
        assertThrows(IOException.class, () -> {
            try (var reader = ServerTrace.open(file)) {
                while (reader.readLine() != null) {}
            }
        });
        out.close();
    }
}
