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
import java.util.List;

/** A zstd backend trace reads its lines; an unended one does not read and a plain name is refused. */
public class ServerTraceTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void everyReaderRejectsPlainTraceFiles() throws Exception {
        var file = temporary.newFile("pg-9.jsonl").toPath();
        Files.writeString(file, "{\"event\":\"transaction_commit\",\"pid\":9}\n");
        assertThrows(IOException.class, () -> ServerTrace.open(file));
        assertThrows(IOException.class, () -> new ServerTrace.Records(file));
        assertThrows(IOException.class, () -> ServerTrace.Records.rowChanges(file));
    }

    @Test
    public void namesAndZstdRead() throws Exception {
        var directory = temporary.getRoot().toPath();
        assertFalse(ServerTrace.isServerTrace("pg-42.jsonl"));
        assertTrue(ServerTrace.isServerTrace("pg-42.jsonl.zst"));
        assertFalse(ServerTrace.isServerTrace("jdbc-1-2.jsonl"));
        assertEquals(42, ServerTrace.pid("pg-42.jsonl.zst"));
        // The instrumentation flushes at every transaction end and ends the frame at exit; a
        // later write starts another frame of the same stream.
        try (var out = new ZstdOutputStream(Files.newOutputStream(directory.resolve("pg-2.jsonl.zst")))) {
            out.write("{\"event\":\"a\"}\n".getBytes(StandardCharsets.UTF_8));
            out.flush();
            out.write("{\"event\":\"b\"}\n".getBytes(StandardCharsets.UTF_8));
        }
        var lines = new ArrayList<String>();
        try (var reader = ServerTrace.open(ServerTrace.locate(directory, 2))) {
            for (String line; (line = reader.readLine()) != null; ) lines.add(line);
        }
        assertEquals(java.util.List.of("{\"event\":\"a\"}", "{\"event\":\"b\"}"), lines);
        assertEquals("pg-2.jsonl.zst", ServerTrace.locate(directory, 2).getFileName().toString());
        assertThrows(IOException.class, () -> ServerTrace.locate(directory, 1));
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

    @Test
    public void rowRecordsPreserveSourcePositionsAndNoncanonicalInput() throws Exception {
        var file = temporary.getRoot().toPath().resolve("pg-4.jsonl.zst");
        String row = "{\"event\":\"row_change\",\"note\":\"é中\"}";
        String commit = "{\"event\":\"transaction_commit\",\"xid\":1}";
        String abort = "{\"event\":\"transaction_abort\",\"xid\":2}";
        String noncanonical = "  " + row;
        String large = "{\"event\":\"row_change\",\"note\":\"" + "é中".repeat(400_000) + "\"}";
        String text = "{\"event\":\"executor_start\",\"pad\":\"" + "x".repeat(1 << 20)
                + "\"}\r\n" + row + "\rignored malformed input\n" + commit
                + "\n{\"event\":\"executor_end\"}\r" + noncanonical + "\r\n\n"
                + abort + "\n" + large + "\r{\"event\":\"transaction_";
        try (var out = new ZstdOutputStream(Files.newOutputStream(file))) {
            out.write(text.getBytes(StandardCharsets.UTF_8));
        }
        var actual = new ArrayList<String>();
        try (var reader = ServerTrace.Records.rowChanges(file)) {
            for (String line; (line = reader.next()) != null; )
                actual.add(reader.lines() + " " + line);
            assertEquals(1, reader.lines());
        }
        assertEquals(List.of("2 " + row, "1 ignored malformed input", "1 " + commit,
                "2 " + noncanonical, "1 ", "1 " + abort, "1 " + large), actual);
        assertThrows(IOException.class,
                () -> ServerTrace.Records.rowChanges(file.resolveSibling("pg-4.jsonl")));
        byte[] compressed = Files.readAllBytes(file);
        Files.write(file, java.util.Arrays.copyOf(compressed, compressed.length - 1));
        assertThrows(IOException.class, () -> {
            try (var reader = ServerTrace.Records.rowChanges(file)) {
                while (reader.next() != null) {}
            }
        });
    }
}
