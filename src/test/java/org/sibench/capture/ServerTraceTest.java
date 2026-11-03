package org.sibench.capture;

import static org.junit.Assert.*;

import com.github.luben.zstd.ZstdOutputStream;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
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
    /** The records and per-chunk line counts readLine finds, for comparing the byte scanner. */
    private static List<String> byReadLine(java.io.BufferedReader reader) throws IOException {
        var result = new ArrayList<String>();
        long lines = 0;
        for (String line; (line = reader.readLine()) != null; ) {
            lines++;
            if (line.startsWith(ServerTrace.COMMIT) || line.startsWith(ServerTrace.ABORT)
                    || line.startsWith("{\"event\":\"row_change\"") || !line.startsWith("{\"event\":\"")) {
                result.add(lines + " " + line);
                lines = 0;
            }
        }
        result.add(lines + " end");
        return result;
    }

    private static List<String> byScanner(Path file) throws IOException {
        var result = new ArrayList<String>();
        try (var scanner = ServerTrace.Records.rowChanges(file)) {
            for (String line; (line = scanner.next()) != null; ) result.add(scanner.lines() + " " + line);
            result.add(scanner.lines() + " end");
        }
        return result;
    }

    /**
     * Chunk ends and line counts are those readLine gives: LF, CR and CRLF ends, empty and short
     * lines, near-miss prefixes, lines and records across the scanner's buffer, a last line without
     * an end, in the supported zstd stream.
     */
    @Test
    public void rowRecordBoundariesMatchBufferedReader() throws Exception {
        var random = new java.util.Random(7);
        String[] ends = {"\n", "\r", "\r\n"};
        var text = new StringBuilder();
        for (int i = 0; i < 6000; i++) {
            String line =
                    switch (random.nextInt(200) == 0 ? 5 + random.nextInt(2) : random.nextInt(5) == 0 ? 7 : random.nextInt(5)) {
                        case 0 -> "";
                        case 1 -> "{\"event\":\"transaction_";
                        case 2 -> "{\"event\":\"transaction_commit\",\"csn\":" + i + ",\"note\":\"\u00e9\u4e2d\"}";
                        case 3 -> "{\"event\":\"transaction_abort\",\"csn\":" + i + "}";
                        case 4 -> "{\"event\":\"row_change\",\"note\":\"é中\"}";
                        case 5 -> "x".repeat(300_000 + random.nextInt(900_000));
                        case 6 -> "{\"event\":\"transaction_commit\",\"pad\":\"" + "y".repeat(700_000) + "\"}";
                        default -> "{\"event\":\"executor_start\",\"pid\":1,\"sql\":\"select \u00fc\"}";
                    };
            text.append(line);
            if (i < 5999 || random.nextBoolean()) text.append(ends[random.nextInt(3)]);
        }
        byte[] bytes = text.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        Path directory = temporary.newFolder().toPath();
        Path zstd = write(directory.resolve("pg-2.jsonl.zst"), bytes);
        List<String> expected;
        try (var reader = ServerTrace.open(zstd)) {
            expected = byReadLine(reader);
        }
        assertTrue(expected.size() > 500);
        assertEquals(expected, byScanner(zstd));
        assertEquals(List.of("0 end"), byScanner(write(directory.resolve("pg-3.jsonl.zst"), new byte[0])));
    }

    private static Path write(Path file, byte[] bytes) throws IOException {
        try (var out = new ZstdOutputStream(Files.newOutputStream(file))) {
            out.write(bytes);
        }
        return file;
    }

}
