package org.sibench.capture;

import static org.junit.Assert.*;

import com.google.gson.Gson;
import com.github.luben.zstd.ZstdOutputStream;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public class SessionMergeTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    private static String record(String event, int pid, long csn) {
        return record(event, pid, csn, 1);
    }

    /** A record of one backend; transaction records carry their XID (0 for a reader). */
    private static String record(String event, int pid, long csn, long xid) {
        var fields = new LinkedHashMap<String, Object>();
        fields.put("event", event);
        fields.put("pid", pid);
        fields.put("xid", xid);
        if (csn >= 0) fields.put("csn", csn);
        return new Gson().toJson(fields);
    }

    /** One backend trace: a filler event and a commit for each completion counter. */
    private Path session(Path directory, int pid, long... counters) throws Exception {
        var lines = new ArrayList<String>();
        for (long csn : counters) {
            lines.add(record("executor_start", pid, -1));
            lines.add(record("transaction_commit", pid, csn));
        }
        Path file = directory.resolve("pg-" + pid + ".jsonl.zst");
        write(file, lines);
        return file;
    }

    @Test
    public void chunksMergeOnTheCompletionCounterAlone() throws Exception {
        Path directory = temporary.newFolder().toPath();
        Path first = session(directory, 101, 10, 30, 50);
        Path second = session(directory, 102, 20, 40);
        var counters = new ArrayList<Long>();
        var sessions = new ArrayList<Integer>();
        var lines = new ArrayList<Integer>();
        try (var merge = SessionMerge.open(List.of(first, second))) {
            while (merge.hasNext()) {
                var chunk = merge.next();
                counters.add(chunk.csn());
                sessions.add(chunk.session());
                int streamed = 0;
                while (merge.event() != null) streamed++;
                assertEquals(chunk.lineCount(), streamed);
                lines.add(streamed);
            }
        }
        assertEquals(List.of(10L, 20L, 30L, 40L, 50L), counters);
        assertEquals(List.of(0, 1, 0, 1, 0), sessions);
        assertEquals(List.of(2, 2, 2, 2, 2), lines);
    }

    @Test
    public void oneChunkHoldsEveryEventUpToItsCommit() throws Exception {
        Path directory = temporary.newFolder().toPath();
        var lines = new ArrayList<String>();
        lines.add(record("executor_start", 101, -1));
        lines.add(record("scan_rescan", 101, -1));
        lines.add(record("row_change", 101, -1));
        lines.add(record("transaction_commit", 101, 7));
        lines.add(record("executor_start", 101, -1));
        lines.add(record("transaction_abort", 101, 9));
        Path file = directory.resolve("pg-101.jsonl.zst");
        write(file, lines);
        try (var merge = SessionMerge.open(List.of(file))) {
            assertTrue(merge.hasNext());
            assertEquals(4, merge.next().lineCount());
            // The events are streamed from the file, in order, and stop at the chunk's end.
            for (int i = 0; i < 4; i++) assertEquals(com.google.gson.JsonParser.parseString(lines.get(i)), merge.event().json());
            assertNull(merge.event());
            assertTrue(merge.hasNext());
            var aborted = merge.next();
            assertEquals(9, aborted.csn());
            assertEquals(2, aborted.lineCount());
            assertEquals(com.google.gson.JsonParser.parseString(lines.get(4)), merge.event().json());
            // A chunk must be read to its end before the next one is taken.
            assertThrows(IOException.class, merge::next);
            assertEquals(com.google.gson.JsonParser.parseString(lines.get(5)), merge.event().json());
            assertFalse(merge.hasNext());
        }
    }

    @Test
    public void transactionEndsWithoutCountersAreRejected() throws Exception {
        for (String event : List.of("transaction_commit", "transaction_abort")) {
            Path file = temporary.newFolder().toPath().resolve("pg-101.jsonl.zst");
            write(file, List.of(record("executor_start", 101, -1), record(event, 101, -1)));
            var failure = assertThrows(IOException.class, () -> drain(file));
            assertTrue(failure.getMessage(), failure.getMessage().contains("completion counter"));
        }
    }

    @Test
    public void aCounterThatDoesNotGrowInOneSessionIsRejected() throws Exception {
        Path directory = temporary.newFolder().toPath();
        Path file = session(directory, 101, 20, 10);
        var failure = assertThrows(IOException.class, () -> drain(file));
        assertTrue(failure.getMessage(), failure.getMessage().contains("does not grow"));
    }

    @Test
    public void eventsAfterTheLastCommitAreRejected() throws Exception {
        Path directory = temporary.newFolder().toPath();
        var lines = new ArrayList<String>();
        lines.add(record("transaction_commit", 101, 5));
        lines.add(record("executor_start", 101, -1));
        Path file = directory.resolve("pg-101.jsonl.zst");
        write(file, lines);
        var failure = assertThrows(IOException.class, () -> drain(file));
        assertTrue(failure.getMessage(), failure.getMessage().contains("without a commit or abort"));
    }

    /**
     * A reader increments nothing and records the counter it observed, so it sorts after the
     * writer at that position whatever its session, and keeps its own session's order.
     */
    @Test
    public void readersSortAfterTheWriterAtTheirPosition() throws Exception {
        Path directory = temporary.newFolder().toPath();
        Path readers = directory.resolve("pg-101.jsonl.zst");
        write(
                readers,
                List.of(
                        record("transaction_commit", 101, 5, 0),
                        record("transaction_commit", 101, 5, 0),
                        record("transaction_commit", 101, 7, 0)));
        Path writers = directory.resolve("pg-102.jsonl.zst");
        write(
                writers,
                List.of(record("transaction_commit", 102, 5, 40), record("transaction_commit", 102, 6, 41)));
        var order = new ArrayList<String>();
        try (var merge = SessionMerge.open(List.of(readers, writers))) {
            while (merge.hasNext()) {
                var chunk = merge.next();
                while (merge.event() != null) {}
                order.add(chunk.csn() + (chunk.writer() ? "w" : "r"));
            }
        }
        assertEquals(List.of("5w", "5r", "5r", "6w", "7r"), order);
    }

    @Test
    public void twoWritersNeverShareAPosition() throws Exception {
        Path directory = temporary.newFolder().toPath();
        Path first = directory.resolve("pg-101.jsonl.zst"), second = directory.resolve("pg-102.jsonl.zst");
        write(first, List.of(record("transaction_commit", 101, 5, 40)));
        write(second, List.of(record("transaction_abort", 102, 5, 41)));
        var failure =
                assertThrows(
                        IOException.class,
                        () -> {
                            try (var merge = SessionMerge.open(List.of(first, second))) {
                                while (merge.hasNext()) {
                                    merge.next();
                                    while (merge.event() != null) {}
                                }
                            }
                        });
        assertTrue(failure.getMessage(), failure.getMessage().contains("share completion position"));
    }

    /** Reads every chunk, so failures past the first one surface. */
    private static void drain(Path file) throws IOException {
        try (var merge = SessionMerge.open(List.of(file))) {
            while (merge.hasNext()) {
                merge.next();
                while (merge.event() != null) {}
            }
        }
    }

    /** A line that does not parse fails only when it is reached, after every event before it. */
    @Test
    public void anUnparsableLineFailsWhereItIsReached() throws Exception {
        Path directory = temporary.newFolder().toPath();
        var lines = new ArrayList<String>();
        lines.add(record("executor_start", 101, -1));
        lines.add(record("executor_start", 101, -1).substring(0, 10));
        lines.add(record("transaction_commit", 101, 7));
        Path file = directory.resolve("pg-101.jsonl.zst");
        write(file, lines);
        try (var merge = SessionMerge.open(List.of(file))) {
            merge.next();
            assertEquals(com.google.gson.JsonParser.parseString(lines.get(0)), merge.event().json());
            assertThrows(com.google.gson.JsonParseException.class, merge::event);
        }
    }

    private static Path write(Path file, List<String> lines) throws IOException {
        try (var out = new ZstdOutputStream(Files.newOutputStream(file))) {
            for (String line : lines) {
                out.write(line.getBytes(java.nio.charset.StandardCharsets.UTF_8));
                out.write('\n');
            }
        }
        return file;
    }
}
