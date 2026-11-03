package org.sibench.capture;

import static org.junit.Assert.*;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class CaptureTransactionsTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    private static void append(CaptureTransactions.Chunk chunk, int pid, String value) throws IOException {
        String line = "{\"event\":\"sample\",\"pid\":" + pid + ",\"value\":\"" + value + "\"}";
        chunk.append(line, Event.parse(line));
    }

    @Test public void waitsForMissingEarlierCompletionAndEmitsWriterBeforeReader() throws Exception {
        Path scratch = temporary.getRoot().toPath().resolve("transactions");
        try (var source = new CaptureTransactions(scratch, 1 << 20, 8)) {
            var watermark = new AtomicLong(0);
            var later = source.begin(22);
            append(later, 22, "later");
            source.complete(later, 2, true, true);
            var executor = Executors.newSingleThreadExecutor();
            try {
                var next = executor.submit(() -> source.next(watermark::get));
                assertThrows(TimeoutException.class, () -> next.get(50, TimeUnit.MILLISECONDS));
                var reader = source.begin(33);
                append(reader, 33, "reader");
                source.complete(reader, 1, false, true);
                var earlier = source.begin(11);
                append(earlier, 11, "earlier");
                source.complete(earlier, 1, true, true);
                watermark.set(1);
                assertSame(earlier, next.get(2, TimeUnit.SECONDS));
                earlier.close();
                assertSame(reader, source.next(watermark::get));
                reader.close();
                source.finish();
                assertSame(later, source.next(() -> { throw new AssertionError("No DB query after drain"); }));
                later.close();
                assertNull(source.next(watermark::get));
            } finally { executor.shutdownNow(); }
        }
        assertFalse(Files.exists(scratch));
    }

    @Test public void spillsOnlyPendingTransactionAndDeletesItsFileWhenProcessed() throws Exception {
        Path scratch = temporary.getRoot().toPath().resolve("transactions");
        try (var source = new CaptureTransactions(scratch, 1 << 20, 2)) {
            var chunk = source.begin(1);
            append(chunk, 1, "x".repeat(300_000));
            source.complete(chunk, 1, true, true);
            Path file;
            try (var files = Files.list(scratch)) {
                var paths = files.toList();
                assertEquals(1, paths.size());
                file = paths.get(0);
            }
            assertTrue(Files.size(file) < 10_000);
            try (var input = new com.github.luben.zstd.ZstdInputStream(Files.newInputStream(file))) {
                assertEquals("x".repeat(300_000), Event.parse(new String(input.readAllBytes(),
                        java.nio.charset.StandardCharsets.UTF_8).strip()).get("value").getAsString());
            }
            long[] count = {0};
            chunk.read(event -> {
                assertEquals(300_000, event.get("value").getAsString().length());
                count[0]++;
            });
            assertEquals(1, count[0]);
            source.endStream(1);
            chunk.close();
            try (var files = Files.list(scratch)) { assertEquals(0, files.count()); }
            var next = source.begin(2);
            append(next, 2, "y".repeat(300_000));
            source.complete(next, 2, true, false);
            next.close();
        }
    }

    @Test public void damagedSpoolFailsInsteadOfProducingChangedFacts() throws Exception {
        Path scratch = temporary.getRoot().toPath().resolve("damaged");
        try (var source = new CaptureTransactions(scratch, 1 << 20, 2)) {
            var first = source.begin(1);
            append(first, 1, "earlier".repeat(50_000));
            source.complete(first, 1, true, true);
            var chunk = source.begin(1);
            append(chunk, 1, "x".repeat(300_000));
            source.complete(chunk, 2, true, true);
            Path file;
            try (var files = Files.list(scratch)) { file = files.findFirst().orElseThrow(); }
            byte[] bytes = Files.readAllBytes(file);
            bytes[bytes.length - 1] ^= 1;
            Files.write(file, bytes);
            first.read(event -> assertEquals("earlier".repeat(50_000), event.get("value").getAsString()));
            assertThrows(IOException.class, () -> chunk.read(event -> {}));
        }
    }

    @Test public void failureDetailsAreBoundedWhileAggregateMetricsRemainComplete() throws Exception {
        Path scratch = temporary.getRoot().toPath().resolve("diagnostics");
        try (var source = new CaptureTransactions(scratch, 1 << 20, 65_537)) {
            for (int pid = 1; pid <= 65_537; pid++) source.begin(pid);
            source.fail(new IOException("original source failure"));
            source.retainFailure();
            Path file = scratch.resolve("failure.json");
            var report = com.google.gson.JsonParser.parseString(Files.readString(file)).getAsJsonObject();
            assertTrue(report.get("error").getAsString().contains("original source failure"));
            assertEquals(65_536, report.getAsJsonArray("pending_transactions").size());
            assertTrue(report.get("pending_transactions_truncated").getAsBoolean());
            assertEquals(65_537, report.getAsJsonObject("budgets").get("pending_transactions").getAsInt());
            assertEquals(0, report.getAsJsonObject("budgets").get("pending_encoded_bytes").getAsLong());
            Files.delete(file);
        }
    }

    @Test public void slowSpoolCloseDoesNotBlockOtherTransactionsOrPrematurelyFinish() throws Exception {
        for (boolean complete : new boolean[] {false, true}) {
            var entered = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            var executor = Executors.newFixedThreadPool(2);
            try (var source = new CaptureTransactions(temporary.newFolder().toPath().resolve("pending"), 1 << 20, 3)) {
                var slow = source.begin(1);
                append(slow, 1, "x".repeat(300_000));
                // Inject storage latency without adding a production hook.
                var field = slow.getClass().getDeclaredField("output");
                field.setAccessible(true);
                var output = (java.io.BufferedWriter) field.get(slow);
                field.set(slow, new java.io.BufferedWriter(new java.io.StringWriter()) {
                    private void pause() throws IOException {
                        entered.countDown();
                        try { if (!release.await(5, TimeUnit.SECONDS)) throw new IOException("Test storage timeout"); }
                        catch (InterruptedException error) { Thread.currentThread().interrupt(); throw new IOException(error); }
                    }
                    @Override public void flush() throws IOException { pause(); output.flush(); }
                    @Override public void close() throws IOException { pause(); output.close(); }
                });
                var blocked = executor.submit(() -> {
                    if (complete) source.complete(slow, 1, true, true); else slow.close();
                    return null;
                });
                Future<?> other = null;
                try {
                    assertTrue(entered.await(2, TimeUnit.SECONDS));
                    other = executor.submit(() -> {
                        var chunk = source.begin(2);
                        append(chunk, 2, "independent");
                        source.complete(chunk, 2, true, true);
                        chunk.close();
                        return null;
                    });
                    other.get(500, TimeUnit.MILLISECONDS);
                    assertThrows(IOException.class, source::finish);
                } finally {
                    release.countDown();
                    blocked.get(2, TimeUnit.SECONDS);
                    if (other != null) other.get(2, TimeUnit.SECONDS);
                }
                source.finish();
                if (complete) {
                    assertSame(slow, source.next(() -> 2));
                    slow.close();
                }
                assertNull(source.next(() -> 2));
            } finally { release.countDown(); executor.shutdownNow(); }
        }
    }

    @Test public void sourceTransactionsShareFramesWithoutReadingOrDeletingTheirNeighbours() throws Exception {
        Path scratch = temporary.getRoot().toPath().resolve("frames");
        try (var source = new CaptureTransactions(scratch, 1 << 20, 4)) {
            String firstValue = "α🙂".repeat(60_000), secondValue = "second".repeat(50_000);
            var first = source.begin(1);
            append(first, 1, firstValue);
            assertThrows(IOException.class, () -> source.endStream(1));
            source.complete(first, 1, false, true);
            var second = source.begin(1);
            append(second, 1, secondValue);
            source.complete(second, 2, false, true);
            try (var files = Files.list(scratch)) { assertEquals(1, files.count()); }
            long[] counts = {0, 0};
            first.read(e -> { assertEquals(firstValue, e.get("value").getAsString()); counts[0]++; });
            first.close();
            source.endStream(1);
            second.read(e -> { assertEquals(secondValue, e.get("value").getAsString()); counts[1]++; });
            assertArrayEquals(new long[] {1, 1}, counts);
            try (var files = Files.list(scratch)) { assertEquals(1, files.count()); }
            second.close();
            try (var files = Files.list(scratch)) { assertEquals(0, files.count()); }
        }
    }

    @Test public void rotatedSegmentsWaitForTheirLastTransactionThenDisappear() throws Exception {
        Path scratch = temporary.getRoot().toPath().resolve("rotation");
        byte[] random = new byte[9 << 20];
        new java.util.Random(7).nextBytes(random);
        String value = java.util.Base64.getEncoder().encodeToString(random);
        try (var source = new CaptureTransactions(scratch, 32L << 20, 3)) {
            var first = source.begin(1);
            append(first, 1, value);
            source.complete(first, 1, true, true);
            var second = source.begin(1);
            append(second, 1, "x".repeat(300_000));
            source.complete(second, 2, true, true);
            try (var files = Files.list(scratch)) { assertEquals(2, files.count()); }
            first.read(e -> assertEquals(value, e.get("value").getAsString()));
            first.close();
            try (var files = Files.list(scratch)) { assertEquals(1, files.count()); }
            source.finish();
            second.close();
            try (var files = Files.list(scratch)) { assertEquals(0, files.count()); }
        }
    }

    @Test public void consumedActiveTailCanAcceptMoreFramesAndIsReclaimedAtSourceEnd() throws Exception {
        Path scratch = temporary.getRoot().toPath().resolve("tail");
        try (var source = new CaptureTransactions(scratch, 1 << 20, 2)) {
            for (int i = 0; i < 10; i++) {
                String value = Integer.toString(i).repeat(300_000);
                var chunk = source.begin(1);
                append(chunk, 1, value);
                source.complete(chunk, i, false, true);
                chunk.read(e -> assertEquals(value, e.get("value").getAsString()));
                chunk.close();
                assertEquals(0L, source.metrics().get("pending_encoded_bytes"));
                try (var files = Files.list(scratch)) { assertEquals(1, files.count()); }
            }
            source.endStream(1);
            try (var files = Files.list(scratch)) { assertEquals(0, files.count()); }
        }
    }

    @Test public void manySmallTransactionsSpillWithinTheSharedMemoryBudget() throws Exception {
        Path scratch = temporary.getRoot().toPath().resolve("small-transactions");
        String value = "x".repeat(70_000);
        try (var source = new CaptureTransactions(scratch, 128L << 20, 2048)) {
            for (int pid = 1; pid <= 1024; pid++) {
                var chunk = source.begin(pid);
                append(chunk, pid, value);
                source.complete(chunk, pid, true, true);
            }
            // Each transaction is below 256KiB; their aggregate crosses the 64MiB memory budget.
            try (var files = Files.list(scratch)) { assertTrue(files.count() > 0); }
            var metrics = source.metrics();
            assertEquals(1024, metrics.get("peak_transactions"));
            assertTrue(((Number) metrics.get("peak_memory_encoded_bytes")).longValue() <= 64L << 20);
            assertTrue(((Number) metrics.get("pending_encoded_bytes")).longValue() > 64L << 20);
            source.finish();
            for (int pid = 1; pid <= 1024; pid++) {
                try (var chunk = source.next(() -> { throw new AssertionError("Source already ended"); })) {
                    assertEquals(pid, chunk.pid);
                    chunk.read(event -> assertEquals(value, event.get("value").getAsString()));
                }
            }
            assertEquals(0L, source.metrics().get("pending_memory_encoded_bytes"));
            assertEquals(0L, source.metrics().get("pending_encoded_bytes"));
            try (var files = Files.list(scratch)) { assertEquals(0, files.count()); }
        }
    }

    @Test public void quotasAndUnfinishedTransactionsFailWithoutDroppingFacts() throws Exception {
        try (var source = new CaptureTransactions(temporary.getRoot().toPath().resolve("bytes"), 100, 2)) {
            var chunk = source.begin(1);
            assertTrue(assertThrows(IOException.class, () -> append(chunk, 1, "x".repeat(101)))
                    .getMessage().contains("byte budget"));
            assertTrue(assertThrows(IOException.class, source::finish).getMessage().contains("unfinished"));
        }
        try (var source = new CaptureTransactions(temporary.getRoot().toPath().resolve("count"), 1000, 1)) {
            source.begin(1);
            assertTrue(assertThrows(IOException.class, () -> source.begin(2)).getMessage().contains("limit"));
        }
    }

    @Test public void closingAQueuedChunkDoesNotEmitItOrLoseTheClaimedChunk() throws Exception {
        try (var source = new CaptureTransactions(temporary.getRoot().toPath().resolve("closing"), 1 << 20, 3)) {
            var first = source.begin(1);
            var cancelled = source.begin(2);
            var last = source.begin(3);
            source.complete(first, 1, true, true);
            source.complete(cancelled, 2, true, true);
            source.complete(last, 3, true, true);
            assertSame(first, source.next(() -> 3));
            cancelled.close();
            first.close();
            source.finish();
            assertSame(last, source.next(() -> 3));
            last.close();
            assertNull(source.next(() -> 3));
            assertEquals(0, source.metrics().get("pending_transactions"));
        }
    }
}
