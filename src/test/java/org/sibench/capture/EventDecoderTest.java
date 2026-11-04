package org.sibench.capture;

import static org.junit.Assert.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.Test;

public class EventDecoderTest {
    private static String input(int records) {
        var text = new StringBuilder();
        for (int i = 0; i < records; i++) text.append("{\"id\":").append(i).append("}\n");
        return text.toString();
    }

    private static BufferedReader reader(String text) { return new BufferedReader(new StringReader(text)); }

    @Test(timeout = 10_000)
    public void decodesConcurrentlyButDeliversEveryEventOnTheCallerInOrder() throws Exception {
        int count = EventDecoder.BATCH_EVENTS * 5 + 7;
        var secondStarted = new CountDownLatch(1);
        Set<Thread> workers = ConcurrentHashMap.newKeySet();
        Thread caller = Thread.currentThread();
        try (var decoder = new EventDecoder(2, line -> {
            workers.add(Thread.currentThread());
            var event = Event.parse(line);
            int id = event.get("id").getAsInt();
            if (id == EventDecoder.BATCH_EVENTS) secondStarted.countDown();
            if (id == 0) {
                try { assertTrue(secondStarted.await(2, TimeUnit.SECONDS)); }
                catch (InterruptedException error) { throw new AssertionError(error); }
            }
            return event;
        })) {
            int[] next = {0};
            decoder.read(reader(input(count)), event -> {
                assertSame(caller, Thread.currentThread());
                assertEquals(next[0]++, event.get("id").getAsInt());
            });
            assertEquals(count, next[0]);
            assertEquals(2, workers.size());
            assertFalse(workers.contains(caller));
        }
    }

    @Test(timeout = 10_000)
    public void boundsWideEventLookaheadAndHandlesAnOversizedEventAlone() throws Exception {
        String wide = "{\"value\":\"" + "中".repeat(140_000) + "\"}";
        String huge = "{\"value\":\"" + "x".repeat(EventDecoder.BATCH_BYTES) + "\"}";
        var lines = new ArrayList<String>(Collections.nCopies(40, wide));
        lines.add(huge);
        lines.add("{\"value\":\"tail\"}");
        var read = new AtomicInteger();
        var delivered = new AtomicInteger();
        Thread caller = Thread.currentThread();
        try (var decoder = new EventDecoder(2, line -> {
            if (line.length() > EventDecoder.BATCH_BYTES) {
                assertSame(caller, Thread.currentThread());
                assertEquals(40, delivered.get());
            }
            return Event.parse(line);
        }); var input = new BufferedReader(new StringReader("")) {
            @Override public String readLine() {
                int index = read.getAndIncrement();
                return index < lines.size() ? lines.get(index) : null;
            }
        }) {
            decoder.read(input, event -> {
                int index = delivered.getAndIncrement();
                // At most four three-event batches plus one unsubmitted line.
                if (index < 39) assertTrue("Lookahead must be byte bounded", read.get() - index <= 13);
                assertEquals(index < 40 ? "中".repeat(140_000) : index == 40
                        ? "x".repeat(EventDecoder.BATCH_BYTES) : "tail", event.get("value").getAsString());
            });
            assertEquals(42, delivered.get());
        }
    }

    @Test(timeout = 10_000)
    public void propagatesParserAndConsumerFailuresAndStopsWorkers() throws Exception {
        var failure = new IllegalArgumentException("deliberate decoder failure");
        Set<Thread> threads = ConcurrentHashMap.newKeySet();
        try (var decoder = new EventDecoder(2, line -> {
            threads.add(Thread.currentThread());
            if (line.equals("{\"id\":0}")) throw failure;
            return Event.parse(line);
        })) {
            assertSame(failure, assertThrows(IllegalArgumentException.class,
                    () -> decoder.read(reader(input(20_000)), event -> {})));
        }
        assertTrue(threads.stream().noneMatch(Thread::isAlive));
        var consumerFailure = new IOException("deliberate projection failure");
        try (var decoder = new EventDecoder(2, Event::parse)) {
            assertSame(consumerFailure, assertThrows(IOException.class,
                    () -> decoder.read(reader(input(20_000)), event -> { throw consumerFailure; })));
            assertThrows(com.google.gson.JsonSyntaxException.class,
                    () -> decoder.read(reader(input(5000) + "{invalid}\n"), event -> {}));
        }
    }

    @Test(timeout = 10_000)
    public void interruptedConsumerCancelsPendingDecoders() throws Exception {
        var parsing = new CountDownLatch(1);
        var interrupted = new CountDownLatch(1);
        var finished = new CompletableFuture<Throwable>();
        var caller = new Thread(() -> {
            try (var decoder = new EventDecoder(2, line -> {
                parsing.countDown();
                try { new CountDownLatch(1).await(); }
                catch (InterruptedException expected) {
                    interrupted.countDown();
                    Thread.currentThread().interrupt();
                    throw new CancellationException();
                }
                throw new AssertionError();
            })) {
                decoder.read(reader(input(20_000)), event -> {});
                finished.complete(new AssertionError("Expected interruption"));
            } catch (Throwable failure) { finished.complete(failure); }
        });
        caller.start();
        try {
            assertTrue(parsing.await(2, TimeUnit.SECONDS));
            caller.interrupt();
            assertTrue(finished.get(2, TimeUnit.SECONDS) instanceof IOException);
            assertTrue(interrupted.await(2, TimeUnit.SECONDS));
        } finally {
            caller.interrupt();
            caller.join(2000);
            assertFalse(caller.isAlive());
        }
    }
}
