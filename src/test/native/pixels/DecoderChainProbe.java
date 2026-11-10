package org.sibench.pixels;

import io.pixelsdb.pixels.sink.util.concurrent.StreamOrderedDecoder;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/** Isolates a stalled ordered decoder without PostgreSQL, Pixels or source instrumentation. */
public class DecoderChainProbe {
    public static void main(String[] args) throws Exception {
        int count = 8192;
        var decoder = new StreamOrderedDecoder(4, "decoder-probe");
        var release = new CountDownLatch(1);
        var decoded = new CountDownLatch(count - 1);
        var published = new AtomicInteger();
        var pending = new ArrayList<CompletableFuture<Void>>();
        decoder.start();
        try {
            for (int i = 0; i < count; i++) {
                pending.add(decoder.submit("one-table", i, value -> {
                    if (value == 0) release.await(); else decoded.countDown();
                    return value;
                }, result -> {
                    if (result.failure() != null || result.value() != published.getAndIncrement())
                        throw new AssertionError("Decode failure or incorrect publication order");
                }));
            }
            if (!decoded.await(3, TimeUnit.SECONDS)) throw new AssertionError("Decode did not finish");
            release.countDown();
            try {
                CompletableFuture.allOf(pending.toArray(CompletableFuture[]::new)).get(3, TimeUnit.SECONDS);
                if (published.get() != count) throw new AssertionError("Missing published events");
                System.out.println("PASSED: published=" + published.get());
            } catch (TimeoutException stalled) {
                throw new AssertionError("Decoder stalled: published=" + published.get() + "/" + count
                        + ", unfinished=" + pending.stream().filter(f -> !f.isDone()).count(), stalled);
            }
        } finally {
            release.countDown();
            decoder.abort();
        }
    }
}
