package org.sibench.capture;

import com.google.gson.*;
import java.nio.file.*;
import java.util.*;

/** Manual queue-capacity comparison with no PostgreSQL, compression or spill-file I/O. */
public final class CaptureTransactionsBenchmark {
    public static void main(String[] args) throws Exception {
        int count = Integer.parseInt(args[0]), rounds = Integer.parseInt(args[1]);
        var results = new ArrayList<Map<String, Object>>();
        for (int round = 0; round < rounds; round++) {
            Path root = Files.createTempDirectory("capture-queue-");
            try (var source = new CaptureTransactions(root.resolve("pending"), 1L << 30, count)) {
                long filling = System.nanoTime();
                for (int id = 1; id <= count; id++) {
                    var object = new JsonObject();
                    object.addProperty("id", id);
                    var chunk = source.begin(id);
                    chunk.append(object.toString(), Event.of(object));
                    source.complete(chunk, id, true, true);
                }
                source.finish();
                double fillSeconds = (System.nanoTime() - filling) / 1e9;
                long started = System.nanoTime(), expected = 0;
                long[] sum = {0}, events = {0};
                for (CaptureTransactions.Chunk next; (next = source.next(() -> {
                    throw new AssertionError("Finished source needs no watermark read");
                })) != null; ) {
                    try (var chunk = next) {
                        if (chunk.csn != ++expected || chunk.pid != expected) throw new AssertionError("Wrong source order");
                        chunk.read(event -> { sum[0] += event.get("id").getAsLong(); events[0]++; });
                    }
                }
                double seconds = (System.nanoTime() - started) / 1e9;
                if (expected != count || events[0] != count || sum[0] != (long) count * (count + 1) / 2)
                    throw new AssertionError("Lost, duplicated or changed source facts");
                results.add(Map.of("round", round, "transactions", count, "fill_seconds", fillSeconds,
                        "drain_seconds", seconds, "sum", sum[0]));
            } finally { Files.delete(root); }
        }
        System.out.println(new GsonBuilder().setPrettyPrinting().create().toJson(results));
    }
}
