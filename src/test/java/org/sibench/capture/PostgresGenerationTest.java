package org.sibench.capture;

import static org.junit.Assert.*;

import com.google.gson.JsonParser;
import java.io.IOException;
import java.util.Map;
import org.junit.Test;

/** Hand-derived TP windows; the passing case is the recorded 8,706-row probe (24 samples, 23 commits, 1 abort). */
public class PostgresGenerationTest {
    private static void check(String samples, long commits, long aborts, int sessions) throws IOException {
        PostgresGeneration.requireTpWindow(JsonParser.parseString(samples).getAsJsonObject(),
                Map.of("commits", commits, "aborts", aborts), sessions, 1);
    }

    @Test public void tpClientSamplesBoundCapturedOutcomes() throws Exception {
        check("{\"tp1\":20,\"tp2\":4}", 23, 1, 1);
        check("{\"tp1\":23}", 23, 1, 1); // The abort was a conflict, which HyBench does not sample.
        check("{\"tp1\":24}", 24, 0, 1);
        // A lost captured transaction, a phantom commit and a missing or extra session are rejected.
        assertThrows(IOException.class, () -> check("{\"tp1\":25}", 23, 1, 1));
        assertThrows(IOException.class, () -> check("{\"tp1\":22}", 23, 1, 1));
        assertThrows(IOException.class, () -> check("{\"tp1\":24}", 23, 1, 0));
        assertThrows(IOException.class, () -> check("{\"tp1\":24}", 23, 1, 2));
    }
}
