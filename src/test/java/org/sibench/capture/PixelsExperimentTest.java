package org.sibench.capture;

import static org.junit.Assert.*;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.*;
import org.junit.Test;

public class PixelsExperimentTest {
    @Test public void failedConcurrentQueryCancelsAnIncompleteApplication() {
        var application = new CompletableFuture<Void>();
        var query = CompletableFuture.failedFuture(new IOException("deliberate query failure"));
        var error = assertThrows(IOException.class, () -> PixelsExperiment.await(application, query, Duration.ofSeconds(2)));
        assertEquals("deliberate query failure", error.getMessage());
        assertTrue(application.isCancelled());
    }
}
