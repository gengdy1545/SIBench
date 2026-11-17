package org.sibench.capture;

import static org.junit.Assert.*;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class RequestBudgetTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test public void drainedOvershootIsReportedAndTheRunMustReachItsTarget() throws Exception {
        var budget = new RequestBudget(10, Duration.ofSeconds(10));
        budget.begin(null);
        budget.record(7);
        assertFalse(budget.reached());
        assertThrows(IOException.class, budget::end);
        budget.record(1); budget.record(1); budget.record(2);
        assertTrue(budget.reached());
        budget.end();
        var result = budget.finish();
        assertEquals(11L, result.get("effective_requests"));
        assertEquals(1L, result.get("overshoot_requests"));
        assertFalse(result.containsKey("stages"));
        assertThrows(IOException.class, () -> budget.record(1));
    }

    @Test public void rejectsNoProgressIncompleteSupplyAndRestarts() throws Exception {
        var budget = new RequestBudget(10, Duration.ofSeconds(10));
        assertThrows(IOException.class, budget::finish);
        assertThrows(IOException.class, () -> budget.record(1));
        budget.begin(null);
        assertThrows(IOException.class, () -> budget.requireProgress(0));
        assertThrows(IOException.class, budget::end);
        assertThrows(IOException.class, () -> budget.begin(null));
    }

    @Test public void nativeSizedModeStillCountsWithoutStopping() throws Exception {
        var budget = new RequestBudget(0, Duration.ofSeconds(10));
        budget.begin(null);
        budget.record(1);
        assertFalse(budget.reached());
        budget.requireProgress(1);
        budget.end();
        assertEquals(1L, budget.finish().get("effective_requests"));
        assertEquals(0L, budget.finish().get("overshoot_requests"));
    }

    @Test public void reachingTheTargetCreatesTheStopFileOnceAndRefusesAStaleOne() throws Exception {
        var stop = temporary.getRoot().toPath().resolve("budget.stop");
        var budget = new RequestBudget(3, Duration.ofSeconds(10));
        budget.begin(stop);
        budget.record(2);
        assertFalse(Files.exists(stop));
        budget.record(2);
        assertTrue(Files.exists(stop));
        budget.record(1);
        budget.end();
        Files.writeString(temporary.getRoot().toPath().resolve("stale.stop"), "x");
        assertThrows(IOException.class, () -> new RequestBudget(3, Duration.ofSeconds(10)).begin(temporary.getRoot().toPath().resolve("stale.stop")));
    }

    @Test public void countsCompleteGroupsAndRejectsInvalidCounts() throws Exception {
        var budget = new RequestBudget(4, Duration.ofSeconds(10));
        budget.begin(null);
        assertThrows(IllegalArgumentException.class, () -> budget.record(0));
        assertThrows(IllegalArgumentException.class, () -> budget.record(-1));
        budget.record(2);
        assertFalse(budget.reached());
        budget.record(3);
        assertTrue(budget.reached());
        budget.end();
        assertEquals(5L, budget.finish().get("effective_requests"));
        assertEquals(1L, budget.finish().get("overshoot_requests"));
    }
}
