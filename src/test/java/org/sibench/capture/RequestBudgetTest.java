package org.sibench.capture;

import static org.junit.Assert.*;
import java.io.IOException;
import java.time.Duration;
import org.junit.Test;

public class RequestBudgetTest {
    @Test public void carriesDrainedOvershootIntoRemainingWorkloads() throws Exception {
        var budget = new RequestBudget(10, Duration.ofSeconds(10));
        budget.begin("first", 2, null);
        for (int i=0; i<7; i++) budget.record(1);
        assertTrue(budget.reached());
        budget.end();
        budget.begin("second", 1, null);
        budget.record(1); budget.record(1);
        assertFalse(budget.reached());
        assertThrows(IOException.class, budget::end);
        budget.record(1);
        budget.end();
        assertEquals(10L, budget.finish().get("effective_requests"));
        assertThrows(IOException.class, () -> budget.record(1));
    }

    @Test public void rejectsNoProgressAndIncompleteSupply() throws Exception {
        var budget = new RequestBudget(10, Duration.ofSeconds(10));
        assertThrows(IOException.class, budget::finish);
        budget.begin("empty", 1, null);
        assertThrows(IOException.class, () -> budget.requireProgress(0));
        assertThrows(IOException.class, budget::end);
        assertThrows(IOException.class, () -> budget.begin("other", 1, null));
    }

    @Test public void nativeSizedModeStillCountsWithoutStopping() throws Exception {
        var budget = new RequestBudget(0, Duration.ofSeconds(10));
        budget.begin("native", 1, null);
        budget.record(1);
        assertFalse(budget.reached());
        budget.end();
        assertEquals(1L, budget.finish().get("effective_requests"));
    }

    @Test public void countsCompleteGroupsAndRejectsInvalidCounts() throws Exception {
        var budget = new RequestBudget(4, Duration.ofSeconds(10));
        budget.begin("grouped", 1, null);
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
