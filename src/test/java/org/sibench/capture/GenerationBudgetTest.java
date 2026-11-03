package org.sibench.capture;

import static org.junit.Assert.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class GenerationBudgetTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void requestsStopAtACompleteTransactionAndReportsTheDrainedOvershoot() throws Exception {
        Path stop = temporary.getRoot().toPath().resolve("stop");
        var budget = new GenerationBudget(Map.of("operation_target", 5, "output_byte_target", 100), stop);
        budget.baseline(40);
        budget.observe(4, 99);
        assertFalse(Files.exists(stop));
        budget.observe(6, 99);
        assertTrue(budget.requested());
        assertEquals("operation_target\n", Files.readString(stop));
        budget.observe(9, 150);
        var report = budget.report(9, 150);
        assertEquals(6L, report.get("operations_at_request"));
        assertEquals(99L, report.get("proto_bytes_at_request"));
        assertEquals(9L, report.get("actual_operations"));
        assertEquals(4L, report.get("operation_overshoot"));
        assertEquals(50L, report.get("output_byte_overshoot"));
    }

    @Test
    public void outputTargetIsIndependentAndAnExistingStopSignalIsNeverOverwritten() throws Exception {
        Path stop = temporary.getRoot().toPath().resolve("stop");
        var budget = new GenerationBudget(Map.of("output_byte_target", 100), stop);
        budget.observe(1000, 99);
        assertFalse(budget.requested());
        budget.observe(1001, 100);
        assertEquals("output_byte_target\n", Files.readString(stop));
        assertEquals(0L, budget.report(2000, 110).get("operation_overshoot"));
        var duplicate = new GenerationBudget(Map.of("operation_target", 1), stop);
        assertThrows(java.nio.file.FileAlreadyExistsException.class, () -> duplicate.observe(1, 101));
        assertFalse(duplicate.requested());
    }

    @Test
    public void disabledTargetsKeepTheNativeBudgetAndImpossibleBaselinesFailBeforeWorkload() throws Exception {
        Path stop = temporary.getRoot().toPath().resolve("stop");
        var disabled = new GenerationBudget(Map.of(), stop);
        disabled.baseline(1000);
        disabled.observe(Long.MAX_VALUE, Long.MAX_VALUE);
        assertFalse(disabled.enabled());
        assertFalse(Files.exists(stop));
        assertEquals("native_workload_completed", disabled.report(100, 1000).get("stop_reason"));
        var budget = new GenerationBudget(Map.of("output_byte_target", 100), stop);
        assertThrows(java.io.IOException.class, () -> budget.baseline(100));
        assertFalse(Files.exists(stop));
        assertThrows(IllegalArgumentException.class,
                () -> new GenerationBudget(Map.of("operation_target", -1), stop));
    }
}
