package org.sibench.capture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import java.util.Map;

public class PgSettingsTest {
    @Test
    public void plannerAndParallelismParametersAffectPlans() {
        assertTrue(PgSettings.affectsPlans("random_page_cost", "Query Tuning / Planner Cost Constants"));
        assertTrue(PgSettings.affectsPlans("enable_seqscan", "Query Tuning / Planner Method Configuration"));
        assertTrue(PgSettings.affectsPlans("work_mem", "Resource Usage / Memory"));
        assertTrue(PgSettings.affectsPlans("max_parallel_workers_per_gather", "Resource Usage / Asynchronous Behavior"));
    }

    @Test
    public void speedOnlyParametersDoNot() {
        for (String name :
                new String[] {"shared_buffers", "max_wal_size", "min_wal_size", "checkpoint_timeout",
                    "checkpoint_completion_target", "wal_buffers", "synchronous_commit", "maintenance_work_mem"})
            assertFalse(name, PgSettings.affectsPlans(name, "Resource Usage / Memory"));
    }

    @Test
    public void identityKeepsTheBuildAndAddsOnlyPlanSettings() {
        var settings = new PgSettings.Snapshot(Map.of("shared_buffers", "16GB", "work_mem", "64MB"), Map.of("work_mem", "64MB"));
        var identity = PgSettings.identity(Map.of("sibench_jar_sha256", "a"), settings);
        assertEquals("a", identity.get("sibench_jar_sha256"));
        assertEquals(Map.of("work_mem", "64MB"), identity.get("postgres_plan_settings"));
    }
}
