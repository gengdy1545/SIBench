package org.sibench.capture;

import static org.junit.Assert.*;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.util.Map;

/** One factor scales every native knob of a source, so the workload mix is kept. */
public class WriteRatioPlanTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void scalesEachSourcesOwnKnobs() throws Exception {
        var root = temporary.getRoot().toPath();
        var trace = Files.createDirectory(root.resolve("trace"));
        Files.writeString(trace.resolve("manifest.json"), "{\"baseline_rows\":1000000,\"logical_row_version_writes\":10000}");
        Files.writeString(root.resolve("sysbench.yaml"),
                "benchmark: sysbench\nnative:\n  workloads:\n    oltp_insert: {events: 100}\n    oltp_point_select: {events: 300}\n");
        var plan = WriteRatioPlan.plan(root, root.relativize(root.resolve("sysbench.yaml")), trace, 0.05);
        assertEquals(5.0, (Double) plan.get("factor"), 1e-9);
        assertEquals(Map.of("native.workloads.oltp_insert.events", 500L, "native.workloads.oltp_point_select.events", 1500L),
                plan.get("parameters"));
        Files.writeString(root.resolve("hybench.yaml"),
                "benchmark: hybench\nnative:\n  duration_minutes: 1\n  phase_duration_minutes: {tp: 2}\n");
        var hybench = WriteRatioPlan.plan(root, root.relativize(root.resolve("hybench.yaml")), trace, 0.02);
        // TP keeps its own duration, XP the default; both scale by the same factor of 2.
        assertEquals(Map.of("native.phase_duration_minutes.tp", 4L, "native.phase_duration_minutes.xp", 2L),
                hybench.get("parameters"));
        Files.writeString(trace.resolve("manifest.json"), "{\"baseline_rows\":1000000,\"logical_row_version_writes\":0}");
        assertThrows(java.io.IOException.class,
                () -> WriteRatioPlan.plan(root, root.relativize(root.resolve("sysbench.yaml")), trace, 0.05));
    }
}
