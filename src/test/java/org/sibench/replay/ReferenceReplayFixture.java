package org.sibench.replay;

import org.sibench.capture.IndexOperationTrace;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/** Component tests with hand-built protobuf operations; never certifies a published Trace. */
final class ReferenceReplayFixture {
    private ReferenceReplayFixture() {}

    static Map<String, Object> check(Path baseline, Path workload, ReplayConfig config) throws IOException {
        var report = ReplayInput.report(config,
                ReplayInput.checkStream(reader(baseline), config, false),
                ReplayInput.checkStream(reader(workload), config, true));
        report.put("validation_scope", "hand-built operation streams; no Trace-directory certification");
        return report;
    }

    static Map<String, Object> run(Path baseline, Path workload, ReplayConfig config, Path report)
            throws IOException {
        if (Files.exists(report)) throw new IOException("Replay report already exists: " + report);
        return ReferenceReplay.runChecked(reader(baseline), reader(workload), config,
                List.of(false, true), check(baseline, workload, config), report, true);
    }

    private static IndexOperationTrace.Source reader(Path file) {
        return consumer -> org.sibench.capture.ProtoTrace.read(file, null, null, consumer);
    }
}
