package org.sibench.adapters;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.adapters.proto.ValueKind;
import org.sibench.replay.ReplayConfig;

public class ServiceReplayTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test public void deploymentDescriptionRejectsWrongValueModeAndMissingEvidence() throws Exception {
        var description = new IndexDescription("fixture",ValueKind.ROW_ID,
                org.sibench.adapters.proto.Acknowledgement.ACK_MEMORY,"async persistence","fixture");
        description.require(ValueKind.ROW_ID);
        assertThrows(IOException.class, () -> description.require(ValueKind.LOCATOR));
        assertThrows(IOException.class, () -> new IndexDescription("fixture",ValueKind.ROW_ID,
                org.sibench.adapters.proto.Acknowledgement.ACK_UNSPECIFIED,"async persistence","").require(ValueKind.ROW_ID));
    }

    @Test
    public void missingTraceDoesNotCreateIndex() {
        Path root = temporary.getRoot().toPath();
        Path index = root.resolve("index");
        Path report = root.resolve("report.json");
        var service = new ServiceConfig("rocksdb", null, index, 0, ValueKind.ROW_ID, null, false);
        var error = assertThrows(IOException.class, () -> ServiceReplay.run(
                root.resolve("missing-trace"), new ReplayConfig(true, 0, 0, 0, 0), service, report));
        assertTrue(error.getMessage().contains("Incomplete Trace"));
        assertFalse(Files.exists(index));
        assertFalse(Files.exists(report));
        assertEquals(0, temporary.getRoot().list().length);
    }
}
