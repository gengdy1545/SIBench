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

    @Test public void capabilitiesFollowRetainedQueryGroups() throws Exception {
        var limited = org.sibench.adapters.proto.DescribeResponse.newBuilder()
                .setValueKind(ValueKind.ROW_ID).setVersionedReads(true)
                .setWriteAck(org.sibench.adapters.proto.Acknowledgement.ACK_WAL)
                .setFlushMakesDurable(true).build();
        ServiceReplay.require(limited, ValueKind.ROW_ID, Map.of("search_groups", 0L));
        assertTrue(assertThrows(IOException.class, () -> ServiceReplay.require(
                limited, ValueKind.ROW_ID, Map.of("search_groups", 1L)))
                .getMessage().contains("search groups"));
        ServiceReplay.require(limited.toBuilder().setSearchGroups(true).build(),
                ValueKind.ROW_ID, Map.of("search_groups", 1L));
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
