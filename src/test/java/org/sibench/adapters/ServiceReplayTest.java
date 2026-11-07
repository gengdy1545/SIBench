package org.sibench.adapters;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.adapters.proto.ValueKind;
import org.sibench.replay.ReplayConfig;
import org.sibench.replay.ReplayProjection;
import org.sibench.capture.IndexOperationTrace;
import org.sibench.trace.IndexKey;

public class ServiceReplayTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test public void capabilitiesFollowVerifiedRequestsAfterSecondaryFiltering() throws Exception {
        var operations = new ArrayList<IndexOperationTrace.Operation>();
        operations.add(IndexOperationTrace.transaction(0, IndexOperationTrace.TRANSACTION_BEGIN, 1, 1, 0));
        operations.add(IndexOperationTrace.write(1, IndexOperationTrace.UPSERT, 1, 0, IndexKey.ofInt(1), 1, 0, 0, 1)
                .withTransaction(1, 1).withExpansion(0, 1).withWorkUnitId(1));
        for (int i = 0; i < 2; i++) operations.add(IndexOperationTrace.search(2, 1, i + 1,
                IndexOperationTrace.Range.equal(IndexKey.ofInt(2)), null, 0, 1,
                IndexOperationTrace.UNION_GROUP, i, 2).withTransaction(1, 1).withWorkUnitId(2));
        operations.add(IndexOperationTrace.search(3, 1, 1, IndexOperationTrace.Range.equal(IndexKey.ofInt(2)),
                0, 1, IndexOperationTrace.TRANSACTION_LOCAL_SEARCH)
                .withTransaction(1, 1).withVersion(1).withWorkUnitId(3));
        operations.add(IndexOperationTrace.transaction(0, IndexOperationTrace.TRANSACTION_COMMIT, 1, 1, 1));
        IndexOperationTrace.Source source = consumer -> {
            for (var operation : operations) consumer.accept(operation);
            return operations.size();
        };
        var limited = org.sibench.adapters.proto.DescribeResponse.newBuilder()
                .setCompleteCommitPublication(true).setValueKind(ValueKind.ROW_ID)
                .setVersionedReads(true).setOrderedSameVersionOverwrite(true)
                .setWriteAck(org.sibench.adapters.proto.Acknowledgement.ACK_WAL)
                .setFlushMakesDurable(true).build();
        Map<String, Object> primary = project(source, false);
        assertEquals(0L, primary.get("search_groups"));
        assertEquals(0L, primary.get("transaction_local_searches"));
        ServiceReplay.require(limited, ValueKind.ROW_ID, primary);
        var all = project(source, true);
        assertEquals(1L, all.get("search_groups"));
        assertEquals(1L, all.get("transaction_local_searches"));
        assertTrue(assertThrows(IOException.class, () -> ServiceReplay.require(limited, ValueKind.ROW_ID, all))
                .getMessage().contains("search groups"));
        var groups = limited.toBuilder().setSearchGroups(true).build();
        assertTrue(assertThrows(IOException.class, () -> ServiceReplay.require(groups, ValueKind.ROW_ID, all))
                .getMessage().contains("transaction-local"));
        var capable = groups.toBuilder().setTransactionLocalReads(true).build();
        ServiceReplay.require(capable, ValueKind.ROW_ID, all);
        operations.set(operations.size() - 1, IndexOperationTrace.transaction(0, IndexOperationTrace.TRANSACTION_ABORT, 1, 1, 1));
        var aborted = project(source, true);
        assertTrue(assertThrows(IOException.class, () -> ServiceReplay.require(capable, ValueKind.ROW_ID, aborted))
                .getMessage().contains("Abort"));
    }

    private static Map<String, Object> project(IndexOperationTrace.Source source, boolean secondary) throws IOException {
        try (var projection = new ReplayProjection(new ReplayConfig(secondary, 0, 0, 0, 0), true, operation -> {})) {
            source.read(projection);
            projection.finish();
            return projection.describe();
        }
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
