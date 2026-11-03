package org.sibench.capture;

import static org.junit.Assert.*;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

public class BaselineOperationsTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test(timeout = 10_000)
    public void failedConsumerReleasesTheBlockedReadScheduler() throws Exception {
        var config = org.sibench.config.ConfigValues.parse(java.nio.file.Files.readString(
                java.nio.file.Path.of("src/test/resources/config/ycsb_1k.yaml")));
        var schema = org.sibench.schema.SourceSchema.read(java.nio.file.Path.of(""), config);
        var table = schema.tables().get(0);
        var rows = new java.util.ArrayList<com.google.gson.JsonObject>();
        for (int i = 0; i < 20; i++) {
            var row = new com.google.gson.JsonObject();
            for (var column : table.columns()) row.addProperty(column.name(), "value" + i);
            rows.add(row);
        }
        var baseline = RowAuditTestSupport.baseline(temporary.newFolder().toPath(), schema,
                java.util.Map.of(table.name(), rows), 1);
        var existing = Thread.getAllStackTraces().keySet();
        var failure = new java.io.IOException("consumer failed");
        assertSame(failure, assertThrows(java.io.IOException.class, () -> BaselineOperations.derive(
                baseline, IndexCatalog.fromSchema(schema), operation -> { throw failure; },
                new BaselineOperations.Rows() {
                    public void file(String name, int tableId) {}
                    public void row(String name, org.apache.parquet.schema.GroupType type,
                            IndexOperationTrace.Operation primary) {}
                    public void fileFinished() {}
                })));
        for (var thread : Thread.getAllStackTraces().keySet()) {
            if (!existing.contains(thread) && thread.getName().equals("baseline-read-start")) {
                thread.join(2_000);
                assertFalse("Failed derivation retained its blocked read scheduler", thread.isAlive());
            }
        }
    }

    @Test
    public void projectionKeepsSecondaryKeysAndOracleMetadataWithoutDecodingPayload() throws Exception {
        var catalog = IndexCatalog.fromSchema(org.sibench.schema.SourceSchema.hybench(
                java.nio.file.Path.of("benchmark/HyBench-2024")));
        var schema = org.apache.parquet.schema.MessageTypeParser.parseMessageType("""
                message transfer {
                  required int64 _sib_timestamp;
                  required int64 _sib_row_id;
                  required int32 id;
                  optional binary amount;
                  optional int32 sourceid;
                  optional int32 targetid;
                  optional binary type (UTF8);
                  required int64 _sib_mutation_ordinal;
                }
                """);
        var projection = BaselineOperations.projection(catalog, "transfer", schema,
                "_sib_timestamp", "_sib_mutation_ordinal");
        assertEquals(java.util.List.of("_sib_timestamp", "_sib_row_id", "id", "sourceid",
                        "targetid", "type", "_sib_mutation_ordinal"),
                projection.getFields().stream().map(org.apache.parquet.schema.Type::getName).toList());
        assertEquals(schema.getType("type"), projection.getType("type"));
        assertFalse(BaselineOperations.projection(catalog, "transfer", schema)
                .containsField("_sib_timestamp"));
        var missing = new org.apache.parquet.schema.MessageType("transfer",
                java.util.List.of(schema.getType("_sib_row_id")));
        assertFalse(BaselineOperations.projection(catalog, "transfer", missing).containsField("id"));
    }

    @Test
    public void smallHeapCannotPrefetchSeveralLargeDecodedGroups() {
        var small = BaselineOperations.window(2L << 30, 480L << 20, 5, 1048576);
        assertEquals(1, small.groups());
        assertTrue(small.chunks() < BaselineOperations.CHUNKS_AHEAD);
        assertTrue(small.encoders() <= small.chunks());
        var large = BaselineOperations.window(16L << 30, 480L << 20, 5, 1048576);
        assertEquals(BaselineOperations.GROUPS_AHEAD, large.groups());
        assertEquals(BaselineOperations.CHUNKS_AHEAD, large.chunks());
        // Even a group larger than its scheduling budget must make progress with one permit.
        var oversized = BaselineOperations.window(256L << 20, 480L << 20, 5, 1048576);
        assertEquals(1, oversized.groups());
        assertTrue(oversized.chunks() >= 1);
    }
}
