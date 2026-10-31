package org.sibench.capture;

import static org.junit.Assert.*;
import org.junit.Test;

public class BaselineOperationsTest {
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
