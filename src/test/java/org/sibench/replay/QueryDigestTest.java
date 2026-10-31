package org.sibench.replay;

import static org.junit.Assert.*;

import java.nio.file.Files;
import java.util.HexFormat;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.capture.IndexOperationTrace;
import org.sibench.capture.IndexReferenceReplay.LookupResult;
import org.sibench.trace.IndexKey;

public class QueryDigestTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void bigEndianVectorsCoverSignedLongBitsAndBothDigestPaths() {
        var rows = List.of(
                new LookupResult(IndexKey.ofInt(-1), Long.MIN_VALUE, Long.MAX_VALUE, -1),
                new LookupResult(IndexKey.ofInt(0), 0, -1, Long.MIN_VALUE));
        // SHA-256 of hand-written big-endian count/key/version/value bytes, independently
        // calculated from hex; an empty member list contributes one additional zero int.
        for (boolean rowIds : new boolean[] {false, true}) {
            String expectedResults = rowIds
                    ? "7e9e91d9befd5f14d8af0d28c1578b334f00f65f165d49fb17ec967e4652b9b1"
                    : "f10f0369ad29c87bb80c8c1c086a16bcd28222fa7d656bf3e4f86722f91665b0";
            String expectedQuery = rowIds
                    ? "324516937dc1e3995bf7408d88cebd1d546e66e69b617440d5b2c0d84f61faa7"
                    : "3e9034de5371dbb39c1fce4097fc527147e000e89ed7b90e69bbcd461d82ef77";
            assertEquals(expectedResults, QueryDigest.results(rows, rowIds));
            assertEquals(expectedQuery, QueryDigest.of(List.of(), rows, rowIds));
            var responses = rows.stream().map(row -> {
                var value = org.sibench.adapters.proto.Value.newBuilder();
                if (rowIds) value.setRowId(row.value());
                else value.setFile(row.value()).setPosition(row.position());
                return org.sibench.adapters.proto.Result.newBuilder()
                        .setPrimaryKey(com.google.protobuf.ByteString.copyFrom(row.key().bytes()))
                        .setVersion(row.commitVersion()).setValue(value).build();
            }).toList();
            assertEquals(expectedQuery, QueryDigest.ofResponse(List.of(), responses, rowIds));
        }
    }

    @Test
    public void publishedHashCoversEmptyFilesHeadersAndBufferedQueryRecords() throws Exception {
        var query = IndexOperationTrace.search(7, 1, 0,
                IndexOperationTrace.Range.equal(IndexKey.ofInt(1)), 0, 1, 0);
        var results = List.of(new LookupResult(IndexKey.ofInt(1), 0, 42, 0));
        for (boolean rowIds : new boolean[] {false, true}) {
            for (String binding : new String[] {null, "workload=a baseline=b mode=rowid"}) {
                for (int count : new int[] {0, 2048}) {
                    var path = temporary.getRoot().toPath().resolve("queries-" + rowIds + "-" + (binding != null) + "-" + count);
                    var writer = new QueryDigest.File(path, rowIds, binding);
                    try (writer) {
                        assertThrows(IllegalStateException.class, writer::sha256);
                        assertFalse(Files.exists(path));
                        for (int i = 0; i < count; i++) writer.accept(List.of(query), results);
                    }
                    var expected = HexFormat.of().formatHex(java.security.MessageDigest
                            .getInstance("SHA-256").digest(Files.readAllBytes(path)));
                    assertEquals(expected, writer.sha256());
                    assertEquals(count, writer.count());
                    writer.close();
                    assertEquals(expected, writer.sha256());
                    assertFalse(Files.exists(path.resolveSibling(path.getFileName() + ".partial")));
                }
            }
        }
    }
}
