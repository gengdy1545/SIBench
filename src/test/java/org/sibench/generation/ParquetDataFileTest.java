package org.sibench.generation;

import static org.junit.Assert.*;

import java.nio.file.*;
import java.util.*;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ParquetDataFileTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    public static GenerationConfig layout(Path output, long target, int percent) {
        return layout(output, target, percent, CompressionCodecName.ZSTD, false);
    }

    private static GenerationConfig layout(Path output, long target, int percent,
            CompressionCodecName codec, boolean dictionary) {
        return new GenerationConfig(Path.of(""), Map.of(), "ycsb", 7, target, percent,
                128, 65536, 1024, 1024, dictionary, codec, codec == CompressionCodecName.ZSTD ? 3 : 0, 1, 0, output, 1);
    }

    @Test
    public void supportedCodecsAndDictionariesKeepReadablePageIndexes() throws Exception {
        for (var codec : List.of(CompressionCodecName.ZSTD, CompressionCodecName.SNAPPY, CompressionCodecName.UNCOMPRESSED)) {
            for (boolean dictionary : new boolean[] {false, true}) {
                Path directory = temporary.newFolder().toPath();
                var config = layout(directory, 65536, 5, codec, dictionary);
                var schema = MessageTypeParser.parseMessageType("message t { required int64 id; required binary payload; }");
                var table = new TableData("t", 2000, 1, "id", schema, null);
                var factory = new SimpleGroupFactory(schema);
                int next = 0;
                while (next < 2000) {
                    Path file = directory.resolve(next + ".parquet");
                    int first = next;
                    try (var writer = new ParquetDataFile(file, table, config, Map.of())) {
                        while (next < 2000) {
                            byte[] bytes = new byte[256]; new Random(next).nextBytes(bytes);
                            boolean full = writer.write(factory.newGroup().append("id", (long) next)
                                    .append("payload", org.apache.parquet.io.api.Binary.fromConstantByteArray(bytes)));
                            next++;
                            if (full) break;
                        }
                        writer.finish();
                        assertTrue(Files.size(file) <= 65536L * 105 / 100);
                        if (next < 2000) assertTrue(Files.size(file) >= 65536);
                    }
                    try (var reader = ParquetReader.<Group>builder(new GroupReadSupport(), new org.apache.hadoop.fs.Path(file.toUri())).build()) {
                        for (int id = first; id < next; id++) {
                            var row = reader.read(); byte[] expected = new byte[256]; new Random(id).nextBytes(expected);
                            assertEquals(id, row.getLong("id", 0));
                            assertArrayEquals(expected, row.getBinary("payload", 0).getBytes());
                        }
                        assertNull(reader.read());
                    }
                    try (var reader = org.apache.parquet.hadoop.ParquetFileReader.open(
                            org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(file.toUri()), new org.apache.hadoop.conf.Configuration(false)))) {
                        for (var block : reader.getFooter().getBlocks()) for (var column : block.getColumns()) {
                            assertNotNull(reader.readColumnIndex(column));
                            assertNotNull(reader.readOffsetIndex(column));
                        }
                    }
                }
            }
        }
    }

    @Test
    public void completeCompressedBytesRespectConfigAcrossChangingRowWidths() throws Exception {
        for (long target : new long[] {64 * 1024, 192 * 1024, 1024 * 1024}) {
            var directory = temporary.newFolder().toPath();
            var config = layout(directory, target, 5);
            var schema = MessageTypeParser.parseMessageType("message t { required int64 id; required binary payload; }");
            var table = new TableData("t", 12000, 1, "id", schema, null);
            var factory = new SimpleGroupFactory(schema);
            var random = new Random(73);
            int next = 0, files = 0, tails = 0;
            while (next < 12000) {
                Path file = directory.resolve((files++) + ".parquet");
                int first = next;
                try (var writer = new ParquetDataFile(file, table, config, Map.of())) {
                    while (next < 12000) {
                        // Both compressibility and width change within a file.
                        byte[] bytes = new byte[next % 7 == 0 ? 256 : 512];
                        if (next % 3 != 0) random.nextBytes(bytes);
                        boolean full = writer.write(factory.newGroup().append("id", (long) next).append("payload",
                                org.apache.parquet.io.api.Binary.fromConstantByteArray(bytes)));
                        next++;
                        if (full) break;
                    }
                    writer.finish();
                    assertTrue(Files.size(file) <= target * 105 / 100);
                    boolean tail = (boolean) writer.sizing().get("tail");
                    if (tail) {
                        tails++;
                        assertEquals(12000, next);
                    } else assertTrue(Files.size(file) >= target);
                }
                try (var reader = ParquetReader.<Group>builder(new GroupReadSupport(),
                        new org.apache.hadoop.fs.Path(file.toUri())).build()) {
                    for (int id = first; id < next; id++) assertEquals(id, reader.read().getLong("id", 0));
                    assertNull(reader.read());
                }
            }
            assertTrue(files > 1);
            assertTrue(tails <= 1);
        }
    }

    @Test
    public void rejectsOversizedRowInsteadOfCertifyingTheFile() throws Exception {
        Path file = temporary.newFile().toPath();
        Files.delete(file);
        var config = layout(file.getParent(), 65536, 1);
        var schema = MessageTypeParser.parseMessageType("message t { required binary payload; }");
        var bytes = new byte[90000];
        new Random(7).nextBytes(bytes);
        try (var writer = new ParquetDataFile(file, new TableData("t", 1, 1, "payload", schema, null), config, Map.of())) {
            writer.write(new SimpleGroupFactory(schema).newGroup().append("payload",
                    org.apache.parquet.io.api.Binary.fromConstantByteArray(bytes)));
            var error = assertThrows(java.io.IOException.class, writer::finish);
            assertTrue(error.getMessage(), error.getMessage().contains("exceeds its configured byte limit"));
            assertThrows(IllegalStateException.class, writer::sizing);
        }
    }
}
