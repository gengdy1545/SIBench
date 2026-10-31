package org.sibench.capture;

import static org.junit.Assert.*;

import java.nio.file.*;
import java.util.*;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.generation.ParquetDataFileTest;
import org.sibench.schema.SourceSchema;

public class PgBaselineExportTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void groupsDoNotBecomeFilesAndFileIdsDoNotDependOnWorkerOrder() throws Exception {
        Path output = temporary.newFolder().toPath();
        var config = ParquetDataFileTest.layout(output, 192 * 1024, 5);
        var schema = MessageTypeParser.parseMessageType("message t { optional int64 id; optional binary payload (UTF8); required int64 _sib_row_id; required int64 _sib_timestamp; }");
        var source = new SourceSchema.Table("t", List.of(), List.of());
        var table = new PgBaselineExport.Table(source, "id", true, schema, 3000, true, List.of());
        var random = new Random(17);
        var rows = new ArrayList<String[]>();
        for (int i = 0; i < 3000; i++) {
            byte[] bytes = new byte[256];
            random.nextBytes(bytes);
            rows.add(new String[] {Integer.toString(i), Base64.getEncoder().encodeToString(bytes)});
        }
        var iterator = rows.iterator();
        var written = PgBaselineExport.writeTable(table, 1, 3, 41,
                () -> iterator.hasNext() ? iterator.next() : null, output, config);
        assertTrue(written.size() > 1);
        assertTrue(written.size() < (3000 + config.maxRows() - 1) / config.maxRows());
        assertTrue(written.stream().anyMatch(w -> (long) w.file().get("rows") > config.maxRows()));
        long count = 0;
        for (int i = 0; i < written.size(); i++) {
            var metadata = written.get(i).file();
            assertEquals(2L + 3L * i, metadata.get("file_id"));
            Path file = output.resolve((String) metadata.get("path"));
            long size = Files.size(file);
            assertTrue(size <= 192 * 1024 * 105 / 100);
            if (i + 1 < written.size()) assertTrue(size >= 192 * 1024);
            try (var reader = ParquetReader.<Group>builder(new GroupReadSupport(),
                    new org.apache.hadoop.fs.Path(file.toUri())).build()) {
                Group row;
                while ((row = reader.read()) != null) {
                    assertEquals(count, row.getLong("id", 0));
                    assertEquals(41 + count, row.getLong("_sib_row_id", 0));
                    assertEquals(rows.get((int) count)[1], row.getString("payload", 0));
                    count++;
                }
            }
        }
        assertEquals(3000, count);
    }
}
