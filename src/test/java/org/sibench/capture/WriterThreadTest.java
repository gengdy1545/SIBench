package org.sibench.capture;

import static org.junit.Assert.*;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.trace.IndexKey;

import java.nio.file.Files;
import java.nio.file.Path;

public class WriterThreadTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    /** Several blocks and a partial one: the same file, digest and count as the writer called directly. */
    @Test
    public void writesTheStreamTheWriterWrites() throws Exception {
        Path directory = temporary.newFolder().toPath();
        Path direct = directory.resolve("direct.ops"), threaded = directory.resolve("threaded.ops");
        int operations = IndexOperationTrace.BLOCK_RECORDS * 3 + 17;
        String expected;
        try (var writer = new IndexOperationTrace.Writer(direct)) {
            for (int i = 0; i < operations; i++) writer.append(operation(i));
            writer.close();
            expected = writer.sha256();
        }
        var writer = new WriterThread(new IndexOperationTrace.Writer(threaded), "test-writer");
        try (writer) {
            for (int i = 0; i < operations; i++) writer.append(operation(i));
        }
        assertEquals(operations, writer.count());
        assertEquals(expected, writer.sha256());
        assertArrayEquals(Files.readAllBytes(direct), Files.readAllBytes(threaded));
    }

    /** An operation the writer refuses fails a later append or the close, not silently. */
    @Test
    public void aWriterFailureReachesTheAppender() throws Exception {
        var writer = new WriterThread(new IndexOperationTrace.Writer(null), "test-writer");
        var invalid = IndexOperationTrace.write(0, IndexOperationTrace.INSERT, 1, 0, null, 0, 1, 0, 1);
        assertThrows(Exception.class, () -> {
            try (writer) {
                writer.append(invalid);
                for (int i = 0; i < IndexOperationTrace.BLOCK_RECORDS * (WriterThread.AHEAD + 4); i++)
                    writer.append(operation(i));
            }
        });
    }

    private static IndexOperationTrace.Operation operation(int i) {
        return IndexOperationTrace.write(0, IndexOperationTrace.INSERT, 1, 0, IndexKey.ofInt(i), 0, 1, i, i + 1)
                .withWorkUnitId(1 + i / IndexOperationTrace.BLOCK_RECORDS);
    }
}
