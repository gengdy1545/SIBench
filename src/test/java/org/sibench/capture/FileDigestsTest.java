package org.sibench.capture;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.util.List;
import static org.junit.Assert.*;

public class FileDigestsTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void sameSizeAndRestoredMtimeCannotCertifyChangedBytes() throws Exception {
        var file = temporary.newFile().toPath();
        Files.writeString(file, "one");
        var modified = Files.getLastModifiedTime(file);
        String before = FileDigests.sha256(file);
        Files.writeString(file, "two");
        Files.setLastModifiedTime(file, modified);
        assertNotEquals(before, FileDigests.sha256(file));
        assertEquals(org.sibench.config.Digests.sha256(file), FileDigests.sha256(file));
    }

    @Test
    public void batchReadsDistinctFilesAndPropagatesMissingInput() throws Exception {
        var first = temporary.newFile().toPath();
        var second = temporary.newFile().toPath();
        Files.writeString(first, "one");
        Files.writeString(second, "two");
        var digests = FileDigests.hashFiles(List.of(first, first, second));
        assertEquals(2, digests.size());
        assertEquals(FileDigests.sha256(first), digests.get(first));
        assertEquals(FileDigests.sha256(second), digests.get(second));
        Files.delete(second);
        assertThrows(java.io.IOException.class, () -> FileDigests.hashFiles(List.of(first, second)));
    }
}
