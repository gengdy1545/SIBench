package org.sibench.capture;

import static org.junit.Assert.*;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;

public class BaselineDigestsTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    /** A record holds across processes and hard links, and not for a file that changed. */
    @Test
    public void aRecordNamesFilesAsTheyAre() throws Exception {
        Path a = Files.createDirectory(temporary.getRoot().toPath().resolve("a"));
        Path b = Files.createDirectory(temporary.getRoot().toPath().resolve("b"));
        Path file = Files.writeString(a.resolve("1.parquet"), "one");
        var digests = BaselineDigests.of(a);
        assertNull(digests.get(file));
        digests.put(file, "d1");
        digests.save();
        assertEquals("d1", BaselineDigests.of(a).get(file));
        Path link = Files.createLink(b.resolve("1.parquet"), file);
        assertNull(BaselineDigests.of(b).get(link));
        BaselineDigests.merge(a, b);
        assertEquals("d1", BaselineDigests.of(b).get(link));
        Files.writeString(file, "two!");
        assertNull(BaselineDigests.of(a).get(file));
        Files.writeString(file, "one");
        Files.setLastModifiedTime(file, FileTime.fromMillis(System.currentTimeMillis() + 5_000));
        assertNull(BaselineDigests.of(a).get(file));
        // An unreadable record is no record.
        Files.writeString(BaselineDigests.location(a), "{");
        assertNull(BaselineDigests.of(a).get(file));
    }
}
