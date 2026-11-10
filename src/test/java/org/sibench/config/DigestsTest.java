package org.sibench.config;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import static org.junit.Assert.*;

public class DigestsTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void sameSizeAndRestoredMtimeCannotCertifyChangedBytes() throws Exception {
        var file = temporary.newFile().toPath();
        Files.writeString(file, "one");
        var modified = Files.getLastModifiedTime(file);
        String before = Digests.sha256(file);
        Files.writeString(file, "two");
        Files.setLastModifiedTime(file, modified);
        assertNotEquals(before, Digests.sha256(file));
        assertEquals("3fc4ccfe745870e2c0d99f71f30ff0656c8dedd41cc1d7d3d376b0dbe685e2f3", Digests.sha256(file));
        Files.delete(file);
        assertThrows(java.io.IOException.class, () -> Digests.sha256(file));
    }

}
