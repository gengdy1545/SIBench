package org.sibench.capture;

import static org.junit.Assert.*;

import com.google.gson.JsonParser;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class InputFilesTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test public void manifestsBindExactBytesAndRejectLinksAndOversizedFiles() throws Exception {
        Path directory = temporary.newFolder().toPath();
        Path file = Files.writeString(directory.resolve("manifest.json"), "{}\n");
        var first = InputFiles.manifest(directory);
        assertEquals("{}", first.document().toString());
        Files.writeString(file, "{}");
        assertNotEquals(first.sha256(), InputFiles.manifest(directory).sha256());
        Path original = temporary.newFile().toPath();
        Files.move(file, original, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        Files.createSymbolicLink(file, original);
        assertThrows(IOException.class, () -> InputFiles.manifest(directory));
        Files.delete(file);
        try (var large = new RandomAccessFile(file.toFile(), "rw")) { large.setLength((16L << 20) + 1); }
        assertThrows(IOException.class, () -> InputFiles.manifest(directory));
        assertThrows(IOException.class, () -> CsvInput.open(directory));
        assertThrows(IOException.class, () -> CdcInput.open(directory));
        assertThrows(IOException.class, () -> PixelsBaseline.open(directory));
    }

    @Test public void descriptorChecksRejectSameSizeReplacementAndNonintegerCounts() throws Exception {
        Path directory = temporary.newFolder().toPath();
        Path file = Files.writeString(directory.resolve("payload"), "old");
        var descriptor = InputFiles.file(directory, file);
        assertEquals(file, InputFiles.verified(directory, descriptor));
        Files.writeString(file, "new");
        assertThrows(IOException.class, () -> InputFiles.verified(directory, descriptor));
        for (String value : new String[] {"\"3\"", "0.5", "1.0", "1e3", "-1", "9223372036854775808", "true", "null"}) {
            var document = JsonParser.parseString("{\"rows\":" + value + "}").getAsJsonObject();
            assertThrows(value, IOException.class, () -> InputFiles.number(document, "rows"));
        }
        assertEquals(3L, InputFiles.number(JsonParser.parseString("{\"rows\":3}").getAsJsonObject(), "rows"));
    }
}
