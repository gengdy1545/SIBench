package org.sibench.capture;

import static org.junit.Assert.*;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;

public class WorkUnitsTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    private static String unit(long id, long event) {
        return ("{\"work_unit_id\":%d,\"application\":\"sibench:test\",\"pid\":1,\"request\":%d,"
                        + "\"source_event\":%d}\n")
                .formatted(id, id, event);
    }

    @Test
    public void readsOrderedCatalogOnceAndKeepsOnlyItsFirstEvents() throws Exception {
        Path directory = temporary.newFolder().toPath();
        List<String> lines = List.of(unit(1, 7), unit(2, 21), unit(3, 22));
        var output = WorkUnits.create(directory);
        for (String line : lines) output.lines().write(line);
        output.lines().close();
        var catalog = WorkUnits.read(directory);
        assertEquals(3, catalog.count());
        assertEquals(7, catalog.firstEvent(1));
        assertEquals(21, catalog.firstEvent(2));
        assertEquals(22, catalog.firstEvent(3));
        assertEquals(-1, catalog.firstEvent(0));
        assertEquals(-1, catalog.firstEvent(4));
        assertEquals(digest(lines), catalog.sha256());
    }

    @Test
    public void rejectsNonContiguousUnitsAndBackwardsSourceEvents() throws Exception {
        for (List<String> lines :
                List.of(List.of(unit(1, 7), unit(3, 21)), List.of(unit(1, 7), unit(2, 7)))) {
            Path directory = temporary.newFolder().toPath();
            var output = WorkUnits.create(directory);
            for (String line : lines) output.lines().write(line);
            output.lines().close();
            assertThrows(java.io.IOException.class, () -> WorkUnits.read(directory));
        }
    }

    private static String digest(List<String> lines) throws Exception {
        var digest = MessageDigest.getInstance("SHA-256");
        for (String line : lines) digest.update(line.getBytes(StandardCharsets.UTF_8));
        return HexFormat.of().formatHex(digest.digest());
    }
}