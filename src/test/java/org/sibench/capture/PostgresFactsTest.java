package org.sibench.capture;

import static org.junit.Assert.*;
import com.github.luben.zstd.ZstdOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class PostgresFactsTest {
    @Rule public TemporaryFolder temporary=new TemporaryFolder();
    private Path stream(String text)throws IOException {
        Path file=temporary.newFolder().toPath().resolve("pg-42.jsonl.zst");
        try(var output=new ZstdOutputStream(Files.newOutputStream(file))){output.write(text.getBytes(StandardCharsets.UTF_8));}return file;
    }
    private String event(String kind,int sequence){return "{\"event\":\""+kind+"\",\"sequence\":"+sequence+",\"pid\":42}\n";}
    @Test public void requiresContiguousEventsAndCleanNativeClose()throws Exception {
        String start=event("capture_start",0),abort=event("transaction_abort",1),end=event("capture_end",2);
        var seen=new java.util.ArrayList<String>();
        assertEquals(3,PostgresFacts.read(stream(start+abort+end),e->seen.add(e.get("event").getAsString())).events());
        assertEquals(java.util.List.of("capture_start","transaction_abort","capture_end"),seen);
        for(String invalid:java.util.List.of("",start,start+end,start+abort+end+abort,abort+end,
                start+abort+end.replace("42","43"),start+event("capture_start",1)+end,
                start+event("unrecognized_index_read",1)+end,start+abort+end.replace(":2,",":2.5,")))
            assertThrows(IOException.class,()->PostgresFacts.read(stream(invalid),e->{}));
        Path truncated=stream(start+abort+end);byte[] bytes=Files.readAllBytes(truncated);
        Files.write(truncated,java.util.Arrays.copyOf(bytes,bytes.length-1));
        assertThrows(IOException.class,()->PostgresFacts.read(truncated,e->{}));
    }
}
