package org.sibench.capture;

import static org.junit.Assert.*;
import java.io.IOException;
import org.junit.Test;

public class PostgresFactsTest {
    private String event(String kind,int sequence){return "{\"event\":\""+kind+"\",\"sequence\":"+sequence+",\"pid\":42}\n";}
    private static void read(String text)throws IOException {
        var stream=new PostgresFacts.Stream("pg-42.jsonl");
        for(String line:text.lines().toList())stream.accept(PostgresFacts.parse(line));
        stream.finish();
    }
    @Test public void eventLimitAppliesAcrossReadBoundaries()throws Exception {
        byte[] line=new byte[PostgresFacts.MAX_EVENT_BYTES];java.util.Arrays.fill(line,(byte)'x');
        try(var input=PostgresFacts.bounded(new java.io.ByteArrayInputStream(line))) {
            assertEquals(line.length,input.transferTo(java.io.OutputStream.nullOutputStream()));
        }
        try(var input=PostgresFacts.bounded(new java.io.SequenceInputStream(new java.io.ByteArrayInputStream(line),
                new java.io.ByteArrayInputStream(new byte[]{'x'})))) {
            assertTrue(assertThrows(IOException.class,()->input.transferTo(java.io.OutputStream.nullOutputStream()))
                    .getMessage().contains("16 MiB"));
        }
        try(var input=PostgresFacts.bounded(new java.io.SequenceInputStream(new java.io.ByteArrayInputStream(line),
                new java.io.ByteArrayInputStream(new byte[]{'\n','x'})))) {
            assertEquals(line.length+2L,input.transferTo(java.io.OutputStream.nullOutputStream()));
        }
    }
    @Test public void requiresContiguousEventsAndCleanNativeClose()throws Exception {
        String start=event("capture_start",0),abort=event("transaction_abort",1),end=event("capture_end",2);
        read(start+abort+end);
        // Per-row and write-attempt events are not part of the capture format.
        for(String invalid:java.util.List.of("",start,start+abort,start+end,start+abort+end+abort,abort+end,
                start+abort+end.replace("42","43"),start+event("capture_start",1)+end,
                start+event("unrecognized_index_read",1)+end,start+event("scan_next",1)+end,
                start+event("index_insert_attempt",1)+end,start+abort+end.replace(":2,",":2.5,")))
            assertThrows(invalid,IOException.class,()->read(invalid));
        assertTrue(assertThrows(IOException.class,()->read(start+event("scan_next",1)+end)).getMessage().contains("Unknown PostgreSQL event"));
        assertThrows(IOException.class,()->new PostgresFacts.Stream("pg-42.jsonl.zst").accept(PostgresFacts.parse(start.strip())));
    }
}
