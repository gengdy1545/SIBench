package org.sibench.capture;

import com.github.luben.zstd.ZstdInputStream;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/** Stream integrity is established while decoding, before any capture can be published. */
public final class PostgresFacts {
    private static final java.util.Set<String> EVENTS=java.util.Set.of("capture_start","capture_end",
            "executor_start","executor_run_end","executor_end","transaction_commit","transaction_abort",
            "row_change","row_identity","plan_scan","plan_bitmap_heap","scan_rescan","scan_next","scan_end",
            "scan_summary","scan_orphan","heap_visibility","vm_visible","bitmap_candidate","bitmap_end",
            "bitmap_heap_candidate","bitmap_heap_lossy","bitmap_heap_unbound","bitmap_verdicts",
            "index_insert_attempt","index_build_start","index_build_end");
    private PostgresFacts() {}
    @FunctionalInterface public interface Consumer { void accept(JsonObject event)throws IOException; }
    public record Receipt(long process,long events) {}

    public static Receipt read(Path file,Consumer consumer)throws IOException {
        var stream=new Stream(file.getFileName().toString());
        try(var input=new BufferedReader(new InputStreamReader(new ZstdInputStream(Files.newInputStream(file)),StandardCharsets.UTF_8))) {
            String line;
            while((line=input.readLine())!=null) {
                JsonObject event=parse(line);
                stream.accept(event);
                consumer.accept(event);
            }
        }catch(RuntimeException invalid){throw new IOException("Invalid PostgreSQL capture: "+file,invalid);}
        return stream.finish();
    }

    static JsonObject parse(String line)throws IOException {
        try(var json=new com.google.gson.stream.JsonReader(new java.io.StringReader(line))) {
            json.setStrictness(com.google.gson.Strictness.STRICT);
            var event=JsonParser.parseReader(json).getAsJsonObject();
            if(json.peek()!=com.google.gson.stream.JsonToken.END_DOCUMENT)throw new IOException("Trailing capture data");
            return event;
        }catch(RuntimeException invalid){throw new IOException("Invalid PostgreSQL capture event",invalid);}
    }

    /** The same sequence check applies to live transport and explicit diagnostic files. */
    static final class Stream {
        private final String name;
        private long pid,sequence;
        private boolean opened,closed;
        Stream(String name){this.name=name;}
        void accept(JsonObject event)throws IOException {
            try {
                String kind=event.get("event").getAsString();
                if(!EVENTS.contains(kind))throw new IOException("Unknown PostgreSQL event: "+kind);
                long current=event.get("sequence").getAsBigDecimal().longValueExact(),process=event.get("pid").getAsBigDecimal().longValueExact();
                if(closed)throw new IOException("PostgreSQL events follow capture_end: "+name);
                if(!opened) {
                    if(!kind.equals("capture_start")||current!=0||process<=0
                            ||!name.equals("pg-"+process+".jsonl.zst"))
                        throw new IOException("Invalid PostgreSQL capture opening: "+name);
                    pid=process;opened=true;
                }else {
                    if(process!=pid||current!=++sequence||kind.equals("capture_start"))
                        throw new IOException("Missing or reordered PostgreSQL events: "+name);
                    closed=kind.equals("capture_end");
                }
            }catch(RuntimeException invalid){throw new IOException("Invalid PostgreSQL capture: "+name,invalid);}
        }
        Receipt finish()throws IOException {
            if(!opened||!closed)throw new IOException("Incomplete PostgreSQL capture: "+name);
            return new Receipt(pid,sequence+1);
        }
    }
}
