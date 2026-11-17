package org.sibench.capture;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;

/** Stream integrity is established while decoding, before any capture can be published. */
public final class PostgresFacts {
    static final int MAX_EVENT_BYTES = 16 << 20;
    private static final java.util.Set<String> EVENTS=java.util.Set.of("capture_start","capture_end",
            "executor_start","executor_run_end","executor_end","transaction_commit","transaction_abort",
            "subtransaction_begin","subtransaction_commit","subtransaction_abort",
            "row_change","row_identity","plan_scan","plan_bitmap_heap","scan_rescan","scan_end",
            "scan_summary","scan_orphan","bitmap_heap_unbound","bitmap_verdicts");
    private PostgresFacts() {}

    /** Bound each decoded event before readLine or JSON parsing can materialize it. */
    static java.io.InputStream bounded(java.io.InputStream input) {
        return new java.io.FilterInputStream(input) {
            int bytes;
            private void count(int value)throws IOException {
                if(value=='\n')bytes=0;
                else if(++bytes>MAX_EVENT_BYTES)throw new IOException("PG native event exceeds 16 MiB");
            }
            @Override public int read()throws IOException {
                int value=in.read();if(value>=0)count(value);return value;
            }
            @Override public int read(byte[] buffer,int offset,int length)throws IOException {
                int count=in.read(buffer,offset,length);
                for(int i=0;i<count;i++)count(buffer[offset+i]);
                return count;
            }
        };
    }

    static JsonObject parse(String line)throws IOException {
        try(var json=new com.google.gson.stream.JsonReader(new java.io.StringReader(line))) {
            json.setStrictness(com.google.gson.Strictness.STRICT);
            var event=JsonParser.parseReader(json).getAsJsonObject();
            if(json.peek()!=com.google.gson.stream.JsonToken.END_DOCUMENT)throw new IOException("Trailing capture data");
            return event;
        }catch(RuntimeException invalid){throw new IOException("Invalid PostgreSQL capture event",invalid);}
    }

    /** One backend's live stream: opened by capture_start, contiguous, closed by capture_end. */
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
                            ||!name.equals("pg-"+process+".jsonl"))
                        throw new IOException("Invalid PostgreSQL capture opening: "+name);
                    pid=process;opened=true;
                }else {
                    if(process!=pid||current!=++sequence||kind.equals("capture_start"))
                        throw new IOException("Missing or reordered PostgreSQL events: "+name);
                    closed=kind.equals("capture_end");
                }
            }catch(RuntimeException invalid){throw new IOException("Invalid PostgreSQL capture: "+name,invalid);}
        }
        void finish()throws IOException {
            if(!opened||!closed)throw new IOException("Incomplete PostgreSQL capture: "+name);
        }
    }
}
