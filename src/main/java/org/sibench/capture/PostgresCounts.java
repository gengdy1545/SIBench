package org.sibench.capture;

import com.google.gson.JsonObject;
import java.io.IOException;
import java.util.Map;

/** Source completeness counters collected during the necessary live stream read. */
final class PostgresCounts {
    private long requests,successful,failed,commits,aborts,zeroRows,processed;
    private boolean active,ended,returned,transaction,modification;

    void accept(JsonObject event)throws IOException {
        String type=event.get("event").getAsString();
        int depth=event.has("depth")?event.get("depth").getAsInt():0;
        switch(type) {
            case "executor_start" -> {
                if(depth!=1)return;
                complete(false);
                if(event.get("req").getAsLong()!=requests+1)throw new IOException("PG request sequence is incomplete");
                requests++;active=true;ended=false;returned=false;transaction=true;processed=0;
                String sql=event.get("sql").getAsString().stripLeading().toLowerCase(java.util.Locale.ROOT);
                modification=sql.startsWith("insert ")||sql.startsWith("update ")||sql.startsWith("delete ");
            }
            case "executor_run_end" -> {
                if(depth!=1)return;
                if(!active)throw new IOException("PG execution returned without a request");
                long count=event.get("processed").getAsLong();
                if(count<0)throw new IOException("Negative PG processed count");
                processed=Math.addExact(processed,count);returned=true;
            }
            case "executor_end" -> {if(depth==1){if(!active)throw new IOException("PG execution ended without a request");ended=true;}}
            case "transaction_commit","transaction_abort" -> {
                boolean abort=type.equals("transaction_abort");complete(abort);
                if(transaction){if(abort)aborts++;else commits++;}transaction=false;
            }
            case "capture_end" -> {if(active||transaction)throw new IOException("PG session ended inside a transaction");}
            default -> {}
        }
    }
    private void complete(boolean abort)throws IOException {
        if(!active)return;
        if(returned&&ended){successful++;if(modification&&processed==0)zeroRows++;}
        else if(abort)failed++;
        else throw new IOException("PG request lacks successful execution completion");
        active=false;
    }
    Map<String,Long> report(){return Map.of("requests",requests,"successful_executions",successful,
            "failed_executions",failed,"commits",commits,"aborts",aborts,"zero_row_modifications",zeroRows);}
}
