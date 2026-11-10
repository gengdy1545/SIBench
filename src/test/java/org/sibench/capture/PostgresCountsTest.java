package org.sibench.capture;

import static org.junit.Assert.*;
import java.io.IOException;
import org.junit.Test;

public class PostgresCountsTest {
    private static void event(PostgresCounts counts,String type,String fields)throws Exception {
        counts.accept(PostgresFacts.parse("{\"event\":\""+type+"\",\"depth\":1"+fields+"}"));
    }
    @Test public void countsSuccessfulZeroRowAndAbortedExecutionsWithoutReplayingResults()throws Exception {
        var counts=new PostgresCounts();
        event(counts,"executor_start",",\"req\":1,\"sql\":\"UPDATE t SET x=1\"");
        event(counts,"executor_run_end",",\"processed\":0");event(counts,"executor_end","");
        event(counts,"executor_start",",\"req\":2,\"sql\":\"INSERT INTO t VALUES(1)\"");
        event(counts,"transaction_abort","");event(counts,"capture_end","");
        assertEquals(Long.valueOf(2),counts.report().get("requests"));
        assertEquals(Long.valueOf(1),counts.report().get("successful_executions"));
        assertEquals(Long.valueOf(1),counts.report().get("failed_executions"));
        assertEquals(Long.valueOf(1),counts.report().get("zero_row_modifications"));
        assertEquals(Long.valueOf(1),counts.report().get("aborts"));
    }
    @Test public void rejectsMissingExecutionAndCommitOfAnUnfinishedStatement()throws Exception {
        var missing=new PostgresCounts();
        assertThrows(IOException.class,()->event(missing,"executor_start",",\"req\":2,\"sql\":\"SELECT 1\""));
        var unfinished=new PostgresCounts();event(unfinished,"executor_start",",\"req\":1,\"sql\":\"SELECT 1\"");
        assertThrows(IOException.class,()->event(unfinished,"transaction_commit",""));
    }
}
