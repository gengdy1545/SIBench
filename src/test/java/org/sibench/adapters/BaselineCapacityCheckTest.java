package org.sibench.adapters;

import static org.junit.Assert.*;
import java.nio.file.*;
import java.util.concurrent.TimeUnit;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.adapters.proto.*;
import org.sibench.capture.IndexOperationTrace;
import org.sibench.trace.IndexKey;

public class BaselineCapacityCheckTest {
    @Rule public TemporaryFolder temporary=new TemporaryFolder();

    @Test
    public void capacityChecksSelectOnlyIndexesProvidedByTheTrace() throws Exception {
        Path trace = temporary.newFolder().toPath();
        var manifest = new com.google.gson.JsonObject();
        manifest.add("value_kinds", new com.google.gson.Gson().toJsonTree(java.util.List.of("row_id")));
        manifest.addProperty("output", "proto");
        for (String name : java.util.List.of("baseline", "workload")) manifest.addProperty(name + "_trace_sha256", "test");
        for (String name : java.util.List.of("baseline_rows", "source_work_units", "baseline_operations", "workload_operations"))
            manifest.addProperty(name, 0);
        for (boolean secondary : new boolean[] {false, true}) {
            manifest.addProperty("secondary_index_coverage", secondary ? "all_source_definitions" : "primary_only");
            Files.writeString(trace.resolve("manifest.json"), manifest.toString());
            assertEquals(secondary, DriverCapacityCheck.config(trace, 2).secondaryIndexes());
        }
        manifest.remove("secondary_index_coverage");
        Files.writeString(trace.resolve("manifest.json"), manifest.toString());
        assertThrows(java.io.IOException.class, () -> DriverCapacityCheck.config(trace, 2));
    }

    @Test(timeout=15000)
    public void actualBatchAckPassesAndAcceptedButDroppedWriteFailsIndependentFieldCheck() throws Exception {
        for(boolean drop:new boolean[]{false,true}) {
            Path report=temporary.getRoot().toPath().resolve("received-"+drop+".json");
            var sink=new BaselineCapacityCheck.Sink(report,drop);
            var expected=new BaselineCapacityCheck.Fingerprint();
            var first=IndexOperationTrace.write(1,IndexOperationTrace.INSERT,1,0,IndexKey.ofInt(7),0,1,0,70);
            var second=IndexOperationTrace.write(2,IndexOperationTrace.INSERT,1,0,IndexKey.ofInt(7),0,1,0,70)
                    .withIndex(1,IndexKey.ofInt(20),IndexOperationTrace.INSERT);
            expected.operation(first);expected.operation(second);
            var requests=new TraceRequests(ValueKind.ROW_ID);requests.baselineBatch(2);
            try(var server=IndexServer.start(0,sink);var remote=new GrpcService("127.0.0.1:"+server.port(),requests, "independent baseline test", 5)) {
              var before=sink.received.summary();
              for(int round=0;round<2;round++) {
                var client=remote.factory().open(0);
                var a=client.send(1,first);var b=client.send(2,second);
                a.response().toCompletableFuture().get(5,TimeUnit.SECONDS);
                b.response().toCompletableFuture().get(5,TimeUnit.SECONDS);
                remote.flush("independent baseline test");
                var delta=BaselineCapacityCheck.difference(sink.received.summary(),before);
                if(drop&&round==0)assertThrows(java.io.IOException.class,()->BaselineCapacityCheck.verify(expected,delta));
                else BaselineCapacityCheck.verify(expected,delta);
                assertTrue(Files.isRegularFile(report));
                client.close();before=sink.received.summary();
              }
            }
        }
    }
}
