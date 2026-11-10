package org.sibench.capture;

import static org.junit.Assert.*;
import static org.sibench.capture.RocksYcsb.Status.*;
import java.nio.file.*;
import java.util.*;
import org.junit.Test;
import org.sibench.generation.*;
import org.sibench.schema.SourceSchema;

/** API counterexamples independent of the upstream workload's random choices. */
public class RocksYcsbCheck {
    private static Path library() throws Exception {
        Path path=Path.of(System.getProperty("sibench.rocksdb.library"));
        RocksDbTimestamps.load(path);return path;
    }
    private static RocksDbRows.Settings settings() { return new RocksDbRows.Settings(8L<<20,1L<<20,2); }

    @Test public void bufferedLoadOwnsBytesAndFlushesItsTail() throws Exception {
        library();Path path=Files.createTempDirectory("rocks-load-");
        var loaded=new ArrayList<RocksDbRows.Row>();
        try(var rows=new RocksDbRows(path.resolve("data"),2,settings(),event->{})) {
            var binding=new RocksYcsb(rows,loaded::add,2);binding.stage(true);RocksYcsb.install(binding);
            try {
                try(var load=RocksYcsb.open(true,2)) {
                    for(int i=0;i<513;i++) {
                        byte[] value={0,-1,(byte)i};
                        assertEquals(OK,load.change("insert","key'雪"+i,Map.of("field0",value,"field1",new byte[0])));
                        value[0]=9;
                    }
                    assertEquals(512,loaded.size());
                }
                assertEquals(513,binding.completed(1));assertEquals(513,loaded.size());rows.finishLoad();
                for(int i=0;i<513;i++)assertArrayEquals(new byte[]{0,-1,(byte)i},rows.read("key'雪"+i,0).get(0).fields()[0]);
            } finally { RocksYcsb.install(null); }
        }
    }

    @Test public void duplicateOrMalformedBaselineCannotPublishCompletion() throws Exception {
        library();
        for(boolean duplicate:List.of(false,true)) {
            Path path=Files.createTempDirectory("rocks-invalid-load-");
            var loaded=new ArrayList<RocksDbRows.Row>();
            try(var rows=new RocksDbRows(path.resolve("data"),1,settings(),event->{})) {
                var binding=new RocksYcsb(rows,loaded::add,1);binding.stage(true);RocksYcsb.install(binding);
                try {
                    var load=RocksYcsb.open(true,1);
                    load.change("insert","a",Map.of("field0",new byte[]{1}));
                    if(duplicate)load.change("insert","a",Map.of("field0",new byte[]{2}));
                    else assertThrows(IllegalArgumentException.class,()->load.change("insert","b",Map.of()));
                    assertThrows(Exception.class,load::close);
                    assertThrows(java.io.IOException.class,()->binding.completed(1));
                    assertTrue(loaded.isEmpty());
                } finally { RocksYcsb.install(null); }
            }
        }
    }

    @Test public void apiSemanticsAndFailedWritesProduceConsumableTrace() throws Exception {
        Path library=library(),root=Files.createTempDirectory("rocks-api-");
        var source=SourceSchema.of("ycsb",Path.of("benchmark/YCSB"),Map.of("fieldcount",2));
        assertTrue(source.fingerprints().containsKey("core/src/main/java/site/ycsb/DB.java"));
        assertFalse(source.fingerprints().keySet().stream().anyMatch(p->p.startsWith("jdbc/")));
        try(var trace=new RocksTrace(root.resolve("trace"),source,true)) {
            var rows=new RocksDbRows(root.resolve("trace/data"),2,settings(),trace::event);
            try(rows) {
                var binding=new RocksYcsb(rows,trace::baseline,2);binding.stage(true);RocksYcsb.install(binding);
                try {
                    try(var load=RocksYcsb.open(true,2)) {
                        for(String key:List.of("user2","user20","user10","user1","\ue000","\ud800\udc00"))
                            load.change("insert",key,Map.of("field0",new byte[]{0,-1,39,92},"field1",new byte[]{2}));
                    }
                    rows.finishLoad();trace.finishBaseline(6);binding.stage(false);
                    var worker=RocksYcsb.open(false,2);
                    try(var competitor=RocksYcsb.open(false,2)) {
                        assertEquals(List.of("user10","user2"),worker.read("user10",null,2).rows().stream().map(RocksYcsb.Row::key).toList());
                        assertEquals(List.of("\ue000","\ud800\udc00"),worker.read("\ue000",null,2).rows().stream().map(RocksYcsb.Row::key).toList());
                        assertEquals(Set.of("field1"),worker.read("user1",Set.of("field1"),0).rows().get(0).fields().keySet());
                        assertTrue(worker.read("user1",Set.of(),0).rows().get(0).fields().isEmpty());
                        assertEquals(NOT_FOUND,worker.read("missing",null,0).status());
                        // Upstream F is a read followed by a separate update; another worker may commit between them.
                        assertArrayEquals(new byte[]{0,-1,39,92},worker.read("user1",null,0).rows().get(0).fields().get("field0"));
                        assertEquals(OK,competitor.change("update","user1",Map.of("field0",new byte[]{3})));
                        assertEquals(OK,worker.change("update","user1",Map.of("field1",new byte[]{4})));
                        var read=worker.read("user1",null,0).rows().get(0);
                        assertArrayEquals(new byte[]{3},read.fields().get("field0"));assertArrayEquals(new byte[]{4},read.fields().get("field1"));
                        assertEquals(ERROR,worker.change("insert","user1",Map.of("field0",new byte[]{8})));
                        assertEquals(OK,worker.change("delete","user1",Map.of()));
                        assertEquals(NOT_FOUND,worker.change("delete","user1",Map.of()));
                        assertEquals(OK,worker.change("insert","user1",Map.of("field0",new byte[]{9})));
                        assertEquals(NOT_FOUND,worker.change("update","missing",Map.of("field0",new byte[]{1})));
                        worker.close();
                        assertThrows(IllegalStateException.class,()->worker.change("insert","late",Map.of()));
                    }
                    assertEquals(14,binding.completed(2));
                } finally { RocksYcsb.install(null); }
            }
            trace.close();var manifest=trace.publish(Map.of("input","hand-controlled API"));
            assertEquals(1L,manifest.get("aborted_transactions"));
        }
        org.sibench.replay.ReferenceReplay.run(root.resolve("trace"),new org.sibench.replay.ReplayConfig(true,0,0,0,0,"operation"),root.resolve("reference.json"));
    }
}
