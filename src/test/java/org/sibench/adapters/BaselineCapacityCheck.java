package org.sibench.adapters;

import com.google.gson.GsonBuilder;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import org.sibench.adapters.proto.*;
import org.sibench.capture.IndexOperationTrace;
import org.sibench.replay.*;
import org.sibench.trace.IndexKey;

/** Diagnostic receiver consumes every baseline field and emits the normal empty write ACK. */
public final class BaselineCapacityCheck {
    private static final com.google.gson.Gson JSON = new GsonBuilder().setPrettyPrinting().create();
    private static final class EndWindow extends IOException {}
    static final class Fingerprint {
        private final LongAdder count = new LongAdder(), sum = new LongAdder();
        private final LongAccumulator xor = new LongAccumulator((a,b) -> a ^ b, 0);
        private final ThreadLocal<ByteBuffer> buffer = ThreadLocal.withInitial(() -> ByteBuffer.allocate(256));
        private static long mix(long x) { x ^= x >>> 30; x *= 0xbf58476d1ce4e5b9L; x ^= x >>> 27; x *= 0x94d049bb133111ebL; return x ^ (x >>> 31); }
        private static long bytes(long h, ByteString bytes) { h = mix(h ^ bytes.size()); for (int i=0;i<bytes.size();i++) h=(h ^ (bytes.byteAt(i)&255))*0x100000001b3L; return h; }
        private long bytes(long h, IndexKey key) {
            ByteBuffer b=buffer.get(); if (b.capacity()<key.length()) { b=ByteBuffer.allocate(key.length()); buffer.set(b); }
            b.clear(); key.writeTo(b); h=mix(h ^ b.position()); for (int i=0;i<b.position();i++) h=(h ^ (b.get(i)&255))*0x100000001b3L; return h;
        }
        private void add(long h) { h=mix(h); count.increment();sum.add(h);xor.accumulate(h); }
        void operation(IndexOperationTrace.Operation op) throws IOException {
            if (op.kind()!=IndexOperationTrace.INSERT || op.version()!=0) throw new IOException("Not a baseline INSERT");
            long h=mix(((long)op.table()<<32)^op.index())^mix(op.rowId());
            h=bytes(h,op.key());h=op.index()==0?mix(h):bytes(h,op.indexedValue());add(h);
        }
        void write(Write write) throws IOException {
            if (write.getKind()!=Write.Kind.INSERT || write.getVersion()!=0 || write.getValue().getFile()!=0 || write.getValue().getPosition()!=0)
                throw new IOException("Not a RowID baseline INSERT");
            var key=write.getKey();long h=mix(((long)key.getTable()<<32)^key.getIndex())^mix(write.getValue().getRowId());
            h=bytes(h,key.getPrimaryKey());h=bytes(h,key.getIndexedValue());add(h);
        }
        Map<String,Long> summary() { return Map.of("members",count.sum(),"sum64",sum.sum(),"xor64",xor.get()); }
    }
    static final class Sink implements IndexBackend {
        final Fingerprint received=new Fingerprint();final Path report;final boolean dropFirst;final AtomicBoolean dropped=new AtomicBoolean();String binding="";
        Sink(Path report,boolean dropFirst) { this.report=report;this.dropFirst=dropFirst; }
        public DescribeResponse describe() { return DescribeResponse.newBuilder().setName("baseline-transport-field-check-sink").setValueKind(ValueKind.ROW_ID).setBaselineWriteBatches(true).setWriteAck(Acknowledgement.ACK_MEMORY).setDurability("diagnostic field receiver; no index storage").build(); }
        public void write(Write write) throws IOException { if (dropFirst && dropped.compareAndSet(false,true)) return;received.write(write); }
        public void search(Search search,ResultSink output) throws IOException { throw new IOException("Baseline-only diagnostic receiver"); }
        public void flush() throws IOException { Files.writeString(report,JSON.toJson(received.summary())+"\n"); }
        public void bind(String value) throws IOException { if (!binding.isEmpty()&&!binding.equals(value)) throw new IOException("Different binding");binding=value; }
        public String location() { return "diagnostic-baseline-sink"; }
        public long publishedVersion() { return 0; }
        public void recordPublishedVersion(long version) throws IOException { if(version!=0)throw new IOException("Baseline version must be zero"); }
        public void close() throws IOException { flush(); }
    }
    static void verify(Fingerprint expected,Map<String,Long> actual) throws IOException {
        if (!expected.summary().equals(actual)) throw new IOException("Baseline transport fields differ");
    }
    static Map<String,Long> difference(Map<String,Long> after,Map<String,Long> before) {
        return Map.of("members",after.get("members")-before.get("members"),"sum64",after.get("sum64")-before.get("sum64"),"xor64",after.get("xor64")^before.get("xor64"));
    }
    private static Map<String,Long> received(Path path) throws IOException {
        if(!Files.exists(path))return Map.of("members",0L,"sum64",0L,"xor64",0L);
        var json=com.google.gson.JsonParser.parseString(Files.readString(path)).getAsJsonObject();var out=new HashMap<String,Long>();
        for(var key:List.of("members","sum64","xor64"))out.put(key,json.get(key).getAsLong());return out;
    }
    static Map<String,Object> run(Path trace,String endpoint,int clients,Path receiver,Path report,long windowSeconds,ReferenceReplay.Input prepared) throws Exception {
        long preparing=System.nanoTime();
        var config=new ReplayConfig(true,"none",0,0,0,0,"operation",clients,4096,60);
        var input=prepared==null?ReferenceReplay.prepare(trace,config):prepared;var expected=new Fingerprint();var requests=new TraceRequests(ValueKind.ROW_ID);var before=received(receiver);
        requests.baselineBatch(Math.max(1,config.maxInFlight()/clients));boolean complete=true;Map<String,Object> dispatch;
        double setupSeconds=(System.nanoTime()-preparing)/1e9;
        long memberLimit=Long.getLong("sibench.capacity.members",Long.MAX_VALUE);
        long started=System.nanoTime();Files.writeString(report.resolveSibling(report.getFileName()+".started"),Long.toString(System.currentTimeMillis()));
        try(var remote=new GrpcService(endpoint,requests, "baseline-capacity " + trace.toAbsolutePath(), 60);var projection=ReplayProjection.withClients(config,false,remote.factory(),true)) {
            try { input.baseline().read(op -> {
                if (expected.count.sum()>=memberLimit || windowSeconds>0 && (expected.count.sum()&4095)==0 && System.nanoTime()-started>=windowSeconds*1_000_000_000L) throw new EndWindow();
                expected.operation(op);projection.accept(op);
            }); } catch(EndWindow stop) { complete=false; }
            Files.writeString(report.resolveSibling(report.getFileName()+".submission-stopped"),Long.toString(System.currentTimeMillis()));
            projection.finish();dispatch=projection.describe();remote.flush("baseline-capacity "+trace.toAbsolutePath());
        }
        double seconds=(System.nanoTime()-started)/1e9;requests.finish();
        verify(expected,difference(received(receiver),before));
        var out=new LinkedHashMap<String,Object>();out.put("status","passed");out.put("clients",clients);out.put("complete_baseline",complete);out.put("setup_seconds",setupSeconds);out.put("seconds_including_flush",seconds);out.put("field_fingerprint",expected.summary());out.put("request_bytes",requests.requestBytes());out.put("dispatch",dispatch);out.put("preflight",input.preflight());out.put("scope","same verified input; actual batch conversion/RPC/ACK path plus independent commutative field fingerprint; receiver has no index storage; full canonical SHA checked only if complete_baseline=true, otherwise prior full reference/oracle proof reused for immutable input; finite window, not backend throughput");
        Files.writeString(report,JSON.toJson(out)+"\n",StandardOpenOption.CREATE_NEW);return out;
    }
    public static void main(String[] args) throws Exception {
        if (args[0].startsWith("serve")) {
            var sink=new Sink(Path.of(args[2]),args[0].equals("serve-drop"));var server=IndexServer.start(Integer.parseInt(args[1]),sink);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> { try {server.close();}catch(IOException ignored){} }));System.out.println("READY "+server.port());server.awaitTermination();
        } else if(args[0].equals("series")) {
            Path trace=Path.of(args[1]),receiver=Path.of(args[3]),output=Path.of(args[4]);
            var config=new ReplayConfig(true,"none",0,0,0,0,"operation",128,4096,60);long start=System.nanoTime();
            var input=ReferenceReplay.prepare(trace,config);double setup=(System.nanoTime()-start)/1e9;var paths=new ArrayList<String>();
            int[] clients={64,1,128,128,1,64};
            for(int i=0;i<clients.length;i++){Path report=output.resolve("clients-"+clients[i]+"-"+i+".json");run(trace,args[2],clients[i],receiver,report,Long.parseLong(args[5]),input);paths.add(report.getFileName().toString());System.out.println("completed "+report.getFileName());}
            Files.writeString(output.resolve("series.json"),JSON.toJson(Map.of("status","passed","setup_seconds",setup,"reports",paths,"cache","one driver and receiver JVM across balanced trials; same immutable input; each trial starts from baseline beginning, no cycling within a trial"))+"\n",StandardOpenOption.CREATE_NEW);
        } else run(Path.of(args[1]),args[2],Integer.parseInt(args[3]),Path.of(args[4]),Path.of(args[5]),Long.parseLong(args[6]),null);
    }
}
