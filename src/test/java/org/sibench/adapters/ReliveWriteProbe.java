package org.sibench.adapters;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.sibench.adapters.proto.*;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/** Finite individual gRPC writes through the real HTTP bridge, with an independent ordered-value oracle. */
public final class ReliveWriteProbe {
    private static Object write(IndexServiceGrpc.IndexServiceStub stub, long timestamp, int firstValue) throws Exception {
        stub = stub.withDeadlineAfter(90, TimeUnit.SECONDS);
        var calls = new ArrayList<CompletableFuture<Void>>();
        var failed = new CompletableFuture<Void>();
        double[] latency = new double[256];
        long begin = System.nanoTime();
        for (int i = 0; i < latency.length; i++) {
            int number = i;
            var done = new CompletableFuture<Void>();
            calls.add(done);
            var put = PutRequest.newBuilder().setKey(ByteString.copyFrom(ByteBuffer.allocate(4).putInt(i).array()))
                    .setTs(timestamp).setValue(ByteString.copyFrom(ByteBuffer.allocate(8).putLong(firstValue + i).array())).build();
            long started = System.nanoTime();
            stub.put(put, new StreamObserver<PutResponse>() {
                int replies;
                public void onNext(PutResponse response) { replies++; }
                public void onError(Throwable error) { done.completeExceptionally(error); failed.completeExceptionally(error); }
                public void onCompleted() {
                    if (replies != 1) { onError(new AssertionError("Incomplete unary ACK")); return; }
                    latency[number] = (System.nanoTime() - started) / 1e6;
                    done.complete(null);
                }
            });
        }
        CompletableFuture.anyOf(CompletableFuture.allOf(calls.toArray(CompletableFuture[]::new)), failed).get(90, TimeUnit.SECONDS);
        double seconds = (System.nanoTime() - begin) / 1e9;
        Arrays.sort(latency);
        return java.util.Map.of("acknowledged_operations", latency.length, "seconds", seconds,
                "operations_per_second", latency.length / seconds, "p50_ms", latency[128], "p99_ms", latency[253]);
    }

    private static void verify(IndexServiceGrpc.IndexServiceBlockingStub stub, long ts, int firstValue, int expectedCount) {
        var chunks = stub.withDeadlineAfter(30, TimeUnit.SECONDS).scan(ScanRequest.newBuilder().setReadTs(ts).build());
        int seen = 0;
        while (chunks.hasNext()) for (var value : chunks.next().getValuesList()) {
            if (value.size() != 8 || value.asReadOnlyByteBuffer().getLong() != firstValue + seen)
                throw new AssertionError("Wrong ordered value at snapshot " + ts + ", row " + seen);
            seen++;
        }
        if (seen != expectedCount) throw new AssertionError("Incomplete snapshot " + ts + ": " + seen);
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) throw new IllegalArgumentException("FRESH_RELIVE_HTTP_URL FRESH_REPORT");
        Path report = Path.of(args[1]);
        if (Files.exists(report)) throw new IllegalArgumentException("Report already exists");
        var result = new LinkedHashMap<String, Object>();
        result.put("scope", "two waves of 256 individual gRPC Puts; fresh state; exact ordered histories; not Trace replay");
        try (var backend = new ReliveBackend(args[0], ValueKind.ROW_ID);
             var service = IndexTestSupport.start(0, backend)) {
            var stats = java.net.http.HttpClient.newHttpClient().send(java.net.http.HttpRequest.newBuilder(
                    java.net.URI.create(args[0] + "/stats")).timeout(java.time.Duration.ofSeconds(10)).GET().build(),
                    java.net.http.HttpResponse.BodyHandlers.ofString());
            var state = com.google.gson.JsonParser.parseString(stats.body()).getAsJsonObject();
            if (stats.statusCode() != 200 || state.get("logicalMutationBytes").getAsLong() != 0
                    || state.get("activeSegmentBytes").getAsLong() != 0 || state.get("runCount").getAsLong() != 0)
                throw new IllegalArgumentException("Fresh empty Relive state required");
            backend.bind("relive-write-probe");
            var channel = ManagedChannelBuilder.forAddress("127.0.0.1", service.port()).usePlaintext().disableRetry().build();
            try {
                var reader = IndexServiceGrpc.newBlockingStub(channel).withDeadlineAfter(90, TimeUnit.SECONDS);
                var writer = IndexServiceGrpc.newStub(channel).withDeadlineAfter(90, TimeUnit.SECONDS);
                verify(reader, 0, 0, 0);
                result.put("insert", write(writer, 10, 1000));
                verify(reader, 15, 1000, 256);
                result.put("update", write(writer, 20, 2000));
                verify(reader, 5, 0, 0);
                verify(reader, 15, 1000, 256);
                verify(reader, 25, 2000, 256);
                result.put("status", "passed");
            } finally { channel.shutdownNow(); channel.awaitTermination(10, TimeUnit.SECONDS); }
        } finally {
            Files.writeString(report, new com.google.gson.GsonBuilder().setPrettyPrinting().create().toJson(result),
                    java.nio.file.StandardOpenOption.CREATE_NEW);
        }
    }
}
