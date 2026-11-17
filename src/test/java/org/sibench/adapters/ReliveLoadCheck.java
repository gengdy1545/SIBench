package org.sibench.adapters;

import static org.junit.Assert.*;
import static org.sibench.adapters.IndexServiceCheck.*;
import static org.sibench.adapters.ReliveIntegrationCheck.maintenance;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.sibench.adapters.proto.*;

/** Explicit finite concurrency diagnostic; source versions are fixed before any request is sent. */
public final class ReliveLoadCheck {
    private static ByteString key(int key) { return ByteString.copyFromUtf8("mc/key/" + key); }
    private static ByteString value(long value) { return ByteString.copyFrom(ByteBuffer.allocate(8).putLong(value).array()); }
    static List<Mutation> plan(int operations) {
        if (operations < 64 || operations > 8192 || operations % 64 != 0)
            throw new IllegalArgumentException("Mutation count must be a multiple of 64 between 64 and 8192");
        return java.util.stream.IntStream.range(0, operations).mapToObj(i -> {
            long ts = operations - i;
            return ts % 11 == 0
                    ? Mutation.newBuilder().setDelete(DeleteRequest.newBuilder().setKey(key(i % 8)).setTs(ts)).build()
                    : Mutation.newBuilder().setPut(PutRequest.newBuilder().setKey(key(i % 8)).setTs(ts).setValue(value(200000 + ts))).build();
        }).toList();
    }

    static List<ByteString> expected(long readTs, int operations) {
        var result = new ArrayList<ByteString>();
        for (int k = 0; k < 8; k++) {
            long selected = 0;
            ByteString found = value(100000 + k);
            for (int i = k; i < operations; i += 8) {
                long ts = operations - i;
                if (ts <= readTs && ts > selected) {
                    selected = ts;
                    found = ts % 11 == 0 ? null : value(200000 + ts);
                }
            }
            if (found != null) result.add(found);
        }
        return result;
    }

    private static void snapshot(IndexServiceGrpc.IndexServiceBlockingStub api, long readTs, int operations) {
        assertEquals(expected(readTs, operations), scan(api.withDeadlineAfter(30, TimeUnit.SECONDS), ScanRequest.newBuilder()
                .setStartKey(key(0)).setEndKey(ByteString.copyFromUtf8("mc/key0")).setReadTs(readTs).build()));
    }

    static boolean drained(com.google.gson.JsonObject stats) {
        return stats.get("memoryCharge").getAsLong() == 0
                && stats.get("frozenMemtables").getAsLong() == 0
                && stats.get("compactionEligibleBytes").getAsLong() == 0
                && !stats.get("backgroundFlushing").getAsBoolean()
                && !stats.get("backgroundCompacting").getAsBoolean();
    }

    private static void drain(String writer, String observer) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
        maintenance(writer, "/flush");
        var client = java.net.http.HttpClient.newHttpClient();
        while (System.nanoTime() < deadline) {
            boolean idle = true;
            for (String endpoint : List.of(writer, observer)) {
                var request = java.net.http.HttpRequest.newBuilder(java.net.URI.create(endpoint + "/stats"))
                        .timeout(java.time.Duration.ofNanos(Math.max(1,
                                Math.min(TimeUnit.SECONDS.toNanos(10), deadline - System.nanoTime())))).build();
                var response = client.send(request, java.net.http.HttpResponse.BodyHandlers.ofString());
                assertEquals(response.body(), 200, response.statusCode());
                idle &= drained(com.google.gson.JsonParser.parseString(response.body()).getAsJsonObject());
            }
            if (idle) return;
            Thread.sleep(1000);
        }
        throw new java.io.IOException("Normal maintenance drain exceeded 120 seconds");
    }

    private static Map<String, Object> latency(long[] values) {
        Arrays.sort(values);
        return Map.of("count", values.length, "p50_ms", values[(values.length - 1) / 2] / 1e6,
                "p95_ms", values[(int) Math.ceil(values.length * .95) - 1] / 1e6,
                "p99_ms", values[(int) Math.ceil(values.length * .99) - 1] / 1e6);
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4 && args.length != 5)
            throw new IllegalArgumentException("HTTP_WRITER HTTP_OBSERVER CLIENTS FRESH_REPORT [MUTATIONS]");
        int clients = Integer.parseInt(args[2]);
        if (clients != 1 && clients != 4) throw new IllegalArgumentException("Diagnostic supports 1 or 4 write clients");
        int operations = args.length == 5 ? Integer.parseInt(args[4]) : 64;
        var plan = plan(operations);
        Path output = Path.of(args[3]);
        if (Files.exists(output)) throw new IllegalArgumentException("Report already exists");
        // Hand-derived history at 15: key 5 is hidden by its version-11 tombstone.
        assertEquals(List.of(value(200008), value(200015), value(200014), value(200013),
                value(200012), value(200010), value(200009)), expected(15, operations));
        var report = new LinkedHashMap<String, Object>();
        var digest = MessageDigest.getInstance("SHA-256");
        for (var mutation : plan) {
            digest.update(ByteBuffer.allocate(4).putInt(mutation.getSerializedSize()).array());
            digest.update(mutation.toByteArray());
        }
        report.put("plan_sha256", HexFormat.of().formatHex(digest.digest()));
        report.put("write_clients", clients);
        report.put("mutations", operations);
        report.put("scope", operations + " fixed source-version mutations, " + operations / 2
                + " concurrent snapshot-zero Gets; finite closed-loop diagnostic, not sustained throughput");
        report.put("status", "failed");
        var channels = new ArrayList<ManagedChannel>();
        try (var backend = new ReliveBackend(args[0], ValueKind.ROW_ID);
             var follower = new ReliveBackend(args[1], ValueKind.ROW_ID);
             var server = IndexServer.start(0, backend);
             var observer = IndexServer.start(0, follower)) {
            var writers = new ArrayList<IndexServiceGrpc.IndexServiceBlockingStub>();
            for (int i = 0; i < clients + 2; i++) {
                var channel = IndexServerTest.channel(i == clients + 1 ? observer.port() : server.port());
                channels.add(channel);
                writers.add(bound(channel, "relive-load-check", ValueKind.ROW_ID));
            }
            var reader = writers.get(clients);
            var other = writers.get(clients + 1);
            long begin = System.nanoTime();
            for (int first = 0; first < 8; first += 4) {
                var batch = WriteBatchRequest.newBuilder();
                for (int i = first; i < first + 4; i++) batch.addMutations(Mutation.newBuilder().setPut(
                        PutRequest.newBuilder().setKey(key(i)).setTs(0).setValue(value(100000 + i))));
                writers.get(0).withDeadlineAfter(30, TimeUnit.SECONDS).writeBatch(batch.build());
                maintenance(args[0], "/flush");
            }
            maintenance(args[0], "/compact");
            snapshot(reader, 0, operations);
            snapshot(other, 0, operations);
            report.put("load_and_prime_seconds", (System.nanoTime() - begin) / 1e9);
            long[] writeTimes = new long[operations], getTimes = new long[operations / 2];
            var lastAck = new AtomicLong();
            var lastSubmit = new AtomicLong();
            var ready = new CountDownLatch(clients + 1);
            var start = new CountDownLatch(1);
            var workers = Executors.newFixedThreadPool(clients + 1);
            var work = new ArrayList<java.util.concurrent.Future<?>>();
            try {
                for (int c = 0; c < clients; c++) {
                    final int number = c;
                    work.add(workers.submit(() -> {
                        ready.countDown();
                        assertTrue(start.await(5, TimeUnit.SECONDS));
                        for (int i = number; i < plan.size(); i += clients) {
                            var mutation = plan.get(i);
                            long sent = System.nanoTime();
                            lastSubmit.accumulateAndGet(sent, Math::max);
                            var call = writers.get(number).withDeadlineAfter(30, TimeUnit.SECONDS);
                            if (mutation.hasPut()) call.put(mutation.getPut());
                            else call.delete(mutation.getDelete());
                            long ack = System.nanoTime();
                            writeTimes[i] = ack - sent;
                            lastAck.accumulateAndGet(ack, Math::max);
                        }
                        return null;
                    }));
                }
                var reads = workers.submit(() -> {
                    ready.countDown();
                    assertTrue(start.await(5, TimeUnit.SECONDS));
                    for (int i = 0; i < getTimes.length; i++) {
                        long sent = System.nanoTime();
                        var answer = reader.withDeadlineAfter(30, TimeUnit.SECONDS)
                                .get(GetRequest.newBuilder().setKey(key(i % 8)).setReadTs(0).build());
                        getTimes[i] = System.nanoTime() - sent;
                        assertTrue(answer.hasValue());
                        assertEquals(value(100000 + i % 8), answer.getValue());
                    }
                    return null;
                });
                assertTrue(ready.await(5, TimeUnit.SECONDS));
                report.put("workload_mountstats_before", Files.readString(Path.of("/proc/self/mountstats")));
                begin = System.nanoTime();
                long deadline = begin + TimeUnit.SECONDS.toNanos(600);
                start.countDown();
                for (var writer : work) writer.get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
                report.put("write_submission_span_seconds", (lastSubmit.get() - begin) / 1e9);
                report.put("all_write_ack_seconds", (lastAck.get() - begin) / 1e9);
                report.put("submitted_mutations_per_second", operations * 1e9 / (lastSubmit.get() - begin));
                report.put("successful_mutations_per_second", operations * 1e9 / (lastAck.get() - begin));
                snapshot(other, operations, operations);
                report.put("last_ack_to_other_instance_correct_scan_ms", (System.nanoTime() - lastAck.get()) / 1e6);
                reads.get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
                report.put("work_seconds", (System.nanoTime() - begin) / 1e9);
                report.put("workload_mountstats_after", Files.readString(Path.of("/proc/self/mountstats")));
            } finally { workers.shutdownNow(); }
            report.put("write_latency", latency(writeTimes));
            report.put("get_latency", latency(getTimes));
            begin = System.nanoTime();
            drain(args[0], args[1]);
            long idleObserved = System.nanoTime();
            report.put("normal_drain_seconds", (idleObserved - begin) / 1e9);
            report.put("last_write_submission_to_observed_idle_seconds", (idleObserved - lastSubmit.get()) / 1e9);
            report.put("last_write_ack_to_observed_idle_seconds", (idleObserved - lastAck.get()) / 1e9);
            report.put("maintenance_scope", "Flush after all foreground ACKs, then require both instances to report no memory, frozen maps, eligible compaction or active maintenance; no forced compaction");
            for (long ts : new long[]{0, 15, 25, 35, 45, 55, 64, operations / 2, operations}) {
                snapshot(reader, ts, operations);
                snapshot(other, ts, operations);
            }
            report.put("status", "passed");
        } catch (Throwable error) {
            report.put("status", "failed");
            report.put("first_error", error.toString());
            throw error;
        } finally {
            for (var channel : channels) channel.shutdownNow();
            Files.writeString(output, new com.google.gson.GsonBuilder().setPrettyPrinting().create().toJson(report) + "\n");
        }
    }
}
