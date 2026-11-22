package org.sibench.adapters;

import com.google.gson.*;
import com.google.protobuf.ByteString;
import com.sun.net.httpserver.HttpServer;
import org.junit.Test;
import org.sibench.adapters.proto.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;

import static org.junit.Assert.*;

/** Tests the HTTP trust boundary independently of Relive's storage implementation. */
public class ReliveBackendTest {
    @Test(timeout = 25000) public void stalledWriteBodiesDoNotDelayAnotherCompletedAcknowledgement() throws Exception {
        var process = new ProcessBuilder(java.nio.file.Path.of(System.getProperty("java.home"), "bin/java").toString(),
                "-Djava.util.concurrent.ForkJoinPool.common.parallelism=2", "-cp", System.getProperty("java.class.path"),
                ReliveAsyncResponseCheck.class.getName()).redirectErrorStream(true).start();
        try {
            assertTrue("Response probe timed out", process.waitFor(20, TimeUnit.SECONDS));
            String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            assertEquals(output, 0, process.exitValue());
            assertTrue(output, output.contains("fast_client_completed_while_other_bodies_stalled=true"));
        } finally { process.destroyForcibly(); process.waitFor(2, TimeUnit.SECONDS); }
    }

    @Test(timeout = 5000) public void requestPayloadCannotBeResubscribedForAnUnknownOutcomeRetry() throws Exception {
        var publisher = ReliveBackend.requestBody("{\"rows\":[]}");
        class Subscriber implements java.util.concurrent.Flow.Subscriber<java.nio.ByteBuffer> {
            final java.io.ByteArrayOutputStream received = new java.io.ByteArrayOutputStream();
            final CompletableFuture<Void> completion = new CompletableFuture<>();
            public void onSubscribe(java.util.concurrent.Flow.Subscription subscription) { subscription.request(Long.MAX_VALUE); }
            public void onNext(java.nio.ByteBuffer bytes) {
                byte[] copy = new byte[bytes.remaining()]; bytes.get(copy); received.writeBytes(copy);
            }
            public void onError(Throwable error) { completion.completeExceptionally(error); }
            public void onComplete() { completion.complete(null); }
        }
        var first = new Subscriber(); publisher.subscribe(first);
        first.completion.get(1, TimeUnit.SECONDS);
        assertEquals("{\"rows\":[]}", first.received.toString(StandardCharsets.UTF_8));
        assertEquals(first.received.size(), publisher.contentLength());
        var repeated = new Subscriber(); publisher.subscribe(repeated);
        var failure = assertThrows(ExecutionException.class, () -> repeated.completion.get(1, TimeUnit.SECONDS));
        assertTrue(failure.getCause() instanceof IOException);
        assertTrue(failure.getCause().getMessage().contains("cannot be retransmitted"));
        assertEquals(0, repeated.received.size());
    }

    private static ByteString bytes(String text) { return ByteString.copyFromUtf8(text); }
    private static Mutation put(String key, long ts, String value) {
        return Mutation.newBuilder().setPut(PutRequest.newBuilder().setKey(bytes(key)).setTs(ts).setValue(bytes(value))).build();
    }
    private static Mutation delete(String key) {
        return Mutation.newBuilder().setDelete(DeleteRequest.newBuilder().setKey(bytes(key))).build();
    }
    private static String frames(long ts, String... entries) {
        var result = new StringBuilder("{\"ts\":" + ts + ",\"scans\":1}\n");
        for (String entry : entries) result.append("{\"scan\":0,\"entry\":").append(entry).append("}\n");
        return result.append("{\"done\":true,\"counts\":[").append(entries.length).append("]}\n").toString();
    }
    private static String row(String key, long ts, String value) {
        return "{\"key\":\"00" + HexFormat.of().formatHex(bytes(key).toByteArray()) + "\",\"ts\":" + ts
                + ",\"value\":\"" + HexFormat.of().formatHex(bytes(value).toByteArray()) + "\"}";
    }

    private static final class Fixture implements AutoCloseable {
        final HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        final ExecutorService executor = Executors.newCachedThreadPool();
        final List<JsonObject> writes = new CopyOnWriteArrayList<>();
        final List<JsonObject> reads = new CopyOnWriteArrayList<>();
        volatile Function<JsonObject, String> stream = request -> frames(request.get("ts").getAsLong());
        volatile Function<JsonObject, String> writeReply = request -> "{\"rows\":" + request.getAsJsonArray("rows").size() + "}";
        volatile String binding;
        volatile int writeStatus = 200;
        volatile Integer readConcurrency = 4;
        volatile Function<byte[], byte[]> writeBytes = Function.identity();
        volatile int writeDeclaredLength;
        volatile CountDownLatch bodyStarted, releaseBody;
        Fixture() throws IOException {
            server.setExecutor(executor);
            server.createContext("/", exchange -> {
                try (exchange) {
                    String path = exchange.getRequestURI().getPath();
                    String response; int status = 200;
                    if (path.equals("/describe")) response = "{\"name\":\"relive\",\"versionedKeys\":true,\"streamingReads\":true,"
                            + "\"transactionLocalReads\":false,\"searchGroups\":false,\"maxWriteRows\":10000,"
                            + "\"durability\":\"shared_nfs_fsync\",\"levelOneStorage\":\"object_storage\",\"location\":\"/fixture/cfs cos://fixture/l1\","
                            + "\"readConcurrency\":" + readConcurrency + "}";
                    else {
                        var request = JsonParser.parseString(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8)).getAsJsonObject();
                        switch (path) {
                            case "/bind" -> {
                                synchronized (this) {
                                    String supplied = request.get("binding").getAsString();
                                    if (binding == null) binding = supplied;
                                    if (!binding.equals(supplied)) { status = 409; response = "{}"; }
                                    else {
                                        var result = new JsonObject(); result.addProperty("binding", binding);
                                        result.addProperty("identity", "fixture-index"); result.addProperty("location", "/fixture/cfs");
                                        response = result.toString();
                                    }
                                }
                            }
                            case "/write" -> {
                                writes.add(request); status = writeStatus;
                                response = writeReply.apply(request);
                            }
                            case "/stream" -> { reads.add(request); response = stream.apply(request); }
                            default -> { status = 404; response = "{}"; }
                        }
                    }
                    byte[] encoded = response.getBytes(StandardCharsets.UTF_8);
                    boolean write = path.equals("/write");
                    if (write) encoded = writeBytes.apply(encoded);
                    exchange.sendResponseHeaders(status, write && writeDeclaredLength > 0 ? writeDeclaredLength : encoded.length);
                    if (write && bodyStarted != null) {
                        exchange.getResponseBody().write(encoded, 0, 1);
                        exchange.getResponseBody().flush(); bodyStarted.countDown();
                        try { if (!releaseBody.await(5, TimeUnit.SECONDS)) throw new AssertionError("Body gate timed out"); }
                        catch (InterruptedException error) { throw new AssertionError(error); }
                        exchange.getResponseBody().write(encoded, 1, encoded.length - 1);
                        return;
                    }
                    exchange.getResponseBody().write(encoded);
                }
            });
            server.start();
        }
        String endpoint() { return "http://127.0.0.1:" + server.getAddress().getPort(); }
        @Override public void close() { server.stop(0); executor.shutdownNow(); }
    }

    @Test(timeout = 15000) public void asynchronousResponsesRetainSizeEncodingAndCompletionChecks() throws Exception {
        try (var fixture = new Fixture(); var backend = new ReliveBackend(fixture.endpoint(), ValueKind.ROW_ID)) {
            for (byte[] invalid : List.of(new byte[(64 << 10) + 1], new byte[]{(byte) 0xc3},
                    "{".getBytes(StandardCharsets.UTF_8), "{\"rows\":1}{}".getBytes(StandardCharsets.UTF_8))) {
                fixture.writeBytes = ignored -> invalid;
                var error = assertThrows(ExecutionException.class,
                        () -> backend.writeAsync(put("key", 1, "value")).get(2, TimeUnit.SECONDS));
                assertTrue(error.toString(), error.getCause() instanceof IOException);
            }
            fixture.writeBytes = Function.identity(); fixture.writeDeclaredLength = 100;
            assertThrows(ExecutionException.class,
                    () -> backend.writeAsync(put("truncated", 2, "value")).get(2, TimeUnit.SECONDS));
        }
    }

    @Test(timeout = 10000) public void pendingWriteBodiesFailOnCloseWithoutAcknowledgingTheWrite() throws Exception {
        try (var fixture = new Fixture(); var backend = new ReliveBackend(fixture.endpoint(), ValueKind.ROW_ID)) {
            fixture.bodyStarted = new CountDownLatch(1); fixture.releaseBody = new CountDownLatch(1);
            var reply = backend.writeAsync(put("key", 1, "value"));
            try {
                assertTrue(fixture.bodyStarted.await(2, TimeUnit.SECONDS));
                assertFalse(reply.isDone());
                backend.close();
                var error = assertThrows(ExecutionException.class, () -> reply.get(2, TimeUnit.SECONDS));
                assertTrue(error.toString(), error.getCause() instanceof IOException);
            } finally { fixture.releaseBody.countDown(); }
        }
    }

    @Test(timeout = 10000) public void engineWorkerHintsDoNotDefineTheBridgeContract() throws Exception {
        try (var fixture = new Fixture()) {
            for (Integer hint : new Integer[]{null, 0, 1, 128, 4097}) {
                fixture.readConcurrency = hint;
                try (var backend = new ReliveBackend(fixture.endpoint(), ValueKind.ROW_ID)) {
                    assertEquals("relive", backend.describe().name());
                }
            }
        }
    }

    @Test(timeout = 15000) public void bridgeReadBudgetAndStatisticsAreIndependentOfEngineWorkers() throws Exception {
        var entered = new CountDownLatch(4);
        var release = new CountDownLatch(1);
        var execution = IndexTestSupport.execution(4, 2);
        try (var fixture = new Fixture()) {
            fixture.readConcurrency = 1;
            fixture.stream = request -> {
                entered.countDown();
                try { if (!release.await(5, TimeUnit.SECONDS)) throw new AssertionError("Read gate timed out"); }
                catch (InterruptedException error) { throw new AssertionError(error); }
                return frames(0);
            };
            try (var backend = new ReliveBackend(fixture.endpoint(), ValueKind.ROW_ID);
                 var executor = new IndexExecutor(backend, execution)) {
                var calls = new ArrayList<CompletableFuture<IndexResponse>>();
                try {
                    for (int i = 0; i < 4; i++) calls.add(executor.submit(GetRequest.getDefaultInstance()));
                    assertTrue("bridge budget must not be the engine worker hint", entered.await(3, TimeUnit.SECONDS));
                } finally { release.countDown(); }
                for (var call : calls) assertEquals(IndexStatus.OK, call.get(2, TimeUnit.SECONDS).status());
            }
        } finally { release.countDown(); }
        var lines = java.nio.file.Files.readAllLines(execution.statistics());
        var sample = JsonParser.parseString(lines.get(lines.size() - 1)).getAsJsonObject();
        assertEquals("relive", sample.getAsJsonObject("backend").get("name").getAsString());
        assertEquals(4L, sample.getAsJsonObject("service").getAsJsonObject("read_execute").get("requests").getAsLong());
        assertTrue(sample.getAsJsonObject("engine").entrySet().isEmpty());
    }

    @Test(timeout = 15000) public void asynchronousWritesReachHttpBeforeEarlierAcknowledgements() throws Exception {
        var entered = new CountDownLatch(16);
        var release = new CountDownLatch(1);
        try (var fixture = new Fixture(); var backend = new ReliveBackend(fixture.endpoint(), ValueKind.ROW_ID)) {
            fixture.writeReply = request -> {
                entered.countDown();
                try {
                    if (!release.await(5, TimeUnit.SECONDS)) throw new AssertionError("ACK gate timed out");
                } catch (InterruptedException error) { Thread.currentThread().interrupt(); throw new AssertionError(error); }
                return "{\"rows\":1}";
            };
            var calls = new ArrayList<CompletableFuture<Void>>();
            try {
                for (int i = 0; i < 16; i++) calls.add(backend.writeAsync(put("key" + i, 1, "value")));
                assertTrue(entered.await(3, TimeUnit.SECONDS));
                assertTrue(calls.stream().noneMatch(CompletableFuture::isDone));
            } finally { release.countDown(); }
            for (var call : calls) call.get(2, TimeUnit.SECONDS);
            fixture.writeReply = request -> "{\"rows\":0}";
            var failed = assertThrows(ExecutionException.class, () -> backend.writeAsync(put("last", 1, "value")).get(2, TimeUnit.SECONDS));
            assertTrue(failed.getCause().toString(), failed.getCause() instanceof IOException);
            assertTrue(failed.getCause().getMessage().contains("Incomplete Relive write acknowledgement"));
        } finally { release.countDown(); }
    }

    @Test(timeout = 15000) public void asyncChunksRetainTheValidatedInputUntilEveryAck() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (var fixture = new Fixture(); var backend = new ReliveBackend(fixture.endpoint(), ValueKind.ROW_ID)) {
            fixture.writeReply = request -> {
                entered.countDown();
                try {
                    if (!release.await(5, TimeUnit.SECONDS)) throw new AssertionError("ACK gate timed out");
                } catch (InterruptedException error) { Thread.currentThread().interrupt(); throw new AssertionError(error); }
                return "{\"rows\":" + request.getAsJsonArray("rows").size() + "}";
            };
            var writes = new ArrayList<Mutation>();
            for (int i = 0; i < 10_001; i++) writes.add(put("key" + i, 10, "original"));
            CompletableFuture<Void> complete;
            try {
                complete = backend.writeBatchAsync(writes);
                assertTrue(entered.await(3, TimeUnit.SECONDS));
                writes.set(10_000, put("replacement", 20, "X".repeat(65_537)));
                assertFalse(complete.isDone());
            } finally { release.countDown(); }
            complete.get(3, TimeUnit.SECONDS);
            assertEquals(2, fixture.writes.size());
            var row = fixture.writes.get(1).getAsJsonArray("rows").get(0).getAsJsonObject();
            assertEquals(10, row.get("ts").getAsLong());
            assertEquals(HexFormat.of().formatHex(bytes("original").toByteArray()), row.get("value").getAsString());
        } finally { release.countDown(); }
    }

    @Test(timeout = 15000) public void preservesOpaqueKeysVersionsDeletesAndEmptyValues() throws Exception {
        try (var fixture = new Fixture(); var backend = new ReliveBackend(fixture.endpoint(), ValueKind.ROW_ID)) {
            IndexTestSupport.awaitWrite(backend.writeBatchAsync(List.of(put("", 20, ""), put("a", 10, "old"), delete("a"))));
            var rows = fixture.writes.get(0).getAsJsonArray("rows");
            assertEquals("00", rows.get(0).getAsJsonObject().get("key").getAsString());
            assertEquals(20, rows.get(0).getAsJsonObject().get("ts").getAsLong());
            assertEquals("", rows.get(0).getAsJsonObject().get("value").getAsString());
            assertEquals("0061", rows.get(1).getAsJsonObject().get("key").getAsString());
            assertEquals(10, rows.get(1).getAsJsonObject().get("ts").getAsLong());
            assertTrue(rows.get(2).getAsJsonObject().get("value").isJsonNull());
            assertEquals(Acknowledgement.ACK_WAL, backend.describe().writeAck());
            assertEquals("shared_nfs_fsync", backend.describe().durability());
            assertEquals("/fixture/cfs cos://fixture/l1", backend.describe().location());
            fixture.stream = ignored -> frames(25, row("", 20, ""));
            assertEquals(ByteString.EMPTY, backend.get(GetRequest.newBuilder().setReadTs(25).build()));
            fixture.stream = ignored -> frames(15);
            assertNull(backend.get(GetRequest.newBuilder().setReadTs(15).build()));
            assertEquals(15, fixture.reads.get(1).get("ts").getAsLong());
        }
    }

    @Test(timeout = 5000) public void queuedResponseBytesDoNotBypassTheAbsoluteBodyDeadline() throws Exception {
        try (var fixture = new Fixture(); var backend = new ReliveBackend(fixture.endpoint(), ValueKind.ROW_ID)) {
            var type = Class.forName(ReliveBackend.class.getName() + "$Body");
            var constructor = type.getDeclaredConstructor(ReliveBackend.class, long.class);
            constructor.setAccessible(true);
            try (var body = (AutoCloseable) constructor.newInstance(backend, System.nanoTime() - 1)) {
                @SuppressWarnings("unchecked") var subscriber =
                        (Flow.Subscriber<List<java.nio.ByteBuffer>>) body;
                subscriber.onSubscribe(new Flow.Subscription() {
                    public void request(long count) {}
                    public void cancel() {}
                });
                subscriber.onNext(List.of(java.nio.ByteBuffer.wrap("{}".getBytes(StandardCharsets.UTF_8))));
                subscriber.onComplete();
                var read = type.getDeclaredMethod("all"); read.setAccessible(true);
                var failure = assertThrows(java.lang.reflect.InvocationTargetException.class, () -> read.invoke(body));
                assertTrue(failure.getCause() instanceof IOException);
                assertEquals("Relive HTTP body deadline exceeded", failure.getCause().getMessage());
            }
        }
    }

    @Test(timeout = 15000) public void privateScanMergePrecedesDirectionAndLimitWithoutWriting() throws Exception {
        try (var fixture = new Fixture(); var backend = new ReliveBackend(fixture.endpoint(), ValueKind.ROW_ID)) {
            fixture.stream = ignored -> frames(25, row("a", 10, "old"), row("c", 20, "same"), row("e", 0, "same"));
            var request = ScanRequest.newBuilder().setReadTs(25).setLimit(3)
                    .addLocalWrites(delete("a")).addLocalWrites(put("b", 0, "private"))
                    .addLocalWrites(put("c", 0, "replacement")).build();
            assertEquals(List.of(bytes("private"), bytes("replacement"), bytes("same")), IndexTestSupport.scan(backend, request));
            assertEquals(0, fixture.reads.get(0).getAsJsonArray("scans").get(0).getAsJsonObject().get("limit").getAsLong());
            fixture.stream = ignored -> frames(25, row("e", 0, "same"), row("c", 20, "same"), row("a", 10, "old"));
            assertEquals(List.of(bytes("same"), bytes("replacement")),
                    IndexTestSupport.scan(backend, request.toBuilder().setReverse(true).setLimit(2).build()));
            assertEquals(bytes("private"), backend.get(GetRequest.newBuilder().setKey(bytes("b")).setReadTs(25)
                    .addLocalWrites(put("b", 0, "private")).build()));
            assertTrue(fixture.writes.isEmpty());
            fixture.stream = ignored -> frames(25, row("c", 20, "same"), row("e", 0, "same"));
            assertEquals(List.of(bytes("same"), bytes("same")), IndexTestSupport.scan(backend, ScanRequest.newBuilder().setReadTs(25).build()));
        }
    }

    @Test(timeout = 15000) public void rejectsMalformedOrTruncatedStreamsEvenAfterThePrivateLimit() throws Exception {
        try (var fixture = new Fixture(); var backend = new ReliveBackend(fixture.endpoint(), ValueKind.ROW_ID)) {
            String good = frames(25, row("a", 10, "A"), row("b", 20, "B"));
            var invalid = List.of(
                    good.substring(0, good.lastIndexOf("{\"done\"")),
                    good.replace("\"counts\":[2]", "\"counts\":[1]"),
                    good + "{}\n", good.replace("\"ts\":25", "\"ts\":24"),
                    frames(25, row("b", 20, "B"), row("a", 10, "A")),
                    frames(25, row("a", 26, "future")), frames(25, row("a", 10, "A"), row("a", 10, "A")),
                    good.replace("\"value\":\"41\"", "\"value\":null"),
                    good.replace("\"key\":\"0061\"", "\"key\":\"0161\""),
                    good.replace("\"scan\":0", "\"scan\":1"),
                    good.substring(0, good.length() - 1));
            var request = ScanRequest.newBuilder().setReadTs(25).setLimit(1).addLocalWrites(put("", 0, "private")).build();
            for (String malformed : invalid) {
                fixture.stream = ignored -> malformed;
                assertThrows(IOException.class, () -> IndexTestSupport.scan(backend, request));
            }
            fixture.stream = ignored -> frames(25, row("b", 20, "B"));
            assertThrows(IOException.class, () -> IndexTestSupport.scan(backend,
                    ScanRequest.newBuilder().setReadTs(25).setEndKey(bytes("b")).build()));
        }
    }

    @Test(timeout = 15000) public void bindingSurvivesBridgeReplacementAndRejectsWrongTraceOrValueMode() throws Exception {
        try (var fixture = new Fixture()) {
            try (var backend = new ReliveBackend(fixture.endpoint(), ValueKind.ROW_ID)) { backend.bind("trace-one"); }
            try (var reopened = new ReliveBackend(fixture.endpoint(), ValueKind.ROW_ID)) {
                reopened.bind("trace-one");
                IndexTestSupport.awaitWrite(reopened.writeAsync(put("a", 20, "A")));
                assertThrows(IOException.class, () -> reopened.bind("trace-two"));
            }
            try (var other = new ReliveBackend(fixture.endpoint(), ValueKind.LOCATOR)) {
                assertThrows(IOException.class, () -> other.bind("trace-one"));
            }
            try (var unbound = new ReliveBackend(fixture.endpoint(), ValueKind.ROW_ID)) {
                assertThrows(IOException.class, () -> IndexTestSupport.awaitWrite(unbound.writeAsync(put("b", 30, "B"))));
            }
            assertEquals(1, fixture.writes.size());
        }
    }

    @Test(timeout = 20000) public void validatesWholeBatchBeforeSplittingAndNeverRetriesFailedWrites() throws Exception {
        try (var fixture = new Fixture(); var backend = new ReliveBackend(fixture.endpoint(), ValueKind.ROW_ID)) {
            var invalid = new ArrayList<Mutation>(Collections.nCopies(10001, put("a", 10, "A")));
            invalid.add(put("z", -1, "invalid"));
            // The service boundary validates the whole batch before the backend sees any member.
            assertEquals(IndexStatus.INVALID, IndexTestSupport.execute(backend,
                    WriteBatchRequest.newBuilder().addAllMutations(invalid).build()).status());
            assertTrue(fixture.writes.isEmpty());
            var writes = new ArrayList<Mutation>();
            for (int i = 0; i < 10001; i++) writes.add(put("key-" + i, i, "A"));
            IndexTestSupport.awaitWrite(backend.writeBatchAsync(writes));
            assertEquals(List.of(10000, 1), fixture.writes.stream().map(request -> request.getAsJsonArray("rows").size()).toList());
            fixture.writeStatus = 503;
            assertThrows(IOException.class, () -> IndexTestSupport.awaitWrite(backend.writeAsync(put("outcome-unknown", 10002, "A"))));
            assertEquals(3, fixture.writes.size());
            var oversized = Mutation.newBuilder().setPut(PutRequest.newBuilder().setValue(ByteString.copyFrom(new byte[65537]))).build();
            assertThrows(IllegalArgumentException.class, () -> backend.writeBatchAsync(List.of(put("x", 1, "X"), oversized)));
            assertEquals(3, fixture.writes.size());
        }
    }

    @Test(timeout = 20000) public void splitsAtEncodedByteLimitAndPreservesEveryMemberTimestamp() throws Exception {
        try (var fixture = new Fixture(); var backend = new ReliveBackend(fixture.endpoint(), ValueKind.ROW_ID)) {
            var writes = new ArrayList<Mutation>();
            for (int i = 0; i < 70; i++) writes.add(put("key-" + i, i, "v").toBuilder()
                    .setPut(put("key-" + i, i, "v").getPut().toBuilder().setValue(ByteString.copyFrom(new byte[65536]))).build());
            IndexTestSupport.awaitWrite(backend.writeBatchAsync(writes));
            assertEquals(2, fixture.writes.size());
            int next = 0;
            for (var request : fixture.writes) {
                assertTrue(request.toString().length() <= 8 << 20);
                for (var row : request.getAsJsonArray("rows")) assertEquals(next++, row.getAsJsonObject().get("ts").getAsLong());
            }
            assertEquals(70, next);
        }
    }

    @Test(timeout = 15000) public void grpcWrapperPreservesMetadataAndRejectsPartialResults() throws Exception {
        try (var fixture = new Fixture(); var backend = new ReliveBackend(fixture.endpoint(), ValueKind.ROW_ID);
             var server = IndexTestSupport.start(0, backend)) {
            var channel = IndexServerTest.channel(server.port());
            try {
                var api = IndexServerTest.stub(channel);
                fixture.stream = ignored -> frames(25, row("a", 20, ""));
                assertTrue(api.get(GetRequest.newBuilder().setKey(bytes("a")).setReadTs(25).build()).hasValue());
                fixture.stream = ignored -> "{\"ts\":25,\"scans\":1}\n";
                assertThrows(io.grpc.StatusRuntimeException.class, () -> api.scan(ScanRequest.newBuilder().setReadTs(25).build()).hasNext());
            } finally { channel.shutdownNow(); }
        }
    }
}
