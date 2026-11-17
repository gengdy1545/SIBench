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
        volatile String binding;
        volatile int writeStatus = 200;
        Fixture() throws IOException {
            server.setExecutor(executor);
            server.createContext("/", exchange -> {
                try (exchange) {
                    String path = exchange.getRequestURI().getPath();
                    String response; int status = 200;
                    if (path.equals("/describe")) response = "{\"name\":\"relive\",\"versionedKeys\":true,\"streamingReads\":true,"
                            + "\"transactionLocalReads\":false,\"searchGroups\":false,\"maxWriteRows\":10000,"
                            + "\"durability\":\"shared_nfs_fsync\",\"levelOneStorage\":\"object_storage\",\"location\":\"/fixture/cfs cos://fixture/l1\"}";
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
                                response = "{\"rows\":" + request.getAsJsonArray("rows").size() + "}";
                            }
                            case "/stream" -> { reads.add(request); response = stream.apply(request); }
                            default -> { status = 404; response = "{}"; }
                        }
                    }
                    byte[] encoded = response.getBytes(StandardCharsets.UTF_8);
                    exchange.sendResponseHeaders(status, encoded.length);
                    exchange.getResponseBody().write(encoded);
                }
            });
            server.start();
        }
        String endpoint() { return "http://127.0.0.1:" + server.getAddress().getPort(); }
        @Override public void close() { server.stop(0); executor.shutdownNow(); }
    }

    @Test(timeout = 15000) public void preservesOpaqueKeysVersionsDeletesAndEmptyValues() throws Exception {
        try (var fixture = new Fixture(); var backend = new ReliveBackend(fixture.endpoint(), ValueKind.ROW_ID)) {
            backend.writeBatch(List.of(put("", 20, ""), put("a", 10, "old"), delete("a")));
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
                reopened.bind("trace-one"); reopened.write(put("a", 20, "A"));
                assertThrows(IOException.class, () -> reopened.bind("trace-two"));
            }
            try (var other = new ReliveBackend(fixture.endpoint(), ValueKind.LOCATOR)) {
                assertThrows(IOException.class, () -> other.bind("trace-one"));
            }
            try (var unbound = new ReliveBackend(fixture.endpoint(), ValueKind.ROW_ID)) {
                assertThrows(IOException.class, () -> unbound.write(put("b", 30, "B")));
            }
            assertEquals(1, fixture.writes.size());
        }
    }

    @Test(timeout = 20000) public void validatesWholeBatchBeforeSplittingAndNeverRetriesFailedWrites() throws Exception {
        try (var fixture = new Fixture(); var backend = new ReliveBackend(fixture.endpoint(), ValueKind.ROW_ID)) {
            var invalid = new ArrayList<Mutation>(Collections.nCopies(10001, put("a", 10, "A")));
            invalid.add(put("z", -1, "invalid"));
            assertThrows(IllegalArgumentException.class, () -> backend.writeBatch(invalid));
            assertTrue(fixture.writes.isEmpty());
            var writes = new ArrayList<Mutation>();
            for (int i = 0; i < 10001; i++) writes.add(put("key-" + i, i, "A"));
            backend.writeBatch(writes);
            assertEquals(List.of(10000, 1), fixture.writes.stream().map(request -> request.getAsJsonArray("rows").size()).toList());
            fixture.writeStatus = 503;
            assertThrows(IOException.class, () -> backend.write(put("outcome-unknown", 10002, "A")));
            assertEquals(3, fixture.writes.size());
            var oversized = Mutation.newBuilder().setPut(PutRequest.newBuilder().setValue(ByteString.copyFrom(new byte[65537]))).build();
            assertThrows(IllegalArgumentException.class, () -> backend.writeBatch(List.of(put("x", 1, "X"), oversized)));
            assertEquals(3, fixture.writes.size());
        }
    }

    @Test(timeout = 20000) public void splitsAtEncodedByteLimitAndPreservesEveryMemberTimestamp() throws Exception {
        try (var fixture = new Fixture(); var backend = new ReliveBackend(fixture.endpoint(), ValueKind.ROW_ID)) {
            var writes = new ArrayList<Mutation>();
            for (int i = 0; i < 70; i++) writes.add(put("key-" + i, i, "v").toBuilder()
                    .setPut(put("key-" + i, i, "v").getPut().toBuilder().setValue(ByteString.copyFrom(new byte[65536]))).build());
            backend.writeBatch(writes);
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
             var server = IndexServer.start(0, backend)) {
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
