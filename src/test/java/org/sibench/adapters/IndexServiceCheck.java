package org.sibench.adapters;

import static org.junit.Assert.*;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.sibench.adapters.proto.*;

/** Hand-derived protocol cases shared by service acceptance and backend deployment checks. */
public final class IndexServiceCheck {
    static ByteString bytes(String value) { return ByteString.copyFromUtf8(value); }
    static PutRequest put(ByteString key, long ts, ByteString value) {
        return PutRequest.newBuilder().setKey(key).setTs(ts).setValue(value).build();
    }
    static Mutation put(String key, long ts, String value) {
        return Mutation.newBuilder().setPut(put(bytes(key), ts, bytes(value))).build();
    }
    static Mutation delete(String key, long ts) {
        return Mutation.newBuilder().setDelete(DeleteRequest.newBuilder().setKey(bytes(key)).setTs(ts)).build();
    }
    static GetRequest get(String key, long ts) {
        return GetRequest.newBuilder().setKey(bytes(key)).setReadTs(ts).build();
    }
    static List<ByteString> scan(IndexServiceGrpc.IndexServiceBlockingStub api, ScanRequest request) {
        var values = new ArrayList<ByteString>();
        api.scan(request).forEachRemaining(chunk -> values.addAll(chunk.getValuesList()));
        return values;
    }
    static IndexServiceGrpc.IndexServiceBlockingStub bound(ManagedChannel channel, String binding, ValueKind kind) {
        var headers = new Metadata();
        headers.put(GrpcService.INDEX_BINDING, binding);
        headers.put(GrpcService.VALUE_KIND, kind.name());
        return IndexServerTest.stub(channel).withDeadlineAfter(300, TimeUnit.SECONDS)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
    }
    static void value(IndexServiceGrpc.IndexServiceBlockingStub api, String key, long ts, String expected) {
        var actual = api.get(get(key, ts));
        assertTrue("Missing key at snapshot " + ts, actual.hasValue());
        assertEquals(bytes(expected), actual.getValue());
    }
    static void history(IndexServiceGrpc.IndexServiceBlockingStub api) {
        value(api, "a", 0, "base");
        value(api, "a", 15, "A");
        value(api, "a", 25, "B");
        assertFalse("Tombstone must hide older values", api.get(get("a", 35)).hasValue());
        value(api, "a", 45, "C");
        value(api, "x-batch", 4, "earlier");
        value(api, "x-batch", 8, "later");
        assertFalse(api.get(get("x-batch", 9)).hasValue());
        value(api, "x-multi", 55, "fifty");
        value(api, "x-multi", 65, "sixty");
    }

    static void batchAndConcurrentWrites(IndexServiceGrpc.IndexServiceBlockingStub writer,
            IndexServiceGrpc.IndexServiceBlockingStub other) throws Exception {
        writer.writeBatch(WriteBatchRequest.newBuilder()
                .addMutations(put("x-batch", 8, "later"))
                .addMutations(put("x-batch", 3, "earlier"))
                .addMutations(delete("x-batch", 9)).build());
        var clients = Executors.newFixedThreadPool(2);
        var ready = new java.util.concurrent.CountDownLatch(2);
        try {
            var one = clients.submit(() -> {
                ready.countDown(); assertTrue(ready.await(5, TimeUnit.SECONDS));
                return writer.put(put("x-multi", 60, "sixty").getPut());
            });
            var two = clients.submit(() -> {
                ready.countDown(); assertTrue(ready.await(5, TimeUnit.SECONDS));
                return other.put(put("x-multi", 50, "fifty").getPut());
            });
            one.get(15, TimeUnit.SECONDS);
            two.get(15, TimeUnit.SECONDS);
        } finally { clients.shutdownNow(); }
    }

    static void reads(IndexServiceGrpc.IndexServiceBlockingStub writer,
            IndexServiceGrpc.IndexServiceBlockingStub reader, IndexServiceGrpc.IndexServiceBlockingStub other) {
        // Equal result counts cannot certify the historical value oracle.
        assertThrows(AssertionError.class, () -> value(reader, "a", 15, "B"));
        writer.put(put(ByteString.EMPTY, 0, bytes("empty-key")));
        ByteString binary = ByteString.copyFrom(new byte[]{0, (byte) 255});
        ByteString high = ByteString.copyFrom(new byte[]{(byte) 255});
        writer.put(put(binary, 0, bytes("binary")));
        writer.put(put(high, 0, bytes("high")));
        for (var write : List.of(put("aa", 0, "duplicate"), put("b", 0, "duplicate"), put("empty", 0, "")))
            writer.put(write.getPut());
        assertEquals(bytes("empty-key"), reader.get(GetRequest.newBuilder().setReadTs(45).build()).getValue());
        assertEquals(bytes("binary"), reader.get(GetRequest.newBuilder().setKey(binary).setReadTs(45).build()).getValue());
        assertTrue(reader.get(get("empty", 45)).hasValue());
        assertEquals(ByteString.EMPTY, reader.get(get("empty", 45)).getValue());
        assertFalse(reader.get(get("missing", 45)).hasValue());
        assertEquals(List.of(bytes("empty-key"), bytes("binary")), scan(reader,
                ScanRequest.newBuilder().setEndKey(bytes("a")).setReadTs(45).build()));
        assertEquals(List.of(bytes("high")), scan(reader,
                ScanRequest.newBuilder().setStartKey(high).setReadTs(45).build()));

        var range = ScanRequest.newBuilder().setStartKey(bytes("a")).setEndKey(bytes("c")).setReadTs(45).build();
        assertEquals(List.of(bytes("C"), bytes("duplicate"), bytes("duplicate")), scan(reader, range));
        assertEquals(List.of(bytes("duplicate"), bytes("duplicate")), scan(reader, range.toBuilder().setReverse(true).setLimit(2).build()));
        var privateRange = range.toBuilder().addAllLocalWrites(List.of(delete("a", 0), put("aa", 0, "private"),
                put("ab", 0, ""), delete("b", 0), put("bb", 0, "duplicate"))).build();
        assertEquals(List.of(bytes("private"), ByteString.EMPTY, bytes("duplicate")), scan(reader, privateRange));
        assertEquals(List.of(bytes("private"), ByteString.EMPTY), scan(reader, privateRange.toBuilder().setLimit(2).build()));
        assertEquals(List.of(bytes("duplicate"), ByteString.EMPTY), scan(reader, privateRange.toBuilder().setReverse(true).setLimit(2).build()));
        var privateGet = get("a", 45).toBuilder().addLocalWrites(put("a", 0, "")).build();
        assertTrue(reader.get(privateGet).hasValue());
        assertEquals(ByteString.EMPTY, reader.get(privateGet).getValue());
        assertFalse(reader.get(privateGet.toBuilder().clearLocalWrites().addLocalWrites(delete("a", 0)).build()).hasValue());
        value(other, "a", 45, "C");
        assertFalse(other.get(get("ab", 45)).hasValue());
        assertEquals(List.of(bytes("C"), bytes("duplicate"), bytes("duplicate")), scan(other, range));
    }

    static void check(IndexServiceGrpc.IndexServiceBlockingStub writer,
            IndexServiceGrpc.IndexServiceBlockingStub reader) throws Exception {
        writer.put(put("a", 0, "base").getPut());
        writer.put(put("a", 20, "B").getPut());
        value(reader, "a", 25, "B");
        writer.put(put("a", 10, "A").getPut());
        writer.delete(delete("a", 30).getDelete());
        writer.put(put("a", 40, "C").getPut());
        batchAndConcurrentWrites(writer, reader);
        history(writer);
        history(reader);
        reads(writer, reader, writer);
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) throw new IllegalArgumentException("CONFIG FRESH_REPORT");
        var runtime = org.sibench.config.SibenchConfig.read(Path.of(args[0]));
        var config = ServiceTarget.of(runtime);
        var replay = org.sibench.replay.ReplayConfig.from(runtime);
        Path report = Path.of(args[1]);
        if (Files.exists(report)) throw new java.io.IOException("Report already exists: " + report);
        String binding = "index-service-check";
        var evidence = new java.util.LinkedHashMap<String, Object>();
        evidence.put("configuration", runtime);
        evidence.put("endpoint", config.endpoint());
        evidence.put("scope", "hand-derived version, deletion, private-context and ordered-value cases through one service instance; not cross-instance visibility, durability or performance");
        Throwable failure = null;
        try (var actual = new GrpcIndexCheck(config, binding, replay.requestTimeoutSeconds());
             var server = IndexServer.start(0, actual)) {
            evidence.put("service", actual.describe());
            var writerChannel = IndexServerTest.channel(server.port());
            var readerChannel = IndexServerTest.channel(server.port());
            try {
                var writer = bound(writerChannel, binding, config.value());
                var reader = bound(readerChannel, binding, config.value());
                check(writer, reader);
            } finally { writerChannel.shutdownNow(); readerChannel.shutdownNow(); }
        } catch (Exception | Error error) {
            failure = error;
            throw error;
        } finally {
            evidence.put("status", failure == null ? "passed" : "failed");
            if (failure != null) evidence.put("first_error", failure.toString());
            try {
                Files.writeString(report, new com.google.gson.GsonBuilder().setPrettyPrinting().create().toJson(evidence) + "\n",
                        StandardOpenOption.CREATE_NEW);
            } catch (java.io.IOException error) {
                if (failure != null) failure.addSuppressed(error); else throw error;
            }
        }
    }
}
