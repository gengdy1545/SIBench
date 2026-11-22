package org.sibench.adapters;

import static org.junit.Assert.*;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.adapters.proto.*;

public class PrivateReadTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();
    private static ByteString bytes(String text) { return ByteString.copyFromUtf8(text); }
    private static Mutation put(String key, String value, long timestamp) {
        return Mutation.newBuilder().setPut(PutRequest.newBuilder().setKey(bytes(key)).setValue(bytes(value)).setTs(timestamp)).build();
    }
    private static Mutation delete(String key) {
        return Mutation.newBuilder().setDelete(DeleteRequest.newBuilder().setKey(bytes(key))).build();
    }

    @Test public void privateScansMergeBeforeOrderingAndLimitWithoutChangingStoredSnapshots() throws Exception {
        try (var backend = VersionedIndexBackend.create(temporary.getRoot().toPath().resolve("index"), ValueKind.ROW_ID)) {
            for (var write : List.of(put("k", "base-k", 0), put("ka", "base-ka", 0), put("z", "base-z", 0), put("ka", "future-ka", 2)))
                backend.writeAsync(write);
            var scan = ScanRequest.newBuilder().setStartKey(bytes("")).setEndKey(bytes("zzz")).setReadTs(0)
                    .addAllLocalWrites(List.of(put("j", "", 0), delete("k"), put("ka", "private-ka", 0), put("zz", "private-zz", 0))).build();
            assertEquals(List.of(bytes(""), bytes("private-ka"), bytes("base-z"), bytes("private-zz")), IndexTestSupport.scan(backend, scan));
            assertEquals(List.of(bytes(""), bytes("private-ka")), IndexTestSupport.scan(backend, scan.toBuilder().setLimit(2).build()));
            assertEquals(List.of(bytes("private-zz"), bytes("base-z")), IndexTestSupport.scan(backend, scan.toBuilder().setReverse(true).setLimit(2).build()));
            assertEquals(List.of(bytes("base-k"), bytes("base-ka"), bytes("base-z")), IndexTestSupport.scan(backend, scan.toBuilder().clearLocalWrites().build()));
            assertEquals(bytes("future-ka"), backend.get(GetRequest.newBuilder().setKey(bytes("ka")).setReadTs(2).build()));
            var empty = GetRequest.newBuilder().setKey(bytes("k")).addLocalWrites(put("k", "", 0)).build();
            assertEquals(ByteString.EMPTY, backend.get(empty));
            assertNull(backend.get(empty.toBuilder().clearLocalWrites().addLocalWrites(delete("k")).build()));
            assertEquals(bytes("base-k"), backend.get(empty.toBuilder().clearLocalWrites().build()));
        }
    }

    @Test public void invalidPrivateContextsAreRejectedWithoutSideEffects() throws Exception {
        try (var backend = VersionedIndexBackend.create(temporary.getRoot().toPath().resolve("index"), ValueKind.ROW_ID)) {
            backend.writeAsync(put("k", "base", 0));
            for (var writes : List.of(List.of(put("k", "wrong-ts", 1)), List.of(put("j", "outside", 0)),
                    List.of(put("k", "one", 0), delete("k")), List.of(Mutation.getDefaultInstance()))) {
                var read = GetRequest.newBuilder().setKey(bytes("k")).addAllLocalWrites(writes).build();
                assertEquals(IndexStatus.INVALID, IndexTestSupport.execute(backend, read).status());
            }
            var reversed = ScanRequest.newBuilder().setStartKey(bytes("k")).setEndKey(bytes("z"))
                    .addAllLocalWrites(List.of(put("m", "first", 0), put("l", "second", 0))).build();
            assertEquals(IndexStatus.INVALID, IndexTestSupport.execute(backend, reversed).status());
            assertEquals(bytes("base"), backend.get(GetRequest.newBuilder().setKey(bytes("k")).build()));
        }
    }

    @Test(timeout = 15000) public void concurrentGrpcRequestsHaveIndependentPrivateContexts() throws Exception {
        try (var backend = VersionedIndexBackend.create(temporary.getRoot().toPath().resolve("index"), ValueKind.ROW_ID);
                var server = IndexTestSupport.start(0, backend)) {
            var executor = java.util.concurrent.Executors.newFixedThreadPool(2);
            var start = new java.util.concurrent.CountDownLatch(1);
            var futures = new ArrayList<java.util.concurrent.Future<?>>();
            try {
                for (String value : List.of("left", "right")) futures.add(executor.submit(() -> {
                    var channel = IndexServerTest.channel(server.port());
                    try {
                        start.await();
                        var api = IndexServerTest.stub(channel);
                        var request = GetRequest.newBuilder().setKey(bytes("k")).addLocalWrites(put("k", value, 0)).build();
                        for (int i = 0; i < 10; i++) {
                            assertEquals(bytes(value), api.get(request).getValue());
                            assertFalse(api.get(request.toBuilder().clearLocalWrites().build()).hasValue());
                        }
                        var scan = ScanRequest.newBuilder().setStartKey(bytes("a")).setEndKey(bytes("z"))
                                .addLocalWrites(put("k", value, 0)).setLimit(1).build();
                        var values = new ArrayList<ByteString>();
                        api.scan(scan).forEachRemaining(reply -> values.addAll(reply.getValuesList()));
                        assertEquals(List.of(bytes(value)), values);
                    } catch (Exception error) { throw new RuntimeException(error); }
                    finally { channel.shutdownNow(); }
                }));
                start.countDown();
                for (var future : futures) future.get(10, TimeUnit.SECONDS);
                assertNull(backend.get(GetRequest.newBuilder().setKey(bytes("k")).build()));
            } finally { executor.shutdownNow(); }
        }
    }
}
