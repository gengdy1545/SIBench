package org.sibench.adapters;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.adapters.proto.*;
import org.sibench.trace.IndexEntry;
import org.sibench.trace.IndexKey;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * Explicit check of the embedded Relive backend on a mounted CFS directory and a DynamoDB-compatible
 * endpoint: -Dsibench.relive.cfs=DIR -Dsibench.relive.dynamodb=URL, with AWS signing variables set.
 */
public class ReliveBackendCheck {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    private Path clientConfig() throws IOException {
        Path cfs = Files.createDirectories(Path.of(required("sibench.relive.cfs")).resolve(UUID.randomUUID().toString()));
        Path config = temporary.getRoot().toPath().resolve("relive-client.yaml");
        Files.writeString(config, """
                cfs_root: %s
                index: check
                shards: [default]
                metastore:
                  backend: dynamodb
                  endpoint: %s
                  region: us-east-1
                  table: relive_sibench_check
                  namespace: %s
                  access_key_env: AWS_ACCESS_KEY_ID
                  secret_key_env: AWS_SECRET_ACCESS_KEY
                """.formatted(cfs, required("sibench.relive.dynamodb"), UUID.randomUUID()));
        return config;
    }

    private static String required(String name) {
        String value = System.getProperty(name);
        if (value == null || value.isBlank()) throw new IllegalArgumentException("Required property: " + name);
        return value;
    }

    @Test(timeout = 120_000)
    public void historyBindingAndPrivateWritesAcrossIndependentClients() throws Exception {
        Path config = clientConfig();
        try (var backend = new ReliveBackend(config, 4, ValueKind.ROW_ID)) {
            // Every RPC binds; the first calls arrive concurrently.
            var pool = java.util.concurrent.Executors.newFixedThreadPool(16);
            try {
                var calls = new java.util.ArrayList<java.util.concurrent.Future<?>>();
                for (int i = 0; i < 64; i++) calls.add(pool.submit(() -> { backend.bind("trace-a"); return null; }));
                for (var call : calls) call.get(30, TimeUnit.SECONDS);
            } finally { pool.shutdownNow(); }
            IndexVersionCheck.check(backend);

            var key = ByteString.copyFrom(IndexKey.ofInt(9).bytes());
            var put = Mutation.newBuilder().setPut(PutRequest.newBuilder().setKey(key).setTs(10)
                    .setValue(IndexEntry.rowId(90))).build();
            backend.writeAsync(put).join();
            var local = Mutation.newBuilder().setDelete(DeleteRequest.newBuilder().setKey(key)).build();
            assertNull(backend.get(GetRequest.newBuilder().setKey(key).setReadTs(10).addLocalWrites(local).build()));
            assertEquals(IndexEntry.rowId(90), backend.get(GetRequest.newBuilder().setKey(key).setReadTs(10).build()));
            assertNull(backend.get(GetRequest.newBuilder().setKey(key).setReadTs(9).build()));
            var empty = ByteString.EMPTY;
            backend.writeAsync(Mutation.newBuilder().setPut(PutRequest.newBuilder().setKey(empty).setTs(1)
                    .setValue(ByteString.EMPTY)).build()).join();
            assertEquals(ByteString.EMPTY, backend.get(GetRequest.newBuilder().setKey(empty).setReadTs(1).build()));
            assertEquals(List.of(IndexEntry.rowId(90)), IndexTestSupport.scan(backend, ScanRequest.newBuilder()
                    .setStartKey(key).setReadTs(20).setLimit(1).build()));
        }
        try (var reopened = new ReliveBackend(config, 1, ValueKind.ROW_ID);
             var foreign = new ReliveBackend(config, 1, ValueKind.ROW_ID)) {
            reopened.bind("trace-a");
            assertThrows(IOException.class, () -> foreign.bind("trace-b"));
        }
    }

    @Test(timeout = 180_000)
    public void grpcPreservesValuesAcrossIndependentBackendsAndReopening() throws Exception {
        Path config = clientConfig();
        try (var writerBackend = new ReliveBackend(config, 2, ValueKind.ROW_ID);
             var readerBackend = new ReliveBackend(config, 2, ValueKind.ROW_ID);
             var writerServer = IndexTestSupport.start(0, writerBackend);
             var readerServer = IndexTestSupport.start(0, readerBackend)) {
            var writerChannel = IndexServerTest.channel(writerServer.port());
            var readerChannel = IndexServerTest.channel(readerServer.port());
            try {
                var writer = IndexServiceCheck.bound(writerChannel, "trace-grpc", ValueKind.ROW_ID)
                        .withDeadlineAfter(30, TimeUnit.SECONDS);
                var reader = IndexServiceCheck.bound(readerChannel, "trace-grpc", ValueKind.ROW_ID)
                        .withDeadlineAfter(30, TimeUnit.SECONDS);
                IndexServiceCheck.check(writer, reader);
                for (var rejected : List.of(
                        IndexServiceCheck.bound(writerChannel, "foreign", ValueKind.ROW_ID),
                        IndexServiceCheck.bound(writerChannel, "trace-grpc", ValueKind.LOCATOR))) {
                    var error = assertThrows(StatusRuntimeException.class, () -> rejected
                            .withDeadlineAfter(30, TimeUnit.SECONDS)
                            .put(IndexServiceCheck.put("rejected", 1, "must-not-exist").getPut()));
                    assertEquals(Status.Code.FAILED_PRECONDITION, error.getStatus().getCode());
                }
                assertFalse(reader.get(IndexServiceCheck.get("rejected", 45)).hasValue());
            } finally {
                writerChannel.shutdownNow();
                readerChannel.shutdownNow();
            }
        }
        try (var backend = new ReliveBackend(config, 1, ValueKind.ROW_ID);
             var server = IndexTestSupport.start(0, backend)) {
            var channel = IndexServerTest.channel(server.port());
            try {
                var reader = IndexServiceCheck.bound(channel, "trace-grpc", ValueKind.ROW_ID)
                        .withDeadlineAfter(30, TimeUnit.SECONDS);
                IndexServiceCheck.history(reader);
                assertTrue(reader.get(IndexServiceCheck.get("empty", 45)).hasValue());
                assertEquals(ByteString.EMPTY, reader.get(IndexServiceCheck.get("empty", 45)).getValue());
                assertFalse(reader.get(IndexServiceCheck.get("ab", 45)).hasValue());
                var error = assertThrows(StatusRuntimeException.class, () -> IndexServiceCheck
                        .bound(channel, "foreign", ValueKind.ROW_ID).withDeadlineAfter(30, TimeUnit.SECONDS)
                        .get(IndexServiceCheck.get("a", 45)));
                assertEquals(Status.Code.FAILED_PRECONDITION, error.getStatus().getCode());
            } finally { channel.shutdownNow(); }
        }
    }
}
