package org.sibench.adapters;

import static org.junit.Assert.*;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.sibench.adapters.proto.*;

public class ServiceValueCheckTest {
    private static ByteString bytes(String text) { return ByteString.copyFromUtf8(text); }

    private static void timing(ServiceValueCheck check, String role, String operation, long calls, long failed) {
        var roles = (Map<?, ?>) check.observations().get("call_timing");
        var operations = (Map<?, ?>) roles.get(role);
        var sample = (Map<?, ?>) operations.get(operation);
        assertEquals(calls, sample.get("calls"));
        assertEquals(failed, sample.get("failed_calls"));
        double total = (double) sample.get("total_seconds"), max = (double) sample.get("max_seconds");
        assertTrue(total >= max && max >= 0);
        if (calls == 0) assertEquals(0.0, total, 0.0);
    }

    private static final class Answers implements IndexBackend {
        ByteString value;
        final List<ByteString> values;
        final List<Mutation> written = new ArrayList<>();
        boolean failWrite;
        int batches;
        Answers(ByteString value, List<ByteString> values) { this.value = value; this.values = values; }
        public IndexDescription describe() {
            return new IndexDescription("test", ValueKind.ROW_ID, Acknowledgement.ACK_MEMORY,
                    false, "test memory", "test");
        }
        public void write(Mutation mutation) throws IOException {
            if (failWrite) throw new IOException("Injected write failure");
            written.add(mutation);
        }
        public void writeBatch(List<Mutation> batch) throws IOException {
            batches++;
            IndexBackend.super.writeBatch(batch);
        }
        public ByteString get(GetRequest request) { return value; }
        public void scan(ScanRequest request, ValueSink sink) throws IOException {
            for (var value : values) sink.accept(value);
        }
        public void bind(String binding) {}
        public void flush() {}
        public void close() {}
    }

    @Test public void exactGetsDistinguishMissingEmptyAndWrongHistory() throws Exception {
        for (ByteString wrong : List.of(ByteString.EMPTY, bytes("old"))) {
            var actual = new Answers(wrong, List.of());
            var expected = new Answers(wrong.isEmpty() ? null : bytes("new"), List.of());
            try (var check = new ServiceValueCheck(actual, expected)) {
                var error = assertThrows(IOException.class, () -> check.get(GetRequest.getDefaultInstance()));
                assertTrue(error.getMessage().contains("differs from reference"));
                assertEquals(0L, check.observations().get("verified_gets"));
                assertThrows(IOException.class, () -> check.write(Mutation.getDefaultInstance()));
                assertTrue(actual.written.isEmpty());
            }
        }
    }

    @Test public void scanDigestRejectsEqualCountsWithWrongValuesOrderOrFraming() throws Exception {
        var pairs = List.of(
                List.of(List.of(bytes("a"), bytes("b")), List.of(bytes("b"), bytes("a"))),
                List.of(List.of(bytes("a"), bytes("a")), List.of(bytes("a"), bytes("b"))),
                List.of(List.of(bytes("ab"), bytes("c")), List.of(bytes("a"), bytes("bc"))),
                List.of(List.of(ByteString.EMPTY, bytes("a")), List.of(bytes("a"), ByteString.EMPTY)));
        for (var pair : pairs) {
            try (var check = new ServiceValueCheck(new Answers(null, pair.get(0)), new Answers(null, pair.get(1)))) {
                var partial = new ArrayList<ByteString>();
                assertThrows(IOException.class, () -> check.scan(ScanRequest.getDefaultInstance(), partial::add));
                assertEquals(pair.get(0), partial);
                assertEquals(0L, check.observations().get("verified_scans"));
            }
        }
    }

    @Test public void matchingValuesAndOriginalMutationsArePreserved() throws Exception {
        var values = List.of(ByteString.EMPTY, bytes("a"), bytes("a"));
        var actual = new Answers(null, values);
        var reference = new Answers(null, values);
        var mutations = List.of(
                Mutation.newBuilder().setPut(PutRequest.newBuilder().setKey(bytes("k")).setTs(20).setValue(bytes("B"))).build(),
                Mutation.newBuilder().setPut(PutRequest.newBuilder().setKey(bytes("k")).setTs(10).setValue(bytes("A"))).build(),
                Mutation.newBuilder().setDelete(DeleteRequest.newBuilder().setKey(bytes("k")).setTs(30)).build());
        try (var check = new ServiceValueCheck(actual, reference)) {
            check.writeBatch(mutations);
            assertEquals(mutations, actual.written);
            assertEquals(mutations, reference.written);
            assertNull(check.get(GetRequest.getDefaultInstance()));
            actual.value = reference.value = ByteString.EMPTY;
            assertEquals(ByteString.EMPTY, check.get(GetRequest.getDefaultInstance()));
            var returned = new ArrayList<ByteString>();
            check.scan(ScanRequest.getDefaultInstance(), returned::add);
            assertEquals(values, returned);
            assertEquals(3L, check.observations().get("successful_mutations"));
            assertEquals(2L, check.observations().get("verified_gets"));
            assertEquals(1L, check.observations().get("verified_scans"));
            for (String role : List.of("actual", "reference")) {
                timing(check, role, "write", 1, 0);
                timing(check, role, "get", 2, 0);
                timing(check, role, "scan", 1, 0);
            }
        }
    }

    @Test public void individualMutationsAreNotConvertedToBatches() throws Exception {
        var actual = new Answers(null, List.of());
        var reference = new Answers(null, List.of());
        var put = Mutation.newBuilder().setPut(PutRequest.newBuilder().setKey(bytes("k")).setTs(20).setValue(bytes("v"))).build();
        var delete = Mutation.newBuilder().setDelete(DeleteRequest.newBuilder().setKey(bytes("k")).setTs(30)).build();
        try (var check = new ServiceValueCheck(actual, reference)) {
            check.write(put);
            check.write(delete);
            assertEquals(List.of(put, delete), actual.written);
            assertEquals(actual.written, reference.written);
            assertEquals(0, actual.batches);
            assertEquals(0, reference.batches);
            check.writeBatch(List.of(put, delete));
            assertEquals(1, actual.batches);
            assertEquals(1, reference.batches);
        }
    }

    @Test public void partialWriteFailureDoesNotRetryOrKeepServing() throws Exception {
        var actual = new Answers(null, List.of());
        var reference = new Answers(null, List.of());
        reference.failWrite = true;
        try (var check = new ServiceValueCheck(actual, reference)) {
            assertThrows(IOException.class, () -> check.write(Mutation.getDefaultInstance()));
            assertEquals(1, actual.written.size());
            assertEquals(0L, check.observations().get("successful_mutations"));
            assertThrows(IOException.class, () -> check.get(GetRequest.getDefaultInstance()));
            assertEquals(1, actual.written.size());
            timing(check, "actual", "write", 1, 0);
            timing(check, "reference", "write", 1, 1);
            timing(check, "actual", "get", 0, 0);
            timing(check, "reference", "get", 0, 0);
        }
    }

    @Test public void failedActualCallDoesNotRecordUnattemptedReferenceWork() throws Exception {
        var actual = new Answers(null, List.of());
        actual.failWrite = true;
        try (var check = new ServiceValueCheck(actual, new Answers(null, List.of()))) {
            assertThrows(IOException.class, () -> check.write(Mutation.getDefaultInstance()));
            timing(check, "actual", "write", 1, 1);
            timing(check, "reference", "write", 0, 0);
            assertEquals(0L, check.observations().get("successful_mutations"));
        }
    }

    @Test public void concurrentCallsRetainEveryTimingSample() throws Exception {
        var pool = java.util.concurrent.Executors.newFixedThreadPool(4);
        try (var check = new ServiceValueCheck(new Answers(bytes("value"), List.of()),
                new Answers(bytes("value"), List.of()))) {
            var calls = new ArrayList<java.util.concurrent.Callable<Void>>();
            for (int i = 0; i < 64; i++) calls.add(() -> {
                assertEquals(bytes("value"), check.get(GetRequest.getDefaultInstance()));
                return null;
            });
            for (var result : pool.invokeAll(calls)) result.get();
            assertEquals(64L, check.observations().get("verified_gets"));
            timing(check, "actual", "get", 64, 0);
            timing(check, "reference", "get", 64, 0);
        } finally { pool.shutdownNow(); }
    }

    @Test public void streamedMismatchCannotEndAsSuccessfulRpc() throws Exception {
        var actual = new Answers(null, java.util.Collections.nCopies(17000, bytes("actual")));
        var reference = new Answers(null, java.util.Collections.nCopies(17000, bytes("wrong")));
        try (var check = new ServiceValueCheck(actual, reference)) {
            var server = IndexServer.start(0, check);
            var channel = IndexServerTest.channel(server.port());
            try {
                var results = IndexServiceGrpc.newBlockingStub(channel).scan(ScanRequest.getDefaultInstance());
                int[] received = {0};
                assertThrows(io.grpc.StatusRuntimeException.class, () -> {
                    while (results.hasNext()) received[0] += results.next().getValuesCount();
                });
                assertTrue(received[0] > 0);
            } finally {
                channel.shutdownNow();
                server.close();
            }
        }
    }

}
