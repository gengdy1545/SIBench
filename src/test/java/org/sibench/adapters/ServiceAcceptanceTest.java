package org.sibench.adapters;

import static org.junit.Assert.*;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.adapters.proto.*;

/** Exercises the public acceptance commands against configured gRPC services. */
public class ServiceAcceptanceTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    private Path config(Path root, int port, Path trace, ValueKind kind) throws Exception {
        Path config = root.resolve("runtime.yaml");
        Files.writeString(config, new com.google.gson.Gson().toJson(Map.of(
                "schema_version", 1, "purpose", "sibench",
                "target", Map.of("secondary_indexes", Map.of("enabled", false), "service", Map.of(
                        "endpoint", "127.0.0.1:" + port,
                        "value", kind == ValueKind.ROW_ID ? "row_id" : "locator")),
                "replay", Map.of("dispatch_mode", "operation", "client_count", 1, "max_in_flight", 4,
                        "request_timeout_seconds", 5, "baseline_batch_operations", 2),
                "benchmarks", Map.of("case", Map.of("config", "unused.yaml", "engine", "postgres",
                        "trace", Map.of("generate", false, "directory", trace.toString(), "upload", Map.of("enabled", false)))))));
        return config;
    }

    private static JsonObject read(Path report) throws IOException {
        return JsonParser.parseString(Files.readString(report)).getAsJsonObject();
    }

    @Test(timeout = 30000) public void commonOracleUsesConfiguredEndpointForBothValueModes() throws Exception {
        for (var kind : ValueKind.values()) {
            Path root = temporary.newFolder().toPath();
            try (var backend = VersionedIndexBackend.create(root.resolve("index"), kind);
                 var server = IndexTestSupport.start(0, backend)) {
                Path config = config(root, server.port(), root.resolve("unused-trace"), kind);
                Path report = root.resolve("protocol.json");
                IndexServiceCheck.main(new String[]{config.toString(), report.toString()});
                var result = read(report);
                assertEquals("passed", result.get("status").getAsString());
                assertEquals("127.0.0.1:" + server.port(), result.get("endpoint").getAsString());
                assertEquals(backend.describe().name(), result.getAsJsonObject("service").get("name").getAsString());
                assertThrows(IOException.class, () -> IndexServiceCheck.main(new String[]{config.toString(), report.toString()}));
            }
        }
    }

    @Test(timeout = 30000) public void fullValueCommandReusesReplayAndRejectsWrongValuesWithCorrectCounts() throws Exception {
        for (boolean wrong : new boolean[]{false, true}) {
            Path root = temporary.newFolder().toPath();
            Path trace = ServiceReplayTest.trace(Files.createDirectory(root.resolve("trace")));
            try (var store = VersionedIndexBackend.create(root.resolve("index"), ValueKind.ROW_ID)) {
                var backend = new IndexBackend() {
                    public IndexDescription describe() { return store.describe(); }
                    public java.util.concurrent.CompletableFuture<Void> writeAsync(Mutation mutation) throws IOException { return store.writeAsync(mutation); }
                    public java.util.concurrent.CompletableFuture<Void> writeBatchAsync(java.util.List<Mutation> writes) throws IOException { return store.writeBatchAsync(writes); }
                    public ByteString get(GetRequest get) throws IOException {
                        return wrong ? org.sibench.trace.IndexEntry.rowId(999) : store.get(get);
                    }
                    public void scan(ScanRequest scan, ValueSink sink) throws IOException { store.scan(scan, sink); }
                    public void bind(String binding) throws IOException { store.bind(binding); }
                    public void flush() throws IOException { store.flush(); }
                    public void close() {}
                };
                try (var server = IndexTestSupport.start(0, backend)) {
                    Path config = config(root, server.port(), trace, ValueKind.ROW_ID);
                    Path report = root.resolve("replay.json");
                    String[] args = {config.toString(), "case", root.resolve("reference").toString(), report.toString()};
                    if (wrong) assertThrows(IOException.class, () -> ServiceValueCheck.main(args));
                    else ServiceValueCheck.main(args);
                    var result = read(root.resolve("replay.json.values.json"));
                    assertEquals(wrong ? "failed" : "passed", result.get("status").getAsString());
                    assertEquals(wrong ? 0 : 1, result.getAsJsonObject("values").get("verified_gets").getAsInt());
                    assertEquals(2, result.getAsJsonObject("values").get("successful_mutations").getAsInt());
                    assertEquals(!wrong, Files.exists(report));
                    assertTrue(Files.exists(root.resolve("replay.json.baseline.json")));
                    if (wrong) assertTrue(result.toString().contains("Get value differs from reference"));
                    byte[] retained = Files.readAllBytes(root.resolve("replay.json.values.json"));
                    assertThrows(IOException.class, () -> ServiceValueCheck.main(args));
                    assertArrayEquals(retained, Files.readAllBytes(root.resolve("replay.json.values.json")));
                }
            }
        }
    }

    @Test(timeout = 15000) public void acceptanceTransportRejectsMissingMetadataWithoutWriting() throws Exception {
        var raw = io.grpc.ServerBuilder.forPort(0).addService(new IndexServiceGrpc.IndexServiceImplBase() {
            @Override public void scan(ScanRequest request, io.grpc.stub.StreamObserver<ScanResponse> out) { out.onCompleted(); }
        }).build().start();
        try {
            var config = new ServiceTarget("127.0.0.1:" + raw.getPort(), ValueKind.ROW_ID);
            assertThrows(IOException.class, () -> new GrpcIndexCheck(config, "test", 5));
        } finally { raw.shutdownNow(); }
    }
}
