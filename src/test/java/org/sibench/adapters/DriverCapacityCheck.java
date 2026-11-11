package org.sibench.adapters;

import org.sibench.replay.ReplayInput;
import com.google.gson.*;
import io.grpc.*;
import io.grpc.stub.*;
import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import org.sibench.adapters.proto.*;
import org.sibench.capture.*;
import org.sibench.config.Digests;
import org.sibench.replay.*;

/** Bounded diagnostic: record a transaction-complete prefix once, then replay its full responses over RPC. */
public final class DriverCapacityCheck {
    private static final Gson JSON = new GsonBuilder().setPrettyPrinting().create();
    private static final long MAX_BYTES = 1L << 30;
    private static final int MAGIC = 0x53494243;
    private static final class EndPrefix extends IOException {}

    public static void main(String[] args) throws Exception {
        switch (args[0]) {
            case "record" -> record(Path.of(args[1]), Path.of(args[2]), Long.parseLong(args[3]));
            case "serve", "serve-corrupt" -> serve(Path.of(args[1]), Integer.parseInt(args[2]), args[0].equals("serve-corrupt"));
            case "run" -> run(Path.of(args[1]), args[2], Integer.parseInt(args[3]), Path.of(args[4]));
            case "check-baseline" -> checkBaseline(Path.of(args[1]), Path.of(args[2]));
            case "check-work-units" -> checkWorkUnits(Path.of(args[1]), Path.of(args[2]));
            case "plan" -> plan(Path.of(args[1]), Long.parseLong(args[2]), Path.of(args[3]));
            default -> throw new IllegalArgumentException("record TRACE OUTPUT RECORD_LIMIT | serve FIXTURE PORT | run FIXTURE ENDPOINT CLIENTS REPORT");
        }
    }

    private static void checkBaseline(Path trace, Path report) throws Exception {
        try (var timing = RunTiming.open(report.resolveSibling(report.getFileName() + ".timing.json"))) {
            timing.phase("baseline-only");
            long[] operations = {0};
            long started = System.nanoTime();
            var manifest = org.sibench.trace.TraceManifest.read(trace);
            IndexCatalog.fromManifest(manifest);
            ProtoTrace.source(trace, manifest, "baseline").read(operation -> operations[0]++);
            if (operations[0] != manifest.get("baseline_operations").getAsLong())
                throw new IOException("Proto baseline record count mismatch");
            Files.writeString(report, JSON.toJson(Map.of("status", "passed", "operations", operations[0],
                    "seconds", (System.nanoTime() - started) / 1e9,
                    "scope", "full protobuf baseline count and digests matched its retained manifest; no RPC/backend execution")), StandardOpenOption.CREATE_NEW);
        }
    }

    private static void checkWorkUnits(Path trace, Path report) throws Exception {
        long started = System.nanoTime();
        var units = WorkUnits.read(trace);
        var source = JsonParser.parseString(Files.readString(trace.resolve("manifest.json"))).getAsJsonObject();
        if (!units.sha256().equals(source.get("work_units_sha256").getAsString())
                || units.count() != source.get("source_work_units").getAsLong()) throw new IOException("Work-unit identity mismatch");
        double seconds = (System.nanoTime() - started) / 1e9;
        var indexDigest = java.security.MessageDigest.getInstance("SHA-256");
        var word = java.nio.ByteBuffer.allocate(8);
        for (long id = 1; id <= units.count(); id++) {
            word.clear(); word.putLong(units.firstEvent(id)); indexDigest.update(word.array());
        }
        Files.writeString(report, JSON.toJson(Map.of("status", "passed", "count", units.count(),
                "sha256", units.sha256(), "first_source_event", units.firstEvent(1),
                "last_source_event", units.firstEvent(units.count()), "seconds", seconds,
                "first_event_index_sha256", java.util.HexFormat.of().formatHex(indexDigest.digest()))), StandardOpenOption.CREATE_NEW);
    }

    static ReplayConfig config(Path trace, int clients) throws IOException {
        boolean secondary = org.sibench.trace.TraceManifest.secondaryIndexes(org.sibench.trace.TraceManifest.read(trace));
        return new ReplayConfig(secondary, 0, 0, 0, 0, "operation", clients, 4096, 60);
    }

    private static void plan(Path trace, long wanted, Path report) throws Exception {
        var candidates = new ArrayList<Map<String, Object>>();
        var routing = new ReplayDispatcher(config(trace, 128), client -> { throw new IOException("Planning cannot send"); });
        var clients = new BitSet();
        long[] records = {0}, queries = {0}, items = {0}, largest = {0};
        long[] next = {1000};
        try {
            ProtoTrace.source(trace, org.sibench.trace.TraceManifest.read(trace), "workload").read( operation -> {
                records[0]++;
                if (!IndexOperationTrace.isControl(operation))
                    clients.set(routing.route(operation));
                if (operation.kind() == IndexOperationTrace.SEARCH
                        && (!operation.grouped() || operation.expansionPosition() + 1 == operation.expansionSize())) {
                    queries[0]++;
                    items[0] = Math.addExact(items[0], operation.expectedCount());
                    largest[0] = Math.max(largest[0], operation.expectedCount());
                }
                if ((operation.kind() == IndexOperationTrace.TRANSACTION_COMMIT || operation.kind() == IndexOperationTrace.TRANSACTION_ABORT) && records[0] >= next[0]) {
                    candidates.add(Map.of("records", records[0], "queries", queries[0], "expected_result_items", items[0],
                            "largest_query_items", largest[0], "used_clients_128", clients.cardinality()));
                    next[0] = Math.max(next[0] * 2, records[0] + 1);
                    if (records[0] >= wanted) throw new EndPrefix();
                }
            });
        } catch (EndPrefix expected) {}
        Files.writeString(report, JSON.toJson(Map.of("scope", "read-only transaction-complete-prefix planning; not a capacity measurement",
                "trace", trace.toAbsolutePath().toString(), "candidates", candidates)), StandardOpenOption.CREATE_NEW);
    }

    private static void prefix(Path trace, long limit, IndexOperationTrace.Consumer consumer) throws IOException {
        long[] count = {0};
        try {
            ProtoTrace.source(trace, org.sibench.trace.TraceManifest.read(trace), "workload").read( operation -> {
                consumer.accept(operation);
                if (++count[0] == limit) throw new EndPrefix();
            });
        } catch (EndPrefix expected) {
            return;
        }
        if (count[0] != limit) throw new IOException("Trace prefix was truncated");
    }

    private static void record(Path trace, Path output, long wanted) throws Exception {
        Files.createDirectory(output);
        Files.writeString(output.resolve("INCOMPLETE"), "Response recording not committed\n");
        // The full directory check is outside measured dispatch, and remains part of setup cost.
        System.out.println("phase=preflight");
        var inputTrace = ReplayInput.prepare(trace, config(trace, 128));
        long[] records = {0}, queries = {0};
        try {
            ProtoTrace.source(trace, org.sibench.trace.TraceManifest.read(trace), "workload").read( operation -> {
                records[0]++;
                if (operation.kind() == IndexOperationTrace.SEARCH
                        && (!operation.grouped() || operation.expansionPosition() + 1 == operation.expansionSize())) queries[0]++;
                if (records[0] >= wanted && (operation.kind() == IndexOperationTrace.TRANSACTION_COMMIT || operation.kind() == IndexOperationTrace.TRANSACTION_ABORT)) throw new EndPrefix();
            });
        } catch (EndPrefix expected) {}
        String binding = ReplayInput.binding(trace, config(trace, 128)) + " mode=rowid";
        // The serialized-byte cap is not an object-heap bound; setup also needs its external heap/deadline limits.
        var recorded = new ConcurrentHashMap<Request, List<Response>>();
        var bytes = new AtomicLong();
        var requests = new TraceRequests(ValueKind.ROW_ID);
        IndexDescription description;
        try (var backend = VersionedIndexBackend.create(output.resolve("index"), ValueKind.ROW_ID);
                var executor = new IndexExecutor(backend)) {
            description = backend.describe();
            var baselineRequests = new TraceRequests(ValueKind.ROW_ID);
            baselineRequests.baselineBatch(64);
            System.out.println("phase=baseline");
            try (var load = ReplayProjection.withClients(config(trace, 64), false,
                    InProcessService.factory(executor, baselineRequests))) {
                inputTrace.baseline().read(load);
                load.finish();
            }
            executor.flush();
            System.out.println("phase=record records=" + records[0] + " queries=" + queries[0]);
            ReplayTarget.Factory factory = client -> new ServiceClient(requests) {
                @Override CompletableFuture<Response> transmit(Request request, java.util.function.Consumer<Response> partial) throws IOException {
                    var chunks = new ArrayList<Response>();
                    java.util.function.Consumer<Response> keep = response -> {
                        long size = bytes.addAndGet(response.getSerializedSize());
                        if (size > MAX_BYTES) throw new IllegalStateException("Response fixture exceeds 1 GiB budget");
                        chunks.add(response);
                    };
                    if (bytes.addAndGet(request.getSerializedSize()) > MAX_BYTES) throw new IOException("Fixture budget exceeded");
                    return executor.submit(request, chunk -> { keep.accept(chunk); partial.accept(chunk); })
                            .thenApply(response -> {
                                keep.accept(response);
                                var prior=recorded.putIfAbsent(request,List.copyOf(chunks));
                                if(prior!=null && !prior.equals(chunks)) throw new IllegalStateException("Identical requests returned different answers");
                                return response;
                            });
                }
            };
            try (var replay = ReplayProjection.withClients(config(trace, 128), true, factory)) {
                prefix(trace, records[0], replay);
                replay.finish();
                replay.requireClientCoverage();
            }
        }
        if (requests.queries() != queries[0]) throw new IOException("Incomplete response fixture validation");
        Path binary = output.resolve("responses.bin");
        try (var data = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(binary)))) {
            data.writeInt(MAGIC);
            data.writeUTF(new com.google.gson.Gson().toJson(description));
            data.writeInt(recorded.size());
            // Stable order makes the diagnostic fixture reproducible for the same requests/results.
            for (var request : recorded.keySet().stream().sorted((a,b) -> Arrays.compareUnsigned(a.toByteArray(),b.toByteArray())).toList()) {
                request.writeDelimitedTo(data);
                var chunks = recorded.get(request);
                data.writeInt(chunks.size());
                for (var response : chunks) response.writeDelimitedTo(data);
            }
        }
        var manifest = new LinkedHashMap<String, Object>();
        manifest.put("status", "passed");
        manifest.put("trace", trace.toAbsolutePath().toString());
        manifest.put("trace_manifest_sha256", Digests.sha256(trace.resolve("manifest.json")));
        manifest.put("records", records[0]);
        manifest.put("queries", queries[0]);
        manifest.put("binding", binding);
        manifest.put("rpc_requests", recorded.size());
        manifest.put("serialized_request_response_bytes", bytes.get());
        manifest.put("fixture_sha256", Digests.sha256(binary));
        manifest.put("scope", "transaction-complete prefix of the original trace; recorded response counts matched Trace source counts; no trace looping");
        Files.writeString(output.resolve("manifest.json"), JSON.toJson(manifest));
        Files.delete(output.resolve("INCOMPLETE"));
        System.out.println("fixture=complete requests=" + recorded.size());
    }

    private static JsonObject manifest(Path fixture) throws IOException {
        if (Files.exists(fixture.resolve("INCOMPLETE"))) throw new IOException("Incomplete response fixture");
        var manifest = JsonParser.parseString(Files.readString(fixture.resolve("manifest.json"))).getAsJsonObject();
        if (!Digests.sha256(fixture.resolve("responses.bin")).equals(manifest.get("fixture_sha256").getAsString()))
            throw new IOException("Response fixture changed");
        return manifest;
    }

    private static void serve(Path fixture, int port, boolean corrupt) throws Exception {
        manifest(fixture);
        var recorded = new HashMap<Request, List<Response>>();
        IndexDescription description;
        try (var data = new DataInputStream(new BufferedInputStream(Files.newInputStream(fixture.resolve("responses.bin"))))) {
            if (data.readInt() != MAGIC) throw new IOException("Invalid fixture");
            description = new com.google.gson.Gson().fromJson(data.readUTF(), IndexDescription.class);
            int count = data.readInt();
            for (int i = 0; i < count; i++) {
                Request request = Request.parseDelimitedFrom(data);
                int chunks = data.readInt();
                var responses = new ArrayList<Response>();
                for (int c = 0; c < chunks; c++) {
                    var response = Response.parseDelimitedFrom(data);
                    if (corrupt && response.getResultsCount() != 0) {
                        var result = response.getResults(0);
                        response = response.toBuilder().setResults(0, result.toBuilder()
                                .setValue(org.sibench.trace.IndexEntry.rowId(result.getValue().asReadOnlyByteBuffer().getLong() + 1))).build();
                    }
                    responses.add(response);
                }
                recorded.put(request, responses);
            }
            if (data.read() != -1) throw new IOException("Extra fixture records");
        }
        IndexBackend backend = new IndexBackend() {
            public IndexDescription describe() {
                return new IndexDescription("recorded-response-fixture",ValueKind.ROW_ID,Acknowledgement.ACK_MEMORY,
                        "diagnostic responses; no persistent index",fixture.toString());
            }
            private List<Response> find(Request request) throws IOException {
                var found=recorded.get(request);
                if(found==null) throw new IOException("Request differs from recorded prefix");
                for(var response:found) if(response.getStatus()!=org.sibench.adapters.proto.Status.OK) throw new IOException(response.getMessage());
                return found;
            }
            public void write(Mutation mutation) throws IOException {
                var request=Request.newBuilder();
                if(mutation.hasPut())request.setPut(mutation.getPut());else request.setDelete(mutation.getDelete());
                find(request.build());
            }
            public Result get(GetRequest get) throws IOException {
                var results=new ArrayList<Result>();
                for(var response:find(Request.newBuilder().setGet(get).build()))results.addAll(response.getResultsList());
                if(results.size()>1)throw new IOException("Get fixture has multiple results");
                return results.isEmpty()?null:results.get(0);
            }
            public void scan(ScanRequest scan,ResultSink output) throws IOException {
                for(var response:find(Request.newBuilder().setScan(scan).build()))
                    for(var result:response.getResultsList()) output.accept(result);
            }
            public void flush() {}
            public void bind(String value) throws IOException {
                if(!manifest(fixture).get("binding").getAsString().equals(value)) throw new IOException("Fixture binding differs");
            }
            public String location() { return fixture.toString(); }
            public void close() {}
        };
        var server = IndexServer.start(port,backend);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> { try { server.close(); } catch(IOException ignored) {} }));
        System.out.println("READY port=" + server.port());
        server.awaitTermination();
    }

    private static void run(Path fixture, String endpoint, int clients, Path report) throws Exception {
        var manifest = manifest(fixture);
        Path trace = Path.of(manifest.get("trace").getAsString());
        if (!Digests.sha256(trace.resolve("manifest.json")).equals(manifest.get("trace_manifest_sha256").getAsString()))
            throw new IOException("Source trace manifest changed");
        var requests = new TraceRequests(ValueKind.ROW_ID);
        var config = config(trace, clients);
        long started = System.nanoTime();
        Map<String, Object> dispatch;
        try (var remote = new GrpcService(endpoint, requests, manifest.get("binding").getAsString(), 60);
                var replay = ReplayProjection.withClients(config, true, remote.factory())) {
            prefix(trace, manifest.get("records").getAsLong(), replay);
            replay.finish();
            replay.requireClientCoverage();
            dispatch = replay.describe();
        }
        if (requests.queries() != manifest.get("queries").getAsLong()) throw new IOException("Query coverage mismatch");
        var result = new LinkedHashMap<String, Object>();
        result.put("status", "passed");
        result.put("clients", clients);
        result.put("seconds", (System.nanoTime() - started) / 1e9);
        result.put("dispatch", dispatch);
        result.put("queries", requests.queries());
        result.put("result_items", requests.checkedResults());
        result.put("request_bytes", requests.requestBytes());
        result.put("result_bytes", requests.resultBytes());
        result.put("rpc_requests", manifest.get("rpc_requests").getAsLong());
        result.put("fixture_sha256", manifest.get("fixture_sha256").getAsString());
        result.put("scope", "finite same-prefix full-response RPC calibration; fixture emitter/transport/checker can bottleneck; no sustained-rate or index-upper-bound claim");
        Files.writeString(report, JSON.toJson(result), StandardOpenOption.CREATE_NEW);
        System.out.println("passed clients=" + clients + " queries=" + requests.queries());
    }
}
