package org.sibench.adapters;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.sibench.adapters.proto.GetRequest;
import org.sibench.adapters.proto.Mutation;
import org.sibench.adapters.proto.ScanRequest;

/** Explicit acceptance bridge; reference work is included in its latency, never backend performance. */
public final class ServiceValueCheck implements IndexTestSupport.BlockingBackend {
    private final IndexBackend actual, reference;
    private final AtomicReference<IOException> failure = new AtomicReference<>();
    private final AtomicLong writes = new AtomicLong(), gets = new AtomicLong(), scans = new AtomicLong();
    private final Timing actualWrites = new Timing(), actualGets = new Timing(), actualScans = new Timing();
    private final Timing referenceWrites = new Timing(), referenceGets = new Timing(), referenceScans = new Timing();

    ServiceValueCheck(IndexBackend actual, IndexBackend reference) throws IOException {
        this.actual = actual;
        this.reference = reference;
        reference.describe().require(actual.describe().valueKind());
    }

    private void healthy() throws IOException {
        if (failure.get() != null) throw new IOException("Value check already failed", failure.get());
    }

    @FunctionalInterface private interface Action { void run() throws IOException; }

    private static final class Timing {
        private long calls, failedCalls;
        private double totalSeconds, maxSeconds;
        synchronized void record(long nanos, boolean completed) {
            double seconds = nanos / 1e9;
            calls++;
            if (!completed) failedCalls++;
            totalSeconds += seconds;
            maxSeconds = Math.max(maxSeconds, seconds);
        }
        synchronized Map<String, Object> describe() {
            return Map.of("calls", calls, "failed_calls", failedCalls,
                    "total_seconds", totalSeconds, "max_seconds", maxSeconds);
        }
    }

    private static void timed(Timing timing, Action action) throws IOException {
        long started = System.nanoTime();
        boolean completed = false;
        try { action.run(); completed = true; }
        finally { timing.record(System.nanoTime() - started, completed); }
    }

    private void checked(Action action) throws IOException {
        healthy();
        try { action.run(); }
        catch (IOException error) { failure.compareAndSet(null, error); throw error; }
        catch (RuntimeException error) {
            var unavailable = new IOException("Value check failed", error);
            failure.compareAndSet(null, unavailable);
            throw unavailable;
        }
    }

    @Override public IndexDescription describe() {
        var original = actual.describe();
        return new IndexDescription(original.name() + "+value-check", original.valueKind(), original.writeAck(),
                original.wal(), original.durability() + "; ACK additionally waits for the reference; validation only",
                original.location());
    }

    @Override public void write(Mutation mutation) throws IOException {
        checked(() -> {
            timed(actualWrites, () -> IndexTestSupport.awaitWrite(actual.writeAsync(mutation)));
            timed(referenceWrites, () -> IndexTestSupport.awaitWrite(reference.writeAsync(mutation)));
            writes.incrementAndGet();
        });
    }

    @Override public void writeBatch(List<Mutation> batch) throws IOException {
        checked(() -> {
            timed(actualWrites, () -> IndexTestSupport.awaitWrite(actual.writeBatchAsync(batch)));
            timed(referenceWrites, () -> IndexTestSupport.awaitWrite(reference.writeBatchAsync(batch)));
            writes.addAndGet(batch.size());
        });
    }

    @Override public ByteString get(GetRequest request) throws IOException {
        ByteString[] answer = new ByteString[2];
        checked(() -> {
            timed(actualGets, () -> answer[0] = actual.get(request));
            timed(referenceGets, () -> answer[1] = reference.get(request));
            if (!Objects.equals(answer[0], answer[1]))
                throw new IOException("Get value differs from reference");
            gets.incrementAndGet();
        });
        return answer[0];
    }

    /** Length framing preserves empty values, duplicates and boundaries in the ordered digest. */
    private static final class Values {
        final MessageDigest digest;
        final ByteBuffer length = ByteBuffer.allocate(Integer.BYTES);
        long count;
        Values() {
            try { digest = MessageDigest.getInstance("SHA-256"); }
            catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
        }
        void add(ByteString value) {
            length.clear().putInt(value.size()).flip();
            digest.update(length);
            digest.update(value.asReadOnlyByteBuffer());
            count = Math.incrementExact(count);
        }
    }

    @Override public void scan(ScanRequest request, ValueSink sink) throws IOException {
        checked(() -> {
            var returned = new Values();
            timed(actualScans, () -> actual.scan(request, value -> { returned.add(value); sink.accept(value); }));
            var expected = new Values();
            timed(referenceScans, () -> reference.scan(request, expected::add));
            if (returned.count != expected.count || !Arrays.equals(returned.digest.digest(), expected.digest.digest()))
                throw new IOException("Scan ordered values differ from reference");
            scans.incrementAndGet();
        });
    }

    @Override public void bind(String binding) throws IOException {
        checked(() -> { actual.bind(binding); reference.bind(binding); });
    }

    @Override public void flush() throws IOException {
        checked(() -> { actual.flush(); reference.flush(); });
    }

    @Override public void close() throws IOException { try (actual; reference) {} }

    Map<String, Object> observations() {
        var error = failure.get();
        return Map.of("status", error == null ? "no_mismatch_observed" : "failed",
                "successful_mutations", writes.get(), "verified_gets", gets.get(), "verified_scans", scans.get(),
                "first_error", error == null ? "" : error.toString(),
                "call_timing", Map.of(
                        "actual", Map.of("write", actualWrites.describe(), "get", actualGets.describe(), "scan", actualScans.describe()),
                        "reference", Map.of("write", referenceWrites.describe(), "get", referenceGets.describe(), "scan", referenceScans.describe()),
                        "scope", "Attempted backend calls, including failures; concurrent call durations overlap. Actual calls include adapter/network/server work; actual Scan includes downstream delivery. Excludes setup, bind, flush, shutdown and comparison outside the calls; not isolated backend throughput or client ACK latency"),
                "scope", "Get exact equality and Scan ordered length-framed SHA-256 plus count; replay completion and input coverage must be checked separately; end-to-end replay timings include reference work");
    }

    /** Runs the ordinary replay through a value-checking service against the configured gRPC target. */
    public static void main(String[] args) throws Exception {
        if (args.length != 4)
            throw new IllegalArgumentException("CONFIG BENCHMARK FRESH_REFERENCE REPORT");
        Path root = Path.of("").toAbsolutePath();
        var runtime = org.sibench.config.SibenchConfig.read(Path.of(args[0]));
        var service = ServiceTarget.of(runtime);
        var replay = org.sibench.replay.ReplayConfig.from(runtime);
        Path trace = org.sibench.config.BenchmarkSelection.directory(root, runtime, args[1]);
        Path referencePath = Path.of(args[2]).toAbsolutePath(), report = Path.of(args[3]).toAbsolutePath();
        Path values = report.resolveSibling(report.getFileName() + ".values.json");
        for (Path path : List.of(referencePath, report, values,
                report.resolveSibling(report.getFileName() + ".baseline.json"),
                report.resolveSibling(report.getFileName() + ".index-manifest.json")))
            if (Files.exists(path)) throw new IOException("Fresh reference and report paths required: " + path);
        var manifest = org.sibench.trace.TraceManifest.read(trace);
        org.sibench.trace.TraceManifest.requireValue(manifest, service.value());
        String binding = org.sibench.replay.ReplayInput.binding(manifest, replay);
        var evidence = new java.util.LinkedHashMap<String, Object>();
        evidence.put("configuration", runtime);
        evidence.put("index_binding", binding);
        evidence.put("endpoint", service.endpoint());
        ServiceValueCheck checker = null;
        GrpcIndexCheck actual = null;
        VersionedIndexBackend reference = null;
        IndexServer server = null;
        Throwable failure = null;
        try {
            actual = new GrpcIndexCheck(service, binding, replay.requestTimeoutSeconds());
            reference = VersionedIndexBackend.create(referencePath, service.value());
            checker = new ServiceValueCheck(actual, reference);
            server = IndexTestSupport.start(0, checker);
            var checkedService = new ServiceTarget("127.0.0.1:" + server.port(), service.value());
            ServiceReplay.run(trace, replay, checkedService, report);
        } catch (Exception | Error error) {
            failure = error;
            throw error;
        } finally {
            try {
                // A failed server shutdown may leave native calls alive; retain their reference handles.
                if (server != null) server.close();
                try (var closingActual = actual; var closingReference = reference) {}
            } catch (Exception | Error error) {
                if (failure != null) failure.addSuppressed(error);
                else { failure = error; throw error; }
            } finally {
                evidence.put("status", failure == null ? "passed" : "failed");
                if (checker != null) evidence.put("values", checker.observations());
                if (failure != null) evidence.put("first_error", failure.toString());
                try {
                    Files.writeString(values, new com.google.gson.GsonBuilder().setPrettyPrinting().create().toJson(evidence) + "\n",
                            StandardOpenOption.CREATE_NEW);
                } catch (IOException error) {
                    if (failure != null) failure.addSuppressed(error); else throw error;
                }
            }
        }
    }
}
