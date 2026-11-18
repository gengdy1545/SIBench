package org.sibench.analysis;

import org.sibench.capture.IndexOperationTrace;
import org.sibench.capture.IndexOperationTrace.Operation;
import org.sibench.capture.IndexOperationTrace.Source;
import org.sibench.capture.ProtoTrace;
import org.sibench.config.Digests;
import org.sibench.state.ExternalSort;
import org.sibench.trace.IndexEntry;
import org.sibench.trace.TraceManifest;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Counts the keys represented by baseline and workload Puts, including private read contexts. */
public final class TraceKeyCounts {
    private TraceKeyCounts() {}

    private record Baseline(String physical, String identity) {}

    private record Trace(String name, Path directory, String manifestSha256, long baselineRows,
                         Source baseline, Source workload) {}

    public static Map<String, Map<String, Object>> run(Map<String, Path> directories, long sortBytes)
            throws IOException {
        var groups = new LinkedHashMap<Baseline, List<Trace>>();
        for (var entry : directories.entrySet()) {
            var directory = entry.getValue();
            var identity = Digests.sha256(directory.resolve("manifest.json"));
            var manifest = TraceManifest.read(directory);
            var baseline = ProtoTrace.source(directory, manifest, "baseline");
            var workload = ProtoTrace.source(directory, manifest, "workload");
            var descriptor = manifest.getAsJsonObject("proto_files").getAsJsonObject("baseline");
            var key = new Baseline(descriptor.get("sha256").getAsString(),
                    manifest.get("baseline_trace_sha256").getAsString());
            groups.computeIfAbsent(key, ignored -> new ArrayList<>()).add(new Trace(entry.getKey(), directory,
                    identity, manifest.get("baseline_rows").getAsLong(), baseline, workload));
        }

        var result = new LinkedHashMap<String, Map<String, Object>>();
        for (var group : groups.entrySet()) {
            var traces = group.getValue();
            // Reuse a decoded baseline only after verifying each other payload is byte-identical.
            for (var trace : traces.subList(1, traces.size())) {
                if (!group.getKey().physical().equals(Digests.sha256(trace.directory().resolve("baseline.pb.zst"))))
                    throw new IOException("Baseline payload digest mismatch in " + trace.directory());
            }
            var scratch = Files.createTempDirectory("sibench-access-keys-");
            long[] counts;
            try (var keys = new ExternalSort(scratch.resolve("keys"), sortBytes)) {
                counts = count(traces.get(0).baseline(), traces.stream().map(Trace::workload).toList(), keys);
            } finally {
                Files.delete(scratch);
            }
            for (int i = 0; i < traces.size(); i++) {
                var trace = traces.get(i);
                if (counts[0] != trace.baselineRows())
                    throw new IOException("Distinct baseline PK count differs from baseline rows in " + trace.directory());
                if (!trace.manifestSha256().equals(Digests.sha256(trace.directory().resolve("manifest.json"))))
                    throw new IOException("Trace manifest changed while counting keys in " + trace.directory());
                result.put(trace.name(), Map.of("manifest_sha256", trace.manifestSha256(),
                        "baseline_distinct_pk", counts[0], "total_distinct_pk", Math.addExact(counts[0], counts[i + 1])));
            }
        }
        return result;
    }

    private static void addPut(Operation operation, ExternalSort keys, byte[] source) throws IOException {
        if (operation.kind() == IndexOperationTrace.INSERT || operation.kind() == IndexOperationTrace.UPSERT)
            keys.add(new IndexEntry(operation.table(), 0, null, operation.key()).bytes().toByteArray(), source);
    }

    /** The first count is the baseline size, followed by the keys each workload adds to it. */
    static long[] count(Source baseline, List<Source> workloads, ExternalSort keys) throws IOException {
        byte[] baselineTag = new byte[Integer.BYTES];
        baseline.read(operation -> {
            if (!IndexOperationTrace.isBaselineInsert(operation))
                throw new IOException("Baseline key counting requires version-zero inserts");
            if (operation.index() == 0) addPut(operation, keys, baselineTag);
        });
        for (int i = 0; i < workloads.size(); i++) {
            byte[] tag = ByteBuffer.allocate(Integer.BYTES).putInt(i + 1).array();
            workloads.get(i).read(operation -> {
                addPut(operation, keys, tag);
                for (var privateWrite : operation.localWrites()) addPut(privateWrite, keys, tag);
            });
        }
        long[] counts = new long[workloads.size() + 1];
        try (var sorted = keys.sorted()) {
            byte[] previous = null;
            int lastSource = -1;
            boolean inBaseline = false;
            while (sorted.hasNext()) {
                byte[] key = sorted.peekKey();
                int source = ByteBuffer.wrap(sorted.peekValue()).getInt();
                if (!Arrays.equals(previous, key)) {
                    previous = key;
                    lastSource = -1;
                    // Equal-key records retain source order, with the baseline first.
                    inBaseline = source == 0;
                }
                if (source != lastSource && (source == 0 || !inBaseline)) counts[source]++;
                lastSource = source;
                sorted.advance();
            }
        }
        return counts;
    }
}
