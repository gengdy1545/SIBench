package org.sibench.analysis;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.sibench.adapters.proto.*;
import org.sibench.capture.IndexOperationTrace;
import org.sibench.capture.ProtoTrace;
import org.sibench.config.Digests;
import org.sibench.trace.KeyHash;
import org.sibench.trace.TraceManifest;

/** Explicit streaming workload profile through the canonical reader; no index or replay substitute. */
public final class TraceProfileCheck {
    static final class Profile implements IndexOperationTrace.Consumer {
        final Map<String, Long> counts = new java.util.TreeMap<>();
        final long[] getKeys = new long[4096], writeKeys = new long[4096], scanStarts = new long[4096];
        long minWrite = Long.MAX_VALUE, maxWrite, minRead = Long.MAX_VALUE, maxRead;
        long firstSource = Long.MAX_VALUE, lastSource;

        private void count(String name, long amount) { counts.merge(name, amount, Math::addExact); }
        private void key(long[] bins, ByteString key) { bins[KeyHash.of(key.toByteArray()) & (bins.length - 1)]++; }
        void read(long ts, long source, List<Mutation> local) {
            minRead = Math.min(minRead, ts); maxRead = Math.max(maxRead, ts);
            count(source == 0 ? "reads_outside_exported_transactions" : "reads_in_exported_transactions", 1);
            if (source > 0 && ts != source) count("reads_with_read_ts_different_from_source_ts", 1);
            if (!local.isEmpty()) count("reads_with_private_context", 1);
            for (var write : local) count(write.hasPut() ? "private_put_entries" : "private_delete_entries", 1);
        }
        void operation(TraceOperation op, long source) {
            if (op.hasGet()) {
                var get = op.getGet(); count("get", 1); key(getKeys, get.getKey());
                read(get.getReadTs(), source, get.getLocalWritesList());
            } else if (op.hasScan()) {
                var scan = op.getScan(); count("scan", 1);
                if (scan.hasStartKey()) key(scanStarts, scan.getStartKey()); else count("scans_unbounded_start", 1);
                if (!scan.hasEndKey()) count("scans_unbounded_end", 1);
                if (scan.getReverse()) count("reverse_scans", 1);
                if (scan.getLimit() == 0) count("unlimited_scans", 1);
                read(scan.getReadTs(), source, scan.getLocalWritesList());
            } else if (op.hasReadGroup()) {
                count("read_groups", 1);
                for (var member : op.getReadGroup().getRequestsList()) {
                    var item = TraceOperation.newBuilder();
                    if (member.hasGet()) item.setGet(member.getGet()); else item.setScan(member.getScan());
                    operation(item.build(), source);
                }
            } else {
                if (!op.hasPut() && !op.hasDelete()) throw new IllegalArgumentException("Unsupported profile operation");
                long ts = op.hasPut() ? op.getPut().getTs() : op.getDelete().getTs();
                key(writeKeys, op.hasPut() ? op.getPut().getKey() : op.getDelete().getKey());
                count(op.hasPut() ? "put" : "delete", 1);
                minWrite = Math.min(minWrite, ts); maxWrite = Math.max(maxWrite, ts);
            }
        }
        @Override public void accept(IndexOperationTrace.Operation operation) throws IOException {
            throw new IOException("Profile requires canonical service payloads");
        }
        @Override public void accept(TraceRecord record, List<IndexOperationTrace.Operation> views) {
            if (record.hasTransactionBegin()) {
                count("transaction_begin", 1);
                firstSource = Math.min(firstSource, record.getTransactionBegin());
                lastSource = Math.max(lastSource, record.getTransactionBegin());
            } else if (record.hasTransactionCommit()) count("transaction_commit", 1);
            else {
                operation(record.getOperation(), views.get(0).transactionId());
                for (var view : views) count("namespace_" + view.table() + "_" + view.index(), 1);
            }
        }
        Map<String, Object> report() {
            var result = new LinkedHashMap<String, Object>();
            result.put("counts", counts);
            result.put("write_ts_range", counts.containsKey("put") || counts.containsKey("delete") ? List.of(minWrite, maxWrite) : List.of());
            result.put("read_ts_range", counts.containsKey("get") || counts.containsKey("scan") ? List.of(minRead, maxRead) : List.of());
            result.put("source_ts_range", counts.containsKey("transaction_begin") ? List.of(firstSource, lastSource) : List.of());
            result.put("full_encoded_key_hash_bins", Map.of("get", getKeys, "write", writeKeys, "scan_start", scanStarts));
            result.put("scope", "Complete workload stream, preserving each request read_ts and source write ts. Hash bins include full encoded namespaces and collide; they are not exact key frequencies, PK ranges, unique-key counts or cache hit rates. Scan bins describe starts, not returned rows or I/O. Private context entries count each appearance, never shared writes. Baseline and native source completeness are not rechecked here; no service RPC is sent.");
            return result;
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) throw new IllegalArgumentException("TRACE_DIRECTORY FRESH_REPORT");
        Path directory = Path.of(args[0]), output = Path.of(args[1]);
        if (Files.exists(output)) throw new IOException("Profile report already exists");
        var manifest = TraceManifest.read(directory);
        String identity = Digests.sha256(directory.resolve("manifest.json"));
        var profile = new Profile();
        long start = System.nanoTime();
        long operations = ProtoTrace.source(directory, manifest, "workload").read(profile);
        if (operations != manifest.get("workload_operations").getAsLong()) throw new IOException("Workload count differs from manifest");
        if (!identity.equals(Digests.sha256(directory.resolve("manifest.json")))) throw new IOException("Manifest changed during profile");
        var result = profile.report();
        result.put("manifest_sha256", identity);
        result.put("workload_trace_sha256", manifest.get("workload_trace_sha256").getAsString());
        result.put("encoded_operations", operations);
        result.put("seconds", (System.nanoTime() - start) / 1e9);
        result.put("status", "passed");
        Files.writeString(output, new com.google.gson.GsonBuilder().setPrettyPrinting().create().toJson(result) + "\n", StandardOpenOption.CREATE_NEW);
    }
}
