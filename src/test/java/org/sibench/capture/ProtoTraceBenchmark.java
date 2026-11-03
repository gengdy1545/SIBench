package org.sibench.capture;

import com.google.gson.GsonBuilder;
import org.sibench.trace.IndexKey;

import java.nio.file.*;
import java.util.*;

/** Bounded codec diagnostic with fixed primary writes and read-your-writes transactions. */
public final class ProtoTraceBenchmark {
    public static void main(String[] args) throws Exception {
        Path directory = Path.of(args[1]);
        if (args[0].equals("create")) {
            int rows = Integer.parseInt(args[2]), transactions = Integer.parseInt(args[3]);
            Files.createDirectory(directory);
            var files = new LinkedHashMap<String, Object>();
            var baseline = new ProtoTrace.Writer(directory.resolve("baseline.pb.zst"));
            try (baseline) {
                for (int row = 1; row <= rows; row++) baseline.append(IndexOperationTrace.write(0,
                        IndexOperationTrace.INSERT, 1, 0, IndexKey.ofInt(row), 0, 0, 0, row)
                        .withWorkUnitId(1 + (row - 1) / IndexOperationTrace.BLOCK_RECORDS).withExpansion(0, 1));
            }
            files.put("baseline", ProtoTrace.file("baseline.pb.zst", baseline));
            var workload = new ProtoTrace.Writer(directory.resolve("workload.pb.zst"));
            try (workload) {
                for (long version = 1; version <= transactions; version++) {
                    long event = version * 4, row = 1 + (version - 1) % rows;
                    workload.append(IndexOperationTrace.transaction(event - 3,
                            IndexOperationTrace.TRANSACTION_BEGIN, event, version, 0));
                    workload.append(IndexOperationTrace.write(event - 2, IndexOperationTrace.UPSERT,
                            1, 0, IndexKey.ofInt(row), version, 0, 0, row)
                            .withTransaction(event, 1).withWorkUnitId(version).withExpansion(0, 1));
                    workload.append(IndexOperationTrace.search(event - 1, 1, 0,
                            IndexOperationTrace.Range.equal(IndexKey.ofInt(row)), version - 1, 1,
                            IndexOperationTrace.TRANSACTION_LOCAL_SEARCH)
                            .withVersion(version).withTransaction(event, 1).withWorkUnitId(version));
                    workload.append(IndexOperationTrace.transaction(event,
                            IndexOperationTrace.TRANSACTION_COMMIT, event, version, 1));
                }
            }
            files.put("workload", ProtoTrace.file("workload.pb.zst", workload));
            Files.writeString(directory.resolve("fixture.json"), new GsonBuilder().setPrettyPrinting()
                    .create().toJson(Map.of("rows", rows, "transactions", transactions, "files", files)));
            return;
        }
        if (!args[0].equals("read")) throw new IllegalArgumentException("create DIR ROWS TRANSACTIONS | read DIR REPEATS");
        var fixture = com.google.gson.JsonParser.parseString(Files.readString(directory.resolve("fixture.json"))).getAsJsonObject();
        var bean = (com.sun.management.ThreadMXBean) java.lang.management.ManagementFactory.getThreadMXBean();
        bean.setThreadAllocatedMemoryEnabled(true);
        long thread = Thread.currentThread().getId();
        var results = new ArrayList<Map<String, Object>>();
        for (int repeat = -1; repeat < Integer.parseInt(args[2]); repeat++) {
            long started = System.nanoTime(), allocated = bean.getThreadAllocatedBytes(thread);
            long cpu = bean.getCurrentThreadCpuTime(), count = 0;
            long[] fingerprint = {0};
            for (String name : List.of("baseline", "workload")) {
                var file = fixture.getAsJsonObject("files").getAsJsonObject(name);
                count += ProtoTrace.read(directory.resolve(file.get("path").getAsString()),
                        file.get("sha256").getAsString(), file.get("logical_sha256").getAsString(), operation ->
                                fingerprint[0] = Long.rotateLeft(fingerprint[0], 1) ^ operation.sourceEvent()
                                        ^ operation.kind() ^ operation.version() ^ operation.rowId());
            }
            long expected = fixture.get("rows").getAsLong() + 4 * fixture.get("transactions").getAsLong();
            if (count != expected) throw new AssertionError("Fixture operation coverage changed");
            if (repeat >= 0) results.add(Map.of("operations", count, "fingerprint", fingerprint[0],
                    "nanos", System.nanoTime() - started, "cpu_nanos", bean.getCurrentThreadCpuTime() - cpu,
                    "allocated_bytes", bean.getThreadAllocatedBytes(thread) - allocated));
        }
        System.out.println(new GsonBuilder().setPrettyPrinting().create().toJson(results));
    }
}
