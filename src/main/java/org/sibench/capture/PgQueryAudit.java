package org.sibench.capture;

import com.google.gson.*;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;

import java.nio.file.*;
import java.time.*;
import java.util.*;

/** Oracle over captured data, not a replacement for native business execution. */
public final class PgQueryAudit {
    private static final int MAX_ACCOUNT = 302000;
    private static final int BASE_TRANSFERS = 6000000;

    private PgQueryAudit() {}

    private static final class TransferIndex {
        final int[] source = new int[BASE_TRANSFERS + 1];
        final int[] target = new int[BASE_TRANSFERS + 1];
        final long[] timestamp = new long[BASE_TRANSFERS + 1];
        final long[] tidKeys;
        final int[] tidIds;
        final int[] sourceOffsets = new int[MAX_ACCOUNT + 2];
        final int[] targetOffsets = new int[MAX_ACCOUNT + 2];
        final int[] sourceIds = new int[BASE_TRANSFERS];
        final int[] targetIds = new int[BASE_TRANSFERS];
        final Map<Long, JsonObject> appended = new HashMap<>();
        final Map<Long, Long> appendedTids = new HashMap<>();
        final Map<Integer, List<Long>> appendedSources = new HashMap<>();
        final Map<Integer, List<Long>> appendedTargets = new HashMap<>();

        TransferIndex(Path baseline) throws Exception {
            long[] packedTids = new long[BASE_TRANSFERS];
            int at = 0;
            try (var rows = Files.newBufferedReader(baseline.resolve("locators.tsv"))) {
                String line;
                while ((line = rows.readLine()) != null) {
                    if (!line.startsWith("transfer\t")) continue;
                    String[] fields = line.split("\t");
                    long id = Long.parseLong(fields[1]);
                    // SF1's 6M IDs fit 23 bits; capture TIDs are bounded to this fixture.
                    long tid = tid(fields[2]);
                    if (id < 1 || id > BASE_TRANSFERS || tid >= (1L << 40))
                        throw new IllegalStateException("SF1 TID/ID range");
                    packedTids[at++] = (tid << 23) | id;
                }
            }
            if (at != BASE_TRANSFERS)
                throw new IllegalStateException("Missing baseline locator rows");
            Arrays.sort(packedTids);
            tidKeys = new long[at];
            tidIds = new int[at];
            for (int i = 0; i < at; i++) {
                tidKeys[i] = packedTids[i] >>> 23;
                tidIds[i] = (int) (packedTids[i] & ((1L << 23) - 1));
                if (i > 0 && tidKeys[i] == tidKeys[i - 1])
                    throw new IllegalStateException("Duplicate PG TID");
            }
            for (String line : Files.readAllLines(baseline.resolve("files.jsonl"))) {
                JsonObject file = JsonParser.parseString(line).getAsJsonObject();
                if (!file.get("table").getAsString().equals("transfer")) continue;
                try (ParquetReader<Group> reader =
                        ParquetReader.builder(
                                        new GroupReadSupport(),
                                        new org.apache.hadoop.fs.Path(
                                                baseline.resolve(file.get("path").getAsString())
                                                        .toUri()))
                                .build()) {
                    Group row;
                    while ((row = reader.read()) != null) {
                        int id = Math.toIntExact(row.getLong("id", 0));
                        source[id] = Math.toIntExact(row.getLong("sourceid", 0));
                        target[id] = Math.toIntExact(row.getLong("targetid", 0));
                        timestamp[id] = timestamp(row.getString("timestamp", 0));
                        sourceOffsets[source[id] + 1]++;
                        targetOffsets[target[id] + 1]++;
                    }
                }
            }
            for (int i = 1; i < sourceOffsets.length; i++) {
                sourceOffsets[i] += sourceOffsets[i - 1];
                targetOffsets[i] += targetOffsets[i - 1];
            }
            int[] s = sourceOffsets.clone(), t = targetOffsets.clone();
            for (int id = 1; id <= BASE_TRANSFERS; id++) {
                sourceIds[s[source[id]]++] = id;
                targetIds[t[target[id]]++] = id;
            }
        }

        long id(String position) {
            long key = tid(position);
            int index = Arrays.binarySearch(tidKeys, key);
            if (index >= 0) return tidIds[index];
            Long id = appendedTids.get(key);
            if (id == null) throw new IllegalStateException("Unknown transfer TID " + position);
            return id;
        }

        Set<Long> matches(int account, boolean bySource) {
            int[] offsets = bySource ? sourceOffsets : targetOffsets;
            int[] ids = bySource ? sourceIds : targetIds;
            Set<Long> result = new HashSet<>();
            if (account >= 1 && account <= MAX_ACCOUNT) {
                for (int i = offsets[account]; i < offsets[account + 1]; i++)
                    result.add((long) ids[i]);
            }
            result.addAll(
                    (bySource ? appendedSources : appendedTargets)
                            .getOrDefault(account, List.of()));
            return result;
        }

        long time(long id) {
            return id <= BASE_TRANSFERS
                    ? timestamp[(int) id]
                    : timestamp(appended.get(id).get("timestamp").getAsString());
        }

        void append(JsonObject change) {
            JsonObject row = change.getAsJsonObject("new");
            long id = row.get("id").getAsLong();
            if (id <= BASE_TRANSFERS || appended.put(id, row) != null)
                throw new IllegalStateException("Duplicate transfer version");
            appendedTids.put(tid(change.get("new_tid").getAsString()), id);
            appendedSources
                    .computeIfAbsent(row.get("sourceid").getAsInt(), k -> new ArrayList<>())
                    .add(id);
            appendedTargets
                    .computeIfAbsent(row.get("targetid").getAsInt(), k -> new ArrayList<>())
                    .add(id);
        }
    }

    private record Scan(boolean source, int account, Set<Long> actual) {}

    public static void main(String[] args) throws Exception {
        if (args.length != 3)
            throw new IllegalArgumentException("PgQueryAudit BASELINE_DIR PG_TRACE REPORT");
        TransferIndex index = new TransferIndex(Path.of(args[0]));
        Map<String, Scan> scans = new HashMap<>();
        List<JsonObject> pending = new ArrayList<>();
        Set<Long> candidates = new HashSet<>();
        long queries = 0, verifiedScans = 0, returned = 0, ties = 0;
        boolean tp5 = false;
        try (var reader = Files.newBufferedReader(Path.of(args[1]))) {
            String line;
            while ((line = reader.readLine()) != null) {
                JsonObject e = JsonParser.parseString(line).getAsJsonObject();
                String type = e.get("event").getAsString();
                if (type.equals("executor_start")) {
                    if (tp5) throw new IllegalStateException("Nested/unclosed TP5");
                    tp5 =
                            e.get("sql")
                                    .getAsString()
                                    .stripLeading()
                                    .toLowerCase(Locale.ROOT)
                                    .startsWith("select * from transfer where");
                    if (tp5) {
                        candidates.clear();
                        returned = 0;
                        queries++;
                    }
                }
                if (tp5 && type.equals("scan_rescan")) {
                    String name = e.get("relation").getAsString();
                    if (!name.equals("idx_transfer_1") && !name.equals("idx_transfer_2"))
                        throw new IllegalStateException("Unexpected TP5 index " + name);
                    JsonArray keys = e.getAsJsonArray("keys");
                    if (keys.size() != 1
                            || keys.get(0).getAsJsonObject().get("strategy").getAsInt() != 3
                            || keys.get(0).getAsJsonObject().get("flags").getAsInt() != 0)
                        throw new IllegalStateException("Unsupported TP5 scan keys");
                    scans.put(
                            e.get("scan").getAsString(),
                            new Scan(
                                    name.equals("idx_transfer_1"),
                                    keys.get(0).getAsJsonObject().get("value").getAsInt(),
                                    new HashSet<>()));
                }
                if (tp5 && type.equals("bitmap_candidate")) {
                    Scan scan = scans.get(e.get("scan").getAsString());
                    if (scan == null || !scan.actual().add(index.id(e.get("tid").getAsString())))
                        throw new IllegalStateException("Duplicate or unmatched bitmap candidate");
                }
                if (tp5 && type.equals("bitmap_end")) {
                    Scan scan = scans.remove(e.get("scan").getAsString());
                    if (scan == null
                            || !scan.actual().equals(index.matches(scan.account(), scan.source())))
                        throw new IllegalStateException("TP5 candidate set mismatch");
                    candidates.addAll(scan.actual());
                    verifiedScans++;
                }
                if (tp5 && type.equals("result_row")) {
                    long id = e.getAsJsonArray("values").get(0).getAsLong();
                    long maximum =
                            candidates.stream().mapToLong(index::time).max().orElse(Long.MIN_VALUE);
                    if (!candidates.contains(id) || index.time(id) != maximum)
                        throw new IllegalStateException(
                                "TP5 selected row not a maximum timestamp candidate");
                    if (candidates.stream().filter(k -> index.time(k) == maximum).count() > 1)
                        ties++;
                    returned++;
                }
                if (tp5 && type.equals("executor_end")) {
                    if (!scans.isEmpty() || returned != (candidates.isEmpty() ? 0 : 1))
                        throw new IllegalStateException("TP5 result count mismatch");
                    tp5 = false;
                }
                if (type.equals("row_change") && e.get("relation").getAsString().equals("transfer"))
                    pending.add(e);
                if (type.equals("transaction_commit")) {
                    for (var change : pending) index.append(change);
                    pending.clear();
                }
                if (type.equals("transaction_abort")) pending.clear();
            }
        }
        if (tp5 || !pending.isEmpty()) throw new IllegalStateException("Incomplete trace");
        Files.writeString(
                Path.of(args[2]),
                new GsonBuilder()
                        .setPrettyPrinting()
                        .create()
                        .toJson(
                                Map.of(
                                        "status",
                                        "passed",
                                        "tp5_queries",
                                        queries,
                                        "verified_secondary_scans",
                                        verifiedScans,
                                        "tied_maximum_results",
                                        ties,
                                        "scope",
                                        "all TP5 source/target equality candidate sets and ORDER BY"
                                            + " timestamp DESC LIMIT 1 results; single-session"
                                            + " append-only transfer history")),
                StandardOpenOption.CREATE_NEW);
    }

    static long tid(String text) {
        int comma = text.indexOf(',');
        return (Long.parseLong(text.substring(1, comma)) << 16)
                | Long.parseLong(text.substring(comma + 1, text.length() - 1));
    }

    static long timestamp(String text) {
        return LocalDateTime.parse(text.replace(' ', 'T')).toInstant(ZoneOffset.UTC).toEpochMilli();
    }
}
