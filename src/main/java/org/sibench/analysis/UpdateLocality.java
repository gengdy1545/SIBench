package org.sibench.analysis;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.sibench.capture.BaselineOperations;
import org.sibench.capture.IndexCatalog;
import org.sibench.capture.IndexOperationTrace;
import org.sibench.capture.IndexOperationTrace.Operation;
import org.sibench.capture.RunTiming;
import org.sibench.config.ConfigValues;
import org.sibench.state.ExternalSort;
import org.sibench.trace.IndexKey;
import org.sibench.trace.KeyHash;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.GZIPOutputStream;

/**
 * Static update-locality analysis over a trace. A baseline is placed in declared sorted runs of
 * {@code entries_per_block} entries and {@code blocks_per_file} blocks per file; committed workload
 * modifications are then mapped to those runs. The layout is an equal-entry-count approximation,
 * not a Parquet row group or a real SST byte layout.
 *
 * <p>Business row modifications and index maintenance are counted separately. A business row
 * change counts once, at its primary maintenance record; secondary entries are index maintenance.
 * Windows contain a fixed number of business changes, so one source change's expansion is never
 * split. The analysis only reads existing Trace / Baseline products and never captures PG.
 */
public final class UpdateLocality {
    public record Config(
            int entriesPerBlock,
            int blocksPerFile,
            long businessChangesPerWindow,
            int heatmapBins,
            long sortBytes,
            int l0IndexModifications) {
        public static Config read(Path path) throws IOException {
            var document = ConfigValues.object(org.sibench.config.SibenchConfig.read(path), "analysis");
            var layout = ConfigValues.object(document, "layout");
            var window = ConfigValues.object(document, "window");
            int entries = Math.toIntExact(ConfigValues.positive(layout, "entries_per_block"));
            int blocks = Math.toIntExact(ConfigValues.positive(layout, "blocks_per_file"));
            int bins = Math.toIntExact(ConfigValues.positive(window, "heatmap_bins"));
            int l0 = Math.toIntExact(ConfigValues.positive(window, "l0_index_modifications"));
            long changes = ConfigValues.positive(window, "business_changes");
            long sortBytes = ConfigValues.positive(layout, "sort_memory_bytes");
            if (bins < 1 || bins > 1_000_000)
                throw new IllegalArgumentException("heatmap_bins must be in 1..1000000");
            return new Config(entries, blocks, changes, bins, sortBytes, l0);
        }

        Map<String, Object> describe() {
            var report = new LinkedHashMap<String, Object>();
            report.put("entries_per_block", entriesPerBlock);
            report.put("blocks_per_file", blocksPerFile);
            report.put("business_changes_per_window", businessChangesPerWindow);
            report.put("heatmap_bins", heatmapBins);
            report.put("l0_index_modifications", l0IndexModifications);
            report.put("sort_memory_bytes", sortBytes);
            report.put("layout_kind", "equal_entry_count_static_run");
            return report;
        }
    }

    /** One index's baseline run: every block's first composite key, plus its bounds. */
    record IndexLayout(
            int table, int index, String name, long entries, int blocks, int files,
            byte[] min, byte[] max, byte[][] blockFirstKeys) {
        boolean empty() {
            return entries == 0;
        }

        int blockOf(byte[] key) {
            if (empty()) return -1;
            int order = Arrays.compareUnsigned(key, min);
            if (order < 0) return -2;
            if (Arrays.compareUnsigned(key, max) > 0) return -3;
            int low = 0, high = blockFirstKeys.length - 1, at = 0;
            while (low <= high) {
                int middle = (low + high) >>> 1;
                if (Arrays.compareUnsigned(blockFirstKeys[middle], key) <= 0) {
                    at = middle;
                    low = middle + 1;
                } else high = middle - 1;
            }
            return at;
        }
    }

    private static final class LayoutBuilder {
        private final int entriesPerBlock;
        private final List<byte[]> firstKeys = new ArrayList<>();
        private long entries;
        private byte[] min, max, previous;

        LayoutBuilder(int entriesPerBlock) {
            this.entriesPerBlock = entriesPerBlock;
        }

        void add(byte[] key) throws IOException {
            if (previous != null && Arrays.compareUnsigned(previous, key) >= 0)
                throw new IOException("Duplicate or unsorted baseline index key");
            if (entries % entriesPerBlock == 0) firstKeys.add(key.clone());
            if (min == null) min = key.clone();
            max = key;
            previous = key;
            entries++;
        }
    }

    private UpdateLocality() {}

    /** Builds the static layout and writes the analysis into a new output directory. */
    public static void run(Path trace, Path configFile, Path output) throws IOException {
        if (Files.exists(output)) throw new IOException("Locality output already exists: " + output);
        Files.createDirectories(output);
        Files.writeString(output.resolve("INCOMPLETE"), "analysis not committed\n");
        var config = Config.read(configFile);
        var manifest =
                JsonParser.parseString(Files.readString(trace.resolve("manifest.json")))
                        .getAsJsonObject();
        if (!manifest.has("workload_trace_sha256"))
            throw new IOException("Workload trace identity missing from its manifest");
        var catalog = IndexCatalog.fromManifest(manifest);
        long baselineRows = manifest.has("baseline_rows") ? manifest.get("baseline_rows").getAsLong() : -1;
        long baselineOperations =
                manifest.has("baseline_operations") ? manifest.get("baseline_operations").getAsLong() : -1;
        RunTiming.current().measure(output);
        var layouts = buildLayout(trace, catalog, config, output.resolve("sort"), baselineOperations);
        var analysis = analyze(trace, manifest, catalog, layouts, baselineRows, config, output);
        var summary = new LinkedHashMap<String, Object>();
        summary.put("status", "complete_for_declared_layout");
        summary.put("benchmark", manifest.has("benchmark") ? manifest.get("benchmark").getAsString() : "unknown");
        summary.put("trace", trace.toAbsolutePath().normalize().toString());
        summary.put("manifest_workload_operations", manifest.get("workload_operations").getAsLong());
        summary.put("manifest_baseline_operations", baselineOperations);
        summary.put("manifest_baseline_rows", baselineRows);
        summary.put("workload_trace_sha256", manifest.get("workload_trace_sha256").getAsString());
        summary.put("baseline_trace_sha256", manifest.get("baseline_trace_sha256").getAsString());
        summary.put("config", config.describe());
        summary.put("analysis", analysis);
        summary.put(
                "scope",
                "static equal-entry layout only; block/file boundaries are not Parquet row groups or"
                        + " real SST bytes; no compaction selection, dynamic L1 or engine bytes are"
                        + " modelled; unique keys are 64-bit fingerprints");
        Files.writeString(
                output.resolve("summary.json"),
                new GsonBuilder().setPrettyPrinting().serializeNulls().create().toJson(summary));
        Files.deleteIfExists(output.resolve("INCOMPLETE"));
    }

    private static Map<Long, IndexLayout> buildLayout(
            Path trace,
            IndexCatalog catalog,
            Config config,
            Path sortDirectory,
            long expectedOperations)
            throws IOException {
        RunTiming.current().phase("locality-layout");
        var builders = new TreeMap<Long, LayoutBuilder>();
        try (var sort = new ExternalSort(sortDirectory, config.sortBytes())) {
            BaselineOperations.of(trace)
                    .read(
                            operation -> {
                                if (operation.kind() != IndexOperationTrace.INSERT
                                        || operation.version() != 0)
                                    throw new IOException("Baseline contains a non-insert");
                                long prefix =
                                        ((long) operation.table() << 32)
                                                | (operation.index() & 0xffffffffL);
                                byte[] key = composite(operation);
                                byte[] record = new byte[key.length + 8];
                                java.nio.ByteBuffer.wrap(record)
                                        .putInt(operation.table())
                                        .putInt(operation.index());
                                System.arraycopy(key, 0, record, 8, key.length);
                                sort.add(record, new byte[0]);
                            });
            if (expectedOperations >= 0 && sort.count() != expectedOperations)
                throw new IOException(
                        "Baseline operation count differs from manifest: "
                                + sort.count()
                                + " / "
                                + expectedOperations);
            try (var sorted = sort.sorted()) {
                long current = -1;
                LayoutBuilder builder = null;
                while (sorted.hasNext()) {
                    byte[] record = sorted.peekKey();
                    int table = java.nio.ByteBuffer.wrap(record).getInt();
                    int index = java.nio.ByteBuffer.wrap(record, 4, 4).getInt();
                    long prefix = ((long) table << 32) | (index & 0xffffffffL);
                    if (prefix != current) {
                        current = prefix;
                        builder = builders.computeIfAbsent(prefix, ignored -> new LayoutBuilder(config.entriesPerBlock()));
                    }
                    builder.add(Arrays.copyOfRange(record, 8, record.length));
                    sorted.advance();
                }
            }
        }
        var layouts = new LinkedHashMap<Long, IndexLayout>();
        for (var definition : catalog.definitions()) {
            long prefix =
                    ((long) definition.tableId() << 32) | (definition.indexId() & 0xffffffffL);
            var builder = builders.get(prefix);
            long entries = builder == null ? 0 : builder.entries;
            int blocks = (int) ((entries + config.entriesPerBlock - 1) / config.entriesPerBlock);
            int files = (int) ((blocks + (long) config.blocksPerFile - 1) / config.blocksPerFile);
            layouts.put(
                    prefix,
                    new IndexLayout(
                            definition.tableId(),
                            definition.indexId(),
                            definition.name(),
                            entries,
                            blocks,
                            files,
                            builder == null ? null : builder.min,
                            builder == null ? null : builder.max,
                            builder == null ? new byte[0][] : builder.firstKeys.toArray(new byte[0][])));
        }
        deleteTree(sortDirectory);
        return layouts;
    }

    private static Map<String, Object> analyze(
            Path trace,
            JsonObject manifest,
            IndexCatalog catalog,
            Map<Long, IndexLayout> layouts,
            long baselineRows,
            Config config,
            Path output)
            throws IOException {
        RunTiming.current().phase("locality-workload");
        var report =
                analyzeSource(
                        catalog.definitions(),
                        layouts,
                        baselineRows,
                        config,
                        IndexOperationTrace.Source.file(trace.resolve("workload.ops"),
                                manifest.get("workload_trace_sha256").getAsString()),
                        output);
        long encoded = ((Number) report.get("encoded_records")).longValue();
        long expected = manifest.get("workload_operations").getAsLong();
        if (encoded != expected)
            throw new IOException(
                    "Workload operation count differs from manifest: " + encoded + " / " + expected);
        report.put(
                "indexes",
                catalog.definitions().stream()
                        .map(
                                definition -> {
                                    var layout =
                                            layouts.get(
                                                    ((long) definition.tableId() << 32)
                                                            | (definition.indexId() & 0xffffffffL));
                                    var item = new LinkedHashMap<String, Object>();
                                    item.put("table_id", definition.tableId());
                                    item.put("table", definition.table());
                                    item.put("index_id", definition.indexId());
                                    item.put("name", definition.name());
                                    item.put("entries", layout.entries());
                                    item.put("blocks", layout.blocks());
                                    item.put("files", layout.files());
                                    item.put("empty", layout.empty());
                                    item.put("min_key", layout.empty() ? null : hex(layout.min()));
                                    item.put("max_key", layout.empty() ? null : hex(layout.max()));
                                    return item;
                                })
                        .toList());
        return report;
    }

    static Map<String, Object> analyzeSource(
            List<IndexCatalog.Definition> definitions,
            Map<Long, IndexLayout> layouts,
            long baselineRows,
            Config config,
            IndexOperationTrace.Source workload,
            Path output)
            throws IOException {
        var windows = new WindowWriter(output, definitions, layouts, config, baselineRows);
        long[] encoded = {0};
        workload.read(
                operation -> {
                    encoded[0]++;
                    windows.accept(operation);
                });
        windows.finish();
        var report = new LinkedHashMap<String, Object>();
        report.put("encoded_records", encoded[0]);
        Map<String, Object> business = windows.businessByKind(), secondary = windows.secondaryByKind();
        report.put("business_changes", business);
        report.put("secondary_index_maintenance_operations", secondary);
        report.put(
                "all_index_operations",
                Map.of(
                        "insert",
                        ((Long) business.get("insert")) + ((Long) secondary.get("insert")),
                        "update",
                        ((Long) business.get("update")) + ((Long) secondary.get("update")),
                        "delete",
                        ((Long) business.get("delete")) + ((Long) secondary.get("delete"))));
        report.put("searched_accesses", windows.searches);
        report.put("transaction_control_records", windows.controls);
        report.put("windows", windows.windows);
        report.put("last_window_business_changes", windows.lastWindowBusiness);
        report.put("l0_batch_rule",
                "consecutive all-index maintenance operations; a batch closes before the next source"
                        + " maintenance unit once the configured count is reached; no same-key dedup");
        report.put("baseline_rows", baselineRows);
        report.put("progress_checkpoints", windows.checkpoints(baselineRows));
        return report;
    }

    private static final class WindowWriter {
        private final Path output;
        private final List<IndexCatalog.Definition> definitions;
        private final Map<Long, IndexLayout> layouts;
        private final Config config;
        private final long baselineRows;
        private final BufferedWriter windowsCsv;
        private final BufferedWriter blocksCsv;
        private final BufferedWriter heatmapCsv;
        private final BufferedWriter heatmapKindCsv;
        private final L0Tracker l0;
        private final List<WindowSummary> summaries = new ArrayList<>();
        private final Map<Long, BitSet> previousHot = new LinkedHashMap<>();
        private final Map<Long, BitSet> previousHotBusiness = new LinkedHashMap<>();
        private final Map<Long, BitSet> cumulativeBlocks = new LinkedHashMap<>();
        private final Map<Long, BitSet> cumulativeBlocksBusiness = new LinkedHashMap<>();
        private final long[] businessKinds = new long[3], secondaryKinds = new long[3];
        private long searches, controls, businessCount, windows, lastWindowBusiness, cumulativeBusiness;
        private Window current;
        private int currentWindow;

        WindowWriter(
                Path output,
                List<IndexCatalog.Definition> definitions,
                Map<Long, IndexLayout> layouts,
                Config config,
                long baselineRows)
                throws IOException {
            this.output = output;
            this.definitions = definitions;
            this.layouts = layouts;
            this.config = config;
            this.baselineRows = baselineRows;
            windowsCsv = writer(output.resolve("windows.csv"));
            blocksCsv = gzipWriter(output.resolve("blocks.csv.gz"));
            heatmapCsv = gzipWriter(output.resolve("heatmap.csv.gz"));
            heatmapKindCsv = gzipWriter(output.resolve("heatmap-kind.csv.gz"));
            l0 = new L0Tracker(output.resolve("l0-batches.csv"), definitions, layouts, config);
            windowsCsv.write(
                    "window,table,index,name,category,modifications,unique_keys,blocks,hit_blocks,"
                            + "files,hit_files,blocks_for_50pct,blocks_for_90pct,top_1pct_share,"
                            + "top_10pct_share,overflow_left,overflow_right,empty_keys,hot_overlap,"
                            + "cumulative_business_changes,cumulative_business_ratio,"
                            + "cumulative_hit_blocks,cumulative_block_ratio\n");
            blocksCsv.write("window,table,index,category,block,count\n");
            heatmapCsv.write("window,table,index,category,bin,count\n");
            heatmapKindCsv.write("window,table,index,category,bin,count\n");
        }

        void accept(Operation operation) throws IOException {
            if (IndexOperationTrace.isControl(operation)) {
                controls++;
                return;
            }
            if (operation.kind() == IndexOperationTrace.SEARCH) {
                searches++;
                return;
            }
            int category = category(operation);
            l0.before(operation);
            if (operation.index() == 0 && operation.expansionPosition() == 0) {
                businessCount++;
                businessKinds[category]++;
                int wanted = (int) ((businessCount - 1) / config.businessChangesPerWindow) + 1;
                if (current == null) {
                    currentWindow = wanted;
                    startWindow();
                } else if (wanted != currentWindow) {
                    flush();
                    currentWindow = wanted;
                    startWindow();
                }
            } else if (current == null)
                throw new IOException("Index maintenance before the first business change");
            if (operation.index() == 0) current.businessOps++;
            else {
                secondaryKinds[category == 3 ? 0 : category == 4 ? 1 : 2]++;
                current.secondaryOps++;
            }
            current.add(operation, category);
            l0.add(operation);
        }

        private void startWindow() {
            current = new Window(currentWindow, config, definitions, layouts);
        }

        void finish() throws IOException {
            flush();
            windowsCsv.close();
            blocksCsv.close();
            heatmapCsv.close();
            heatmapKindCsv.close();
            l0.finish();
            if (current != null) throw new IllegalStateException("Window left open");
        }

        private void flush() throws IOException {
            if (current == null) return;
            long cumulative = cumulativeBusiness + current.businessOps;
            double cumulativeRatio = baselineRows <= 0 ? 0.0 : (double) cumulative / baselineRows;
            current.write(
                    windowsCsv,
                    blocksCsv,
                    heatmapCsv,
                    heatmapKindCsv,
                    previousHot,
                    previousHotBusiness,
                    cumulativeBlocks,
                    cumulativeBlocksBusiness,
                    cumulative,
                    cumulativeRatio);
            windows++;
            lastWindowBusiness = current.businessOps;
            cumulativeBusiness = cumulative;
            summaries.add(new WindowSummary(current.id, cumulativeBusiness));
            current = null;
        }

        Map<String, Object> businessByKind() {
            return Map.of(
                    "insert", businessKinds[0],
                    "update", businessKinds[1],
                    "delete", businessKinds[2]);
        }

        Map<String, Object> secondaryByKind() {
            return Map.of(
                    "insert", secondaryKinds[0],
                    "update", secondaryKinds[1],
                    "delete", secondaryKinds[2]);
        }

        Map<String, Object> checkpoints(long baselineRows) {
            var result = new LinkedHashMap<String, Object>();
            if (baselineRows <= 0) return result;
            for (double fraction : new double[] {0.01, 0.05, 0.10, 0.50}) {
                String key = Math.round(fraction * 100) + "%";
                long required = (long) Math.ceil(baselineRows * fraction);
                var reached =
                        summaries.stream()
                                .filter(summary -> summary.business >= required)
                                .findFirst()
                                .orElse(null);
                result.put(
                        key,
                        reached == null
                                ? Map.of("status", "N/A", "required_business_changes", required)
                                : Map.of(
                                        "status", "reached",
                                        "window", reached.window,
                                        "business_changes", reached.business,
                                        "required_business_changes", required));
            }
            return result;
        }
    }

    private record WindowSummary(int window, long business) {}

    private static final class Window {
        private final int id;
        private final Config config;
        private final List<IndexCatalog.Definition> definitions;
        private final Map<Long, IndexLayout> layouts;
        private final Map<Long, IndexWindow> indexes = new TreeMap<>();
        long businessOps, secondaryOps;

        Window(
                int id,
                Config config,
                List<IndexCatalog.Definition> definitions,
                Map<Long, IndexLayout> layouts) {
            this.id = id;
            this.config = config;
            this.definitions = definitions;
            this.layouts = layouts;
            for (var definition : definitions) {
                long key = ((long) definition.tableId() << 32) | definition.indexId();
                indexes.put(key, new IndexWindow(layouts.get(key)));
            }
        }

        void add(Operation operation, int category) throws IOException {
            long key = ((long) operation.table() << 32) | operation.index();
            var index = indexes.get(key);
            if (index == null) throw new IOException("Operation names an index outside the catalog");
            index.add(operation, category);
        }

        void write(
                BufferedWriter windowsCsv,
                BufferedWriter blocksCsv,
                BufferedWriter heatmapCsv,
                BufferedWriter heatmapKindCsv,
                Map<Long, BitSet> previousHot,
                Map<Long, BitSet> previousHotBusiness,
                Map<Long, BitSet> cumulativeBlocks,
                Map<Long, BitSet> cumulativeBlocksBusiness,
                long cumulative,
                double cumulativeRatio)
                throws IOException {
            for (var entry : indexes.entrySet()) {
                long key = entry.getKey();
                var definition =
                        definitions.stream()
                                .filter(
                                        item ->
                                                (((long) item.tableId() << 32)
                                                                | (item.indexId() & 0xffffffffL))
                                                        == key)
                                .findFirst()
                                .orElseThrow();
                var index = entry.getValue();
                index.write(
                        id,
                        definition,
                        windowsCsv,
                        blocksCsv,
                        heatmapCsv,
                        heatmapKindCsv,
                        config,
                        previousHot,
                        previousHotBusiness,
                        cumulativeBlocks,
                        cumulativeBlocksBusiness,
                        businessOps,
                        secondaryOps,
                        cumulative,
                        cumulativeRatio);
            }
        }
    }

    private static final class IndexWindow {
        private static final String[] CATEGORY =
                {"business_insert", "business_update", "business_delete",
                    "secondary_index_insert", "secondary_index_update", "secondary_index_delete"};
        private final IndexLayout layout;
        private final TreeMap<Integer, long[]> counts = new TreeMap<>();
        private final LongSet businessKeys = new LongSet(), secondaryKeys = new LongSet();
        private final long[] overflowLeft = new long[6], overflowRight = new long[6], empty = new long[6];

        IndexWindow(IndexLayout layout) {
            this.layout = layout;
        }

        void add(Operation operation, int category) {
            byte[] key = composite(operation);
            int block = layout.blockOf(key);
            long fingerprint = KeyHash.of(key);
            if (category < 3) businessKeys.add(fingerprint);
            else secondaryKeys.add(fingerprint);
            if (block == -1) {
                empty[category]++;
                return;
            }
            if (block == -2) {
                overflowLeft[category]++;
                return;
            }
            if (block == -3) {
                overflowRight[category]++;
                return;
            }
            counts.computeIfAbsent(block, ignored -> new long[6])[category]++;
        }

        void write(
                int window,
                IndexCatalog.Definition definition,
                BufferedWriter windowsCsv,
                BufferedWriter blocksCsv,
                BufferedWriter heatmapCsv,
                BufferedWriter heatmapKindCsv,
                Config config,
                Map<Long, BitSet> previousHot,
                Map<Long, BitSet> previousHotBusiness,
                Map<Long, BitSet> cumulativeBlocks,
                Map<Long, BitSet> cumulativeBlocksBusiness,
                long businessOps,
                long secondaryOps,
                long cumulative,
                double cumulativeRatio)
                throws IOException {
            for (int group = 0; group < 2; group++) {
                boolean business = group == 0;
                int from = business ? 0 : 3, to = business ? 3 : 6;
                var blocks = new TreeMap<Integer, Long>();
                for (var entry : counts.entrySet()) {
                    long sum = 0;
                    for (int category = from; category < to; category++) sum += entry.getValue()[category];
                    if (sum != 0) blocks.put(entry.getKey(), sum);
                }
                var bins = new TreeMap<Integer, Long>();
                for (var entry : blocks.entrySet())
                    bins.merge(
                            bin(entry.getKey(), layout.blocks(), config.heatmapBins),
                            entry.getValue(),
                            Long::sum);
                long modifications = blocks.values().stream().mapToLong(Long::longValue).sum();
                long unique = business ? businessKeys.size() : secondaryKeys.size();
                long overflowL = 0, overflowR = 0, emptyCount = 0;
                for (int category = from; category < to; category++) {
                    overflowL += overflowLeft[category];
                    overflowR += overflowRight[category];
                    emptyCount += empty[category];
                }
                var ordered = new ArrayList<>(blocks.values());
                ordered.sort(Comparator.reverseOrder());
                long p50 = blocksForShare(ordered, modifications, 0.50);
                long p90 = blocksForShare(ordered, modifications, 0.90);
                double top1 = topShare(ordered, layout.blocks(), modifications, 0.01);
                double top10 = topShare(ordered, layout.blocks(), modifications, 0.10);
                long key = ((long) definition.tableId() << 32) | definition.indexId();
                Map<Long, BitSet> previous = business ? previousHotBusiness : previousHot;
                Map<Long, BitSet> cumulativeSet =
                        business ? cumulativeBlocksBusiness : cumulativeBlocks;
                BitSet hot = hotBlocks(blocks, modifications);
                double overlap = jaccard(previous.get(key), hot);
                previous.put(key, hot);
                var hit = new BitSet();
                blocks.keySet().forEach(hit::set);
                var all = cumulativeSet.computeIfAbsent(key, ignored -> new BitSet());
                all.or(hit);
                long cumulativeHitBlocks = all.cardinality();
                double cumulativeBlockRatio =
                        layout.blocks() == 0 ? 0.0 : (double) cumulativeHitBlocks / layout.blocks();
                windowsCsv.write(
                        csv(
                                window,
                                definition.tableId(),
                                definition.indexId(),
                                definition.name(),
                                business ? "business" : "secondary_index",
                                modifications,
                                unique,
                                layout.blocks(),
                                blocks.size(),
                                layout.files(),
                                hitFiles(blocks, config.blocksPerFile),
                                p50,
                                p90,
                                top1,
                                top10,
                                overflowL,
                                overflowR,
                                emptyCount,
                                overlap,
                                cumulative,
                                cumulativeRatio,
                                cumulativeHitBlocks,
                                cumulativeBlockRatio));
                for (var entry : blocks.entrySet()) {
                    for (int category = from; category < to; category++) {
                        long count = counts.get(entry.getKey())[category];
                        if (count == 0) continue;
                        blocksCsv.write(
                                csv(
                                        window,
                                        definition.tableId(),
                                        definition.indexId(),
                                        CATEGORY[category],
                                        entry.getKey(),
                                        count));
                    }
                }
                for (var entry : bins.entrySet()) {
                    heatmapCsv.write(
                            csv(
                                    window,
                                    definition.tableId(),
                                    definition.indexId(),
                                    business ? "business" : "secondary_index",
                                    entry.getKey(),
                                    entry.getValue()));
                }
                for (int category = from; category < to; category++) {
                    var kindBins = new TreeMap<Integer, Long>();
                    for (var entry : counts.entrySet()) {
                        long count = entry.getValue()[category];
                        if (count == 0) continue;
                        kindBins.merge(
                                bin(entry.getKey(), layout.blocks(), config.heatmapBins),
                                count,
                                Long::sum);
                    }
                    for (var entry : kindBins.entrySet())
                        heatmapKindCsv.write(
                                csv(
                                        window,
                                        definition.tableId(),
                                        definition.indexId(),
                                        CATEGORY[category],
                                        entry.getKey(),
                                        entry.getValue()));
                }
            }
        }
    }

    private static final class L0Tracker {
        private final BufferedWriter writer;
        private final List<IndexCatalog.Definition> definitions;
        private final Map<Long, IndexLayout> layouts;
        private final Config config;
        private final Map<Long, BatchIndex> indexes = new TreeMap<>();
        private long entries;
        private int batch;

        L0Tracker(
                Path path,
                List<IndexCatalog.Definition> definitions,
                Map<Long, IndexLayout> layouts,
                Config config)
                throws IOException {
            this.writer = writer(path);
            this.definitions = definitions;
            this.layouts = layouts;
            this.config = config;
            writer.write(
                    "batch,table,index,name,entries,unique_keys,min_block,max_block,span_blocks,"
                            + "hit_blocks,span_files,hit_files,l1_files,hit_block_ratio,"
                            + "span_file_fraction,overflow_left,overflow_right,empty_keys\n");
        }

        /** A new source maintenance unit starts only after the previous one is complete. */
        void before(Operation operation) throws IOException {
            if (entries >= config.l0IndexModifications()
                    && operation.index() == 0
                    && operation.expansionPosition() == 0) flush();
        }

        void add(Operation operation) throws IOException {
            long key = ((long) operation.table() << 32) | operation.index();
            var layout = layouts.get(key);
            if (layout == null) throw new IOException("L0 operation names an unknown index");
            indexes.computeIfAbsent(key, ignored -> new BatchIndex(layout)).add(operation);
            entries++;
        }

        void finish() throws IOException {
            flush();
            writer.close();
        }

        private void flush() throws IOException {
            if (entries == 0) return;
            batch++;
            for (var definition : definitions) {
                long key = ((long) definition.tableId() << 32) | definition.indexId();
                var index = indexes.get(key);
                if (index == null || index.entries == 0) continue;
                int min = index.minBlock, max = index.maxBlock;
                int spanBlocks = max < 0 ? 0 : max - min + 1;
                int spanFiles =
                        max < 0
                                ? 0
                                : max / config.blocksPerFile() - min / config.blocksPerFile() + 1;
                var files = new java.util.TreeSet<Integer>();
                for (int block : index.blocks.keySet()) files.add(block / config.blocksPerFile());
                int l1Files = index.layout.files();
                writer.write(
                        csv(
                                batch,
                                definition.tableId(),
                                definition.indexId(),
                                definition.name(),
                                index.entries,
                                index.keys.size(),
                                max < 0 ? "" : min,
                                max < 0 ? "" : max,
                                spanBlocks,
                                index.blocks.size(),
                                spanFiles,
                                files.size(),
                                l1Files,
                                index.layout.blocks() == 0
                                        ? 0.0
                                        : (double) index.blocks.size() / index.layout.blocks(),
                                l1Files == 0 ? 0.0 : (double) spanFiles / l1Files,
                                index.overflowLeft,
                                index.overflowRight,
                                index.empty));
            }
            indexes.clear();
            entries = 0;
        }

        private static final class BatchIndex {
            private final IndexLayout layout;
            private final TreeMap<Integer, Long> blocks = new TreeMap<>();
            private final LongSet keys = new LongSet();
            private long entries, overflowLeft, overflowRight, empty;
            private int minBlock = Integer.MAX_VALUE, maxBlock = -1;

            BatchIndex(IndexLayout layout) {
                this.layout = layout;
            }

            void add(Operation operation) {
                entries++;
                byte[] key = composite(operation);
                keys.add(KeyHash.of(key));
                int block = layout.blockOf(key);
                if (block == -1) {
                    empty++;
                    return;
                }
                if (block == -2) {
                    overflowLeft++;
                    return;
                }
                if (block == -3) {
                    overflowRight++;
                    return;
                }
                blocks.merge(block, 1L, Long::sum);
                minBlock = Math.min(minBlock, block);
                maxBlock = Math.max(maxBlock, block);
            }
        }
    }

    private static long blocksForShare(List<Long> descending, long total, double share) {
        if (total == 0) return 0;
        long needed = Math.max(1, (long) Math.ceil(total * share));
        long seen = 0;
        for (int i = 0; i < descending.size(); i++) {
            seen += descending.get(i);
            if (seen >= needed) return i + 1;
        }
        return descending.size();
    }

    private static double topShare(List<Long> descending, int totalBlocks, long total, double share) {
        if (total == 0 || totalBlocks == 0) return 0;
        long take = Math.max(1, (long) Math.ceil(totalBlocks * share));
        long seen = 0;
        for (int i = 0; i < descending.size() && i < take; i++) seen += descending.get(i);
        return (double) seen / total;
    }

    private static BitSet hotBlocks(Map<Integer, Long> blocks, long total) {
        var result = new BitSet();
        if (total == 0) return result;
        int take = Math.max(1, (int) Math.ceil(blocks.size() * 0.01));
        blocks.entrySet().stream()
                .sorted(
                        Comparator.<Map.Entry<Integer, Long>>comparingLong(Map.Entry::getValue)
                                .reversed()
                                .thenComparingInt(Map.Entry::getKey))
                .limit(take)
                .forEach(entry -> result.set(entry.getKey()));
        return result;
    }

    private static double jaccard(BitSet previous, BitSet current) {
        if (previous == null) return -1;
        var union = (BitSet) previous.clone();
        union.or(current);
        if (union.isEmpty()) return 1;
        var intersection = (BitSet) previous.clone();
        intersection.and(current);
        return (double) intersection.cardinality() / union.cardinality();
    }

    private static long hitFiles(Map<Integer, Long> blocks, int blocksPerFile) {
        return blocks.keySet().stream().mapToLong(block -> block / blocksPerFile).distinct().count();
    }

    private static int bin(int block, int blocks, int bins) {
        if (blocks <= 0) return 0;
        return Math.min(bins - 1, (int) ((long) block * bins / blocks));
    }

    private static int category(Operation operation) throws IOException {
        if (operation.index() == 0) {
            if (operation.kind() == IndexOperationTrace.LOGICAL_DELETE) return 2;
            if (operation.kind() == IndexOperationTrace.UPSERT)
                return (operation.flags() & IndexOperationTrace.BUSINESS_INSERT) != 0 ? 0 : 1;
        } else {
            if (operation.kind() == IndexOperationTrace.LOGICAL_DELETE) return 5;
            if (operation.kind() == IndexOperationTrace.UPSERT)
                return (operation.flags() & IndexOperationTrace.BUSINESS_INSERT) != 0 ? 3 : 4;
            if (operation.kind() == IndexOperationTrace.INSERT) return 3;
        }
        throw new IOException(
                "Unsupported index maintenance kind for locality: "
                        + IndexOperationTrace.kindName(operation.kind()));
    }

    private static byte[] composite(Operation operation) {
        if (operation.index() == 0) return operation.key().bytes();
        byte[] value = operation.indexedValue().bytes(), key = operation.key().bytes();
        byte[] result = new byte[value.length + key.length];
        System.arraycopy(value, 0, result, 0, value.length);
        System.arraycopy(key, 0, result, value.length, key.length);
        return result;
    }

    private static BufferedWriter writer(Path path) throws IOException {
        return Files.newBufferedWriter(path, StandardCharsets.UTF_8);
    }

    private static BufferedWriter gzipWriter(Path path) throws IOException {
        return new BufferedWriter(
                new OutputStreamWriter(
                        new GZIPOutputStream(Files.newOutputStream(path)), StandardCharsets.UTF_8));
    }

    private static String csv(Object... values) {
        var line = new StringBuilder();
        for (int i = 0; i < values.length; i++) {
            if (i > 0) line.append(',');
            Object value = values[i];
            if (value instanceof Double number) line.append(String.format(java.util.Locale.ROOT, "%.6f", number));
            else line.append(value);
        }
        return line.append('\n').toString();
    }

    private static String hex(byte[] bytes) {
        return java.util.HexFormat.of().formatHex(bytes);
    }

    private static void deleteTree(Path root) throws IOException {
        if (!Files.exists(root)) return;
        try (var paths = Files.walk(root)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList())
                Files.deleteIfExists(path);
        }
    }

    /** A small open-addressed set of 64-bit key fingerprints; enough for window-local counts. */
    private static final class LongSet {
        private long[] values = new long[1024];
        private int size;

        void add(long value) {
            if (value == 0) value = 1;
            if ((size + 1) * 2 >= values.length) grow();
            int mask = values.length - 1, at = slot(value, mask);
            while (values[at] != 0) {
                if (values[at] == value) return;
                at = (at + 1) & mask;
            }
            values[at] = value;
            size++;
        }

        int size() {
            return size;
        }

        private int slot(long value, int mask) {
            long mixed = value * 0x9e3779b97f4a7c15L;
            return (int) (mixed ^ (mixed >>> 32)) & mask;
        }

        private void grow() {
            long[] old = values;
            values = new long[old.length * 2];
            for (long value : old) {
                if (value == 0) continue;
                int mask = values.length - 1, at = slot(value, mask);
                while (values[at] != 0) at = (at + 1) & mask;
                values[at] = value;
            }
        }
    }
}
