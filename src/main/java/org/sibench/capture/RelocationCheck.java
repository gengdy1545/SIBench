package org.sibench.capture;

import com.google.gson.*;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.MessageType;
import org.sibench.capture.IndexOperationTrace.Operation;
import org.sibench.generation.*;
import org.sibench.replay.*;

import java.nio.file.*;
import java.util.*;

/** Manifest-driven, forced latest-only relocation; not natural rewrite, retention or GC. */
public final class RelocationCheck {
    private record Key(int table, long key) {}

    private record Retained(Group row, long file, long position) {}

    private RelocationCheck() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 4)
            throw new IllegalArgumentException("RelocationCheck ROOT CONFIG NORMALIZED OUTPUT");
        var capture = CaptureConfig.read(Path.of(args[0]), Path.of(args[1]));
        validate(capture, Path.of(args[2]), Path.of(args[3]));
    }

    public static void validate(CaptureConfig capture, Path input, Path output) throws Exception {
        ReferenceReplay.check(input, ReplayConfig.defaults());
        JsonObject manifest =
                JsonParser.parseString(Files.readString(input.resolve("manifest.json")))
                        .getAsJsonObject();
        IndexCatalog catalog = IndexCatalog.fromManifest(manifest);
        Map<Key, Operation> latest = new LinkedHashMap<>();
        long[] maximumFile = {0};
        IndexOperationTrace.read(
                input.resolve("baseline.ops"),
                o -> maximumFile[0] = Math.max(maximumFile[0], o.file()));
        IndexOperationTrace.read(
                input.resolve("workload.ops"),
                o -> {
                    maximumFile[0] = Math.max(maximumFile[0], o.file());
                    if (IndexOperationTrace.isMutation(o) && o.index() == 0)
                        latest.put(new Key(o.table(), o.key()), o);
                });
        Map<Integer, SortedMap<Long, Group>> rows = new TreeMap<>();
        for (var item : manifest.getAsJsonArray("incremental_files")) {
            JsonObject file = item.getAsJsonObject();
            Path path = input.resolve(file.get("path").getAsString());
            if (!BaselineGenerator.sha256(path).equals(file.get("sha256").getAsString()))
                throw new java.io.IOException("Incremental checksum mismatch");
            int table = IndexCatalog.tableId(file.get("table").getAsString());
            long fileId = file.get("file_id").getAsLong(), position = 0;
            try (var reader = reader(path)) {
                Group row;
                while ((row = reader.read()) != null) {
                    long key =
                            row.getLong(
                                    PgBaselineExport.TABLES.get(file.get("table").getAsString()),
                                    0);
                    Operation operation = latest.get(new Key(table, key));
                    if (operation != null
                            && operation.kind() != IndexOperationTrace.LOGICAL_DELETE
                            && operation.file() == fileId
                            && operation.position() == position)
                        rows.computeIfAbsent(table, ignored -> new TreeMap<>()).put(key, row);
                    position++;
                }
            }
        }
        if (rows.isEmpty()) throw new java.io.IOException("No live modified rows to relocate");
        Files.createDirectory(output);
        Files.writeString(output.resolve("INCOMPLETE"), "Relocation control not verified\n");
        var retained = new LinkedHashMap<Key, Retained>();
        var files = new ArrayList<Map<String, Object>>();
        long indexEntries = 0;
        try (var trace = new IndexOperationTrace.Writer(output.resolve("relocation.ops"))) {
            for (var tableRows : rows.entrySet()) {
                String table =
                        catalog.definitions().stream()
                                .filter(d -> d.tableId() == tableRows.getKey() && d.indexId() == 0)
                                .findFirst()
                                .orElseThrow()
                                .table();
                long fileId = Math.incrementExact(maximumFile[0]);
                maximumFile[0] = fileId;
                Path path = output.resolve(table + "-retained.parquet");
                var schema =
                        new MessageType(
                                table,
                                tableRows
                                        .getValue()
                                        .values()
                                        .iterator()
                                        .next()
                                        .getType()
                                        .getFields());
                long position = 0;
                try (var writer =
                        new ParquetDataFile(
                                path,
                                new TableData(
                                        table,
                                        tableRows.getValue().size(),
                                        0,
                                        PgBaselineExport.TABLES.get(table),
                                        schema,
                                        null),
                                capture.layout(),
                                Map.of("sibench.control", "forced-layout-only"))) {
                    for (var rowEntry : tableRows.getValue().entrySet()) {
                        Group row = rowEntry.getValue();
                        Operation primary =
                                new Operation(
                                        0,
                                        IndexOperationTrace.RELOCATE,
                                        tableRows.getKey(),
                                        0,
                                        0,
                                        rowEntry.getKey(),
                                        row.getLong("_sib_timestamp", 0),
                                        fileId,
                                        position,
                                        row.getLong("_sib_row_id", 0),
                                        0,
                                        0,
                                        0);
                        IndexMutation.emit(
                                primary, Map.of(), catalog.keys(table, row), trace::append);
                        indexEntries += 1 + catalog.secondary(tableRows.getKey()).size();
                        retained.put(
                                new Key(tableRows.getKey(), rowEntry.getKey()),
                                new Retained(row, fileId, position++));
                        if (writer.write(row))
                            throw new java.io.IOException(
                                    "Relocation control rollover not implemented");
                    }
                    writer.finish();
                }
                try (var check = reader(path)) {
                    for (Group expected : tableRows.getValue().values()) {
                        Group actual = check.read();
                        if (actual == null || !actual.toString().equals(expected.toString()))
                            throw new java.io.IOException("Relocation changed row content");
                    }
                    if (check.read() != null) throw new java.io.IOException("Extra relocated rows");
                }
                files.add(
                        Map.of(
                                "table",
                                table,
                                "file_id",
                                fileId,
                                "rows",
                                position,
                                "path",
                                path.getFileName().toString(),
                                "sha256",
                                BaselineGenerator.sha256(path)));
            }
        }
        var results = new ArrayList<Map<String, Object>>();
        for (boolean rowIds : new boolean[] {false, true}) {
            var model = new IndexReferenceReplay(rowIds);
            try (var load = new ReplayProjection(ReplayConfig.defaults(), false, model::accept);
                    var runtime =
                            new ReplayProjection(ReplayConfig.defaults(), true, model::accept)) {
                IndexOperationTrace.read(input.resolve("baseline.ops"), load);
                load.finish();
                IndexOperationTrace.read(input.resolve("workload.ops"), runtime);
                runtime.finish();
                IndexOperationTrace.read(output.resolve("relocation.ops"), model::accept);
                for (var entry : retained.entrySet()) {
                    Retained row = entry.getValue();
                    Key key = entry.getKey();
                    long version = row.row().getLong("_sib_timestamp", 0);
                    long expected =
                            rowIds
                                    ? row.row().getLong("_sib_row_id", 0)
                                    : IndexReferenceReplay.locator(row.file(), row.position());
                    String table =
                            catalog.definitions().stream()
                                    .filter(d -> d.tableId() == key.table())
                                    .findFirst()
                                    .orElseThrow()
                                    .table();
                    var indexed = new LinkedHashMap<>(catalog.keys(table, row.row()));
                    indexed.put(0, null);
                    for (var index : indexed.entrySet()) {
                        var actual =
                                model.lookupEntry(
                                        key.table(),
                                        index.getKey(),
                                        key.key(),
                                        index.getValue(),
                                        version);
                        if (actual == null
                                || actual.value() != expected
                                || actual.commitVersion() != version)
                            throw new java.io.IOException("Relocated index value/version mismatch");
                    }
                }
                if (model.relocationWrites() != (rowIds ? 0 : indexEntries))
                    throw new java.io.IOException("Incorrect relocation maintenance count");
                results.add(
                        Map.of(
                                "mode",
                                rowIds ? "rowid" : "locator",
                                "relocation_writes",
                                model.relocationWrites()));
            }
        }
        Files.writeString(
                output.resolve("report.json"),
                new GsonBuilder()
                        .setPrettyPrinting()
                        .create()
                        .toJson(
                                Map.of(
                                        "status",
                                        "passed",
                                        "retained_rows",
                                        retained.size(),
                                        "retained_index_entries",
                                        indexEntries,
                                        "files",
                                        files,
                                        "results",
                                        results,
                                        "trace_sha256",
                                        BaselineGenerator.sha256(output.resolve("relocation.ops")),
                                        "scope",
                                        "Forced latest-only control over all live modified tables"
                                            + " and source indexes; no protected historical-version"
                                            + " rewrite, natural trigger, GC or old-file"
                                            + " reclamation.")),
                StandardOpenOption.CREATE_NEW);
        Files.delete(output.resolve("INCOMPLETE"));
    }

    private static ParquetReader<Group> reader(Path path) throws java.io.IOException {
        return ParquetReader.builder(
                        new GroupReadSupport(), new org.apache.hadoop.fs.Path(path.toUri()))
                .build();
    }
}
