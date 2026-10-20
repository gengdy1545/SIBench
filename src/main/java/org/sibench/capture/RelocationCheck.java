package org.sibench.capture;

import com.google.gson.*;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.MessageType;
import org.sibench.capture.IndexOperationTrace.Operation;
import org.sibench.generation.*;

import java.nio.file.*;
import java.util.*;

/** An explicitly forced layout-only control, not a naturally triggered 70% rewrite. */
public final class RelocationCheck {
    private RelocationCheck() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 4)
            throw new IllegalArgumentException("RelocationCheck ROOT CONFIG NORMALIZED OUTPUT");
        var config = CaptureConfig.read(Path.of(args[0]), Path.of(args[1]));
        Path input = Path.of(args[2]), output = Path.of(args[3]);
        Files.createDirectory(output);
        Map<Long, Group> latest = new TreeMap<>();
        try (ParquetReader<Group> reader =
                ParquetReader.builder(
                                new GroupReadSupport(),
                                new org.apache.hadoop.fs.Path(
                                        input.resolve("account-versions.parquet").toUri()))
                        .build()) {
            Group row;
            while ((row = reader.read()) != null) latest.put(row.getLong("accountid", 0), row);
        }
        if (latest.isEmpty()) throw new IllegalStateException("No modified accounts");
        Group first = latest.values().iterator().next();
        MessageType schema = new MessageType("savingaccount", first.getType().getFields());
        long newFile = 11, position = 0;
        Path rewritten = output.resolve("account-retained.parquet");
        try (var writer =
                        new ParquetDataFile(
                                rewritten,
                                new TableData(
                                        "savingaccount",
                                        latest.size(),
                                        0,
                                        "accountid",
                                        schema,
                                        null),
                                config.layout(),
                                Map.of(
                                        "sibench.control",
                                        "forced-layout-only",
                                        "sibench.file_id",
                                        Long.toString(newFile)));
                var trace = new IndexOperationTrace.Writer(output.resolve("relocation.ops"))) {
            for (Group row : latest.values()) {
                if (writer.write(row))
                    throw new IllegalStateException("Control requires file rollover support");
                trace.append(
                        new Operation(
                                0,
                                IndexOperationTrace.RELOCATE,
                                3,
                                0,
                                0,
                                row.getLong("accountid", 0),
                                row.getLong("_sib_timestamp", 0),
                                newFile,
                                position++,
                                row.getLong("_sib_row_id", 0),
                                0,
                                0,
                                0));
            }
            writer.finish();
        }
        long read = 0;
        try (ParquetReader<Group> reader =
                ParquetReader.builder(
                                new GroupReadSupport(),
                                new org.apache.hadoop.fs.Path(rewritten.toUri()))
                        .build()) {
            Group row;
            for (Group expected : latest.values()) {
                row = reader.read();
                if (row == null || !row.toString().equals(expected.toString()))
                    throw new IllegalStateException("Relocation changed logical content");
                read++;
            }
            if (reader.read() != null) throw new IllegalStateException("Extra retained rows");
        }
        List<Map<String, Object>> results = new ArrayList<>();
        for (boolean rowId : new boolean[] {false, true}) {
            var model =
                    new IndexReferenceReplay(
                            rowId,
                            true,
                            IndexOperationTrace.formatVersion(input.resolve("workload.ops")) >= 3);
            IndexOperationTrace.readExpanded(input.resolve("baseline.ops"), model::accept);
            var runtime =
                    new org.sibench.replay.ReplayProjection(
                            org.sibench.replay.ReplayConfig.defaults(),
                            IndexOperationTrace.formatVersion(input.resolve("workload.ops")) >= 3,
                            model::accept);
            IndexOperationTrace.readExpanded(input.resolve("workload.ops"), runtime);
            runtime.finish();
            IndexOperationTrace.read(output.resolve("relocation.ops"), model::accept);
            long index = 0;
            for (Group row : latest.values()) {
                long value =
                        rowId
                                ? row.getLong("_sib_row_id", 0)
                                : IndexReferenceReplay.locator(newFile, index);
                if (model.value(3, Math.toIntExact(row.getLong("accountid", 0))) != value)
                    throw new IllegalStateException("Relocated value mismatch");
                index++;
            }
            if (model.relocationWrites() != (rowId ? 0 : latest.size()))
                throw new IllegalStateException("Incorrect maintenance count");
            results.add(
                    Map.of(
                            "mode",
                            rowId ? "rowid" : "locator",
                            "relocation_writes",
                            model.relocationWrites()));
        }
        Map<String, Object> report = new LinkedHashMap<>();
        report.put("status", "passed");
        report.put("retained_rows", read);
        report.put("results", results);
        report.put("data_sha256", BaselineGenerator.sha256(rewritten));
        report.put("trace_sha256", BaselineGenerator.sha256(output.resolve("relocation.ops")));
        report.put(
                "scope",
                "forced latest-only layout control over modified accounts; no snapshots retained;"
                        + " not a naturally triggered validity rewrite, no old files reclaimed");
        Files.writeString(
                output.resolve("report.json"),
                new GsonBuilder().setPrettyPrinting().create().toJson(report),
                StandardOpenOption.CREATE_NEW);
    }
}
