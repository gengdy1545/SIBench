package org.sibench.generation;

import com.google.gson.GsonBuilder;
import java.nio.file.*;
import java.util.*;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageTypeParser;

/** Explicit bounded full-size check; excluded from ordinary unit tests. */
public final class ParquetSizingCheck {
    public static void main(String[] args) throws Exception {
        Path output = Files.createDirectory(Path.of(args[0]));
        long target = Long.parseLong(args[1]);
        int rows = 100000, width = 8192;
        var config = new GenerationConfig(Path.of(""), Map.of(), "ycsb", 71, target, 5,
                1048576, 67108864, 1048576, 1048576, true,
                CompressionCodecName.ZSTD, 3, 1, 0, output, 21474836480L);
        var schema = MessageTypeParser.parseMessageType("message t { required int64 id; required binary payload; }");
        var table = new TableData("t", rows, 1, "id", schema, null);
        var factory = new SimpleGroupFactory(schema);
        var random = new Random(71);
        var files = new ArrayList<Map<String, Object>>();
        int next = 0;
        long started = System.nanoTime();
        while (next < rows) {
            if (Files.getFileStore(output).getUsableSpace() < config.minFreeBytes()) throw new IllegalStateException("Disk reserve");
            Path file = output.resolve(files.size() + ".parquet");
            int first = next;
            try (var writer = new ParquetDataFile(file, table, config, Map.of("seed", "71"))) {
                while (next < rows) {
                    byte[] payload = new byte[width];
                    random.nextBytes(payload);
                    boolean full = writer.write(factory.newGroup().append("id", (long) next)
                            .append("payload", Binary.fromConstantByteArray(payload)));
                    if (++next % 10000 == 0) System.out.println("generated=" + next);
                    if (full) break;
                }
                writer.finish();
                if (Files.size(file) > target * 105 / 100) throw new AssertionError("overshoot");
                if (next < rows && Files.size(file) < target) throw new AssertionError("premature tail");
                files.add(Map.of("path", file.getFileName().toString(), "first_row", first,
                        "rows", next - first, "size", writer.sizing(), "sha256", BaselineGenerator.sha256(file)));
            }
        }
        double generateSeconds = (System.nanoTime() - started) / 1e9;
        started = System.nanoTime();
        random = new Random(71);
        next = 0;
        for (var entry : files) {
            Path file = output.resolve((String) entry.get("path"));
            try (var reader = ParquetReader.<Group>builder(new GroupReadSupport(),
                    new org.apache.hadoop.fs.Path(file.toUri())).build()) {
                Group row;
                while ((row = reader.read()) != null) {
                    byte[] expected = new byte[width];
                    random.nextBytes(expected);
                    if (row.getLong("id", 0) != next++ || !Arrays.equals(expected, row.getBinary("payload", 0).getBytes()))
                        throw new AssertionError("Independent payload/identity mismatch");
                }
            }
        }
        if (next != rows || files.size() < 2) throw new AssertionError("Incomplete sizing coverage");
        var report = Map.of("status", "passed", "target_bytes", target, "max_overshoot_percent", 5,
                "seed", 71, "rows", rows, "payload_bytes", width, "files", files,
                "generate_seconds", generateSeconds, "verify_seconds", (System.nanoTime() - started) / 1e9);
        Files.writeString(output.resolve("report.json"), new GsonBuilder().setPrettyPrinting().create().toJson(report));
    }
}
