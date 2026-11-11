package org.sibench.capture;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.pixelsdb.pixels.sink.SinkProto;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.DigestInputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.sibench.config.Digests;

/** Portable, closed row-CDC inputs. This is independent of the Index Trace format. */
public final class CdcInput {
    static final int MAX_FRAME = 16 << 20;
    private CdcInput() {}

    record Counts(long transactions, long changes, long businessChanges, long markers) {}
    private record Table(int id, int columns) {}
    record Verified(JsonObject manifest, Counts counts, String manifestSha256) {}

    interface Frame { void accept(int table, byte[] bytes) throws Exception; }

    /** Rechecks consumed file identities while forwarding a complete transaction prefix. */
    static Counts stream(Path directory, JsonObject document, long maximum, Frame target) throws Exception {
        long transactions = 0, changes = 0, business = 0, markers = 0;
        for (var item : document.getAsJsonArray("cdc_files")) {
            if (maximum > 0 && transactions == maximum) break;
            var file = item.getAsJsonObject();
            var digest = Digests.digest();
            long remaining = number(file, "bytes");
            try (var input = new DataInputStream(new DigestInputStream(Files.newInputStream(
                    resolve(directory, file.get("path").getAsString())), digest))) {
                while (remaining > 0) {
                    require(remaining >= 8, "Truncated CDC frame header");
                    int table = input.readInt(), size = input.readInt();
                    remaining -= 8;
                    require(size >= 0 && size <= MAX_FRAME && size <= remaining, "Invalid CDC frame during replay");
                    byte[] bytes = input.readNBytes(size);
                    require(bytes.length == size, "Truncated CDC replay frame");
                    remaining -= size;
                    if (maximum == 0 || transactions < maximum) {
                        target.accept(table, bytes);
                        if (table == -1) {
                            if (SinkProto.TransactionMetadata.parseFrom(bytes).getStatus() == SinkProto.TransactionStatus.END)
                                transactions++;
                        } else {
                            changes++;
                            if (SinkProto.RowRecord.parseFrom(bytes).getSource().getTable().equals("sibench_marker")) markers++;
                            else business++;
                        }
                    }
                }
                require(input.read() == -1, "CDC file grew during replay");
            }
            require(java.util.HexFormat.of().formatHex(digest.digest()).equals(file.get("sha256").getAsString()),
                    "CDC file changed during replay");
        }
        require(maximum == 0 || transactions == maximum, "Requested transaction prefix exceeds CDC input");
        return new Counts(transactions, changes, business, markers);
    }

    static Map<String, Object> publish(Path output, Generation.Input input, Counts expected,
            Map<String, Object> evidence) throws Exception {
        var document = new JsonObject();
        document.addProperty("kind", "pixels_cdc_input");
        document.addProperty("status", "complete");
        document.add("baseline_rows", new Gson().toJsonTree(input.baselineRows()));
        document.add("schema", file(output, output.resolve("pixels-schema.tsv")));
        document.add("source_ddl", file(output, output.resolve("schema.sql")));
        var baseline = new JsonObject();
        var original = JsonParser.parseString(Files.readString(output.resolve("load/inputs.json")))
                .getAsJsonObject().getAsJsonObject("data_sha256");
        Path data = output.resolve("native/Data_" + org.sibench.config.ConfigValues.string(input.nativeConfig(), "sf"));
        var csvTables = new HashMap<String, String>();
        for (String line : Files.readAllLines(input.source().source().resolve("conf/load_data_pg.sql"))) {
            if (line.isBlank()) continue;
            String[] copy = HybenchInputs.copyStatement(line);
            require(copy != null, "Invalid baseline load mapping");
            csvTables.put(copy[1], copy[0].toLowerCase(java.util.Locale.ROOT));
        }
        for (var entry : original.entrySet()) {
            var value = file(output, data.resolve(entry.getKey()));
            require(value.get("sha256").equals(entry.getValue()), "Baseline input changed after source loading: " + entry.getKey());
            if (csvTables.containsKey(entry.getKey())) {
                String table = csvTables.get(entry.getKey());
                value.addProperty("table", table);
                value.addProperty("rows", input.baselineRows().get(table));
            }
            baseline.add(entry.getKey(), value);
        }
        document.add("baseline_files", baseline);
        var inventory = JsonParser.parseString(Files.readString(output.resolve("cdc/files.json"))).getAsJsonArray();
        var files = new JsonArray();
        for (var item : inventory) {
            var value = item.getAsJsonObject().deepCopy();
            value.addProperty("path", "cdc/" + value.remove("name").getAsString());
            files.add(value);
        }
        document.add("cdc_files", files);
        document.add("counts", new Gson().toJsonTree(expected));
        document.add("source_evidence", new Gson().toJsonTree(evidence));
        Counts actual = validate(output, document, false);
        require(expected.equals(actual), "Capture report/file counts differ: " + expected + " / " + actual);
        // Publication is last; a failed verification leaves diagnostics and the closed source database.
        Files.writeString(output.resolve("manifest.json"), new GsonBuilder().setPrettyPrinting().create().toJson(document),
                StandardOpenOption.CREATE_NEW);
        return new Gson().fromJson(document, Map.class);
    }

    public static Map<String, Object> check(Path directory) throws IOException {
        var verified = open(directory);
        return Map.of("status", "passed", "counts", verified.counts(), "manifest_sha256", verified.manifestSha256());
    }

    static Verified open(Path directory) throws IOException {
        directory = directory.toRealPath();
        try {
            require(!Files.isSymbolicLink(directory.resolve("manifest.json")), "Symbolic CDC manifest");
            byte[] bytes = Files.readAllBytes(directory.resolve("manifest.json"));
            var document = JsonParser.parseString(new String(bytes, java.nio.charset.StandardCharsets.UTF_8)).getAsJsonObject();
            Counts counts = validate(directory, document, true);
            return new Verified(document, counts, java.util.HexFormat.of().formatHex(Digests.digest().digest(bytes)));
        } catch (RuntimeException malformed) {
            throw new IOException("Invalid CDC input manifest: " + directory, malformed);
        }
    }

    private static Counts validate(Path directory, JsonObject document, boolean baselineHashes) throws IOException {
        require(document.get("kind").getAsString().equals("pixels_cdc_input")
                && document.get("status").getAsString().equals("complete"), "Unpublished or unsupported CDC input");
        Path schema = verified(directory, document.getAsJsonObject("schema"));
        verified(directory, document.getAsJsonObject("source_ddl"));
        var tables = new LinkedHashMap<String, Table>();
        int id = 0;
        for (String line : Files.readAllLines(schema)) {
            String[] fields = line.split("\t");
            require(fields.length == 3 && fields[0].matches("[a-z][a-z0-9_]*"), "Invalid CDC table schema");
            require(tables.put(fields[0], new Table(++id, fields[2].split(";").length)) == null, "Duplicate CDC table schema");
        }
        require(tables.containsKey("sibench_marker"), "Missing CDC window marker schema");
        var baselines = document.getAsJsonObject("baseline_files");
        require(baselines != null && !baselines.isEmpty(), "Missing bound baseline files");
        var baselineRows = document.getAsJsonObject("baseline_rows");
        require(baselineRows != null, "Missing baseline row counts");
        var expectedTables = new java.util.HashSet<>(tables.keySet());
        expectedTables.remove("sibench_marker");
        require(expectedTables.equals(baselineRows.keySet()), "Baseline table set differs from schema");
        var baselineTables = new java.util.HashSet<String>();
        var paths = new java.util.HashSet<String>();
        paths.add(document.getAsJsonObject("schema").get("path").getAsString());
        require(paths.add(document.getAsJsonObject("source_ddl").get("path").getAsString()), "Aliased CDC schema files");
        for (var item : baselines.entrySet()) {
            var file = item.getValue().getAsJsonObject();
            require(paths.add(file.get("path").getAsString()), "Duplicate baseline file");
            if (file.has("table")) {
                String table = file.get("table").getAsString();
                require(expectedTables.contains(table) && baselineTables.add(table)
                        && number(file, "rows") == number(baselineRows, table), "Baseline table/count binding differs");
            }
            if (baselineHashes) verifyBaseline(directory, file);
            else require(Files.size(resolve(directory, file.get("path").getAsString())) == number(file, "bytes"), "Baseline size changed");
        }
        require(baselineTables.equals(expectedTables), "Missing baseline table CSV");
        var inventory = document.getAsJsonArray("cdc_files");
        require(inventory != null && !inventory.isEmpty(), "Missing closed CDC files");
        long transactions = 0, changes = 0, business = 0, markers = 0;
        int number = 0;
        for (var item : inventory) {
            var file = item.getAsJsonObject();
            String name = file.get("path").getAsString();
            require(name.equals("cdc/" + String.format("%08d.proto", number++)), "Missing, repeated or unordered CDC file: " + name);
            require(paths.add(name), "Duplicate CDC path");
            var counts = scan(resolve(directory, name), file, tables);
            transactions = Math.addExact(transactions, counts.transactions());
            changes = Math.addExact(changes, counts.changes());
            business = Math.addExact(business, counts.businessChanges());
            markers = Math.addExact(markers, counts.markers());
        }
        try (var files = Files.list(directory.resolve("cdc"))) {
            for (Path path : files.filter(p -> p.getFileName().toString().endsWith(".proto")).toList())
                require(paths.contains("cdc/" + path.getFileName()), "Unlisted CDC file: " + path);
        }
        var actual = new Counts(transactions, changes, business, markers);
        var declared = document.getAsJsonObject("counts");
        var expected = new Counts(number(declared, "transactions"), number(declared, "changes"),
                number(declared, "businessChanges"), number(declared, "markers"));
        require(actual.equals(expected), "CDC input count mismatch");
        return actual;
    }

    private static Counts scan(Path path, JsonObject file, Map<String, Table> tables) throws IOException {
        long length = Files.size(path);
        require(length == number(file, "bytes"), "CDC file size differs: " + path);
        var digest = Digests.digest();
        long remaining = length, rows = 0, transactions = 0, changes = 0, business = 0, markers = 0;
        String transaction = null;
        var tableCounts = new HashMap<String, Long>();
        try (var input = new DataInputStream(new DigestInputStream(Files.newInputStream(path), digest))) {
            while (remaining != 0) {
                require(remaining >= 8, "Truncated CDC frame header");
                int key = input.readInt(), size = input.readInt();
                remaining -= 8;
                require(size >= 0 && size <= MAX_FRAME && size <= remaining, "Invalid or truncated CDC frame");
                byte[] bytes = input.readNBytes(size);
                require(bytes.length == size, "CDC file truncated while reading");
                remaining -= size;
                if (key == -1) {
                    var metadata = SinkProto.TransactionMetadata.parseFrom(bytes);
                    require(!metadata.getId().isBlank(), "Missing CDC transaction ID");
                    if (metadata.getStatus() == SinkProto.TransactionStatus.BEGIN) {
                        require(transaction == null, "Nested CDC BEGIN");
                        transaction = metadata.getId();
                        rows = 0;
                        tableCounts.clear();
                    } else {
                        require(metadata.getStatus() == SinkProto.TransactionStatus.END && metadata.getId().equals(transaction)
                                && metadata.getEventCount() == rows, "Missing BEGIN or wrong CDC END count");
                        var expected = new HashMap<String, Long>();
                        for (var table : metadata.getDataCollectionsList())
                            require(table.getEventCount() > 0 && expected.put(table.getDataCollection(), table.getEventCount()) == null,
                                    "Invalid CDC table count");
                        require(expected.equals(tableCounts), "CDC transaction table counts differ");
                        transactions++;
                        changes = Math.addExact(changes, rows);
                        transaction = null;
                    }
                } else {
                    var row = SinkProto.RowRecord.parseFrom(bytes);
                    var source = row.getSource();
                    Table table = tables.get(source.getTable());
                    require(source.getDb().equals("postgres") && source.getSchema().equals("public")
                            && table != null && key == table.id(), "CDC row/schema table binding differs");
                    var tx = row.getTransaction();
                    String collection = "public." + source.getTable();
                    long tableOrder = tableCounts.merge(collection, 1L, Math::addExact);
                    require(transaction != null && tx.getId().equals(transaction) && tx.getTotalOrder() == ++rows
                            && tx.getDataCollectionOrder() == tableOrder, "CDC row identity/order differs");
                    boolean before = row.getOp() == SinkProto.OperationType.UPDATE || row.getOp() == SinkProto.OperationType.DELETE;
                    boolean after = row.getOp() == SinkProto.OperationType.UPDATE || row.getOp() == SinkProto.OperationType.INSERT;
                    require(before || after, "Unsupported CDC row operation");
                    require(row.hasBefore() == before && row.hasAfter() == after, "Missing or unexpected CDC row image");
                    if (before) require(row.getBefore().getValuesCount() == table.columns(), "CDC before-image columns differ");
                    if (after) require(row.getAfter().getValuesCount() == table.columns(), "CDC after-image columns differ");
                    if (source.getTable().equals("sibench_marker")) markers++;
                    else business++;
                }
            }
            require(input.read() == -1, "CDC file grew while reading");
        }
        require(transaction == null, "Incomplete CDC transaction at file end");
        require(transactions > 0 && transactions == number(file, "transactions"), "CDC file transaction count differs");
        require(java.util.HexFormat.of().formatHex(digest.digest()).equals(file.get("sha256").getAsString()), "CDC file digest differs");
        return new Counts(transactions, changes, business, markers);
    }

    static JsonObject file(Path root, Path path) throws IOException {
        var result = new JsonObject();
        result.addProperty("path", root.relativize(path).toString());
        result.addProperty("bytes", Files.size(path));
        result.addProperty("sha256", Digests.sha256(path));
        return result;
    }

    private static Path verified(Path directory, JsonObject file) throws IOException {
        Path path = resolve(directory, file.get("path").getAsString());
        require(Files.size(path) == number(file, "bytes")
                && Digests.sha256(path).equals(file.get("sha256").getAsString()), "CDC input file changed: " + path);
        return path;
    }

    private static void verifyBaseline(Path directory, JsonObject file) throws IOException {
        Path path = resolve(directory, file.get("path").getAsString());
        var digest = Digests.digest();
        long bytes = 0, rows = 0;
        int last = -1;
        try (var stream = new DigestInputStream(Files.newInputStream(path), digest)) {
            byte[] buffer = new byte[1 << 16];
            for (int n; (n = stream.read(buffer)) != -1;) {
                bytes += n;
                if (file.has("table")) for (int i = 0; i < n; i++) if (buffer[i] == '\n') rows++;
                if (n > 0) last = buffer[n - 1];
            }
        }
        if (last != -1 && last != '\n') rows++;
        require(bytes == number(file, "bytes")
                && java.util.HexFormat.of().formatHex(digest.digest()).equals(file.get("sha256").getAsString()), "Baseline input changed: " + path);
        if (file.has("table")) require(rows == number(file, "rows"), "Baseline CSV row count differs: " + path);
    }

    private static Path resolve(Path directory, String name) throws IOException {
        Path relative = Path.of(name);
        require(!name.isBlank() && !relative.isAbsolute() && relative.normalize().toString().equals(name), "Noncanonical CDC input path");
        Path path = directory;
        for (Path part : relative) {
            require(!part.toString().equals("..") && !part.toString().equals("."), "CDC input path traversal");
            path = path.resolve(part);
            require(!Files.isSymbolicLink(path), "Symbolic CDC input path");
        }
        require(Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS), "Missing CDC input file: " + path);
        return path;
    }

    private static void require(boolean valid, String message) throws IOException {
        if (!valid) throw new IOException(message);
    }

    private static long number(JsonObject object, String key) throws IOException {
        try {
            var value = object.get(key);
            require(value != null && value.isJsonPrimitive() && value.getAsJsonPrimitive().isNumber(), "Missing numeric CDC field: " + key);
            long result = value.getAsBigDecimal().longValueExact();
            require(result >= 0, "Negative CDC field: " + key);
            return result;
        } catch (ArithmeticException | NumberFormatException invalid) {
            throw new IOException("Nonintegral or overflowing CDC field: " + key, invalid);
        }
    }
}
