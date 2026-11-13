package org.sibench.capture;

import static org.sibench.capture.InputFiles.*;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.pixelsdb.pixels.sink.SinkProto;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.DigestInputStream;
import java.util.HashMap;
import java.util.Map;
import org.sibench.capture.CsvInput.Table;
import org.sibench.config.Digests;

/** Portable, closed row-CDC inputs. This is independent of the Index Trace format. */
public final class CdcInput {
    static final int MAX_FRAME = 16 << 20;
    private CdcInput() {}

    record Counts(long transactions, long changes, long businessChanges, long markers) {}
    record Verified(Path directory, JsonObject manifest, Counts counts, String manifestSha256) {}

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
        var document = CsvInput.baseline(output, input);
        document.addProperty("kind", "pixels_cdc_input");
        document.addProperty("status", "complete");
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
        var manifest = manifest(directory);
        try {
            Counts counts = validate(directory, manifest.document(), true);
            return new Verified(directory, manifest.document(), counts, manifest.sha256());
        } catch (RuntimeException malformed) {
            throw new IOException("Invalid CDC input manifest: " + directory, malformed);
        }
    }

    private static Counts validate(Path directory, JsonObject document, boolean baselineHashes) throws IOException {
        require(document.get("kind").getAsString().equals("pixels_cdc_input")
                && document.get("status").getAsString().equals("complete"), "Unpublished or unsupported CDC input");
        var tables = CsvInput.validateBaseline(directory, document, baselineHashes);
        var paths = new java.util.HashSet<String>();
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

    private static void require(boolean valid, String message) throws IOException {
        if (!valid) throw new IOException(message);
    }

}
