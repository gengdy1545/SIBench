package org.sibench.capture;

import com.google.gson.JsonObject;

import org.sibench.state.RowIdentities;
import org.sibench.trace.IndexKey;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Files;
import java.util.HashSet;

/** Reads an identity-checked, index-only export without materializing complete table rows. */
final class ProtoBaseline {
    private ProtoBaseline() {}

    /** Retains the exported bytes as final Trace shards; no service payload is rebuilt. */
    static com.google.gson.JsonArray retain(Path source, JsonObject manifest, Path output) throws IOException {
        var files = new com.google.gson.JsonArray();
        Path target = Files.createDirectory(output.resolve("baseline"));
        for (var item : manifest.getAsJsonArray("files")) {
            var file = item.getAsJsonObject();
            String name = file.get("table").getAsString() + ".pb.zst";
            Path input = BaselineManifest.dataPath(source, file), destination = target.resolve(name);
            if (Files.getFileStore(input).equals(Files.getFileStore(target))) Files.createLink(destination, input);
            else Files.copy(input, destination);
            var retained = new JsonObject();
            retained.addProperty("path", "baseline/" + name);
            for (String field : new String[] {"sha256", "logical_sha256", "operations"})
                retained.add(field, file.get(field).deepCopy());
            files.add(retained);
        }
        return files;
    }

    static BaselineOperations.Extent derive(Path directory, JsonObject manifest, IndexCatalog catalog,
            RowIdentities identities, IndexOperationTrace.Consumer consumer) throws IOException {
        long rows = 0, maximum = 0;
        var tables = new HashSet<Integer>();
        for (var item : manifest.getAsJsonArray("files")) {
            var file = item.getAsJsonObject();
            int table = catalog.tableId(file.get("table").getAsString());
            if (!tables.add(table)) throw new IOException("Repeated proto baseline table");
            long[] count = {0}, largest = {maximum};
            IndexKey[] previous = {null};
            try (var identity = identities.baseline(table)) {
                long records = ProtoTrace.read(BaselineManifest.dataPath(directory, file),
                        file.get("sha256").getAsString(), file.get("logical_sha256").getAsString(), operation -> {
                            if (operation.kind() != IndexOperationTrace.INSERT || operation.table() != table
                                    || operation.version() != 0 || operation.transactionId() != 0)
                                throw new IOException("Non-baseline operation in proto export");
                            if (operation.index() == 0) {
                                if (previous[0] != null && previous[0].compareTo(operation.key()) >= 0)
                                    throw new IOException("Unordered proto baseline primary keys");
                                previous[0] = operation.key();
                                identity.add(operation.key(), operation.rowId());
                                count[0]++;
                                largest[0] = Math.max(largest[0], operation.rowId());
                            }
                            consumer.accept(operation);
                        });
                if (records != file.get("operations").getAsLong() || count[0] != file.get("rows").getAsLong())
                    throw new IOException("Proto baseline file coverage mismatch");
                identity.finish();
            }
            rows = Math.addExact(rows, count[0]);
            maximum = largest[0];
        }
        if (rows != manifest.get("rows").getAsLong()
                || tables.size() != catalog.schema().tables().size()) throw new IOException("Proto baseline coverage mismatch");
        return new BaselineOperations.Extent(rows, maximum, 0);
    }
}
