package org.sibench.pixels;

import com.google.gson.*;
import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.index.IndexOption;
import io.pixelsdb.pixels.common.index.RowIdAllocator;
import io.pixelsdb.pixels.common.index.service.RPCIndexService;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.File;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.retina.RetinaService;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.common.utils.EtcdUtil;
import io.pixelsdb.pixels.common.utils.IndexUtils;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.PixelsFileNameUtils;
import io.pixelsdb.pixels.core.PixelsFooterCache;
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsReaderImpl;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.utils.PrimaryKeyBytes;
import io.pixelsdb.pixels.index.IndexProto;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/** File inspection and fresh-state index reconstruction through the normal Pixels APIs. */
final class PixelsBaseline {
    private PixelsBaseline() {}

    static void seed(long maximum) throws Exception {
        if (maximum < 0 || maximum > Long.MAX_VALUE / 2) throw new IOException("Invalid baseline timestamp");
        var etcd = EtcdUtil.Instance();
        for (String key : List.of(Constants.AI_TRANS_ID_KEY, Constants.TRANS_HIGH_WATERMARK_KEY, Constants.TRANS_LOW_WATERMARK_KEY)) {
            if (etcd.getKeyValue(key) != null) throw new IOException("Timestamp initialization requires fresh etcd");
            etcd.putKeyValue(key, Long.toString(maximum + 1));
        }
    }

    static void export(String schema, String tables, Path output) throws Exception {
        var metadata = MetadataService.Instance();
        var inventory = new JsonArray();
        long maximum = 0;
        for (String table : tables.split(",")) {
            var layout = metadata.getLatestLayout(schema, table);
            for (var path : layout.getOrderedPaths()) for (var file : metadata.getRegularFiles(path.getId())) {
                String uri = File.getFilePath(path, file);
                var entry = describe(uri);
                entry.addProperty("table", table);
                inventory.add(entry);
                maximum = Math.max(maximum, entry.get("max_timestamp").getAsLong());
            }
            for (var path : layout.getCompactPaths())
                require(metadata.getRegularFiles(path.getId()).isEmpty(), "Baseline export requires unmodified ordered files");
        }
        var result = new JsonObject();
        result.add("files", inventory);
        result.addProperty("max_timestamp", maximum);
        result.addProperty("index_buckets", IndexUtils.getInstance().getBucketNum());
        write(output, result);
    }

    static void restore(String schema, Path spec, Path input, Path output) throws Exception {
        var inventory = JsonParser.parseString(Files.readString(input)).getAsJsonObject();
        require(inventory.get("index_buckets").getAsInt() == IndexUtils.getInstance().getBucketNum(), "Baseline index bucket count differs");
        var metadata = MetadataService.Instance();
        var keys = new HashMap<String, String>();
        var schemas = new HashMap<String, io.pixelsdb.pixels.core.TypeDescription>();
        for (String line : Files.readAllLines(spec)) {
            String[] fields = line.split("\t"); keys.put(fields[0], fields[1]);
            schemas.put(fields[0], io.pixelsdb.pixels.core.TypeDescription.fromString("struct<" + fields[2].replace(';', ',') + ">"));
        }
        var rows = new LinkedHashMap<String, Long>();
        long copyNanos = 0, indexNanos = 0;
        for (var item : inventory.getAsJsonArray("files")) {
            var entry = item.getAsJsonObject();
            String table = entry.get("table").getAsString(), source = entry.get("uri").getAsString();
            var layout = metadata.getLatestLayout(schema, table);
            require(layout.getOrderedPaths().size() == 1, "Baseline restore requires one ordered path");
            var path = layout.getOrderedPaths().get(0);
            String name = PixelsFileNameUtils.buildOrderedFileName(ConfigFactory.Instance().getProperty("retina.server.host"), 0);
            String destination = path.getUri().replaceAll("/+$", "") + "/" + name;
            var storage = StorageFactory.Instance().getStorage(destination);
            require(storage.getScheme() == StorageFactory.Instance().getStorage(source).getScheme(), "Baseline storage scheme differs");
            long started = System.nanoTime();
            require(!storage.exists(destination) && storage.directCopy(source, destination), "Cannot copy baseline object");
            var actual = describe(destination);
            for (String field : List.of("sha256", "bytes", "rows", "row_groups", "max_timestamp"))
                require(entry.get(field).equals(actual.get(field)), "Copied baseline differs: " + field);
            copyNanos += System.nanoTime() - started;
            started = System.nanoTime();
            var file = new File();
            file.setName(name); file.setPathId(path.getId()); file.setNumRowGroup(actual.get("row_groups").getAsInt());
            file.setType(File.Type.TEMPORARY_INGEST);
            require(metadata.addFiles(List.of(file)), "Cannot register imported file");
            file.setId(metadata.getFileId(destination));
            long tableId = metadata.getTable(schema, table).getId(), indexId = metadata.getPrimaryIndex(tableId).getId();
            long count = index(destination, keys.get(table), schemas.get(table), tableId, indexId, file, actual.get("max_timestamp").getAsLong());
            require(count == actual.get("rows").getAsLong(), "Imported row count differs");
            file.setType(File.Type.REGULAR);
            require(metadata.updateFile(file), "Cannot publish imported file");
            require(RetinaService.Instance().addVisibility(destination), "Cannot initialize imported visibility");
            rows.merge(table, count, Math::addExact);
            indexNanos += System.nanoTime() - started;
        }
        var result = new JsonObject();
        result.add("rows", new Gson().toJsonTree(rows));
        result.addProperty("copy_and_verify_seconds", copyNanos / 1e9);
        result.addProperty("index_and_visibility_seconds", indexNanos / 1e9);
        write(output, result);
    }

    private static long index(String uri, String key, io.pixelsdb.pixels.core.TypeDescription schema,
            long tableId, long indexId, File file, long maximum) throws Exception {
        var service = RPCIndexService.Instance();
        var ids = new RowIdAllocator(tableId, 4096, service);
        long rows = 0, minimumId = Long.MAX_VALUE, maximumId = -1;
        try (var reader = reader(uri)) {
            require(reader.getFileSchema().toString().equals(schema.toString()), "Imported column schema differs from metadata");
            int ordinal = reader.getFileSchema().getFieldNames().indexOf(key);
            require(ordinal >= 0, "Missing baseline primary key");
            var types = List.of(reader.getFileSchema().getChildren().get(ordinal));
            for (int rg = 0; rg < reader.getRowGroupNum(); rg++) {
                var option = new PixelsReaderOption().includeCols(new String[] {key}).rgRange(rg, 1)
                        .tolerantSchemaEvolution(false).exposeHiddenColumn(true);
                try (var records = reader.read(option)) {
                    int offset = 0;
                    while (true) {
                        var batch = records.readBatch(1024);
                        var groups = new HashMap<Integer, List<IndexProto.PrimaryIndexEntry>>();
                        for (int row = 0; row < batch.size; row++) {
                            var timestamps = batch.hiddenColumnVector;
                            require(timestamps != null, "Baseline reader omitted hidden timestamps");
                            long timestamp = timestamps.vector[timestamps.isRepeating() ? 0 : row];
                            require(timestamp >= 0 && timestamp <= maximum, "Baseline hidden timestamp outside its bound");
                            var bytes = ByteString.copyFrom(PrimaryKeyBytes.fromColumnVectors(types, batch.cols, new int[] {0}, row));
                            long id = ids.getRowId();
                            minimumId = Math.min(minimumId, id); maximumId = Math.max(maximumId, id);
                            var value = IndexProto.PrimaryIndexEntry.newBuilder().setRowId(id)
                                    .setIndexKey(IndexProto.IndexKey.newBuilder().setTableId(tableId).setIndexId(indexId).setKey(bytes).setTimestamp(timestamp))
                                    .setRowLocation(IndexProto.RowLocation.newBuilder().setFileId(file.getId()).setRgId(rg).setRgRowOffset(offset++))
                                    .build();
                            groups.computeIfAbsent(IndexUtils.getBucketIdFromByteBuffer(bytes), ignored -> new ArrayList<>()).add(value);
                            rows++;
                        }
                        for (var group : groups.entrySet()) require(service.putPrimaryIndexEntries(tableId, indexId,
                                group.getValue(), IndexOption.builder().vNodeId(group.getKey()).build()), "Cannot rebuild primary index");
                        if (batch.endOfFile) break;
                        require(batch.size > 0, "Baseline reader made no progress");
                    }
                    require(offset == reader.getRowGroupInfo(rg).getNumberOfRows(), "Imported row-group count differs");
                }
            }
        }
        require(rows > 0 && service.flushIndexEntriesOfFile(tableId, indexId, file.getId(), true, new IndexOption()),
                "Cannot rebuild main index");
        file.setMinRowId(minimumId); file.setMaxRowId(maximumId);
        return rows;
    }

    private static JsonObject describe(String uri) throws Exception {
        var storage = StorageFactory.Instance().getStorage(uri);
        var digest = java.security.MessageDigest.getInstance("SHA-256");
        long bytes = 0;
        try (var input = storage.open(uri)) {
            byte[] buffer = new byte[1 << 20];
            for (int count; (count = input.read(buffer)) != -1;) { digest.update(buffer, 0, count); bytes += count; }
        }
        var entry = new JsonObject();
        entry.addProperty("uri", uri); entry.addProperty("bytes", bytes);
        entry.addProperty("sha256", HexFormat.of().formatHex(digest.digest()));
        try (var reader = reader(uri)) {
            var stats = reader.getFooter().getHiddenColumnStats();
            require(stats.hasIntStatistics() && stats.getIntStatistics().hasMinimum()
                    && stats.getIntStatistics().hasMaximum() && stats.getIntStatistics().getMinimum() >= 0,
                    "Baseline lacks hidden timestamp bounds");
            entry.addProperty("max_timestamp", stats.getIntStatistics().getMaximum());
            entry.addProperty("rows", reader.getNumberOfRows()); entry.addProperty("row_groups", reader.getRowGroupNum());
        }
        return entry;
    }

    private static PixelsReader reader(String uri) throws IOException {
        return PixelsReaderImpl.newBuilder().setPath(uri).setStorage(StorageFactory.Instance().getStorage(uri))
                .setEnableCache(false).setCacheOrder(List.of()).setPixelsCacheReader(null).setPixelsFooterCache(new PixelsFooterCache()).build();
    }
    private static void write(Path path, JsonObject value) throws IOException {
        Files.writeString(path, new GsonBuilder().setPrettyPrinting().create().toJson(value), java.nio.file.StandardOpenOption.CREATE_NEW);
    }
    private static void require(boolean value, String message) throws IOException { if (!value) throw new IOException(message); }
}
