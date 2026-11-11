package org.sibench.pixels;

import io.pixelsdb.pixels.cli.executor.LoadExecutor;
import io.pixelsdb.pixels.cli.initmeta.MetadataSchemaInitializer;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.metadata.domain.SinglePointIndex;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.daemon.MetadataProto;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.sourceforge.argparse4j.inf.Namespace;

/**
 * Pixels metadata and bulk-load steps of CDC generation, run on the Pixels host with
 * PIXELS_CONFIG pointing at the run's properties. Every failure exits non-zero.
 *
 * <pre>
 * init-meta
 * create SCHEMA BASE_DIRECTORY SPEC OUTPUT   SPEC lines: table TAB primary-key TAB name:type;...
 * load SCHEMA TABLES DIRECTORY ROWS_PER_FILE THREADS COUNTS
 * </pre>
 */
public final class PixelsSetup {
    private PixelsSetup() {}

    public static void main(String[] args) throws Exception {
        switch (args.length == 0 ? "" : args[0]) {
            case "init-meta" -> {
                if (args.length != 1) usage();
                MetadataSchemaInitializer.initialize();
            }
            case "create" -> {
                if (args.length != 5) usage();
                create(args[1], args[2], Path.of(args[3]), Path.of(args[4]));
            }
            case "seed-timestamps" -> { if (args.length != 2) usage(); PixelsBaseline.seed(Long.parseLong(args[1])); }
            case "init-storage" -> {
                if (args.length != 2) usage();
                if (!io.pixelsdb.pixels.common.physical.StorageFactory.Instance().getStorage(args[1]).listPaths(args[1]).isEmpty())
                    throw new IllegalStateException("COS run prefix already exists");
            }
            case "delete-storage" -> {
                if (args.length != 2) usage();
                var storage = io.pixelsdb.pixels.common.physical.StorageFactory.Instance().getStorage(args[1]);
                // COS requires Content-MD5 for multi-delete; single-object deletion also removes directory markers.
                var client = ((io.pixelsdb.pixels.storage.s3.AbstractS3) storage).getS3Client();
                for (String object : storage.listPaths(args[1])) {
                    var uri = java.net.URI.create(object);
                    client.deleteObject(software.amazon.awssdk.services.s3.model.DeleteObjectRequest.builder()
                            .bucket(uri.getHost()).key(uri.getPath().substring(1)).build());
                }
                if (!storage.listPaths(args[1]).isEmpty())
                    throw new IllegalStateException("Owned COS run prefix is not empty after deletion");
            }
            case "export-baseline" -> { if (args.length != 4) usage(); PixelsBaseline.export(args[1], args[2], Path.of(args[3])); }
            case "import-baseline" -> { if (args.length != 5) usage(); PixelsBaseline.restore(args[1], Path.of(args[2]), Path.of(args[3]), Path.of(args[4])); }
            case "load" -> {
                if (args.length != 7) usage();
                var counts = new StringBuilder();
                for (String table : args[2].split(",")) {
                    long started = System.nanoTime();
                    System.out.println("SIBench LOAD begin " + table + " " + java.time.Instant.now());
                    Map<String, Object> options = new HashMap<>();
                    options.put("schema", args[1]);
                    options.put("table", table);
                    options.put("origin", "file://" + Path.of(args[3]).resolve(table) + "/");
                    options.put("row_num", args[4]);
                    options.put("row_regex", "|");
                    options.put("consumer_thread_num", args[5]);
                    options.put("encoding_level", "2");
                    options.put("nulls_padding", "false");
                    new LoadExecutor().execute(new Namespace(options), "LOAD");
                    long rows = 0;
                    var metadata = MetadataService.Instance();
                    var layout = metadata.getLatestLayout(args[1], table);
                    var paths = new ArrayList<>(layout.getOrderedPaths());
                    paths.addAll(layout.getCompactPaths());
                    for (var path : paths) for (var file : metadata.getRegularFiles(path.getId())) {
                        String uri = io.pixelsdb.pixels.common.metadata.domain.File.getFilePath(path, file);
                        try (var reader = io.pixelsdb.pixels.core.PixelsReaderImpl.newBuilder().setPath(uri)
                                .setStorage(io.pixelsdb.pixels.common.physical.StorageFactory.Instance().getStorage(uri))
                                .setEnableCache(false).setCacheOrder(List.of()).setPixelsCacheReader(null)
                                .setPixelsFooterCache(new io.pixelsdb.pixels.core.PixelsFooterCache()).build()) {
                            rows = Math.addExact(rows, reader.getNumberOfRows());
                        }
                    }
                    counts.append(table).append('\t').append(rows).append('\n');
                    System.out.println("SIBench LOAD complete " + table + " seconds=" + (System.nanoTime() - started) / 1e9);
                }
                Files.writeString(Path.of(args[6]), counts, java.nio.file.StandardOpenOption.CREATE_NEW);
            }
            default -> usage();
        }
        // Pixels clients leave non-daemon gRPC threads behind; the result is complete here.
        System.exit(0);
    }

    private static void create(String schema, String base, Path spec, Path output) throws Exception {
        var metadata = MetadataService.Instance();
        if (!metadata.existSchema(schema) && !metadata.createSchema(schema))
            throw new IllegalStateException("Cannot create Pixels schema: " + schema);
        var result = new StringBuilder();
        for (String line : Files.readAllLines(spec, StandardCharsets.UTF_8)) {
            String[] fields = line.split("\t");
            if (fields.length != 3) throw new IllegalArgumentException("Invalid table spec: " + line);
            String table = fields[0], primary = fields[1];
            var columns = new ArrayList<Column>();
            for (String definition : fields[2].split(";")) {
                int colon = definition.indexOf(':');
                var column = new Column();
                column.setName(definition.substring(0, colon));
                column.setType(definition.substring(colon + 1));
                columns.add(column);
            }
            String directory = base.replaceAll("/+$", "") + "/" + schema + "/" + table;
            var storage = io.pixelsdb.pixels.common.physical.StorageFactory.Instance().getStorage(directory);
            if (storage.getScheme() == Storage.Scheme.file) {
                Path local = Path.of(java.net.URI.create(directory));
                Files.createDirectories(local.resolve("v-0-ordered"));
                Files.createDirectories(local.resolve("v-0-compact"));
            }
            if (!metadata.createTable(schema, table, storage.getScheme(), List.of(directory), columns))
                throw new IllegalStateException("Cannot create Pixels table " + table);
            long tableId = metadata.getTable(schema, table).getId();
            Integer keyColumn = null;
            for (Column column : metadata.getColumns(schema, table, false))
                if (column.getName().equals(primary)) keyColumn = (int) column.getId();
            if (keyColumn == null) throw new IllegalArgumentException("Missing primary key column " + primary + " in " + table);
            long schemaVersion = metadata.getLatestLayout(schema, table).getSchemaVersionId();
            var index = MetadataProto.SinglePointIndex.newBuilder().setId(0L)
                    .setKeyColumns("{\"keyColumnIds\":[" + keyColumn + "]}").setPrimary(true).setUnique(true)
                    .setIndexScheme("rocksdb").setTableId(tableId).setSchemaVersionId(schemaVersion).build();
            if (!metadata.createSinglePointIndex(new SinglePointIndex(index)))
                throw new IllegalStateException("Cannot create primary index of " + table);
            long indexId = metadata.getPrimaryIndex(tableId).getId();
            result.append(table).append('\t').append(tableId).append('\t').append(indexId).append('\n');
        }
        Files.writeString(output, result, StandardCharsets.UTF_8);
    }

    private static void usage() {
        throw new IllegalArgumentException("PixelsSetup init-meta | create SCHEMA BASE SPEC OUTPUT | load SCHEMA TABLE DIR ROWS THREADS COUNTS");
    }
}
