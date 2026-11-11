package org.sibench.pixels;

import com.google.gson.GsonBuilder;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.metadata.domain.KeyColumns;
import io.pixelsdb.pixels.common.metadata.domain.SinglePointIndex;
import io.pixelsdb.pixels.common.metadata.domain.Table;
import io.pixelsdb.pixels.sink.SibenchReport;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.metadata.TableMetadata;
import io.pixelsdb.pixels.sink.metadata.TableMetadataRegistry;
import io.pixelsdb.pixels.sink.source.engine.SinkEngineSource;
import io.pixelsdb.pixels.sink.util.MetricsFacade;
import io.pixelsdb.pixels.sink.writer.proto.ProtoWriter;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

/** Thin sink binding: source decoding and ProtoWriter, controlled by the Java preparation CLI. */
public final class ProtoCapture {
    private ProtoCapture() {}

    public static void main(String[] args) {
        Thread.setDefaultUncaughtExceptionHandler((thread, error) -> SibenchReport.fail("Proto capture failed", error));
        try {
            if (args.length != 3) throw new IllegalArgumentException("ProtoCapture SINK_CONFIG SCHEMA_TSV FRESH_OUTPUT");
            PixelsSinkConfigFactory.initialize(args[0]);
            var config = PixelsSinkConfigFactory.getInstance();
            if (!config.getSinkMonitorFreshnessLevel().equals("none") || config.isSinkMonitorFreshnessTimestamp())
                throw new IllegalArgumentException("CDC preparation disables freshness and timestamp rewriting");
            register(Path.of(args[1]));
            var writer = new ProtoWriter(new FilesOutput(Path.of(args[2]), config.getMaxRecordsPerFile()));
            var source = new SinkEngineSource(writer);
            source.start();
            SibenchReport.ready();
            if (!"close".equals(new BufferedReader(new InputStreamReader(System.in)).readLine()))
                throw new IOException("Preparation controller disconnected without closing capture");
            source.close();
            writer.close();
            MetricsFacade.getInstance().stop();
            SibenchReport.closed();
            System.exit(0);
        } catch (Throwable failure) {
            SibenchReport.fail("CDC file capture failed", failure);
        }
    }

    private static void register(Path spec) throws Exception {
        long tableId = 0;
        for (String line : Files.readAllLines(spec)) {
            String[] fields = line.split("\t");
            if (fields.length != 3) throw new IOException("Invalid capture schema");
            var table = new Table();
            table.setId(++tableId);
            table.setSchemaId(1);
            table.setName(fields[0]);
            var columns = new ArrayList<Column>();
            int key = -1;
            for (String definition : fields[2].split(";")) {
                int colon = definition.indexOf(':');
                if (colon < 1) throw new IOException("Invalid capture column");
                var column = new Column();
                column.setId(columns.size() + 1);
                column.setName(definition.substring(0, colon));
                column.setType(definition.substring(colon + 1));
                columns.add(column);
                if (column.getName().equals(fields[1])) key = (int) column.getId();
            }
            if (key < 0) throw new IOException("Capture primary key is absent");
            var index = new SinglePointIndex();
            index.setId(tableId);
            index.setTableId(tableId);
            index.setPrimary(true);
            index.setUnique(true);
            var keys = new KeyColumns();
            keys.addKeyColumnIds(key);
            index.setKeyColumns(keys);
            TableMetadataRegistry.Instance().register("postgres", new TableMetadata(table, index, columns));
        }
    }

    /** Local files contain whole transactions; the core validates and publishes their catalog. */
    private static final class FilesOutput implements ProtoWriter.Output {
        private final Path root;
        private final long maximum;
        private final List<Map<String, Object>> files = new ArrayList<>();
        private FileChannel channel;
        private MessageDigest digest;
        private long bytes, transactions;
        private Path current;

        FilesOutput(Path root, long maximum) throws IOException {
            if (maximum <= 0) throw new IllegalArgumentException("Invalid proto transaction file limit");
            this.root = Files.createDirectory(root);
            this.maximum = maximum;
        }

        public void append(ByteBuffer buffer, SinkProto.TransactionMetadata transaction) throws IOException {
            if (channel != null && transactions == maximum) finishFile();
            if (channel == null) {
                current = root.resolve(String.format("%08d.proto", files.size()));
                channel = FileChannel.open(current, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
                try { digest = MessageDigest.getInstance("SHA-256"); }
                catch (java.security.NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
                bytes = transactions = 0;
            }
            bytes = Math.addExact(bytes, buffer.remaining());
            digest.update(buffer.duplicate());
            while (buffer.hasRemaining()) channel.write(buffer);
            transactions++;
            SibenchReport.captured(transaction.getId(), transaction.getEventCount());
        }

        private void finishFile() throws IOException {
            channel.force(true);
            channel.close();
            channel = null;
            files.add(Map.of("name", current.getFileName().toString(), "bytes", bytes,
                    "sha256", HexFormat.of().formatHex(digest.digest()), "transactions", transactions));
        }

        public void close(boolean complete) throws IOException {
            if (complete) {
                if (channel != null) finishFile();
                Files.writeString(root.resolve("files.json"), new GsonBuilder().setPrettyPrinting().create().toJson(files),
                        StandardOpenOption.CREATE_NEW);
            } else if (channel != null) {
                channel.close();
                channel = null;
            }
        }
    }
}
