package org.sibench.pixels;

import io.pixelsdb.pixels.sink.SibenchReport;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.conversion.sinkproto.RowRecordConverter;
import io.pixelsdb.pixels.sink.metadata.TableMetadataRegistry;
import io.pixelsdb.pixels.sink.util.MetricsFacade;
import io.pixelsdb.pixels.sink.writer.retina.RetinaWriter;
import io.pixelsdb.pixels.sink.writer.retina.TransactionProxy;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;

/** Applies controller-selected, validated row frames through the sink's normal Retina writer. */
public final class ProtoApply {
    private ProtoApply() {}

    public static void main(String[] args) {
        Thread.setDefaultUncaughtExceptionHandler((thread, failure) -> SibenchReport.fail("File sink failed", failure));
        try {
            if (args.length != 1) throw new IllegalArgumentException("ProtoApply SINK_CONFIG");
            PixelsSinkConfigFactory.initialize(args[0]);
            var writer = new RetinaWriter();
            MetricsFacade.getInstance().setSinkContextManager(io.pixelsdb.pixels.sink.writer.retina.SinkContextManager.getInstance());
            String measuredTable = System.getProperty("sibench.freshness.table"), measuredColumn = System.getProperty("sibench.freshness.column");
            io.pixelsdb.pixels.sink.freshness.FreshnessClient freshness = null;
            int measuredPosition = -1;
            if (measuredTable != null) {
                var columns = TableMetadataRegistry.Instance().getMetadata("postgres", measuredTable).getColumns();
                for (int i = 0; i < columns.size(); i++) if (columns.get(i).getName().equals(measuredColumn)
                        && columns.get(i).getType().equals("timestamp(6)")) measuredPosition = i;
                if (measuredPosition < 0) throw new IOException("Freshness column is absent or is not timestamp(6)");
                freshness = io.pixelsdb.pixels.sink.freshness.FreshnessClient.getInstance();
                freshness.addMonitoredTable(measuredTable);
                freshness.start();
            }
            var converter = new RowRecordConverter(TableMetadataRegistry.Instance());
            var input = new DataInputStream(new BufferedInputStream(System.in, 1 << 20));
            SibenchReport.ready();
            while (true) {
                int table = input.readInt(), length = input.readInt();
                if (table == -2 && length == 0) break;
                if (length < 0 || length > 16 << 20) throw new IOException("Invalid row frame size");
                byte[] bytes = input.readNBytes(length);
                if (bytes.length != length) throw new IOException("Truncated row frame");
                boolean accepted;
                if (table == -1) {
                    var transaction = SinkProto.TransactionMetadata.parseFrom(bytes);
                    // Mapping arrives before END can trigger an asynchronous commit.
                    if (transaction.getStatus() == SinkProto.TransactionStatus.END)
                        SibenchReport.captured(transaction.getId(), transaction.getEventCount());
                    accepted = writer.writeTrans(transaction);
                } else if (table > 0) {
                    // Capture-local table IDs are never used to route target writes.
                    var row = SinkProto.RowRecord.parseFrom(bytes);
                    if (measuredTable != null && measuredTable.equals(row.getSource().getTable()) && row.hasAfter()) {
                        var updated = row.toBuilder();
                        updated.getAfterBuilder().setValues(measuredPosition, SinkProto.ColumnValue.newBuilder()
                                .setValue(com.google.protobuf.ByteString.copyFrom(java.nio.ByteBuffer.allocate(8)
                                        .putLong(System.currentTimeMillis() * 1000).array())).build());
                        row = updated.build();
                    }
                    accepted = writer.writeRow(converter.convert(row));
                } else throw new IOException("Invalid row frame kind");
                if (!accepted) throw new IOException("Sink rejected a CDC operation");
            }
            if (input.readInt() != -3) throw new IOException("Controller disconnected before committed drain");
            if (freshness != null) {
                Thread.sleep(Long.getLong("sibench.freshness.sample_ms"));
                freshness.stop();
                freshness.checkSamples();
            }
            TransactionProxy.staticClose();
            writer.shutdown();
            writer.close();
            MetricsFacade.getInstance().stop();
            SibenchReport.closed();
            System.exit(0);
        } catch (Throwable failure) {
            SibenchReport.fail("CDC file application failed", failure);
        }
    }
}
