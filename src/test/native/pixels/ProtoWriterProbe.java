package io.pixelsdb.pixels.sink.writer.proto;

import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/** Hand-built transactions exercise the production writer without metadata/etcd services. */
public final class ProtoWriterProbe {
    private static final class Output implements ProtoWriter.Output {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        boolean complete, closed, failAppend, failClose;
        final IOException failure = new IOException("injected append failure");

        public void append(ByteBuffer value, SinkProto.TransactionMetadata transaction) throws IOException {
            if (failAppend) throw failure;
            byte[] data = new byte[value.remaining()];
            value.get(data);
            bytes.write(data);
        }

        public void close(boolean complete) throws IOException {
            this.closed = true;
            this.complete = complete;
            if (failClose) throw new IOException("injected close failure");
        }
    }

    public static void main(String[] args) throws Exception {
        orderedTransactions();
        rowBeforeBeginAndEmptyTransaction();
        appendFailureRemainsFatal();
        incompleteClose();
        invalidRows();
        invalidMetadata();
        System.out.println("PASS ProtoWriter ordering, framing, exact counts, failure propagation and close");
    }

    private static void orderedTransactions() throws Exception {
        var output = new Output();
        try (var writer = new ProtoWriter(output)) {
            writer.writeTrans(begin("a"));
            writer.writeTrans(end("a", 2, 1));
            writer.writeTrans(begin("b"));
            writer.writeTrans(end("b", 1, 0));
            writer.writeRow(row("b", "a", 1, 1, SinkProto.OperationType.INSERT));
            require(output.bytes.size() == 0, "A later complete transaction overtook the head");
            writer.writeRow(row("a", "b", 2, 1, SinkProto.OperationType.DELETE));
            writer.writeRow(row("a", "a", 1, 1, SinkProto.OperationType.UPDATE));
            require(output.bytes.size() == 0, "An incomplete transaction was written");
            writer.writeRow(row("a", "a", 3, 2, SinkProto.OperationType.INSERT));
        }
        require(output.complete && output.closed, "A complete file was not closed");
        var frames = ByteBuffer.wrap(output.bytes.toByteArray());
        var actual = new ArrayList<String>();
        while (frames.hasRemaining()) {
            require(frames.remaining() >= 8, "Truncated frame header");
            int key = frames.getInt(), size = frames.getInt();
            require(size >= 0 && size <= frames.remaining(), "Invalid frame size");
            byte[] bytes = new byte[size];
            frames.get(bytes);
            if (key == -1) {
                var tx = SinkProto.TransactionMetadata.parseFrom(bytes);
                actual.add(tx.getId() + ":" + tx.getStatus());
            } else {
                require(key == 0, "Table ID changed");
                var row = SinkProto.RowRecord.parseFrom(bytes);
                actual.add(row.getTransaction().getId() + ":" + row.getTransaction().getTotalOrder());
                require(row.getSource().getSchema().equals("public"), "Source schema changed");
                if (row.getOp() != SinkProto.OperationType.INSERT)
                    require(row.getBefore().getValues(0).getValue().toStringUtf8().equals("before"), "Before image changed");
                if (row.getOp() != SinkProto.OperationType.DELETE)
                    require(row.getAfter().getValues(0).getValue().toStringUtf8().equals("after"), "After image changed");
            }
        }
        require(actual.equals(List.of("a:BEGIN", "a:1", "a:2", "a:3", "a:END", "b:BEGIN", "b:1", "b:END")),
                "Incorrect source/row order: " + actual);
    }

    private static void rowBeforeBeginAndEmptyTransaction() throws Exception {
        var output = new Output();
        try (var writer = new ProtoWriter(output)) {
            writer.writeRow(row("a", "a", 1, 1, SinkProto.OperationType.INSERT));
            writer.writeTrans(begin("a"));
            writer.writeTrans(end("a", 1, 0));
            int prior = output.bytes.size();
            writer.writeTrans(begin("empty"));
            writer.writeTrans(end("empty", 0, 0));
            require(output.bytes.size() > prior, "An empty BEGIN/END pair was lost");
        }
    }

    private static void appendFailureRemainsFatal() throws Exception {
        var output = new Output();
        output.failAppend = true;
        var writer = new ProtoWriter(output);
        writer.writeTrans(begin("a"));
        writer.writeTrans(end("a", 1, 0));
        var first = reject(() -> writer.writeRow(row("a", "a", 1, 1, SinkProto.OperationType.INSERT)));
        require(first.getCause() == output.failure, "The append failure was hidden");
        require(reject(() -> writer.writeTrans(begin("b"))).getCause() == output.failure, "The first failure was replaced");
        output.failClose = true;
        try {
            writer.close();
            throw new AssertionError("Close accepted a failed write");
        } catch (IOException failure) {
            require(failure == output.failure && failure.getSuppressed().length == 1, "Close hid the first failure");
        }
        require(output.closed && !output.complete, "A failed file was marked complete");
    }

    private static void incompleteClose() throws Exception {
        for (int mode = 0; mode < 3; mode++) {
            var output = new Output();
            var writer = new ProtoWriter(output);
            if (mode < 2) writer.writeTrans(begin("a"));
            if (mode == 1) writer.writeTrans(end("a", 1, 0));
            if (mode == 2) writer.writeRow(row("a", "a", 1, 1, SinkProto.OperationType.INSERT));
            try {
                writer.close();
                throw new AssertionError("Incomplete close succeeded: " + mode);
            } catch (IOException expected) {
                require(output.closed && !output.complete, "Incomplete file completion was published");
            }
        }
    }

    private static void invalidRows() throws Exception {
        // Hole in global order; wrong per-table order; unexpected table; duplicate row.
        for (int mode = 0; mode < 4; mode++) {
            var output = new Output();
            var writer = new ProtoWriter(output);
            writer.writeTrans(begin("a"));
            writer.writeTrans(end("a", 2, 0));
            writer.writeRow(row("a", "a", 1, 1, SinkProto.OperationType.INSERT));
            int current = mode;
            reject(() -> writer.writeRow(row("a", current == 2 ? "b" : "a",
                    current == 0 ? 3 : current == 3 ? 1 : 2,
                    current == 1 ? 3 : 2, SinkProto.OperationType.INSERT)));
            try { writer.close(); throw new AssertionError("Bad row accepted at close"); }
            catch (IOException expected) { require(!output.complete && output.bytes.size() == 0, "Bad rows were published"); }
        }
    }

    private static void invalidMetadata() throws Exception {
        var output = new Output();
        var writer = new ProtoWriter(output);
        writer.writeTrans(begin("a"));
        reject(() -> writer.writeTrans(end("a", 1, 0).toBuilder().setEventCount(2).build()));
        try { writer.close(); throw new AssertionError("Bad metadata accepted at close"); }
        catch (IOException expected) { require(!output.complete, "Bad metadata was published"); }
    }

    private static SinkProto.TransactionMetadata begin(String id) {
        return SinkProto.TransactionMetadata.newBuilder().setId(id).setStatus(SinkProto.TransactionStatus.BEGIN).build();
    }

    private static SinkProto.TransactionMetadata end(String id, long a, long b) {
        var builder = SinkProto.TransactionMetadata.newBuilder().setId(id).setStatus(SinkProto.TransactionStatus.END).setEventCount(a + b);
        if (a != 0) builder.addDataCollections(SinkProto.DataCollection.newBuilder().setDataCollection("public.a").setEventCount(a));
        if (b != 0) builder.addDataCollections(SinkProto.DataCollection.newBuilder().setDataCollection("public.b").setEventCount(b));
        return builder.build();
    }

    private static RowChangeEvent row(String id, String table, long order, long tableOrder, SinkProto.OperationType op) throws Exception {
        var builder = SinkProto.RowRecord.newBuilder().setOp(op)
                .setSource(SinkProto.SourceInfo.newBuilder().setDb("postgres").setSchema("public").setTable(table))
                .setTransaction(SinkProto.TransactionInfo.newBuilder().setId(id).setTotalOrder(order).setDataCollectionOrder(tableOrder));
        if (op != SinkProto.OperationType.INSERT) builder.setBefore(value("before"));
        if (op != SinkProto.OperationType.DELETE) builder.setAfter(value("after"));
        return new RowChangeEvent(builder.build(), TypeDescription.fromString("struct<id:string>"), null);
    }

    private static SinkProto.RowValue value(String value) {
        return SinkProto.RowValue.newBuilder().addValues(SinkProto.ColumnValue.newBuilder().setValue(ByteString.copyFromUtf8(value))).build();
    }

    private interface Action { void run() throws Exception; }

    private static UncheckedIOException reject(Action action) throws Exception {
        try { action.run(); }
        catch (UncheckedIOException expected) { return expected; }
        throw new AssertionError("Invalid input/write was accepted");
    }

    private static void require(boolean valid, String message) {
        if (!valid) throw new AssertionError(message);
    }
}
