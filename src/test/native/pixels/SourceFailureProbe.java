package org.sibench.pixels;

import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.source.engine.PixelsDebeziumConsumer;
import io.pixelsdb.pixels.sink.writer.PixelsSinkWriter;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

/** Runs in a separate JVM: a malformed source event must report failure and halt before its ACK. */
public final class SourceFailureProbe {
    public static void main(String[] args) throws Exception {
        PixelsSinkConfigFactory.initialize(Path.of(args[0]).toString());
        var writer=new PixelsSinkWriter() {
            public boolean writeRow(RowChangeEvent row) { throw new AssertionError("Unexpected row"); }
            public boolean writeTrans(SinkProto.TransactionMetadata tx) { throw new AssertionError("Unexpected transaction"); }
            public void flush() { }
            public void close() { }
        };
        var consumer=new PixelsDebeziumConsumer(writer);
        var schema=SchemaBuilder.struct().field("op",Schema.STRING_SCHEMA).build();
        var value=new Struct(schema).put("op","u");
        var record=new SourceRecord(Map.of("server","sibench"),Map.of("lsn",1L),"sibench.public.savingaccount",schema,value);
        if (args.length > 1) {
            String method = args[1].startsWith("row") ? "publishRow" : "publishTransaction";
            var publish = PixelsDebeziumConsumer.class.getDeclaredMethod(method, SourceRecord.class,
                    io.pixelsdb.pixels.sink.util.concurrent.StreamOrderedDecoder.DecodeResult.class);
            publish.setAccessible(true);
            publish.invoke(consumer, record, new io.pixelsdb.pixels.sink.util.concurrent.StreamOrderedDecoder.DecodeResult<>(
                    null, args[1].endsWith("null") ? null : new java.io.IOException("Injected decoder failure")));
            consumer.abort();
            throw new AssertionError("Invalid decoded event was silently skipped: " + args[1]);
        }
        var acknowledged=new java.util.concurrent.atomic.AtomicBoolean();
        consumer.handleBatch(List.of((RecordChangeEvent<SourceRecord>)()->record),new DebeziumEngine.RecordCommitter<>() {
            public void markProcessed(RecordChangeEvent<SourceRecord> event) { acknowledged.set(true); }
            public void markProcessed(RecordChangeEvent<SourceRecord> event,DebeziumEngine.Offsets offsets) { acknowledged.set(true); }
            public void markBatchFinished() { }
            public DebeziumEngine.Offsets buildOffsets() { throw new AssertionError("Unexpected offset update"); }
        });
        throw new AssertionError("Malformed source returned normally; acknowledged="+acknowledged.get());
    }
}
