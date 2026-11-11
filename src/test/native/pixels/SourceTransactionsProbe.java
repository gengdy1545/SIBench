package org.sibench.pixels;

import io.pixelsdb.pixels.sink.SibenchSourceTransactions;
import io.pixelsdb.pixels.sink.source.engine.DebeziumRecordType;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

/** Hand-built PostgreSQL envelopes; no database, metadata service or reference replay is used. */
public final class SourceTransactionsProbe {
    private static final Schema COLLECTION = SchemaBuilder.struct().field("data_collection", Schema.STRING_SCHEMA)
            .field("event_count", Schema.INT64_SCHEMA).build();
    private static final Schema CONTROL = SchemaBuilder.struct().field("id", Schema.STRING_SCHEMA)
            .field("status", Schema.STRING_SCHEMA).field("event_count", Schema.INT64_SCHEMA)
            .field("data_collections", SchemaBuilder.array(COLLECTION).build()).build();
    private static final Schema TX = SchemaBuilder.struct().field("id", Schema.STRING_SCHEMA)
            .field("total_order", Schema.INT64_SCHEMA).field("data_collection_order", Schema.INT64_SCHEMA).build();
    private static final Schema SOURCE = SchemaBuilder.struct().field("schema", Schema.STRING_SCHEMA)
            .field("table", Schema.STRING_SCHEMA).build();
    private static final Schema ROW = SchemaBuilder.struct().field("transaction", TX).field("source", SOURCE).build();

    private static SourceRecord record(Struct value) {
        return new SourceRecord(Map.of("server","sibench"), Map.of("lsn",1L), "sibench", value.schema(), value);
    }
    private static SourceRecord control(String id, String status, long count, List<Struct> collections) {
        return record(new Struct(CONTROL).put("id",id).put("status",status).put("event_count",count).put("data_collections",collections));
    }
    private static Struct collection(String table, long count) {
        return new Struct(COLLECTION).put("data_collection","public."+table).put("event_count",count);
    }
    private static SourceRecord row(String id, String table, long order, long tableOrder) {
        return record(new Struct(ROW).put("transaction",new Struct(TX).put("id",id).put("total_order",order)
                .put("data_collection_order",tableOrder)).put("source",new Struct(SOURCE).put("schema","public").put("table",table)));
    }
    private static SibenchSourceTransactions begin() {
        var guard=new SibenchSourceTransactions();
        guard.accept(control("tx","BEGIN",0,List.of()),DebeziumRecordType.TRANSACTION);
        return guard;
    }
    private static void reject(Runnable action) {
        try { action.run(); } catch (RuntimeException expected) { return; }
        throw new AssertionError("Malformed source transaction was accepted");
    }
    public static void main(String[] args) {
        var postgres=new SibenchSourceTransactions();
        postgres.accept(control("737:23728696","BEGIN",0,List.of()),DebeziumRecordType.TRANSACTION);
        postgres.accept(row("737:23728696","savingaccount",1,1),DebeziumRecordType.ROW);
        postgres.accept(control("737:23728976","END",1,List.of(collection("savingaccount",1))),DebeziumRecordType.TRANSACTION);
        var valid=begin();
        valid.accept(row("tx","a",1,1),DebeziumRecordType.ROW);
        valid.accept(row("tx","b",2,1),DebeziumRecordType.ROW);
        valid.accept(row("tx","a",3,2),DebeziumRecordType.ROW);
        valid.accept(control("tx","END",3,List.of(collection("a",2),collection("b",1))),DebeziumRecordType.TRANSACTION);
        valid.accept(control("empty","BEGIN",0,List.of()),DebeziumRecordType.TRANSACTION);
        valid.accept(control("empty","END",0,List.of()),DebeziumRecordType.TRANSACTION);
        valid.finish();
        reject(()->begin().finish());
        reject(()->new SibenchSourceTransactions().accept(row("tx","a",1,1),DebeziumRecordType.ROW));
        reject(()->begin().accept(row("foreign","a",1,1),DebeziumRecordType.ROW));
        reject(()->begin().accept(row("tx","a",2,1),DebeziumRecordType.ROW));
        reject(()->begin().accept(row("tx","a",1,2),DebeziumRecordType.ROW));
        reject(()->begin().accept(control("nested","BEGIN",0,List.of()),DebeziumRecordType.TRANSACTION));
        reject(()->begin().accept(control("tx","END",1,List.of(collection("a",1))),DebeziumRecordType.TRANSACTION));
        var missing=begin(); missing.accept(row("tx","a",1,1),DebeziumRecordType.ROW);
        reject(()->missing.accept(control("tx","END",1,List.of(collection("b",1))),DebeziumRecordType.TRANSACTION));
        var duplicate=begin(); duplicate.accept(row("tx","a",1,1),DebeziumRecordType.ROW);
        reject(()->duplicate.accept(control("tx","END",1,List.of(collection("a",1),collection("a",1))),DebeziumRecordType.TRANSACTION));
        reject(()->begin().accept(control("tx","END",0,List.of()),DebeziumRecordType.UNKNOWN_CONTROL));
        System.out.println("Source transaction order/count counterexamples passed");
    }
}
