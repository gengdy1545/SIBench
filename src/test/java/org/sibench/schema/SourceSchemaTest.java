package org.sibench.schema;

import static org.junit.Assert.*;

import com.google.gson.JsonParser;

import org.junit.Test;
import org.sibench.capture.IndexCatalog;
import org.sibench.config.ConfigValues;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;

public class SourceSchemaTest {
    static Map<String, Object> config(String path) throws IOException {
        return ConfigValues.parse(Files.readString(Path.of(path)));
    }

    static SourceSchema read(String path) throws Exception {
        return SourceSchema.read(Path.of(""), config(path));
    }

    @Test
    public void pinnedHybenchCoversAllColumnsIndexesAndSequenceDefaults() throws Exception {
        SourceSchema schema = read("src/test/resources/config/hybench_sf1.yaml");
        assertEquals(8, schema.tables().size());
        assertEquals(13, schema.secondaryOrder().size());
        assertEquals(2, schema.fingerprints().size());
        var transfer =
                schema.tables().stream()
                        .filter(t -> t.name().equals("transfer"))
                        .findFirst()
                        .orElseThrow();
        assertEquals(7, transfer.columns().size());
        assertEquals(
                new SourceSchema.DefaultValue("sequence", "transfer_id_seq"),
                transfer.columns().get(0).defaultValue());
        assertEquals(
                new SourceSchema.DefaultValue("current_timestamp", ""),
                transfer.columns().get(6).defaultValue());
        assertEquals("character(10)", transfer.columns().get(4).type());
        assertTrue(transfer.columns().get(4).nullable());
        IndexCatalog catalog = IndexCatalog.fromSchema(schema);
        assertEquals("idx_loanapps_1", catalog.definitions().get(8).name());
        assertEquals("idx_ca_1", catalog.definitions().get(20).name());
        var provenance = new com.google.gson.Gson().toJsonTree(catalog.provenance()).getAsJsonObject();
        assertEquals(catalog.describe(), IndexCatalog.fromManifest(provenance).describe());
    }

    @Test
    public void ycsbUsesConfiguredFieldCountAndStringPrimaryWithoutInventedSecondary()
            throws Exception {
        SourceSchema schema = read("src/test/resources/config/ycsb_1b.yaml");
        assertEquals(1, schema.tables().size());
        var table = schema.tables().get(0);
        assertEquals(11, table.columns().size());
        assertEquals("character varying", table.columns().get(0).type());
        assertFalse(table.columns().get(0).nullable());
        assertEquals("text", table.columns().get(1).type());
        assertTrue(table.columns().get(1).nullable());
        assertEquals(1, table.indexes().size());
        IndexCatalog catalog = IndexCatalog.fromSchema(schema);
        assertEquals(
                org.sibench.trace.IndexKey.ofText("user010 "),
                catalog.named("usertable_pkey").encode("user010 "));
        assertTrue(catalog.secondary(1).isEmpty());
        var config = config("src/test/resources/config/ycsb_1b.yaml");
        var nativeConfig = ConfigValues.object(config, "native");
        nativeConfig.put("fieldcount", 3);
        config.put("native", nativeConfig);
        assertEquals(4, SourceSchema.read(Path.of(""), config).tables().get(0).columns().size());
    }

    @Test
    public void sysbenchResolvesAllConfiguredTableNamespacesAndAutoIncrementOption()
            throws Exception {
        SourceSchema schema = read("src/test/resources/config/sysbench_1b.yaml");
        assertEquals(100, schema.tables().size());
        assertEquals(200, IndexCatalog.fromSchema(schema).definitions().size());
        assertEquals("k_100", schema.tables().get(99).indexes().get(1).name());
        assertEquals(
                new SourceSchema.DefaultValue("sequence", "sbtest100_id_seq"),
                schema.tables().get(99).columns().get(0).defaultValue());
        assertEquals(
                org.sibench.trace.IndexKey.ofInt(10),
                IndexCatalog.fromSchema(schema).named("k_100").encode("010"));
        var config = config("src/test/resources/config/sysbench_1b.yaml");
        var nativeConfig = ConfigValues.object(config, "native");
        nativeConfig.put("tables", 2);
        nativeConfig.put("auto-inc", false);
        config.put("native", nativeConfig);
        SourceSchema changed = SourceSchema.read(Path.of(""), config);
        assertEquals(2, changed.tables().size());
        assertEquals(
                SourceSchema.DefaultValue.none(),
                changed.tables().get(0).columns().get(0).defaultValue());
    }

    @Test
    public void unsupportedSourceOptionsAndPinsFailExplicitly() throws Exception {
        for (var option :
                Map.<String, Object>of(
                                "secondary",
                                true,
                                "create-secondary",
                                false,
                                "pgsql-variant",
                                "redshift",
                                "create-table-options",
                                "WITH (fillfactor=50)")
                        .entrySet()) {
            var config = config("src/test/resources/config/sysbench_1b.yaml");
            var nativeConfig = ConfigValues.object(config, "native");
            nativeConfig.put(option.getKey(), option.getValue());
            config.put("native", nativeConfig);
            assertThrows(
                    IllegalArgumentException.class, () -> SourceSchema.read(Path.of(""), config));
        }
        var config = config("src/test/resources/config/ycsb_1b.yaml");
        var source = ConfigValues.object(config, "source");
        source.put("commit", "unreviewed");
        config.put("source", source);
        assertThrows(IllegalArgumentException.class, () -> SourceSchema.read(Path.of(""), config));
    }

    @Test
    public void ddlGrammarParsesCommentsButRejectsUnsupportedOrTruncatedDefinitions()
            throws Exception {
        String ddl = "/* table */ CREATE TABLE example(id int PRIMARY KEY, value char(10));";
        var parsed = SimpleDdl.read(ddl, "-- index\nCREATE INDEX idx_value ON example(value);");
        assertEquals("character(10)", parsed.tables().get(0).columns().get(1).type());
        for (String sql :
                List.of(
                        "CREATE UNIQUE INDEX x ON example(value);",
                        "CREATE INDEX x ON example(id,value);",
                        "CREATE INDEX x ON example(lower(value));",
                        "CREATE INDEX x ON example(value) WHERE id>0;",
                        "CREATE INDEX x ON example(value DESC);",
                        "CREATE INDEX x ON example(value) INCLUDE (id);",
                        "CREATE INDEX x ON example USING hash(value);",
                        "CREATE INDEX x ON example(value",
                        "ALTER TABLE example ADD COLUMN other int;",
                        "ALTER TABLE example ALTER COLUMN id SET DEFAULT 1;",
                        "CREATE SEQUENCE IF NOT EXISTS example_id_seq;"))
            assertThrows(IOException.class, () -> SimpleDdl.read(ddl, sql));
        assertThrows(IOException.class, () -> SimpleDdl.read(ddl + "/*", ""));
        assertThrows(
                IOException.class,
                () -> SimpleDdl.read(ddl, "CREATE INDEX x ON example(missing);"));
    }
}
