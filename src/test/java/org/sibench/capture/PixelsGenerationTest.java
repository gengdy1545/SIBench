package org.sibench.capture;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.config.ConfigValues;
import org.sibench.config.Engine;
import org.sibench.config.PixelsConfig;
import org.sibench.config.SibenchConfig;

public class PixelsGenerationTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test public void sinkOverridesCannotFilterOrRewriteSourceEventsOrHideTypos() {
        var accepted=new java.util.ArrayList<String>();
        for (String key:java.util.List.of("debezium.skipped.operations","debezium.column.exclude.list", "debezium.unknown",
                "sink.unknown", "debezium.column.truncate.to.1.chars")) {
            try { PixelsConfig.sinkProperties(Map.of(key,"x")); accepted.add(key); }
            catch (IllegalArgumentException expected) { }
        }
        assertEquals(java.util.List.of(),accepted);
    }

    @Test public void postgresTypesMapToPixelsTypesOrFail() throws Exception {
        assertEquals("int", PixelsSchema.type("integer"));
        assertEquals("float", PixelsSchema.type("real"));
        assertEquals("char(6)", PixelsSchema.type("character(6)"));
        assertEquals("varchar", PixelsSchema.type("character varying"));
        assertEquals("varchar(64)", PixelsSchema.type("character varying(64)"));
        assertEquals("timestamp(6)", PixelsSchema.type("timestamp without time zone"));
        assertEquals("timestamp(3)", PixelsSchema.type("timestamp(3) without time zone"));
        assertEquals("decimal(15,2)", PixelsSchema.type("numeric(15,2)"));
        assertEquals(256, PixelsSchema.bound(0));
        assertEquals(256, PixelsSchema.bound(256));
        assertEquals(512, PixelsSchema.bound(257));
        assertThrows(IllegalArgumentException.class, () -> PixelsSchema.bound(70000));
        for (String unsupported : new String[] {"numeric(10)", "numeric", "text", "timestamp with time zone", "boolean"})
            assertThrows(unsupported, IOException.class, () -> PixelsSchema.type(unsupported));
    }

    @Test public void pixelsPropertiesReplaceUpstreamValuesAndRejectUnknownOrOwnedKeys() {
        String defaults = "a.x=1\n#b.x=2\nc.x=3\n";
        assertEquals("a.x=5\n#b.x=2\nc.x=9\nd.x=7\n",
                PixelsHost.properties(defaults, Map.of("c.x", "9"), new java.util.LinkedHashMap<>(Map.of("a.x", "5", "d.x", "7"))));
        assertThrows(IllegalArgumentException.class, () -> PixelsHost.properties(defaults, Map.of("z.x", "1"), Map.of()));
        for (String owned : new String[] {"retina.enable", "index.rocksdb.data.path", "metadata.server.port", "etcd.hosts",
                "retina.buffer.object.storage.folder", "retina.tile.visibility.capacity"})
            assertThrows(owned, IllegalArgumentException.class, () -> PixelsConfig.pixelsProperties(Map.of(owned, "x")));
        for (String owned : new String[] {"debezium.snapshot.mode", "debezium.database.port", "sink.query.url", "sink.trans.mode",
                "debezium.offset.storage.file.filename", "bootstrap.servers"})
            assertThrows(owned, IllegalArgumentException.class, () -> PixelsConfig.sinkProperties(Map.of(owned, "x")));
        assertEquals(Map.of("sink.retina.client", "8"), PixelsConfig.sinkProperties(Map.of("sink.retina.client", "8")));
        assertThrows(IllegalArgumentException.class,
                () -> PixelsConfig.sinkProperties(Map.of("sink.retina.client", "8\ndebezium.skipped.operations=u")));
        assertThrows(IllegalArgumentException.class,
                () -> PixelsConfig.pixelsProperties(Map.of("node.virtual.num", "1\nretina.storage.gc.enabled=true")));
    }

    @Test public void publicEngineConfigInlinesItsOverridesForTheRunSnapshot() throws Exception {
        Path root = Path.of("").toAbsolutePath();
        var settings = SibenchConfig.engine(root, SibenchConfig.read(root.resolve("config/sibench.yaml")), Engine.PIXELS);
        assertEquals("local", ConfigValues.string(settings, "pixels_host"));
        assertEquals("false", ConfigValues.object(settings, "properties").get("index.rocksdb.multicf"));
        assertEquals("none", ConfigValues.object(settings, "sink_properties").get("sink.monitor.freshness.level"));
        assertTrue(SibenchConfig.postgresSettings(ConfigValues.object(ConfigValues.object(settings, "source"), "settings")).containsKey("fsync"));
        // Every public override names an upstream Pixels key, so none is silently ignored.
        String upstream = Files.readString(root.resolve("third_party/pixels/pixels-common/src/main/resources/pixels.properties"));
        PixelsHost.properties(upstream, PixelsConfig.properties(root.resolve("config/engine/pixels/pixels.properties")), Map.of());
    }
}
