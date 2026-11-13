package org.sibench.capture;

import static org.junit.Assert.*;

import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Hand-checked renderings, parameter domains and answer comparison; the oracle itself is a real PostgreSQL. */
public class HybenchQueriesTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    private static JsonArray row(Object... values) { return HybenchQueries.canonicalRow(java.util.Arrays.asList(values)); }
    private static JsonArray json(String text) { return JsonParser.parseString(text).getAsJsonArray(); }

    @Test public void placeholdersBecomeLiteralsAndCountsMustMatch() throws Exception {
        assertEquals("select 1 from t where a=7 and b='it''s'", HybenchQueries.render("select 1 from t where a=? and b=?;\n", List.of(7L, "it's")));
        assertThrows(IOException.class, () -> HybenchQueries.render("a=? and b=?", List.of(1L)));
        assertThrows(IOException.class, () -> HybenchQueries.render("a=?", List.of(1L, 2L)));
    }

    @Test public void parametersStayInHybenchDomainsAndRepeatWithinAStatement() {
        var domains = new HybenchQueries.Domains(300, 2);
        var random = new Random(5);
        for (int i = 0; i < 1000; i++) {
            for (var template : HybenchQueries.TEMPLATES) {
                var values = HybenchQueries.draw(template, domains, random, kind -> kind.name());
                for (int p = 0; p < values.size(); p++) {
                    switch (template.parameters().get(p)) {
                        case CUSTOMER -> assertTrue((long) values.get(p) >= 1 && (long) values.get(p) <= 300);
                        case COMPANY -> assertTrue((long) values.get(p) >= 301 && (long) values.get(p) <= 302);
                        default -> assertEquals(template.parameters().get(p).name(), values.get(p));
                    }
                    if (p > 0 && template.parameters().get(p) == template.parameters().get(0)) assertEquals(values.get(0), values.get(p));
                }
            }
        }
        var seeded = HybenchQueries.TEMPLATES.get(0);
        assertEquals(HybenchQueries.draw(seeded, domains, new Random(9), Object::toString),
                HybenchQueries.draw(seeded, domains, new Random(9), Object::toString));
    }

    @Test public void engineRepresentationsCompareEqualAndWrongAnswersAreRejected() {
        // PostgreSQL JDBC objects on one side, Trino JSON values on the other.
        var postgres = List.of(row(java.sql.Timestamp.valueOf("2023-11-01 10:00:00"), 2400.0f, 7, "acme   "),
                row(java.sql.Timestamp.valueOf("2023-11-02 08:30:00.25"), 0.333333f, 8, null));
        var expected = HybenchQueries.expected(postgres, null);
        var trino = List.of(json("[\"2023-11-02 08:30:00.250\", 0.33333334, 8, null]"),
                json("[\"2023-11-01 10:00:00.000\", 2400.0, 7, \"acme\"]"));
        assertNull(HybenchQueries.difference(expected, trino, null));
        assertNotNull(HybenchQueries.difference(expected, List.of(trino.get(0)), null));
        assertNotNull(HybenchQueries.difference(expected, List.of(trino.get(0), trino.get(1), trino.get(1)), null));
        assertNotNull(HybenchQueries.difference(expected, List.of(trino.get(0), json("[\"2023-11-01 10:00:00.000\", 2401, 7, \"acme\"]")), null));
        assertNotNull(HybenchQueries.difference(expected, List.of(trino.get(0), json("[\"2023-11-01 10:00:00.000\", 2400, 9, \"acme\"]")), null));
    }

    @Test public void integerTypesCompareExactlyWhileFloatingSumsKeepFiveDigits() {
        // Trino 466 writes integer types without a fraction and REAL/DOUBLE with one (Jackson writeNumber).
        var expected = HybenchQueries.expected(List.of(row(1234567, 1234567L, 1234567.0f)), null);
        assertNull(HybenchQueries.difference(expected, List.of(json("[1234567, 1234567, 1234567.1]")), null));
        assertNotNull(HybenchQueries.difference(expected, List.of(json("[1234649, 1234567, 1234567.1]")), null));
        assertNotNull(HybenchQueries.difference(expected, List.of(json("[1234567, 1234649, 1234567.1]")), null));
        assertNotNull(HybenchQueries.difference(expected, List.of(json("[1234567, 1234567, 1234749.0]")), null));
    }

    @Test public void limitedOrderingComparesKeysSoTiesMayChooseDifferentRows() {
        var expected = HybenchQueries.expected(List.of(row(5, "a"), row(3, "b"), row(3, "c")), 0);
        assertNull(HybenchQueries.difference(expected, List.of(json("[5,\"a\"]"), json("[3,\"z\"]"), json("[3,\"b\"]")), 0));
        assertNotNull(HybenchQueries.difference(expected, List.of(json("[5,\"a\"]"), json("[3,\"b\"]"), json("[2,\"c\"]")), 0));
        assertNotNull(HybenchQueries.difference(expected, List.of(json("[3,\"b\"]"), json("[5,\"a\"]"), json("[3,\"c\"]")), 0));
    }

    @Test public void renderedSetsBindEverySqlFileAndOnlyStaticRunsOfTheirCsv() throws Exception {
        Path directory = temporary.newFolder("queries").toPath();
        Files.writeString(directory.resolve("001-IQ-3-0.sql"), "select 1\n");
        var entry = InputFiles.file(directory, directory.resolve("001-IQ-3-0.sql"));
        entry.add("expected", new JsonArray());
        var queries = new JsonArray(); queries.add(entry);
        var manifest = new com.google.gson.JsonObject();
        manifest.addProperty("kind", "hybench_query_set"); manifest.addProperty("status", "complete");
        manifest.addProperty("csv_manifest_sha256", "a".repeat(64)); manifest.add("queries", queries);
        Files.writeString(directory.resolve("manifest.json"), manifest.toString());
        assertNotNull(HybenchQueries.open(directory));
        var baseline = new PixelsBaseline.Verified(directory, JsonParser.parseString("{\"csv_manifest_sha256\":\"" + "a".repeat(64) + "\"}").getAsJsonObject(), "");
        assertNotNull(PixelsExperiment.answers(directory, Map.of("updates", false), baseline));
        assertThrows(IllegalArgumentException.class, () -> PixelsExperiment.answers(directory, Map.of("updates", true), baseline));
        var other = new PixelsBaseline.Verified(directory, JsonParser.parseString("{\"csv_manifest_sha256\":\"" + "b".repeat(64) + "\"}").getAsJsonObject(), "");
        assertThrows(IllegalArgumentException.class, () -> PixelsExperiment.answers(directory, Map.of("updates", false), other));
        Files.writeString(directory.resolve("002-extra.sql"), "select 2\n");
        assertThrows(IOException.class, () -> HybenchQueries.open(directory));
        Files.delete(directory.resolve("002-extra.sql"));
        Files.writeString(directory.resolve("001-IQ-3-0.sql"), "select 3\n");
        assertThrows(IOException.class, () -> HybenchQueries.open(directory));
        assertNull(HybenchQueries.open(temporary.newFolder("plain").toPath()));
        Path failed = temporary.newFolder("failed-render").toPath();
        Files.writeString(failed.resolve("001-IQ-3-0.sql"), "select 1\n");
        Files.writeString(failed.resolve("INCOMPLETE"), "Query rendering pending\n");
        assertThrows(IOException.class, () -> HybenchQueries.open(failed));
    }
}
