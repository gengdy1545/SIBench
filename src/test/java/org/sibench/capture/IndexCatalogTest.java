package org.sibench.capture;

import static org.junit.Assert.*;

import org.junit.Test;
import org.sibench.capture.IndexOperationTrace.Operation;
import org.sibench.capture.IndexOperationTrace.Range;
import org.sibench.trace.IndexKey;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;

public class IndexCatalogTest {
    @Test
    public void sourceCatalogCoversEveryIndexAndEncodesNativeValues() throws Exception {
        var catalog = IndexCatalog.fromSchema(org.sibench.schema.SourceSchema.hybench(Path.of("benchmark/HyBench-2024")));
        assertEquals(21, catalog.definitions().size());
        assertEquals(3, catalog.secondary(5).size());
        assertEquals(5, catalog.tableId("transfer"));
        assertEquals("id", catalog.primary(5).column());
        assertEquals(IndexKey.ofInt(1), catalog.named("idx_transfer_1").encode("001"));
        assertEquals(
                IndexKey.parse("timestamp", "2024-01-01T00:00"),
                catalog.named("idx_loanapps_2").encode("2024-01-01 00:00:00"));
        // DuckDB CHAR is a VARCHAR alias; trailing spaces remain significant.
        assertEquals(IndexKey.ofText("value   "), catalog.named("idx_transfer_3").encode("value   "));
        assertEquals(
                IndexKey.ofText("value\t "), catalog.named("idx_transfer_3").encode("value\t "));
        assertEquals(IndexKey.ofText("value "), catalog.named("idx_company_1").encode("value "));
        assertTrue(catalog.named("idx_transfer_3").encode(null).isNull());
        assertThrows(
                IOException.class,
                () -> catalog.named("idx_loanapps_2").encode("not a timestamp"));

    }

    @Test
    public void rejectsChangedSourceIdentityEvenWithAnIdenticalSchema() throws Exception {
        var catalog = IndexCatalog.fromSchema(org.sibench.schema.SourceSchema.hybench(Path.of("benchmark/HyBench-2024")));
        var manifest = new com.google.gson.Gson().toJsonTree(catalog.provenance()).getAsJsonObject();
        IndexCatalog.fromManifest(manifest);
        manifest.addProperty("source_commit", "different-build-of-the-same-schema");
        assertThrows(IOException.class, () -> IndexCatalog.fromManifest(manifest));
    }

    @Test
    public void consumesAnEmbeddedCatalogWithoutTheSourceCheckoutAndRejectsConflictingIdentities() throws Exception {
        var catalog = IndexCatalog.fromSchema(org.sibench.schema.SourceSchema.hybench(Path.of("benchmark/HyBench-2024")));
        var manifest = new com.google.gson.Gson().toJsonTree(catalog.provenance()).getAsJsonObject();
        manifest.addProperty("source_directory", "/missing/generation-host/source-checkout");
        assertEquals(catalog.definitions(), IndexCatalog.fromManifest(manifest).definitions());
        var duplicate = manifest.deepCopy();
        duplicate.getAsJsonArray("index_definitions").add(duplicate.getAsJsonArray("index_definitions").get(0).deepCopy());
        duplicate.addProperty("catalog_sha256", IndexCatalog.identity(duplicate));
        assertThrows(IOException.class, () -> IndexCatalog.fromManifest(duplicate));
        var hole = manifest.deepCopy();
        hole.getAsJsonArray("index_definitions").get(0).getAsJsonObject().addProperty("index_id", 99);
        hole.addProperty("catalog_sha256", IndexCatalog.identity(hole));
        assertThrows(IOException.class, () -> IndexCatalog.fromManifest(hole));
    }

    @Test
    public void genericMaintenanceSupportsNullLongKeysAndMoreThanTwoIndexes() throws Exception {
        for (boolean rowIds : new boolean[] {false, true}) {
            var model = new IndexReferenceReplay(rowIds);
            IndexKey key = IndexKey.ofInt((long) Integer.MAX_VALUE + 10);
            Map<Integer, IndexKey> before = new LinkedHashMap<>();
            before.put(1, IndexKey.ofInt(1));
            before.put(2, IndexKey.ofText("timestamp"));
            before.put(3, IndexKey.NULL);
            Operation baseline =
                    IndexOperationTrace.write(0, IndexOperationTrace.INSERT, 5, 0, key, 0, 1, 0, 9);
            IndexMutation.emit(baseline, Map.of(), before, model::accept);
            var after = new LinkedHashMap<>(before);
            after.put(3, IndexKey.ofText("value"));
            Operation update =
                    IndexOperationTrace.write(1, IndexOperationTrace.UPSERT, 5, 2, key, 1, 2, 0, 9)
                            .withTransaction(1, 1);
            var operations = new ArrayList<Operation>();
            IndexMutation.emit(update, before, after, operations::add);
            assertEquals(5, operations.size());
            for (Operation operation : operations) model.accept(operation);
            assertEquals(1, model.lookupSecondary(5, 3, IndexKey.NULL, 0).size());
            assertTrue(model.lookupSecondary(5, 3, IndexKey.NULL, 1).isEmpty());
            assertEquals(
                    key, model.lookupSecondary(5, 3, IndexKey.ofText("value"), 1).get(0).key());
            assertEquals(
                    1, model.lookupSecondary(5, 1, IndexKey.ofInt(1), 1).get(0).commitVersion());
            // IS NOT NULL is the interval below the NULL marker.
            assertEquals(
                    1,
                    model.search(5, 3, new Range(null, false, IndexKey.NULL, false, false, 0), 1)
                            .size());
            assertTrue(
                    model.search(5, 3, new Range(null, false, IndexKey.NULL, false, false, 0), 0)
                            .isEmpty());
        }
    }
}
