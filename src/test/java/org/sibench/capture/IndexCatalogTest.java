package org.sibench.capture;

import static org.junit.Assert.*;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.capture.IndexOperationTrace.Operation;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;

public class IndexCatalogTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void sourceCatalogCoversEveryIndexAndCanonicalizesValues() throws Exception {
        var catalog = IndexCatalog.read(Path.of("benchmark/HyBench-2024"));
        assertEquals(21, catalog.definitions().size());
        assertEquals(3, catalog.secondary(5).size());
        assertEquals("1", catalog.named("idx_transfer_1").normalize("001"));
        assertEquals(
                "2024-01-01T00:00",
                catalog.named("idx_loanapps_2").normalize("2024-01-01 00:00:00"));
        assertEquals("value", catalog.named("idx_transfer_3").normalize("value   "));
        assertEquals("value\t", catalog.named("idx_transfer_3").normalize("value\t "));
        assertEquals("value ", catalog.named("idx_company_1").normalize("value "));
        assertNull(catalog.named("idx_transfer_3").normalize(null));
        assertThrows(
                IOException.class,
                () -> catalog.named("idx_loanapps_2").normalize("not a timestamp"));
    }

    @Test
    public void unsupportedSourceIndexDefinitionsAreNotSilentlySkipped() throws Exception {
        for (String sql :
                List.of(
                        "CREATE UNIQUE INDEX changed ON transfer(sourceid);",
                        "CREATE INDEX changed ON transfer(sourceid, targetid);",
                        "CREATE INDEX changed ON transfer(sourceid) WHERE sourceid > 0;")) {
            Path source = temporary.newFolder().toPath();
            Files.createDirectory(source.resolve("conf"));
            Files.copy(
                    Path.of("benchmark/HyBench-2024/conf/ddl_pg.sql"),
                    source.resolve("conf/ddl_pg.sql"));
            Files.writeString(source.resolve("conf/create_index_pg.sql"), sql);
            assertThrows(IOException.class, () -> IndexCatalog.read(source));
        }
    }

    @Test
    public void genericMaintenanceSupportsNullLongKeysAndMoreThanTwoIndexes() throws Exception {
        for (boolean rowIds : new boolean[] {false, true}) {
            var model = new IndexReferenceReplay(rowIds);
            long key = (long) Integer.MAX_VALUE + 10;
            Map<Integer, String> before = new LinkedHashMap<>();
            before.put(1, "1");
            before.put(2, "timestamp");
            before.put(3, null);
            Operation baseline = new Operation(0, 1, 5, 0, 0, key, 0, 1, 0, 9, 0, 0, 0);
            IndexMutation.emit(baseline, Map.of(), before, model::accept);
            var after = new LinkedHashMap<>(before);
            after.put(3, "value");
            Operation update = new Operation(1, 3, 5, 0, 2, key, 1, 2, 0, 9, 0, 0, 0, 1, 1);
            var operations = new ArrayList<Operation>();
            IndexMutation.emit(update, before, after, operations::add);
            assertEquals(5, operations.size());
            for (Operation operation : operations) model.accept(operation);
            assertEquals(1, model.lookupSecondary(5, 3, null, 0).size());
            assertTrue(model.lookupSecondary(5, 3, null, 1).isEmpty());
            assertEquals(key, model.lookupSecondary(5, 3, "value", 1).get(0).key());
            assertEquals(1, model.lookupSecondary(5, 1, "1", 1).get(0).commitVersion());
        }
    }
}
