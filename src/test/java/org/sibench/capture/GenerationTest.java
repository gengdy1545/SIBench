package org.sibench.capture;

import static org.junit.Assert.*;
import java.io.IOException;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** The closed native database is discarded only after its Trace is published. */
public class GenerationTest {
    @Rule public TemporaryFolder temporary=new TemporaryFolder();

    @Test public void preparationSelectsHybenchWithoutRequiringItsNativeOrPixelsEngine()throws Exception {
        var root=temporary.getRoot().toPath();
        Files.writeString(root.resolve("hybench.yaml"),"benchmark: hybench\n");
        var selected=Generation.hybenchSelection(root,Map.of("benchmarks",java.util.List.of(
                Map.of("config","hybench.yaml","mode","capture","engine","postgres"))));
        assertEquals("hybench",selected.benchmark());
        assertEquals(org.sibench.config.Engine.POSTGRES,selected.engine());
    }

    @Test public void failedPublicationRetainsTheClosedDatabase()throws Exception {
        var database=Files.createDirectories(temporary.getRoot().toPath().resolve("database/data"));
        Files.writeString(database.resolve("page"),"state");
        var timings=new LinkedHashMap<String,Double>();
        var failure=assertThrows(IOException.class,()->Generation.publishThenDiscard(()->{throw new IOException("manifest");},
                database.getParent(),timings));
        assertEquals("manifest",failure.getMessage());
        assertTrue(Files.isRegularFile(database.resolve("page")));
        assertTrue(timings.isEmpty());
    }

    @Test public void publishedTraceDiscardsTheDatabase()throws Exception {
        var database=Files.createDirectories(temporary.getRoot().toPath().resolve("database/data"));
        Files.writeString(database.resolve("page"),"state");
        var timings=new LinkedHashMap<String,Double>();
        boolean[] published={false};
        assertEquals(Map.of("status","published"),Generation.publishThenDiscard(()->{
            assertTrue("publication precedes deletion",Files.exists(database));published[0]=true;
            return Map.of("status","published");
        },database.getParent(),timings));
        assertTrue(published[0]);
        assertFalse(Files.exists(database.getParent()));
        assertEquals(java.util.List.of("publication_seconds","database_cleanup_seconds"),java.util.List.copyOf(timings.keySet()));
    }

    @Test public void cleanupFailureReportsTheRemainingDatabaseAfterPublication()throws Exception {
        var missing=temporary.getRoot().toPath().resolve("absent");
        var failure=assertThrows(IOException.class,()->Generation.publishThenDiscard(()->"published",missing,new LinkedHashMap<>()));
        assertTrue(failure.getMessage().startsWith("Artifact published; closed database remains at "+missing));
    }
}
