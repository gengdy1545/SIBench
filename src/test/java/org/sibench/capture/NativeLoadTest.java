package org.sibench.capture;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import org.junit.Test;

public class NativeLoadTest {

    @Test
    public void finalActualRowsMustIncludeEveryLoadedBatch() throws Exception {
        var config = NativeCaptureConfig.read(Path.of(""), Path.of("src/test/resources/config/ycsb_1k.yaml"));
        NativeLoad.requireRows(config, Map.of("usertable", 1000L));
        // Client BATCHED_OK counters do not establish that the final database contains the last batch.
        for (var actual : java.util.List.of(Map.of("usertable", 999L), Map.of("usertable", 1001L),
                Map.<String, Long>of(), Map.of("another_table", 1000L)))
            assertTrue(assertThrows(IOException.class, () -> NativeLoad.requireRows(config, actual))
                    .getMessage().contains("different row count"));
    }
}
