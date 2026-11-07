package org.sibench.capture;

import static org.junit.Assert.*;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Explicit check of the patched upstream sampler, independent of native query execution. */
public class HybenchSamplingCheck {
    @Rule public TemporaryFolder temporary=new TemporaryFolder();

    @Test public void samplesOnlyAvailableIdsAndRejectsAnEmptyRiskPopulation()throws Exception {
        Path build=Path.of(System.getProperty("sibench.hybench.build")).toRealPath();
        try(var loader=new URLClassLoader(new java.net.URL[]{build.resolve("target/classes").toUri().toURL()},getClass().getClassLoader())) {
            var type=loader.loadClass("com.hybench.load.DataGenerator_RiskControlling");
            var generator=type.getConstructor(String.class).newInstance("1x");
            var sample=type.getMethod("reservoir_sampling",String.class,Integer[].class);
            Path candidates=temporary.newFile("candidates.csv").toPath();
            Files.writeString(candidates,"7\n11\n");
            for(int capacity:List.of(2,100))
                assertArrayEquals(new Integer[]{7,11},(Integer[])sample.invoke(generator,candidates.toString(),new Integer[capacity]));
            Files.writeString(candidates,"7\n11\n13\n17\n");
            var selected=(Integer[])sample.invoke(generator,candidates.toString(),new Integer[2]);
            assertEquals(2,selected.length);assertEquals(2,new HashSet<>(Arrays.asList(selected)).size());
            assertTrue(List.of(7,11,13,17).containsAll(Arrays.asList(selected)));
            Files.writeString(candidates,"");
            var failure=assertThrows(java.lang.reflect.InvocationTargetException.class,
                    ()->sample.invoke(generator,candidates.toString(),new Integer[100]));
            assertTrue(failure.getCause() instanceof IllegalStateException);
        }
    }
}
