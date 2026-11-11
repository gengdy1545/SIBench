package org.sibench.capture;

import static org.junit.Assert.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.Test;
import org.sibench.config.ObjectStore;

/** Explicit small remote-storage acceptance; retains its unique input prefix and local evidence. */
public class InputStoreCheck {
    @Test public void remoteRoundTripPreservesThePublishedInput() throws Exception {
        String configuration = System.getProperty("sibench.storage.config");
        String profile = System.getProperty("sibench.storage.profile");
        String prefix = System.getProperty("sibench.storage.prefix");
        assertNotNull("Set sibench.storage.config", configuration);
        assertNotNull("Set sibench.storage.profile", profile);
        assertNotNull("Set a fresh sibench.storage.prefix", prefix);
        Path work = Files.createTempDirectory(Path.of("results/capture"), "input-store-check-");
        var input = CsvInputTest.fixture(Files.createDirectory(work.resolve("source")));
        var original = CsvInput.open(input.output());
        var settings = ObjectStore.of(Path.of(configuration), profile, prefix);
        long started = System.nanoTime();
        try (var store = new InputStore(settings)) {
            var receipt = store.publish(input.output());
            Path restored = work.resolve("restored");
            store.fetch(receipt.get("manifest_sha256").toString(), restored);
            assertEquals(original.sha256(), CsvInput.open(restored).sha256());
            assertEquals(original.sha256(), CsvInput.open(input.output()).sha256());
            Files.writeString(work.resolve("acceptance.json"), new com.google.gson.GsonBuilder().setPrettyPrinting().create().toJson(
                    Map.of("status", "passed", "store", settings.toString(), "publication", receipt,
                            "csv_manifest_sha256", original.sha256(), "seconds", (System.nanoTime() - started) / 1e9)));
        }
        System.out.println("Input storage acceptance: " + work);
    }
}
