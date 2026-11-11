package org.sibench.capture;

import org.junit.Test;
import static org.junit.Assert.*;
import java.nio.file.*;
import java.util.*;
import org.sibench.config.*;
import org.sibench.schema.SourceSchema;
import org.sibench.trace.IndexKey;

/** Explicit bounded PG/COS/Pixels preparation, including durable baseline descriptor round-trip. */
public class PixelsPreparationCheck {
    @Test(timeout = 300_000) public void parallelConsumersPublishTheSameCsvIdentity() throws Exception {
        Path work = CdcPreparationCheck.prepareHybench(true), root = Path.of("").toRealPath();
        var runtime = ConfigValues.parse(Files.readString(work.resolve("runtime.yaml")));
        var experiment = new LinkedHashMap<>(PixelsExperimentConfig.defaults());
        experiment.put("updates", false); experiment.put("trace", true);
        runtime.put("pixels_experiment", experiment);
        Path config = Files.writeString(work.resolve("restore.json"), new com.google.gson.Gson().toJson(runtime));
        Path restored = work.resolve("restored");
        PixelsExperiment.run(root, config, work.resolve("input"), work.resolve("baseline-cache"), restored);
        var manifest = com.google.gson.JsonParser.parseString(Files.readString(restored.resolve("trace/manifest.json"))).getAsJsonObject();
        assertEquals(8706, manifest.get("baseline_rows").getAsLong());
        var csv = CsvInput.open(work.resolve("csv"));
        var tables = SourceSchema.hybench(root.resolve("benchmark/HyBench-2024")).tables().stream().map(SourceSchema.Table::name).toList();
        try (var index = new IndexReferenceReplay(true, false)) {
            ProtoTrace.source(restored.resolve("trace"), manifest, "baseline").read(index::acceptCommitted);
            for (var item : csv.manifest().getAsJsonObject("baseline_files").entrySet()) {
                var file = item.getValue().getAsJsonObject();
                if (!file.has("table")) continue;
                int table = 1 + tables.indexOf(file.get("table").getAsString());
                try (var lines = Files.lines(csv.directory().resolve(file.get("path").getAsString()))) {
                    for (String line : (Iterable<String>) lines::iterator) {
                        int key = Integer.parseInt(line.substring(0, line.indexOf(',')));
                        assertNotNull("Missing baseline key " + table + ":" + key, index.lookupPrimary(table, IndexKey.ofInt(key), 0));
                    }
                }
            }
        }
    }
}
