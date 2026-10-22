package org.sibench.capture;

import org.sibench.config.ConfigValues;
import org.sibench.generation.GenerationConfig;
import org.sibench.schema.SourceSchema;

import java.nio.file.Files;
import java.nio.file.Path;

/** A capture profile of any source, resolved to what the offline stages consume. */
public record CaptureProfile(SourceSchema schema, GenerationConfig layout, Path raw, Path artifacts) {
    public static CaptureProfile read(Path root, Path profile) throws Exception {
        root = root.toRealPath();
        var document = ConfigValues.parse(Files.readString(root.resolve(profile)));
        SourceSchema schema = SourceSchema.read(root, document);
        if (ConfigValues.string(document, "benchmark").equals("hybench")) {
            CaptureConfig capture = CaptureConfig.read(root, profile);
            return new CaptureProfile(schema, capture.layout(), capture.raw(), capture.artifacts());
        }
        NativeCaptureConfig capture = NativeCaptureConfig.read(root, profile);
        return new CaptureProfile(schema, capture.layout(), capture.raw(), capture.artifacts());
    }

    public Path baseline() {
        return layout.output();
    }
}
