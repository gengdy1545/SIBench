package org.sibench.generation;

import java.nio.file.Path;
import java.util.Map;
import org.sibench.config.ConfigValues;
import org.sibench.config.Engine;
import org.sibench.config.SibenchConfig;

/** Native PostgreSQL checks run with the repository's engine settings. */
public final class PostgresTestSettings {
    public static final Map<String, String> SETTINGS = read();

    private PostgresTestSettings() {}

    private static Map<String, String> read() {
        try {
            Path root = Path.of("").toAbsolutePath();
            var settings = SibenchConfig.engine(root, SibenchConfig.read(root.resolve("config/sibench.yaml")), Engine.POSTGRES);
            return SibenchConfig.postgresSettings(ConfigValues.object(settings, "settings"));
        } catch (java.io.IOException error) {
            throw new java.io.UncheckedIOException(error);
        }
    }
}
