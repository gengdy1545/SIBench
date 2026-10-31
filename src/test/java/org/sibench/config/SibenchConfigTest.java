package org.sibench.config;

import static org.junit.Assert.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sibench.adapters.ServiceConfig;
import org.sibench.analysis.UpdateLocality;
import org.sibench.replay.ReplayConfig;
import org.yaml.snakeyaml.Yaml;

public class SibenchConfigTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void replayServiceAndAnalysisUseTheSameConfiguration() throws Exception {
        Path root = Path.of("").toAbsolutePath();
        Path config = root.resolve("config/sibench.yaml");
        var document = SibenchConfig.read(config);
        assertEquals(64, ReplayConfig.read(root, config).clientCount());
        assertEquals("127.0.0.1:50551", ServiceConfig.ofReplay(root, document).endpoint());
        var analysis = UpdateLocality.Config.read(config);
        assertEquals(256, analysis.entriesPerBlock());
        assertEquals(4096, analysis.blocksPerFile());
        assertEquals(10000, analysis.businessChangesPerWindow());
    }

    @Test
    public void postgresSettingsAreEscapedAndNeverOverwriteAnExistingFile() throws Exception {
        var settings = new LinkedHashMap<String, Object>();
        settings.put("timezone", "O'Reilly\\Lab");
        settings.put("compute_query_id", true);
        settings.put("autovacuum", false);
        settings.put("checkpoint_completion_target", 0.9);
        Path config = config(settings);
        Path output = temporary.getRoot().toPath().resolve("postgres.conf");
        SibenchConfig.writePostgres(config, output);
        String expected = "# Generated from the run's SIBench configuration.\n"
                + "timezone = 'O''Reilly\\\\Lab'\n"
                + "compute_query_id = 'on'\n"
                + "autovacuum = 'off'\n"
                + "checkpoint_completion_target = '0.9'\n";
        assertEquals(expected, Files.readString(output));
        assertThrows(java.nio.file.FileAlreadyExistsException.class,
                () -> SibenchConfig.writePostgres(config, output));
        assertEquals(expected, Files.readString(output));
    }

    @Test
    public void unsafePostgresValuesFailBeforeCreatingOutput() throws Exception {
        for (Object value : new Object[] {"UTC\ninclude 'other.conf'", "UTC\r", "UTC\0",
                Double.NaN, Double.POSITIVE_INFINITY, Map.of("nested", "value")}) {
            Path config = config(Map.of("timezone", value));
            Path output = temporary.getRoot().toPath().resolve("rejected.conf");
            assertThrows(IllegalArgumentException.class,
                    () -> SibenchConfig.writePostgres(config, output));
            assertFalse(Files.exists(output));
        }
        Path config = config(Map.of("include", "other.conf"));
        Path output = temporary.getRoot().toPath().resolve("unknown.conf");
        assertThrows(IllegalArgumentException.class,
                () -> SibenchConfig.writePostgres(config, output));
        assertFalse(Files.exists(output));
    }

    @Test
    public void rejectsOldPurposesAndUnknownTopLevelOptions() {
        for (String purpose : new String[] {"index_replay", "update_locality"})
            assertThrows(IllegalArgumentException.class, () -> SibenchConfig.validate(
                    Map.of("schema_version", 1, "purpose", purpose)));
        assertThrows(IllegalArgumentException.class, () -> SibenchConfig.validate(
                Map.of("schema_version", 1, "purpose", "sibench", "replaay", Map.of())));
    }

    private Path config(Map<String, Object> settings) throws Exception {
        Path path = Files.createTempFile(temporary.getRoot().toPath(), "sibench-", ".yaml");
        Files.writeString(path, new Yaml().dump(Map.of(
                "schema_version", 1, "purpose", "sibench", "postgres", settings)));
        return path;
    }
}
