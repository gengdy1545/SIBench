package org.sibench.cli;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.gson.JsonParser;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

public class SibenchTest {
    @org.junit.Rule public org.junit.rules.TemporaryFolder temporary = new org.junit.rules.TemporaryFolder();

    @Test public void staticPixelsCommandAcceptsBaselineWithoutCdc() throws Exception {
        Path root = temporary.getRoot().toPath();
        var settings = org.sibench.config.ConfigValues.object(org.sibench.config.SibenchConfig.read(java.nio.file.Path.of("config/sibench.yaml")), "pixels_experiment");
        settings.put("updates", false);
        java.nio.file.Files.writeString(root.resolve("runtime.json"), new com.google.gson.Gson().toJson(java.util.Map.of(
                "schema_version", 1, "purpose", "sibench", "pixels_experiment", settings)));
        var output = new ByteArrayOutputStream(); var errors = new ByteArrayOutputStream();
        try (var out = new PrintStream(output); var err = new PrintStream(errors)) {
            assertEquals(1, Sibench.run(new String[] {"pixels-experiment", "--root", root.toString(),
                    "runtime.json", "missing-baseline", "output"}, out, err));
        }
        assertTrue(errors.toString(StandardCharsets.UTF_8), errors.toString(StandardCharsets.UTF_8).contains("missing-baseline"));
        assertEquals(0, output.size());
    }

    @Test
    public void producesJsonForAllProfiles() {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ByteArrayOutputStream errors = new ByteArrayOutputStream();
        try (PrintStream out = new PrintStream(output, true, StandardCharsets.UTF_8);
                PrintStream err = new PrintStream(errors, true, StandardCharsets.UTF_8)) {
            assertEquals(
                    0,
                    Sibench.run(
                            new String[] {
                                "inspect-scale", "--root", Path.of("").toAbsolutePath().toString()
                            },
                            out,
                            err));
            var report = JsonParser.parseString(output.toString(StandardCharsets.UTF_8)).getAsJsonObject();
            assertEquals("profiles_valid", report.get("status").getAsString());
            assertEquals(3, report.getAsJsonArray("profiles").size());
            assertEquals(0, errors.size());
        }
    }

    @Test
    public void reportsFailuresWithoutPartialJson() {
        String[][] invalid = {
            {"inspect-scale", "missing.yaml"},
            {"inspect-scale", "--root"},
            {"inspect-scale", "--unknown"},
            {"generate"},
            {"generate-pixels"},
            {"generate-pixels", "missing-config.yaml"},
            {"generate", "missing-config.yaml"},
            {"check-replay", "config/sibench.yaml", "missing-trace"},
            {"inspect-scale", "src/test/resources/config/ycsb_1k.yaml", "missing.yaml"}
        };
        for (String[] args : invalid) {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            ByteArrayOutputStream errors = new ByteArrayOutputStream();
            try (PrintStream out = new PrintStream(output, true, StandardCharsets.UTF_8);
                    PrintStream err = new PrintStream(errors, true, StandardCharsets.UTF_8)) {
                assertEquals(1, Sibench.run(args, out, err));
                assertEquals(0, output.size());
                assertTrue(errors.size() > 0);
                if (java.util.Arrays.asList(args).contains("--unknown"))
                    assertTrue(errors.toString(StandardCharsets.UTF_8).contains("Unknown option: --unknown"));
            }
        }
    }

    @Test
    public void rejectsUnknownCommandsEvenForHelp() {
        for (String[] args : new String[][] {{"no-such-command"}, {"no-such-command", "--help"}}) {
            var output = new ByteArrayOutputStream();
            var errors = new ByteArrayOutputStream();
            try (var out = new PrintStream(output); var err = new PrintStream(errors)) {
                assertEquals(1, Sibench.run(args, out, err));
                assertEquals(0, output.size());
                assertTrue(errors.toString(StandardCharsets.UTF_8).contains("Unknown command: " + args[0]));
            }
        }
    }

    @Test
    public void showsGlobalAndCommandHelp() {
        for (String[] args : new String[][] {{}, {"--help"}, {"inspect-scale", "-h"}}) {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            ByteArrayOutputStream errors = new ByteArrayOutputStream();
            try (PrintStream out = new PrintStream(output, true, StandardCharsets.UTF_8);
                    PrintStream err = new PrintStream(errors, true, StandardCharsets.UTF_8)) {
                assertEquals(0, Sibench.run(args, out, err));
                assertTrue(output.toString(StandardCharsets.UTF_8).contains("Usage:"));
                assertEquals(0, errors.size());
            }
        }
    }
}
