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
            assertEquals(
                    3,
                    JsonParser.parseString(output.toString(StandardCharsets.UTF_8))
                            .getAsJsonObject()
                            .getAsJsonArray("profiles")
                            .size());
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
            {"generate", "--help"},
            {"replay", "config/index_replay.yaml"},
            {"check-replay", "config/index_replay.yaml", "missing-trace"},
            {"replay", "--repeat", "2"},
            {"validate-snapshots", "config/index_replay.yaml"},
            {"validate-snapshots", "config/index_replay.yaml", "missing-trace", "missing-report.json"},
            {"inspect-scale", "config/ycsb_large.yaml", "missing.yaml"}
        };
        for (String[] args : invalid) {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            ByteArrayOutputStream errors = new ByteArrayOutputStream();
            try (PrintStream out = new PrintStream(output, true, StandardCharsets.UTF_8);
                    PrintStream err = new PrintStream(errors, true, StandardCharsets.UTF_8)) {
                assertEquals(1, Sibench.run(args, out, err));
                assertEquals(0, output.size());
                assertTrue(errors.size() > 0);
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
