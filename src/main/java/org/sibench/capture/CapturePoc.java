package org.sibench.capture;

import com.google.gson.GsonBuilder;

import org.sibench.replay.ReferenceReplay;
import org.sibench.replay.ReplayConfig;

import java.nio.file.*;
import java.sql.*;
import java.util.*;

/** Explicit stages: no command implicitly starts PG or generates the original SF1 data. */
public final class CapturePoc {
    private CapturePoc() {}

    public static void main(String[] args) throws Exception {
        if (args.length < 3)
            throw new IllegalArgumentException(
                    "CapturePoc check|export|run|compile|replay ROOT CONFIG [PG_TRACE] [OUTPUT]");
        CaptureConfig config = CaptureConfig.read(Path.of(args[1]), Path.of(args[2]));
        switch (args[0]) {
            case "check" ->
                    System.out.println(
                            new GsonBuilder()
                                    .setPrettyPrinting()
                                    .create()
                                    .toJson(
                                            Map.of(
                                                    "status",
                                                    "configuration_valid",
                                                    "source",
                                                    config.source().toString(),
                                                    "jdbc",
                                                    config.jdbc(),
                                                    "seconds",
                                                    config.seconds(),
                                                    "clients",
                                                    config.clients(),
                                                    "tables",
                                                    8,
                                                    "baseline_rows",
                                                    8706000,
                                                    "artifacts",
                                                    config.artifacts().toString(),
                                                    "scope",
                                                    "single-session TP3/TP5/TP9 capture")));
            case "export" -> {
                checkServer(config);
                PgBaselineExport.export(config.jdbc(), config.layout().output(), config.layout());
            }
            case "run" -> {
                HybenchNativeRun.main(new String[] {args[1], args[2]});
            }
            case "compile" -> {
                if (args.length != 5)
                    throw new IllegalArgumentException("compile requires PG_TRACE and OUTPUT");
                IndexTraceCompiler.compile(config, Path.of(args[3]), Path.of(args[4]));
            }
            case "replay" -> {
                if (args.length != 5 && args.length != 6)
                    throw new IllegalArgumentException(
                            "replay requires NORMALIZED_DIRECTORY REPORT [REPLAY_CONFIG]");
                ReferenceReplay.run(
                        Path.of(args[3]),
                        args.length == 6
                                ? ReplayConfig.read(config.root(), Path.of(args[5]))
                                : ReplayConfig.defaults(),
                        Path.of(args[4]));
            }
            default -> throw new IllegalArgumentException("Unknown PoC stage: " + args[0]);
        }
    }

    static void checkServer(CaptureConfig config) throws Exception {
        try (Connection pg = DriverManager.getConnection(config.jdbc(), config.user(), "");
                Statement query = pg.createStatement();
                ResultSet row =
                        query.executeQuery(
                                "SELECT"
                                    + " current_setting('server_version_num'),current_setting('max_parallel_workers_per_gather'),current_setting('transaction_isolation')")) {
            row.next();
            if (!row.getString(1).equals("140024")
                    || !row.getString(2).equals("0")
                    || !row.getString(3).equals("read committed"))
                throw new IllegalStateException(
                        "PG environment differs from capture configuration");
        }
    }
}
