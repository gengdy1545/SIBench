package org.sibench.capture;

import com.google.gson.GsonBuilder;

import org.sibench.replay.ReferenceReplay;
import org.sibench.replay.ReplayConfig;
import org.sibench.schema.PgSchemaCheck;
import org.sibench.schema.SourceSchema;

import java.nio.file.*;
import java.util.*;

/** Explicit stages: no command implicitly starts PG or generates the original SF1 data. */
public final class CapturePoc {
    private CapturePoc() {}

    public static void main(String[] args) throws Exception {
        if (args.length < 3)
            throw new IllegalArgumentException(
                    "CapturePoc check|export|run|compile|pack|audit|replay ROOT CONFIG [INPUT]"
                            + " [OUTPUT]");
        CaptureConfig config = CaptureConfig.read(Path.of(args[1]), Path.of(args[2]));
        switch (args[0]) {
            case "check" -> {
                Map<String, Object> details = new LinkedHashMap<>();
                details.putAll(
                        Map.of(
                                "status", "configuration_valid",
                                "source", config.source().toString(),
                                "jdbc", config.jdbc(),
                                "native_mode", config.mode(),
                                "tables", 8,
                                "baseline_rows", 8706000,
                                "artifacts", config.artifacts().toString()));
                Properties p = config.nativeProperties();
                details.put("tp_clients", config.clients());
                details.put("change_delivery", "committed_only");
                details.put("default_seconds", config.seconds());
                details.put(
                        "scope",
                        config.mode().equals("runhtap")
                                ? "native AP -> TP -> concurrent AT/IQ"
                                : "single-session TP3/TP5/TP9 capture");
                details.put("schema_preflight", checkServer(config));
                if (config.mode().equals("runhtap")) {
                    details.put("ap_clients", Integer.parseInt(p.getProperty("apclient")));
                    details.put(
                            "xp_transaction_clients", Integer.parseInt(p.getProperty("xtpclient")));
                    details.put("xp_query_clients", Integer.parseInt(p.getProperty("xapclient")));
                    details.put(
                            "phase_minutes",
                            Map.of(
                                    "ap",
                                    Integer.parseInt(p.getProperty("apRunMins")),
                                    "tp",
                                    Integer.parseInt(p.getProperty("tpRunMins")),
                                    "xp",
                                    Integer.parseInt(p.getProperty("xpRunMins"))));
                }
                System.out.println(new GsonBuilder().setPrettyPrinting().create().toJson(details));
            }
            case "export" -> {
                PgBaselineExport.export(config.jdbc(), config.layout().output(), config.layout());
            }
            case "run" -> {
                HybenchNativeRun.main(new String[] {args[1], args[2]});
            }
            case "compile" -> {
                if (args.length != 5 && args.length != 6)
                    throw new IllegalArgumentException(
                            "compile requires PG_TRACE OUTPUT [ROW_AUDIT]");
                IndexTraceCompiler.compile(
                        config,
                        Path.of(args[3]),
                        Path.of(args[4]),
                        args.length == 6
                                ? Path.of(args[5])
                                : RowStateAudit.defaultReport(Path.of(args[3])));
            }
            case "pack" -> {
                if (args.length != 4) throw new IllegalArgumentException("pack requires OUTPUT");
                CaptureRun.verifyInput(config.raw());
                PgTraceBlocks.compile(config.raw(), Path.of(args[3]));
            }
            case "audit" -> {
                if (args.length != 3 && args.length != 4)
                    throw new IllegalArgumentException("audit accepts an optional REPORT");
                RowStateAudit.audit(
                        config.jdbc(),
                        config.user(),
                        SourceSchema.read(config.root(), config.document()),
                        config.layout().output(),
                        config.raw(),
                        args.length == 4
                                ? Path.of(args[3])
                                : RowStateAudit.defaultReport(config.raw()));
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

    static Map<String, Object> checkServer(CaptureConfig config) throws Exception {
        return PgSchemaCheck.check(config.root(), config.document(), config.jdbc(), config.user());
    }
}
