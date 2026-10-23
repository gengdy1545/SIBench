package org.sibench.capture;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * The number of transaction-control statements (BEGIN / COMMIT) a pinned sysbench script issues.
 *
 * <p>The pinned scripts guard every control call with {@code if not sysbench.opt.skip_trx then}
 * (for example {@code oltp_read_write.lua} around {@code prepare_begin()}, {@code begin()} and
 * {@code commit()}), so the run-time {@code --skip-trx} switch decides whether the statements run at
 * all, never the presence of a string in the source. {@code prepare_begin()} is the prepare-time
 * call that only prepares the statement; the per-event calls are {@code begin()} and
 * {@code commit()}. Scripts such as {@code oltp_delete.lua} and {@code oltp_update_index.lua} call
 * neither and run in autocommit, so their control-statement count is zero regardless of the switch.
 */
public final class SysbenchControlStatements {
    private static final String FLAG = "--skip-trx=";
    private static final Pattern BLOCK_COMMENT = Pattern.compile("(?s)--\\[\\[.*?(?:--\\]\\]|\\]\\])");
    private static final Pattern LINE_COMMENT = Pattern.compile("--[^\n]*");

    private SysbenchControlStatements() {}

    /**
     * @param beginCallsPerEvent {@code begin()} calls in one event, ignoring prepare and definitions
     * @param commitCallsPerEvent {@code commit()} calls in one event, ignoring the same
     * @param controlStatements BEGIN plus COMMIT executions the whole run performs
     * @param explicitTransactions whether the script runs its event inside an explicit transaction
     */
    public record Derivation(
            boolean skipTrx,
            int beginCallsPerEvent,
            int commitCallsPerEvent,
            long events,
            long controlStatements,
            boolean explicitTransactions) {
        public Map<String, Object> describe() {
            var result = new LinkedHashMap<String, Object>();
            result.put("skip_trx", skipTrx);
            result.put("begin_calls_per_event", beginCallsPerEvent);
            result.put("commit_calls_per_event", commitCallsPerEvent);
            result.put("control_statements_per_event", controlStatements / Math.max(events, 1));
            result.put("control_statements", controlStatements);
            result.put("explicit_transactions", explicitTransactions);
            return result;
        }
    }

    public static Derivation derive(Path script, boolean skipTrx, long events) throws IOException {
        return derive(Files.readString(script), skipTrx, events);
    }

    /** Pure over the Lua source and the run's own switch. */
    public static Derivation derive(String lua, boolean skipTrx, long events) {
        if (events < 0) throw new IllegalArgumentException("events must not be negative");
        String body = stripComments(lua);
        int begins = calls(body, "begin"), commits = calls(body, "commit");
        long perEvent = skipTrx ? 0L : (long) begins + commits;
        return new Derivation(
                skipTrx, begins, commits, events, perEvent * events, !skipTrx && perEvent > 0L);
    }

    /**
     * The {@code --skip-trx} value of a run read back out of its own argv, never out of the Lua
     * source. The switch must be stated: inferring it would make the control-statement count an
     * assumption again.
     */
    public static boolean skipTrx(List<String> arguments) throws IOException {
        String value = null;
        for (String argument : arguments) {
            if (argument.equals("--skip-trx")) value = "on";
            else if (argument.startsWith(FLAG)) value = argument.substring(FLAG.length());
        }
        if (value == null)
            throw new IOException(
                    "sysbench command does not state --skip-trx; the control-statement count would be inferred");
        return switch (value) {
            case "on", "true", "1" -> true;
            case "off", "false", "0" -> false;
            default -> throw new IOException("Invalid sysbench --skip-trx value: " + value);
        };
    }

    /**
     * Statements the client counted that the server trace does not contain. BEGIN and COMMIT are
     * utility statements: they never reach the executor, so they are exactly this residual.
     */
    public static long residual(long clientStatements, long serverExecutions) {
        return clientStatements - serverExecutions;
    }

    /** Fails loudly when the server-observed residual disagrees with the derived count. */
    public static long requireAgreement(
            String script, long clientStatements, long serverExecutions, Derivation derivation)
            throws IOException {
        long residual = residual(clientStatements, serverExecutions);
        if (residual != derivation.controlStatements())
            throw new IOException(
                    script
                            + ": the script calls begin() "
                            + derivation.beginCallsPerEvent()
                            + " time(s) and commit() "
                            + derivation.commitCallsPerEvent()
                            + " time(s) per event with skip_trx="
                            + derivation.skipTrx()
                            + ", so "
                            + derivation.events()
                            + " events derive "
                            + derivation.controlStatements()
                            + " control statements, but the server trace has only "
                            + residual
                            + " unexecuted statements between the "
                            + clientStatements
                            + " sysbench counted and the "
                            + serverExecutions
                            + " executions it observed");
        return residual;
    }

    private static String stripComments(String lua) {
        String text = BLOCK_COMMENT.matcher(lua).replaceAll(" ");
        return LINE_COMMENT.matcher(text).replaceAll("");
    }

    private static int calls(String body, String name) {
        String calls =
                body.replace("prepare_" + name, " ")
                        .replaceAll("function\\s+" + name + "\\b", " ");
        var matcher = Pattern.compile("\\b" + name + "\\s*\\(\\s*\\)").matcher(calls);
        int count = 0;
        while (matcher.find()) count++;
        return count;
    }
}
