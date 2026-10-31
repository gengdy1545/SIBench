package org.sibench.capture;

import static org.junit.Assert.*;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ServerCaptureAuditTest {
    @Rule public TemporaryFolder folder = new TemporaryFolder();

    private static String event(String type, long req, int depth, String detail) {
        return "{\"event\":\"" + type + "\",\"pid\":9,\"req\":" + req + ",\"depth\":" + depth
                + (detail == null ? "" : "," + detail) + "}";
    }

    private static List<String> statement(long req, String sql, long rows, long processed) {
        var lines = new ArrayList<String>();
        lines.add(event("executor_start", req, 1, "\"sql\":\"" + sql + "\""));
        for (long i = 0; i < rows; i++) lines.add(event("result_row", req, 1, "\"values\":[\"x\"]"));
        lines.add(event("executor_run_end", req, 1, "\"processed\":" + processed + ",\"returned\":" + rows + ",\"row_hashes\":["
                + String.join(",", java.util.Collections.nCopies((int) rows, "\"0000000000000000\"")) + "]"));
        lines.add(event("executor_end", req, 1, "\"processed\":" + processed));
        return lines;
    }

    private int files;

    private Path writeBody(List<String> lines) throws IOException {
        Path file = folder.newFile("pg-" + (++files) + ".jsonl.zst").toPath();
        try (var out = new com.github.luben.zstd.ZstdOutputStream(Files.newOutputStream(file))) {
            out.write((event("capture_start", 0, 0, "\"detail\":\"rows\"") + "\n")
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8));
            for (String line : lines) {
                out.write(line.getBytes(java.nio.charset.StandardCharsets.UTF_8));
                out.write('\n');
            }
        }
        return file;
    }

    @Test
    public void separatesSetupCommitsFromWorkloadAndCountsZeroRowModifications() throws Exception {
        var lines = new ArrayList<String>();
        // The client prepares statements before running any: implicit commits, no executor.
        for (int i = 0; i < 3; i++) lines.add(event("transaction_commit", 0, 0, null));
        lines.addAll(statement(1, "SELECT c FROM t WHERE id=$1", 1, 1));
        lines.add(event("transaction_commit", 1, 0, null));
        lines.addAll(statement(2, "DELETE FROM t WHERE id=$1", 0, 0));
        lines.add(event("transaction_commit", 2, 0, null));
        lines.addAll(statement(3, "UPDATE t SET c=$1 WHERE id=$2", 0, 1));
        lines.add(event("transaction_commit", 3, 0, null));
        var report = ServerCaptureAudit.audit(writeBody(lines));
        assertEquals(3L, report.get("requests"));
        assertEquals(3L, report.get("commits"));
        assertEquals(3L, report.get("setup_commits_before_first_execution"));
        assertEquals(1L, report.get("modifications_affecting_no_row"));
        assertEquals(Map.of("delete", 1L, "select", 1L, "update", 1L), report.get("statements"));
    }

    @Test
    public void nestedExecutorsBelongToTheirRequest() throws Exception {
        var lines = new ArrayList<String>();
        lines.add(event("executor_start", 1, 1, "\"sql\":\"UPDATE t SET c=$1\""));
        lines.add(event("executor_start", 1, 2, "\"sql\":\"SELECT 1 FROM ONLY p\""));
        lines.add(event("result_row", 1, 2, "\"values\":[\"1\"]"));
        lines.add(event("executor_run_end", 1, 2, "\"processed\":1,\"returned\":1,\"row_hashes\":[\"0000000000000000\"]"));
        lines.add(event("executor_end", 1, 1, "\"processed\":1"));
        lines.add(event("executor_run_end", 1, 1, "\"processed\":1,\"returned\":0,\"row_hashes\":[]"));
        lines.add(event("executor_end", 1, 1, "\"processed\":1"));
        lines.add(event("transaction_commit", 1, 0, null));
        var report = ServerCaptureAudit.audit(writeBody(lines));
        assertEquals(1L, report.get("requests"));
        assertEquals(1L, report.get("nested_executions"));
        assertEquals(0L, report.get("result_rows"));
    }

    @Test
    public void rejectsInconsistentOrTruncatedTraces() throws Exception {
        var mismatch = new ArrayList<>(statement(1, "SELECT c FROM t", 2, 3));
        mismatch.add(event("transaction_commit", 1, 0, null));
        assertTrue(message(mismatch).contains("differ from executor return"));

        var gap = new ArrayList<>(statement(1, "SELECT 1", 1, 1));
        gap.add(event("transaction_commit", 1, 0, null));
        gap.addAll(statement(3, "SELECT 1", 1, 1));
        assertTrue(message(gap).contains("not consecutive"));

        var open = new ArrayList<>(statement(1, "SELECT 1", 1, 1));
        assertTrue(message(open).contains("unfinished transaction"));

        var early = new ArrayList<String>();
        early.add(event("executor_start", 1, 1, "\"sql\":\"SELECT 1\""));
        early.add(event("transaction_commit", 1, 0, null));
        assertTrue(message(early).contains("has not returned"));

        var overlap = new ArrayList<String>();
        overlap.add(event("executor_start", 1, 1, "\"sql\":\"SELECT 1\""));
        overlap.add(event("executor_start", 2, 1, "\"sql\":\"SELECT 1\""));
        assertTrue(message(overlap).contains("Overlapping executions"));

        var legacy = List.of("{\"event\":\"executor_start\",\"pid\":9,\"sql\":\"SELECT 1\"}");
        assertTrue(message(legacy).contains("request ordinals"));

        var unknown = new ArrayList<String>();
        unknown.add(event("mystery", 0, 0, null));
        assertTrue(message(unknown).contains("Unclassified raw event"));

        var mixed = new ArrayList<>(statement(1, "SELECT 1", 1, 1));
        mixed.add(event("transaction_commit", 1, 0, null));
        mixed.add(mixed.get(0).replace("\"pid\":9", "\"pid\":10"));
        assertTrue(message(mixed).contains("several backends"));
    }

    @Test
    public void abortedStatementThatNeverReturnedIsReported() throws Exception {
        var lines = new ArrayList<String>();
        lines.add(event("executor_start", 1, 1, "\"sql\":\"INSERT INTO t VALUES ($1)\""));
        lines.add(event("transaction_abort", 1, 0, null));
        assertTrue(message(lines).contains("never returned"));
    }

    @Test
    public void rejectsMissingHeaderAndMissingReturnHashes() throws Exception {
        Path missing = folder.newFile("missing-header.jsonl.zst").toPath();
        RowAuditTestSupport.writeTrace(missing, statement(1, "SELECT 1", 1, 1));
        assertThrows(IOException.class, () -> ServerCaptureAudit.audit(missing));
        var lines = new ArrayList<>(statement(1, "SELECT 1", 1, 1));
        lines.set(2, event("executor_run_end", 1, 1, "\"processed\":1"));
        assertTrue(message(lines).contains("complete result hashes"));
        RowAuditTestSupport.writeTrace(missing, java.util.stream.Stream.concat(
                java.util.stream.Stream.of(event("capture_start", 0, 0, "\"detail\":\"rows\"")
                        .replace("\"pid\":9", "\"pid\":10")),
                statement(1, "SELECT 1", 1, 1).stream()).toList());
        assertThrows(IOException.class, () -> ServerCaptureAudit.audit(missing));
    }

    private String message(List<String> lines) throws IOException {
        Path file = writeBody(lines);
        return assertThrows(IOException.class, () -> ServerCaptureAudit.audit(file)).getMessage();
    }
}
