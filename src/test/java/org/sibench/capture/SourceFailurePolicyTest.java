package org.sibench.capture;

import static org.junit.Assert.*;

import org.junit.Test;

import java.util.Map;

public class SourceFailurePolicyTest {
    private static SourceFailurePolicy.Evidence evidence(
            String sqlState, String clientStatus, String driverStatus, boolean rolledBack,
            boolean retried) {
        return SourceFailurePolicy.Evidence.of(
                sqlState, clientStatus, driverStatus, rolledBack, retried, "request-7", "tx-3");
    }

    @Test
    public void duplicateKeyDeadlockAndSerializationFailureAreExpectedSqlFailures() {
        for (String sqlState : SourceFailurePolicy.IGNORABLE_SQL_STATES) {
            var classification =
                    SourceFailurePolicy.classify(evidence(sqlState, null, null, false, false));
            assertEquals(
                    sqlState,
                    SourceFailurePolicy.Category.EXPECTED_SQL_FAILURE,
                    classification.category());
            assertFalse(classification.category().invalidatesCapture());
            assertTrue(classification.basis().contains(sqlState));
        }
        // The three the pinned sysbench pgsql driver rolls back and continues on.
        assertEquals(
                java.util.Set.of("23505", "40P01", "40001"),
                SourceFailurePolicy.IGNORABLE_SQL_STATES);
    }

    @Test
    public void aStatementThePinnedDriverCountedAsIgnoredIsAnExpectedSqlFailure() {
        var classification =
                SourceFailurePolicy.classify(
                        evidence(null, null, SourceFailurePolicy.DRIVER_IGNORED_ERROR, true, false));
        assertEquals(
                SourceFailurePolicy.Category.EXPECTED_SQL_FAILURE, classification.category());
        assertFalse(classification.category().invalidatesCapture());
    }

    @Test
    public void aClientStatusThePinnedClientDefinesIsAnExpectedApiFailure() {
        for (String status : SourceFailurePolicy.EXPECTED_CLIENT_STATUSES) {
            var classification = SourceFailurePolicy.classify(evidence(null, status, null, false, false));
            assertEquals(
                    status,
                    SourceFailurePolicy.Category.EXPECTED_API_FAILURE,
                    classification.category());
            assertFalse(classification.category().invalidatesCapture());
        }
        assertEquals(
                SourceFailurePolicy.Category.EXPECTED_API_FAILURE,
                SourceFailurePolicy.classify(evidence(null, "NOT_FOUND", null, false, false))
                        .category());
    }

    @Test
    public void aRequestTheSourceRolledBackItselfIsABusinessRollback() {
        var classification = SourceFailurePolicy.classify(evidence(null, null, null, true, false));
        assertEquals(SourceFailurePolicy.Category.BUSINESS_ROLLBACK, classification.category());
        assertFalse(classification.category().invalidatesCapture());
        // An explicit SQLSTATE outranks the rollback: the driver rolled back because of it.
        assertEquals(
                SourceFailurePolicy.Category.EXPECTED_SQL_FAILURE,
                SourceFailurePolicy.classify(evidence("40001", null, null, true, false)).category());
    }

    @Test
    public void aReconnectIsANativeRetry() {
        var classification = SourceFailurePolicy.classify(evidence(null, null, null, false, true));
        assertEquals(SourceFailurePolicy.Category.NATIVE_RETRY, classification.category());
        assertFalse(classification.category().invalidatesCapture());
    }

    @Test
    public void anExplainedFailureNeverInvalidatesACapture() {
        for (SourceFailurePolicy.Category category : SourceFailurePolicy.Category.values())
            assertEquals(
                    category.name(),
                    category == SourceFailurePolicy.Category.UNEXPECTED,
                    category.invalidatesCapture());
    }

    @Test
    public void withoutEvidenceAndForAnUnknownStatusTheFailureStaysUnexpected() {
        assertEquals(
                SourceFailurePolicy.Category.UNEXPECTED,
                SourceFailurePolicy.classify(evidence(null, null, null, false, false)).category());
        assertEquals(
                SourceFailurePolicy.Category.UNEXPECTED,
                SourceFailurePolicy.classify(evidence("08006", null, null, false, false)).category());
        assertEquals(
                SourceFailurePolicy.Category.UNEXPECTED,
                SourceFailurePolicy.classify(evidence(null, "ERROR", null, false, false)).category());
        assertTrue(SourceFailurePolicy.Category.UNEXPECTED.invalidatesCapture());
    }

    @Test
    public void anUnexpectedFailureStillFailsTheRun() {
        var tolerated = new SourceFailurePolicy.Tally();
        tolerated.record(evidence("23505", null, null, true, false));
        tolerated.record(evidence(null, "NOT_FOUND", null, false, false));
        tolerated.record(evidence(null, null, null, false, true));
        tolerated.record(evidence(null, null, null, true, false));
        assertFalse(tolerated.invalidatesCapture());
        assertEquals(1L, tolerated.count(SourceFailurePolicy.Category.EXPECTED_SQL_FAILURE));
        assertEquals(1L, tolerated.count(SourceFailurePolicy.Category.EXPECTED_API_FAILURE));
        assertEquals(1L, tolerated.count(SourceFailurePolicy.Category.NATIVE_RETRY));
        assertEquals(1L, tolerated.count(SourceFailurePolicy.Category.BUSINESS_ROLLBACK));
        assertEquals(0L, tolerated.count(SourceFailurePolicy.Category.UNEXPECTED));

        // One unexplained failure is enough: the run must not be reported as a valid capture.
        var broken = new SourceFailurePolicy.Tally();
        broken.record(evidence("23505", null, null, true, false));
        assertFalse(broken.invalidatesCapture());
        broken.record(evidence(null, "ERROR", null, false, false));
        assertTrue(broken.invalidatesCapture());
    }

    @Test
    public void theReportCountsEveryCategoryAndKeepsEveryAttemptWithItsFinalOutcome() {
        var tally = new SourceFailurePolicy.Tally();
        tally.record(
                new SourceFailurePolicy.Evidence(
                        "40P01", 7, null, null, true, false, "oltp_read_write", "tx-9", 1, 2));
        tally.record(
                new SourceFailurePolicy.Evidence(
                        "40P01", 7, null, null, true, false, "oltp_read_write", "tx-9", 2, 2));
        tally.record(evidence(null, "NOT_FOUND", null, false, false));
        var report = tally.report();
        @SuppressWarnings("unchecked")
        var counts = (Map<String, Object>) report.get("counts");
        assertEquals(2L, counts.get("EXPECTED_SQL_FAILURE"));
        assertEquals(1L, counts.get("EXPECTED_API_FAILURE"));
        assertEquals(0L, counts.get("BUSINESS_ROLLBACK"));
        assertEquals(0L, counts.get("NATIVE_RETRY"));
        assertEquals(0L, counts.get("UNEXPECTED"));
        @SuppressWarnings("unchecked")
        var occurrences = (java.util.List<Map<String, Object>>) report.get("occurrences");
        assertEquals(2, occurrences.size());
        var retried = occurrences.get(0);
        assertEquals("40P01", retried.get("sql_state"));
        assertEquals(7, ((Number) retried.get("error_code")).intValue());
        assertEquals("oltp_read_write", retried.get("request"));
        assertEquals("tx-9", retried.get("transaction"));
        assertEquals("EXPECTED_SQL_FAILURE", retried.get("final_outcome"));
        @SuppressWarnings("unchecked")
        var attempts = (java.util.List<Map<String, Object>>) retried.get("attempts");
        assertEquals(2, attempts.size());
        assertEquals(1, attempts.get(0).get("attempt"));
        assertEquals(2, attempts.get(0).get("of"));
        assertEquals(2, attempts.get(1).get("attempt"));
        assertEquals("request-7", occurrences.get(1).get("request"));
        assertFalse(tally.invalidatesCapture());
    }

    @Test
    public void aYcsbStatusNoPinnedBehaviourExplainsFailsTheWorkload() {
        var measured =
                YcsbNativeRun.parse(
                        """
                        [READ], Operations, 146
                        [READ], Return=OK, 146
                        [UPDATE], Operations, 154
                        [UPDATE], Return=ERROR, 2
                        """);
        var failures = YcsbNativeRun.classifyReturns("workloada", measured);
        assertEquals(SourceFailurePolicy.Category.UNEXPECTED, failures.attempts().get(0).category());
        assertEquals(2L, failures.count(SourceFailurePolicy.Category.UNEXPECTED));
        assertTrue(failures.invalidatesCapture());
        assertEquals("workloada:UPDATE", failures.attempts().get(0).evidence().request());

        var tolerated =
                YcsbNativeRun.classifyReturns(
                        "workloadc",
                        YcsbNativeRun.parse(
                                """
                                [READ], Operations, 100
                                [READ], Return=OK, 98
                                [READ], Return=NOT_FOUND, 2
                                """));
        assertEquals(2L, tolerated.count(SourceFailurePolicy.Category.EXPECTED_API_FAILURE));
        assertFalse(tolerated.invalidatesCapture());
    }

    @Test
    public void sysbenchIgnoredErrorsAndReconnectsAreRecordedNotFatal() {
        var tolerated = new SourceFailurePolicy.Tally();
        SysbenchNativeRun.classifyIgnoredErrors(tolerated, "oltp_read_write", Map.of("ignored errors", 3L));
        SysbenchNativeRun.classifyReconnects(tolerated, "oltp_read_write", Map.of("reconnects", 1L));
        assertEquals(3L, tolerated.count(SourceFailurePolicy.Category.EXPECTED_SQL_FAILURE));
        assertEquals(1L, tolerated.count(SourceFailurePolicy.Category.NATIVE_RETRY));
        assertFalse(tolerated.invalidatesCapture());
        assertEquals("oltp_read_write", tolerated.attempts().get(0).evidence().request());
        assertEquals(1, tolerated.attempts().get(0).evidence().attempt());
        assertEquals(3, tolerated.attempts().get(0).evidence().attempts());
        assertEquals(2, tolerated.attempts().get(1).evidence().attempt());
        assertEquals(3, tolerated.attempts().get(2).evidence().attempt());

        var clean = new SourceFailurePolicy.Tally();
        SysbenchNativeRun.classifyIgnoredErrors(clean, "oltp_delete", Map.of("ignored errors", 0L));
        SysbenchNativeRun.classifyReconnects(clean, "oltp_delete", Map.of("reconnects", 0L));
        assertTrue(clean.attempts().isEmpty());
        assertFalse(clean.invalidatesCapture());
    }
}
