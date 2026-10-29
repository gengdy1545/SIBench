import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "summary", Path(__file__).resolve().parents[3] / "scripts/summarize_index_run.py")
summary = importlib.util.module_from_spec(spec)
spec.loader.exec_module(summary)


class IndexRunSummaryTest(unittest.TestCase):
    def test_missing_logs_mean_unmeasured_not_zero_engine_work(self):
        result = summary.engine_events([])
        self.assertEqual("not_collected", result["coverage"])
        self.assertEqual("N/A", result["totals"])

    def test_job_ids_from_two_processes_do_not_pair_and_missing_finish_is_visible(self):
        with tempfile.TemporaryDirectory() as directory:
            first, second = [Path(directory) / x for x in ("LOG.old", "LOG")]
            def write(path, events):
                path.write_text("\n".join("prefix EVENT_LOG_v1 " + json.dumps(e) for e in events))
            write(first, [
                {"event": "compaction_started", "job": 2, "time_micros": 1_000_000, "input_data_size": 90},
                {"event": "compaction_finished", "job": 2, "time_micros": 4_000_000, "total_output_size": 40},
                {"event": "flush_started", "job": 3, "time_micros": 2_000_000, "total_data_size": 120},
                {"event": "table_file_creation", "job": 3, "time_micros": 3_000_000, "file_size": 30},
                {"event": "flush_finished", "job": 3, "time_micros": 5_000_000},
            ])
            write(second, [{"event": "compaction_started", "job": 2, "time_micros": 6_000_000}])
            result = summary.engine_events([first, second])
            self.assertEqual({"completed_jobs": 1, "incomplete_jobs": 1,
                              "sum_job_seconds": 3, "output_bytes": 40}, result["totals"]["compaction"])
            self.assertEqual(30, result["totals"]["flush"]["output_bytes"])
            # The 120-byte uncompressed memtable must not be reported as 120 SST bytes.
            self.assertEqual(120, result["events"][1]["input_bytes"])

    def test_malformed_engine_events_are_not_silently_dropped(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "LOG"
            path.write_text('prefix EVENT_LOG_v1 {"event":')
            with self.assertRaisesRegex(ValueError, "Malformed engine event"):
                summary.engine_events([path])

    def test_deliberately_wrong_result_and_partial_run_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "service.json"
            path.write_text(json.dumps({"status": "failed"}))
            with self.assertRaisesRegex(ValueError, "failed or incomplete"):
                summary.summarize(path, [])
            path.write_text(json.dumps({"status": "passed", "result": {
                "per_query_digest_mismatches": 1, "per_query_digest_checks": 2, "search_requests": 2}}))
            with self.assertRaisesRegex(ValueError, "mismatched"):
                summary.summarize(path, [])
            path.write_text(json.dumps({"status": "passed", "result": {
                "per_query_digest_mismatches": 0, "per_query_digest_checks": 1, "search_requests": 2}}))
            with self.assertRaisesRegex(ValueError, "incomplete"):
                summary.summarize(path, [])


if __name__ == "__main__":
    unittest.main()
