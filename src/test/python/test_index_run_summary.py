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
    @staticmethod
    def phase():
        dispatch = dict.fromkeys(("pending_responses", "completed_responses", "target_calls",
            "source_operations", "filtered_secondary_operations", "transaction_control_records",
            "basic_operations", "completed_operations",
            "failed_response_operations",
            "timed_out_operations", "backpressure_seconds", "dependency_wait_seconds",
            "deferred_search_operations", "search_deferral_seconds", "peak_deferred_operations",
            "peak_in_flight"), 0)
        dispatch.update(local_send_boundary="transport_acceptance_not_service_execution",
            latency_boundary="client_api_entry_to_final_response_excluding_replay_waits",
            measurement={"unit": "basic_index_operation", "warmup_operations": 0, "measured_operations": 0,
                         "completed_operations": 0})
        for kind in ("search", "write"):
            dispatch[kind + "_latency"] = {"count": 0, "mean_ms": 0.0, "p99_ms": 0.0,
                "percentile_bound": "upper", "percentile_relative_error_bound": 1 / 1024,
                "exact_below_ns": 2048, "mean_and_max": "unquantized"}
        return {**dict.fromkeys(("operations", "rpc_requests", "seconds", "operations_per_second",
            "request_bytes", "result_bytes"), 0), "rpc_by_kind": {}, "dispatch": dispatch}

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

    def test_foreign_engine_log_cannot_become_a_passed_combined_measurement(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "service.json"
            log = Path(directory) / "unrelated.LOG"
            phase = self.phase()
            path.write_text(json.dumps({"status": "passed", "result": {
                "search_requests": 0,
                "service": "test", "baseline": phase, "workload": phase, "queries": 0,
                "checked_result_items": 0}}))
            log.write_text('prefix EVENT_LOG_v1 {"event":"compaction_finished","job":1,'
                           '"time_micros":1000000,"total_output_size":999999}\n')
            result = summary.summarize(path, [log])
            self.assertEqual("unverified_engine_provenance", result["status"])
            self.assertEqual("unverified", result["engine"]["same_run_provenance"])
            self.assertEqual("passed", result["correctness_status"])

    def test_baseline_batches_and_individual_workload_rpcs_remain_distinct(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "service.json"
            phase = self.phase()
            phase.update(operations=4, rpc_requests=4, rpc_by_kind={"Get": 3, "Put": 1})
            phase["dispatch"].update(target_calls=4, completed_responses=4,
                basic_operations=4, completed_operations=4, dependency_wait_seconds=0.02,
                deferred_search_operations=1, search_deferral_seconds=0.5)
            phase["dispatch"]["measurement"].update(warmup_operations=1, measured_operations=3,
                                                   completed_operations=3)
            phase["dispatch"]["search_latency"].update(count=3, p99_ms=0.125)
            baseline = self.phase()
            baseline.update(operations=4, rpc_requests=2, rpc_by_kind={"BatchWrite": 2})
            baseline["dispatch"].update(target_calls=4, completed_responses=4, basic_operations=4, completed_operations=4)
            path.write_text(json.dumps({"status": "passed", "result": {
                "service": "test", "baseline": baseline, "workload": phase,
                "queries": 1, "checked_result_items": 1}}))
            workload = summary.summarize(path, [])["phases"]["workload"]
            self.assertEqual(4, workload["basic_operations"])
            self.assertEqual(0.02, workload["dependency_wait_seconds"])
            self.assertEqual(1, workload["deferred_search_operations"])
            self.assertEqual(0.5, workload["search_deferral_seconds"])
            self.assertEqual(4, workload["rpc_requests"])
            self.assertEqual({"Get": 3, "Put": 1}, workload["rpc_by_kind"])
            self.assertEqual(2, summary.summarize(path, [])["phases"]["baseline"]["rpc_requests"])
            self.assertEqual(3, workload["measurement"]["measured_operations"])
            self.assertEqual(0.125, workload["search_latency"]["p99_ms"])
            self.assertEqual("upper", workload["search_latency"]["percentile_bound"])
            self.assertEqual(1 / 1024, workload["search_latency"]["percentile_relative_error_bound"])

    def test_success_label_cannot_hide_recorded_response_failures(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "service.json"
            for field in ("failed_response_operations",
                          "timed_out_operations", "failure"):
                phase = self.phase()
                phase["dispatch"][field] = 1
                path.write_text(json.dumps({"status": "passed", "result": {"baseline": phase}}))
                with self.assertRaisesRegex(ValueError, "failed or timed-out"):
                    summary.summarize(path, [])

    def test_incomplete_responses_and_failed_run_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "service.json"
            path.write_text(json.dumps({"status": "failed"}))
            with self.assertRaisesRegex(ValueError, "failed or incomplete"):
                summary.summarize(path, [])
            for pending, completed in ((1, 1), (0, 1)):
                path.write_text(json.dumps({"status": "passed", "result": {
                    "baseline": {"dispatch": {"pending_responses": pending,
                        "completed_responses": completed, "target_calls": 2}}}}))
                with self.assertRaisesRegex(ValueError, "responses are incomplete"):
                    summary.summarize(path, [])



if __name__ == "__main__":
    unittest.main()
