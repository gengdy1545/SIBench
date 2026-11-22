import csv
import hashlib
import json
import math
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import matplotlib

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "graph/scripts/replay"))
from plot_replay_performance import load_results, plot, style

TRACES = [{"name": "t", "label": "T"}]
BACKENDS = [{"name": "a", "label": "A", "hatch": ""}, {"name": "b", "label": "B", "hatch": "//"}]
METRICS = [
    {"name": "load", "path": ("baseline", "operations_per_second"), "divisor": 1000, "label": "L", "scale": "linear"},
    {"name": "write_p99", "path": ("workload", "dispatch", "write_latency", "p99_ms"), "divisor": 1,
     "label": "W", "scale": "log"},
]


def report(status="passed", writes=3):
    return {"status": status, "result": {
        "baseline": {"operations_per_second": 2500.0},
        "workload": {"operations_per_second": 1200.0, "dispatch": {
            "search_latency": {"count": 4, "p50_ms": 2.0, "p99_ms": 8.0},
            "write_latency": {"count": writes, "p50_ms": 1.5 if writes else 0.0,
                              "p99_ms": 7.5 if writes else 0.0}}}}}


class ReplayPerformanceTest(unittest.TestCase):
    def write(self, root, backend, data):
        path = root / "t" / backend / "report.json"
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps(data))

    def test_values_divisor_and_empty_latency(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.write(root, "a", report())
            self.write(root, "b", report(writes=0))
            table = load_results(root, TRACES, BACKENDS, METRICS)
            self.assertEqual(table[("t", "a")][0], {"load": 2.5, "write_p99": 7.5})
            # Zero samples reports 0.0 ms; it must not be drawn as a measured latency.
            self.assertTrue(math.isnan(table[("t", "b")][0]["write_p99"]))
            fig = plot(table, TRACES, BACKENDS, METRICS)
            self.assertEqual(len(fig.axes), len(METRICS))
            matplotlib.pyplot.close(fig)

    def test_rejects_missing_or_unpassed_reports(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.write(root, "a", report())
            with self.assertRaisesRegex(FileNotFoundError, "Missing replay report"):
                load_results(root, TRACES, BACKENDS, METRICS)
            self.write(root, "b", report(status="baseline_passed"))
            with self.assertRaisesRegex(ValueError, "did not pass"):
                load_results(root, TRACES, BACKENDS, METRICS)

    def test_single_metric_renders(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for backend in BACKENDS:
                self.write(root, backend["name"], report())
            metrics = METRICS[:1]
            fig = plot(load_results(root, TRACES, BACKENDS, metrics), TRACES, BACKENDS, metrics)
            try:
                fig.canvas.draw()
                self.assertEqual(len(fig.axes), 1)
                self.assertEqual([bar.get_height() for bar in fig.axes[0].patches], [2.5, 2.5])
            finally:
                matplotlib.pyplot.close(fig)

    def test_read_only_selection_renders_missing_write_latency(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for backend in BACKENDS:
                self.write(root, backend["name"], report(writes=0))
            fig = plot(load_results(root, TRACES, BACKENDS, METRICS), TRACES, BACKENDS, METRICS)
            try:
                fig.canvas.draw()
                self.assertEqual([text.get_text() for text in fig.axes[1].texts], ["n/a", "n/a"])
            finally:
                matplotlib.pyplot.close(fig)

    def test_rejects_invalid_measurements(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "t/a/report.json"
            path.parent.mkdir(parents=True)
            for value in (math.nan, math.inf, -1, True, "7.5"):
                with self.subTest(value=value):
                    data = report()
                    data["result"]["baseline"]["operations_per_second"] = value
                    path.write_text(json.dumps(data))
                    with self.assertRaisesRegex(ValueError, "Invalid replay metric.*load"):
                        load_results(root, TRACES, BACKENDS[:1], METRICS)
            for count in (-1, 1.5, True, "0"):
                with self.subTest(count=count):
                    path.write_text(json.dumps(report(writes=count)))
                    with self.assertRaisesRegex(ValueError, "Invalid latency sample count"):
                        load_results(root, TRACES, BACKENDS[:1], METRICS)
            data = report()
            data["result"]["workload"]["dispatch"]["write_latency"]["p99_ms"] = 0.0
            path.write_text(json.dumps(data))
            with self.assertRaisesRegex(ValueError, "Invalid replay metric.*write_p99"):
                load_results(root, TRACES, BACKENDS[:1], METRICS)

    def test_cli_exports_source_units_and_report_identity_from_another_directory(self):
        script = Path(__file__).resolve().parents[3] / "graph/scripts/replay/plot_replay_performance.py"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            identities = {}
            for trace in style.TRACES:
                for backend in style.BACKENDS:
                    path = root / "reports" / trace["name"] / backend["name"] / "report.json"
                    path.parent.mkdir(parents=True)
                    data = json.dumps(report(writes=0 if trace["name"] == "ycsb-c" else 3)).encode()
                    path.write_bytes(data)
                    identities[(trace["name"], backend["name"])] = hashlib.sha256(data).hexdigest()
            subprocess.run([sys.executable, str(script), "--results", "reports", "--out", "plots"],
                           cwd=root, check=True, capture_output=True, text=True, timeout=30)
            output = root / "plots" / style.OUTPUT_NAME
            self.assertTrue(output.with_suffix(".pdf").read_bytes().startswith(b"%PDF-"))
            self.assertTrue(output.with_suffix(".png").read_bytes().startswith(b"\x89PNG\r\n\x1a\n"))
            with output.with_suffix(".csv").open(newline="") as stream:
                rows = list(csv.DictReader(stream))
            self.assertEqual(len(rows), len(style.TRACES) * len(style.BACKENDS) * len(style.METRICS))
            expected = {"load_throughput": 2500.0, "workload_throughput": 1200.0,
                        "read_p50": 2.0, "read_p99": 8.0, "write_p50": 1.5, "write_p99": 7.5}
            for row in rows:
                self.assertEqual(row["report_sha256"], identities[(row["trace"], row["backend"])])
                if row["trace"] == "ycsb-c" and row["metric"].startswith("write_"):
                    self.assertEqual(row["value"], "")
                else:
                    self.assertEqual(float(row["value"]), expected[row["metric"]])


if __name__ == "__main__":
    unittest.main()
