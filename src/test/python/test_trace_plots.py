"""Plot consumers must bind extraction results to the selected analyzer."""

import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location("trace_plot_common", ROOT / "graph/scripts/trace/_common.py")
COMMON = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(COMMON)


class AnalysisIdentityTest(unittest.TestCase):
    def test_plot_selection_uses_java_runtime_paths(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            trace = root / "persistent"
            trace.mkdir()
            (trace / "manifest.json").write_text("{}")
            config = {"jar": str(root / "sibench.jar"), "runtime": str(root / "runtime.yaml"),
                      "analysis_directory": str(root / "analysis"), "plot_directory": str(root / "plots"),
                      "window_requests": 10, "pk_bins": 2, "sort_memory_bytes": 4096,
                      "traces": [{"name": "ycsb-a", "label": "YCSB-A"}]}
            file = root / "plots.json"
            file.write_text(json.dumps(config))
            with patch.object(COMMON.subprocess, "check_output", return_value=json.dumps({"ycsb-a": str(trace)})) as command:
                resolved = COMMON.load_config(file)
                self.assertEqual(resolved["traces"][0]["directory"], trace)
                self.assertEqual(command.call_args.args[0][-2:], ["trace-paths", config["runtime"]])
            with patch.object(COMMON.subprocess, "check_output", return_value="{}"):
                with self.assertRaisesRegex(ValueError, "Unknown benchmark entry"):
                    COMMON.load_config(file)

    def test_changed_analyzer_rejects_previously_matching_extraction(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            trace = {"name": "case", "directory": root / "trace"}
            trace["directory"].mkdir()
            manifest = trace["directory"] / "manifest.json"
            manifest.write_text("{}")
            jar = root / "sibench.jar"
            jar.write_bytes(b"old analyzer")
            config = {"jar": jar, "analysis_directory": root, "window_requests": 10,
                      "pk_bins": 2, "sort_memory_bytes": 4096}
            directory = root / "case/accesses"
            directory.mkdir(parents=True)
            summary = {"status": "complete", "manifest_sha256": hashlib.sha256(manifest.read_bytes()).hexdigest(),
                       "window_requests": 10, "requested_pk_bins": 2, "sort_memory_bytes": 4096}
            (directory / "summary.json").write_text(json.dumps(summary))
            (directory / "preparation.json").write_text(json.dumps({"jar_sha256": hashlib.sha256(jar.read_bytes()).hexdigest()}))
            self.assertEqual(COMMON.load_analysis(config, trace), (directory, summary))
            jar.write_bytes(b"new analyzer")
            with self.assertRaisesRegex(ValueError, "Stale analysis"):
                COMMON.load_analysis(config, trace)


if __name__ == "__main__":
    unittest.main()
