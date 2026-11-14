"""Plot consumers must bind extraction results to the selected analyzer."""

import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location("trace_plot_common", ROOT / "graph/scripts/trace/_common.py")
COMMON = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(COMMON)


class AnalysisIdentityTest(unittest.TestCase):
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
