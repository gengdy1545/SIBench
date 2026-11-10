import json
from pathlib import Path
import subprocess
import tempfile
import unittest


class PixelsResourcesTest(unittest.TestCase):
    def test_memory_hierarchies_and_missing_measurements(self):
        script = Path(__file__).resolve().parents[3] / "scripts/sample-pixels-resources.sh"
        for files, expected, metric in [
            ({"memory.current": "1000", "memory.stat": "anon 700\nfile 300\n"}, 700, "cgroup_v2_anon"),
            ({"memory/memory.usage_in_bytes": "1000", "memory/memory.stat": "rss 300\ntotal_rss 800\n"}, 800, "cgroup_v1_total_rss"),
            ({"memory.current": "1000", "memory.stat": "file 300\n"}, None, "unavailable"),
            ({}, None, "unavailable"),
        ]:
            with self.subTest(metric=metric, files=files), tempfile.TemporaryDirectory() as directory:
                for name, text in files.items():
                    path = Path(directory) / name
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text(text)
                output = subprocess.check_output(
                    ["bash", "-c", 'source "$1"; sample_memory "$2"', "sample-test", str(script), directory], text=True)
                result = json.loads("{" + output + "}")
                self.assertEqual(expected, result["pod_anon"])
                self.assertEqual(metric, result["pod_anon_metric"])
                self.assertEqual(1000 if files else None, result["pod_memory"])
