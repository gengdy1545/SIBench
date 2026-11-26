import gzip
from pathlib import Path
import sys
import tempfile
import unittest

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "graph/scripts/trace"))
from plot_access_coverage import coverage_series, first_reads, key_count, trace_coverage


class AccessCoverageTest(unittest.TestCase):
    def test_first_reads_across_chunks_and_tables(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            rows = [
                "0,01,0,0,1\n0,02,2,0,0\n1,01,0,1,0\n1,02,0,3,0\n2,01,1,0,0\n2,03,1,0,0\n",
                "1,01,1,0,0\n2,04,0,0,1\n",
            ]
            for table_id, data in enumerate(rows, 1):
                with gzip.open(directory / f"table-{table_id}.csv.gz", "wt") as stream:
                    stream.write("window,key,get_hits,scan_hits,put_hits\n" + data)
            first, totals = first_reads(directory / "table-1.csv.gz", 3, chunksize=2)
            np.testing.assert_array_equal(first, [1, 1, 1])
            np.testing.assert_array_equal(totals, [4, 4])

            summary = {"windows": 3, "window_requests": 10, "workload_requests": 23,
                       "tables": [{"id": 1}, {"id": 2}], "row_hits": {"get": 5, "scan": 4}}
            requests, distinct = trace_coverage(directory, summary)
            np.testing.assert_array_equal(requests, [10, 20, 23])
            np.testing.assert_array_equal(distinct, [1, 3, 4])

            summary["row_hits"]["scan"] = 3
            with self.assertRaisesRegex(ValueError, "CSV read counts differ"):
                trace_coverage(directory, summary)

    def test_percentages_use_the_same_complete_key_universe_at_each_point(self):
        trace = {"name": "example", "label": "Example"}
        summary = {"manifest_sha256": "example", "baseline_rows": 3}
        receipt = {"manifest_sha256": "example", "baseline_distinct_pk": 3, "total_distinct_pk": 5}
        requests = np.array([10, 20, 23])
        distinct = np.array([1, 3, 4])
        total = key_count(summary, receipt)
        series = coverage_series(trace, summary, requests, distinct, total)
        np.testing.assert_array_equal(series["total_distinct_pk"], [5, 5, 5])
        np.testing.assert_allclose(series["coverage_percent"], [20, 60, 80])
        with self.assertRaisesRegex(ValueError, "outside the key universe"):
            coverage_series(trace, summary, requests, distinct, 3)
        receipt["manifest_sha256"] = "different"
        with self.assertRaisesRegex(ValueError, "do not match"):
            key_count(summary, receipt)


if __name__ == "__main__":
    unittest.main()
