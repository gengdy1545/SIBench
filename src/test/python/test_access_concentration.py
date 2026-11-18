from collections import Counter
import gzip
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "graph/scripts/trace"))
from plot_access_concentration import concentration_curve, table_frequencies, trace_frequencies


class AccessConcentrationTest(unittest.TestCase):
    def test_all_operations_contribute_and_tables_keep_separate_keys(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            rows = [
                "01,2,0,0,0\n02,0,5,0,0\n03,0,0,1,0\n01,0,2,0,0\n02,0,0,0,1\n01,0,5,0,0\n04,1,0,0,0\n",
                "01,3,0,0,0\n",
            ]
            for table_id, data in enumerate(rows, 1):
                with gzip.open(directory / f"table-{table_id}.csv.gz", "wt") as stream:
                    stream.write("key,get_hits,scan_hits,put_hits,delete_hits\n" + data)
            counts, totals = table_frequencies(directory / "table-1.csv.gz", chunksize=2)
            self.assertEqual(counts, Counter({9: 1, 6: 1, 1: 2}))
            self.assertEqual(totals.tolist(), [3, 12, 1, 1])
            summary = {"tables": [{"id": 1}, {"id": 2}],
                       "row_hits": {"get": 6, "scan": 12, "put": 1, "delete": 1}}
            combined = trace_frequencies(directory, summary)
            self.assertEqual(combined, Counter({9: 1, 6: 1, 3: 1, 1: 2}))
            summary["row_hits"]["put"] = 2
            with self.assertRaisesRegex(ValueError, "PK hit totals differ"):
                trace_frequencies(directory, summary)

    def test_crossing_is_the_first_whole_pk_and_uses_full_key_universe(self):
        curve = concentration_curve(Counter({4: 3, 1: 4}), 10, 90)
        crossing, = curve[curve["is_threshold"]].itertuples()
        self.assertEqual((crossing.ranked_pk, crossing.pk_percent, crossing.cumulative_hits), (6, 60, 15))
        self.assertEqual(curve.loc[curve["ranked_pk"] == 5, "cumulative_hits"].iat[0], 14)
        self.assertEqual(curve.iloc[-1]["pk_percent"], 100)
        self.assertEqual(curve.iloc[-1]["cumulative_hits"], 16)


if __name__ == "__main__":
    unittest.main()
