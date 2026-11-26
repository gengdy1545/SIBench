import gzip
from pathlib import Path
import sys
import tempfile
import unittest

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "graph/scripts/trace"))
from plot_access_locality import key_cuts, summarize_table


class AccessLocalityTest(unittest.TestCase):
    def test_exact_reuse_across_chunk_boundary_and_empty_window(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "table-1.csv.gz"
            with gzip.open(path, "wt") as stream:
                stream.write(
                    "window,pk,key,get_hits,scan_hits,put_hits,delete_hits\n"
                    "0,1,206100,2,0,1,1\n"
                    "0,2,20616200,0,4,0,0\n"
                    "1,1,206100,1,3,0,0\n"
                    "1,2,20616200,2,1,1,0\n"
                    "3,1,206100,5,0,0,0\n"
                )
            for key, cut in (("pk", 2), ("key", b"20616200")):
                with self.subTest(key=key):
                    cuts = key_cuts([path], key)
                    np.testing.assert_array_equal(cuts, [cut])
                    grid, totals, repeats, adjacent, raw = summarize_table(path, key, cuts, 4, 2, chunksize=1)
                    np.testing.assert_array_equal(grid, [[[3, 3, 2], [2, 5, 1]], [[5, 0, 0], [0, 0, 0]]])
                    np.testing.assert_array_equal(totals, [[2, 4, 2], [3, 4, 1], [0, 0, 0], [5, 0, 0]])
                    np.testing.assert_array_equal(repeats, [[1, 3, 1], [1, 2, 0], [0, 0, 0], [4, 0, 0]])
                    np.testing.assert_array_equal(adjacent, [[0, 0, 0], [1, 1, 0], [0, 0, 0], [0, 0, 0]])
                    np.testing.assert_array_equal(raw, [10, 8, 2, 1])


if __name__ == "__main__":
    unittest.main()
