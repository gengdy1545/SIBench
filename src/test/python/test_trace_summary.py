import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location("trace_summary", Path(__file__).resolve().parents[3] / "scripts/summarize_trace.py")
summary = importlib.util.module_from_spec(spec)
spec.loader.exec_module(summary)


class TraceSummaryTest(unittest.TestCase):
    def fixture(self):
        # A union group with one PK Get and two secondary Scans, plus one Put.
        return {"statistics": {"effective_requests": 4, "primary_entries": 2, "secondary_entries": 2,
                "request_types": {"union_query": 1, "put": 1},
                "write_origins": {"business_update": 1},
                "index_operations": {"primary": {"get": 1, "put": 1}, "secondary": {"scan": 2}}}}

    def test_counts_group_members_and_keeps_scope_denominators(self):
        result = summary.phase(self.fixture())
        self.assertEqual(4, result["total"])
        self.assertEqual(2, result["secondary"]["reads"])
        self.assertEqual(0.25, result["primary"]["operation_shares"]["get"])
        self.assertEqual(0.5, result["secondary"]["share"])
        self.assertEqual({"get": 0, "scan": 2, "put": 0, "delete": 0}, result["secondary"]["operations"])
        self.assertEqual({"business_insert": 0, "business_update": 1}, result["write_origins"])

    def test_rejects_wrong_counts_instead_of_printing_plausible_ratios(self):
        for key in ("effective_requests", "primary_entries"):
            fixture = self.fixture()
            fixture["statistics"][key] += 1
            with self.assertRaisesRegex(ValueError, "count mismatch"):
                summary.phase(fixture)
        for value in (-1, 1.5, True):
            fixture = self.fixture()
            fixture["statistics"]["index_operations"]["primary"]["get"] = value
            with self.assertRaises(ValueError):
                summary.phase(fixture)
            fixture = self.fixture()
            fixture["statistics"]["write_origins"]["business_update"] = value
            with self.assertRaises(ValueError):
                summary.phase(fixture)
        fixture = self.fixture()
        fixture["statistics"]["write_origins"]["unknown"] = 1
        with self.assertRaisesRegex(ValueError, "Unknown write origin"):
            summary.phase(fixture)


if __name__ == "__main__":
    unittest.main()
