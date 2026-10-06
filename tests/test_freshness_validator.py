import tempfile
import unittest
from datetime import date
from pathlib import Path

from scripts import check_gsheets_dates as validator


class FreshnessValidatorTests(unittest.TestCase):
    def test_contract_symbols_map_to_market_tabs(self):
        self.assertEqual(validator.ticker_from_symbol("S50Z26"), "S50")
        self.assertEqual(validator.ticker_from_symbol("USDJPYU26"), "USDJPY")
        self.assertEqual(validator.ticker_from_symbol("GF10V26"), "GF10")

    def test_status_requires_market_and_automated_dates(self):
        rows, monitoring_ok = validator.status_rows(
            {"S50": date(2026, 10, 15), "USD": date(2026, 10, 14)},
            {"S50": date(2026, 10, 15), "USD": date(2026, 10, 15)},
            date(2026, 10, 15), date(2026, 10, 15),
        )
        self.assertEqual([row["status"] for row in rows], ["PASS", "FAIL"])
        self.assertTrue(monitoring_ok)

    def test_report_includes_all_result_surfaces(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "report.md"
            validator.write_report(
                [{"ticker": "S50", "market": date(2026, 10, 15), "automated": date(2026, 10, 15), "status": "PASS"}],
                date(2026, 10, 15), True, date(2026, 10, 15), "PASS", report_path=path,
            )
            report = path.read_text()
        self.assertIn("S50", report)
        self.assertIn("B-Monitoring", report)
        self.assertIn("PASS", report)


if __name__ == "__main__":
    unittest.main()
