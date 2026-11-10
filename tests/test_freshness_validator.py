import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import Mock, patch

from scripts import check_gsheets_dates as validator


class FreshnessValidatorTests(unittest.TestCase):
    def test_market_only_skips_manual_monitoring_but_rejects_stale_series(self):
        expected = date(2026, 11, 9)
        for market_date, automated_date, exit_code in (
            (expected, expected, 0),
            (date(2026, 11, 6), expected, 1),
            (expected, date(2026, 11, 6), 1),
        ):
            with self.subTest(market=market_date, automated=automated_date):
                client = Mock()
                client.open_by_url.return_value.worksheet.side_effect = AssertionError("manual monitoring must not be read")
                with patch.object(validator, "auth_client", return_value=client), patch.object(validator, "get_tickers", return_value=["S50"]), patch.object(validator, "market_latest_dates", return_value={"S50": market_date}), patch.object(validator, "automated_latest_dates", return_value={"S50": automated_date}), patch.object(validator, "write_report") as report:
                    self.assertEqual(validator.main(["--market-only", "--expected-date", "2026-11-09"]), exit_code)
                    self.assertTrue(report.call_args.kwargs["market_only"])

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
