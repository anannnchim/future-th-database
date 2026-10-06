import tempfile
import unittest
from datetime import date
from pathlib import Path

from scripts.trading_calendar import expected_trading_date, load_extra_holidays


class ExpectedTradingDateTests(unittest.TestCase):
    def test_regular_weekday(self):
        self.assertEqual(expected_trading_date(date(2026, 9, 10), closed_dates=set()), date(2026, 9, 9))

    def test_monday_uses_previous_friday(self):
        self.assertEqual(expected_trading_date(date(2026, 9, 14), closed_dates=set()), date(2026, 9, 11))

    def test_configured_closure_is_skipped(self):
        self.assertEqual(
            expected_trading_date(date(2026, 5, 5), closed_dates={date(2026, 5, 4)}),
            date(2026, 5, 1),
        )

    def test_extra_holidays_allow_comments(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "tfex_holidays.txt"
            path.write_text("# closure list\n2026-01-02 # replacement holiday\n")
            self.assertEqual(load_extra_holidays(path), {date(2026, 1, 2)})

    def test_extra_holidays_reject_invalid_dates(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "tfex_holidays.txt"
            path.write_text("not-a-date\n")
            with self.assertRaisesRegex(ValueError, "line 1"):
                load_extra_holidays(path)


if __name__ == "__main__":
    unittest.main()
