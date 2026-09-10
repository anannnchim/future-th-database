import unittest
from datetime import date

from scripts.trading_calendar import expected_trading_date


class ExpectedTradingDateTests(unittest.TestCase):
    def test_regular_weekday(self):
        self.assertEqual(
            expected_trading_date(date(2026, 9, 10), closed_dates=set()),
            date(2026, 9, 9),
        )

    def test_monday_uses_previous_friday(self):
        self.assertEqual(
            expected_trading_date(date(2026, 9, 14), closed_dates=set()),
            date(2026, 9, 11),
        )

    def test_saturday_uses_friday(self):
        self.assertEqual(
            expected_trading_date(date(2026, 9, 12), closed_dates=set()),
            date(2026, 9, 11),
        )

    def test_configured_closure_is_skipped(self):
        self.assertEqual(
            expected_trading_date(
                date(2026, 5, 5),
                closed_dates={date(2026, 5, 4)},
            ),
            date(2026, 5, 1),
        )


if __name__ == "__main__":
    unittest.main()
