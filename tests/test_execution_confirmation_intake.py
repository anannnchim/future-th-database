import unittest
from datetime import date, datetime
from zoneinfo import ZoneInfo

from scripts import execution_confirmation_intake as intake


class ExecutionConfirmationIntakeTests(unittest.TestCase):
    def test_expected_rows_only_uses_checked_pending_rows_for_date(self):
        rows = intake.expected_rows(
            [
                {"trade_date": "2026-10-07", "trade_expected": True, "note": "S50", "status": "WAITING_FOR_PDF"},
                {"trade_date": "2026-10-07", "trade_expected": True, "note": "already staged", "status": "STAGED"},
                {"trade_date": "2026-10-07", "trade_expected": False, "status": "WAITING_FOR_PDF"},
                {"trade_date": "2026-10-06", "trade_expected": True, "status": "WAITING_FOR_PDF"},
            ],
            date(2026, 10, 7),
        )
        self.assertEqual(rows, [intake.ExpectedTrade(2, date(2026, 10, 7), "S50", "WAITING_FOR_PDF")])

    def test_missing_confirmation_rows_remain_eligible_for_retry(self):
        rows = intake.expected_rows(
            [{"trade_date": "2026-10-07", "trade_expected": "TRUE", "status": "MISSING_CONFIRMATION"}],
            date(2026, 10, 7),
        )
        self.assertEqual(rows[0].status, "MISSING_CONFIRMATION")

    def test_gmail_query_includes_full_trade_date_and_attachment(self):
        self.assertEqual(intake.gmail_query(date(2026, 10, 7)), '"Daily Derivatives Confirmation Note as of 07 October 2026" has:attachment')

    def test_missing_cutoff_uses_bangkok_clock(self):
        tz = ZoneInfo("Asia/Bangkok")
        self.assertFalse(intake.past_missing_cutoff("07:00", datetime(2026, 10, 8, 6, 59, tzinfo=tz)))
        self.assertTrue(intake.past_missing_cutoff("07:00", datetime(2026, 10, 8, 7, 0, tzinfo=tz)))


if __name__ == "__main__":
    unittest.main()
