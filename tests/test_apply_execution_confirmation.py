import unittest
from datetime import date
from decimal import Decimal

from scripts import apply_execution_confirmation as apply
from scripts.execution_reconciliation import Confirmation, ConfirmationError, Trade


class ApplyExecutionConfirmationTests(unittest.TestCase):
    def setUp(self):
        self.confirmation = Confirmation(
            trade_date=date(2026, 10, 7),
            trades=(Trade("S50", -1, Decimal("1054.30")),),
            begin_equity=Decimal("2000000"),
            end_equity=Decimal("2010000"),
        )

    def test_date_row_uses_actual_date_not_position(self):
        values = ["Date", "Transaction", "2026-10-06", "2026-10-07"]
        self.assertEqual(apply.date_row(values, date(2026, 10, 7)), 4)
        self.assertEqual(apply.previous_date_row(values, date(2026, 10, 7)), 3)

    def test_execution_writes_only_blank_cells(self):
        proposed = apply.proposed_cells(self.confirmation)
        self.assertEqual(apply.execution_writes({"B": "", "I": ""}, proposed), proposed)
        self.assertEqual(apply.execution_writes({"B": "-1", "I": "1054.30"}, proposed), {})

    def test_conflicting_execution_cell_requires_review(self):
        with self.assertRaisesRegex(ConfirmationError, "conflicting"):
            apply.execution_writes({"B": "1", "I": "1054.30"}, apply.proposed_cells(self.confirmation))

    def test_equity_write_is_idempotent_but_never_overwrites_conflict(self):
        self.assertTrue(apply.equity_write("", Decimal("360"), "Equity!K570"))
        self.assertFalse(apply.equity_write("360", Decimal("360.00"), "Equity!K570"))
        with self.assertRaisesRegex(ConfirmationError, "conflicting"):
            apply.equity_write("361", Decimal("360"), "Equity!K570")

    def test_rollback_uses_cell_only_a1_references(self):
        class Worksheet:
            def __init__(self):
                self.cleared = []

            def update_acell(self, cell, value):
                self.cleared.append((cell, value))

        worksheet = Worksheet()
        failures = apply.rollback_writes([(worksheet, "O579"), (worksheet, "K579")])

        self.assertEqual(failures, [])
        self.assertEqual(worksheet.cleared, [("K579", ""), ("O579", "")])

    def test_rollback_reports_failed_cells_without_stopping(self):
        class Worksheet:
            def __init__(self):
                self.cleared = []

            def update_acell(self, cell, value):
                if cell == "O579":
                    raise RuntimeError("network error")
                self.cleared.append((cell, value))

        worksheet = Worksheet()
        failures = apply.rollback_writes([(worksheet, "O579"), (worksheet, "K579")])

        self.assertEqual(failures, ["O579"])
        self.assertEqual(worksheet.cleared, [("K579", "")])


if __name__ == "__main__":
    unittest.main()
