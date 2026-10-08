import tempfile
import unittest
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from scripts import execution_reconciliation as reconciliation


SAMPLE = """
E-Document : Daily Derivatives Confirmation Note as of 07 October 2026
TRADING CONFIRMATION
S50Z26 BUY 2 1054.30
USDZ26 SELL 10 33.36
STATEMENT OF ACCOUNT
Begin Equity Balance THB 2,346,451.95
End Equity Balance THB 2,365,712.38
"""


class ExecutionReconciliationTests(unittest.TestCase):
    def test_parse_confirmation_maps_signed_instruments(self):
        confirmation = reconciliation.parse_confirmation(SAMPLE)
        self.assertEqual(confirmation.trade_date, date(2026, 10, 7))
        self.assertEqual(confirmation.trades[0], reconciliation.Trade("S50", 2, Decimal("1054.30")))
        self.assertEqual(confirmation.trades[1], reconciliation.Trade("USD", -10, Decimal("33.36")))

    def test_parse_confirmation_accepts_statement_date_variants(self):
        for heading in (
            "Daily Derivatives Confirmation Note as at: 07 Oct 2026",
            "Statement as of 07 October 2026",
        ):
            text = SAMPLE.replace("E-Document : Daily Derivatives Confirmation Note as of 07 October 2026", heading)
            self.assertEqual(reconciliation.parse_confirmation(text).trade_date, date(2026, 10, 7))

    def test_duplicate_instrument_requires_review(self):
        with self.assertRaisesRegex(reconciliation.ConfirmationError, "Multiple executions"):
            reconciliation.parse_confirmation(SAMPLE + "S50Z26 SELL 1 1055.00")

    def test_proposed_cells_use_execution_contract(self):
        confirmation = reconciliation.parse_confirmation(SAMPLE)
        cells = reconciliation.proposed_execution_cells(confirmation)
        self.assertEqual(cells, {"B": 2, "I": Decimal("1054.30"), "C": -10, "J": Decimal("33.36")})

    def test_equity_checks_allow_rounding_tolerance(self):
        confirmation = reconciliation.parse_confirmation(SAMPLE)
        checks = reconciliation.equity_checks(
            confirmation,
            reconciliation.EquitySnapshot(Decimal("3346451.954"), Decimal("3365712.387")),
        )
        self.assertEqual(checks, {"begin": "PASS", "end": "PASS"})

    def test_report_masks_values_by_default(self):
        confirmation = reconciliation.parse_confirmation(SAMPLE)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "report.md"
            reconciliation.write_report(confirmation, {"begin": "PASS", "end": "PASS"}, path, False)
            report = path.read_text()
        self.assertIn("Production sheets were not changed", report)
        self.assertNotIn("1054.30", report)

    def test_pdf_read_error_requires_review(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "confirmation.pdf"
            path.write_bytes(b"not a real pdf")
            with patch.object(reconciliation, "PdfReader", side_effect=RuntimeError("crypto unavailable")):
                with self.assertRaisesRegex(reconciliation.ConfirmationError, "Unable to read confirmation PDF"):
                    reconciliation.extract_pdf_text(path, "password")


if __name__ == "__main__":
    unittest.main()
