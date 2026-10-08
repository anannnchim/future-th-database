#!/usr/bin/env python3
"""Review-gated production reconciliation for one staged F1-TH confirmation.

Without --apply this script is a dry run.  --apply is intended only for a
manually dispatched, reviewed GitHub Actions run; it never places trades.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path

if __package__:
    from .check_gsheets_dates import AUTOMATED_F1_URL, auth_client
    from .execution_confirmation_intake import IntakeError, gmail_service, parse_single_confirmation, parse_trade_date
    from .execution_reconciliation import (
        EQUITY_OFFSET, EXECUTION_PRICE_COLUMNS, EXECUTION_QUANTITY_COLUMNS, TOLERANCE,
        Confirmation, ConfirmationError, EquitySnapshot, equity_checks,
    )
else:
    from check_gsheets_dates import AUTOMATED_F1_URL, auth_client
    from execution_confirmation_intake import IntakeError, gmail_service, parse_single_confirmation, parse_trade_date
    from execution_reconciliation import (
        EQUITY_OFFSET, EXECUTION_PRICE_COLUMNS, EXECUTION_QUANTITY_COLUMNS, TOLERANCE,
        Confirmation, ConfirmationError, EquitySnapshot, equity_checks,
    )


CONTROL_SHEET = "order-confirmation"
EXECUTION_SHEET = "Execution"
EQUITY_SHEET = "Equity"
README_SHEET = "README"


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trade-date", required=True, type=date.fromisoformat)
    parser.add_argument("--gmail-token-file", type=Path, default=os.getenv("GMAIL_OAUTH_TOKEN_FILE"))
    parser.add_argument("--pdf-password", default=os.getenv("F1_TH_PDF_PASSWORD"))
    parser.add_argument("--apply", action="store_true", help="Write only after a human-reviewed manual dispatch.")
    parser.add_argument("--report", type=Path, default=Path("execution_reconciliation_apply_report.md"))
    return parser.parse_args(argv)


def decimal_or_none(value) -> Decimal | None:
    if value is None or str(value).strip() == "":
        return None
    try:
        return Decimal(str(value).replace(",", "").strip())
    except InvalidOperation as exc:
        raise ConfirmationError("Existing sheet value is not numeric; review required") from exc


def same_number(existing, expected: Decimal | int) -> bool:
    current = decimal_or_none(existing)
    return current is not None and abs(current - Decimal(str(expected))) <= TOLERANCE


def date_row(values: list, target: date) -> int:
    for row, value in enumerate(values[1:], start=2):
        if value and parse_trade_date(value) == target:
            return row
    raise ConfirmationError(f"No worksheet row for {target.isoformat()}")


def previous_date_row(values: list, target: date) -> int:
    candidates = []
    for row, value in enumerate(values[1:], start=2):
        if value:
            parsed = parse_trade_date(value)
            if parsed < target:
                candidates.append((parsed, row))
    if not candidates:
        raise ConfirmationError("No prior Equity date row")
    return max(candidates)[1]


def proposed_cells(confirmation: Confirmation) -> dict[str, Decimal | int]:
    result: dict[str, Decimal | int] = {}
    for trade in confirmation.trades:
        result[EXECUTION_QUANTITY_COLUMNS[trade.instrument]] = trade.quantity
        result[EXECUTION_PRICE_COLUMNS[trade.instrument]] = trade.price
    return result


def execution_writes(existing: dict[str, str], proposed: dict[str, Decimal | int]) -> dict[str, Decimal | int]:
    writes = {}
    for column, expected in proposed.items():
        current = existing.get(column, "")
        if decimal_or_none(current) is None:
            writes[column] = expected
        elif not same_number(current, expected):
            raise ConfirmationError(f"Execution!{column} already has a conflicting value")
    return writes


def equity_write(existing, expected: Decimal, cell: str) -> bool:
    if decimal_or_none(existing) is None:
        return True
    if not same_number(existing, expected):
        raise ConfirmationError(f"{cell} already has a conflicting value")
    return False


def report(path: Path, trade_date: date, result: str, detail: str, applied: bool):
    lines = [
        "# F1-TH execution reconciliation report", "",
        f"- Trade date: **{trade_date.isoformat()}**",
        f"- Result: **{result}**",
        f"- Detail: {detail}",
        f"- Production writes: **{'applied' if applied else 'not applied'}**",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    if summary_path := os.getenv("GITHUB_STEP_SUMMARY"):
        with open(summary_path, "a", encoding="utf-8") as summary:
            summary.write("\n".join(lines) + "\n")


def main(argv=None):
    args = parse_args(argv)
    if not args.pdf_password:
        raise IntakeError("F1_TH_PDF_PASSWORD is not set")
    client = auth_client()
    workbook = client.open_by_url(AUTOMATED_F1_URL)
    control = workbook.worksheet(CONTROL_SHEET)
    control_rows = control.get_all_records()
    candidates = [
        (row, record) for row, record in enumerate(control_rows, start=2)
        if record.get("trade_date") and parse_trade_date(record.get("trade_date")) == args.trade_date
        and str(record.get("status") or "").strip().upper() == "STAGED"
        and (record.get("trade_expected") is True or str(record.get("trade_expected")).upper() == "TRUE")
    ]
    if len(candidates) != 1:
        raise ConfirmationError("Expected exactly one checked STAGED control row")
    control_row, _ = candidates[0]
    parsed = parse_single_confirmation(gmail_service(args.gmail_token_file), args.trade_date, args.pdf_password)
    if parsed is None:
        raise ConfirmationError("No matching broker confirmation PDF")
    message_id, confirmation = parsed
    if confirmation.trade_date != args.trade_date:
        raise ConfirmationError("PDF statement date does not match requested date")

    execution = workbook.worksheet(EXECUTION_SHEET)
    equity = workbook.worksheet(EQUITY_SHEET)
    readme = workbook.worksheet(README_SHEET)
    execution_row = date_row(execution.col_values(1), args.trade_date)
    equity_dates = equity.col_values(1)
    equity_row = date_row(equity_dates, args.trade_date)
    prior_equity_row = previous_date_row(equity_dates, args.trade_date)
    execution_values = execution.get(f"B{execution_row}:N{execution_row}")
    values = (execution_values[0] if execution_values else []) + [""] * 13
    existing_execution = {column: values[index] for column, index in {**dict(zip("BCDEFG", range(6))), **dict(zip("IJKLMN", range(7, 13)))}.items()}
    writes = execution_writes(existing_execution, proposed_cells(confirmation))

    if not args.apply:
        report(args.report, args.trade_date, "READY_FOR_APPLY", f"Validated staged PDF {message_id} and workbook targets", False)
        return 0

    written_cells: list[tuple[object, str]] = []
    try:
        if writes:
            execution.batch_update([{"range": f"{column}{execution_row}", "values": [[str(value)]]} for column, value in writes.items()])
            written_cells.extend((execution, f"{column}{execution_row}") for column in writes)
            time.sleep(10)
        adjustment = decimal_or_none(readme.acell("P35").value)
        if adjustment is None:
            raise ConfirmationError("README!P35 is blank")
        write_k = equity_write(equity.acell(f"K{equity_row}").value, adjustment, f"Equity!K{equity_row}")
        write_o = equity_write(equity.acell(f"O{equity_row}").value, confirmation.end_equity, f"Equity!O{equity_row}")
        requests = []
        if write_k:
            requests.append({"range": f"K{equity_row}", "values": [[str(adjustment)]]})
        if write_o:
            requests.append({"range": f"O{equity_row}", "values": [[str(confirmation.end_equity)]]})
        if requests:
            equity.batch_update(requests)
            written_cells.extend((equity, request["range"]) for request in requests)
            time.sleep(10)
        previous = decimal_or_none(equity.acell(f"N{prior_equity_row}").value)
        current = decimal_or_none(equity.acell(f"N{equity_row}").value)
        if previous is None or current is None:
            raise ConfirmationError("Equity!N calculation is blank")
        checks = equity_checks(confirmation, EquitySnapshot(previous, current))
        if any(value != "PASS" for value in checks.values()):
            raise ConfirmationError("Equity reconciliation check failed")
        control.update_acell(f"D{control_row}", "RECONCILED")
        report(args.report, args.trade_date, "RECONCILED", "Execution and equity checks passed", True)
        return 0
    except (ConfirmationError, IntakeError):
        for worksheet, cell in reversed(written_cells):
            worksheet.update_acell(cell, "")
        control.update_acell(f"D{control_row}", "REVIEW_REQUIRED")
        raise


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ConfirmationError, IntakeError) as exc:
        print(f"REVIEW_REQUIRED: {exc}", file=sys.stderr)
        sys.exit(2)
