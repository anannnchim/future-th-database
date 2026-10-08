#!/usr/bin/env python3
"""Stage F1-TH broker confirmations without changing Google Sheets.

The production write path is intentionally not implemented. This tool extracts one
broker confirmation and produces a proposed Execution/Equity update for review.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path

from pypdf import PdfReader

if __package__:
    from .check_gsheets_dates import ticker_from_symbol
    from .trading_calendar import expected_trading_date
else:
    from check_gsheets_dates import ticker_from_symbol
    from trading_calendar import expected_trading_date


INSTRUMENTS = ("S50", "USD", "GF10", "EURUSD", "SVF", "USDJPY")
EXECUTION_QUANTITY_COLUMNS = dict(zip(INSTRUMENTS, "BCDEFG"))
EXECUTION_PRICE_COLUMNS = dict(zip(INSTRUMENTS, "IJKLMN"))
EQUITY_OFFSET = Decimal("1000000")
TOLERANCE = Decimal("0.01")


class ConfirmationError(ValueError):
    """Raised when a confirmation cannot safely be staged."""


@dataclass(frozen=True)
class Trade:
    instrument: str
    quantity: int
    price: Decimal


@dataclass(frozen=True)
class Confirmation:
    trade_date: date
    trades: tuple[Trade, ...]
    begin_equity: Decimal
    end_equity: Decimal


@dataclass(frozen=True)
class EquitySnapshot:
    previous_calculated: Decimal
    current_calculated: Decimal


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--pdf", type=Path, help="Encrypted broker confirmation PDF.")
    source.add_argument("--text-file", type=Path, help="Extracted confirmation text for a safe dry run.")
    parser.add_argument("--pdf-password", help="PDF password. Prefer the F1_TH_PDF_PASSWORD environment variable.")
    parser.add_argument("--trade-date", type=date.fromisoformat, help="Expected confirmation trade date.")
    parser.add_argument("--equity-previous", type=Decimal, help="Existing Equity!N for the prior trading date.")
    parser.add_argument("--equity-current", type=Decimal, help="Existing calculated Equity!N for the trade date.")
    parser.add_argument("--report", type=Path, default=Path("execution_reconciliation_report.md"))
    parser.add_argument("--include-values", action="store_true", help="Include sensitive proposed values in the local report.")
    return parser.parse_args(argv)


def parse_money(value: str) -> Decimal:
    cleaned = value.replace(",", "").replace("THB", "").replace("บาท", "").strip()
    try:
        return Decimal(cleaned)
    except InvalidOperation as exc:
        raise ConfirmationError(f"Invalid monetary value: {value!r}") from exc


def extract_pdf_text(path: Path, password: str | None) -> str:
    if not path.exists():
        raise ConfirmationError(f"PDF does not exist: {path}")
    reader = PdfReader(path)
    if reader.is_encrypted and reader.decrypt(password or "") == 0:
        raise ConfirmationError("Unable to decrypt confirmation PDF")
    text = "\n".join(page.extract_text() or "" for page in reader.pages)
    if not text.strip():
        raise ConfirmationError("Confirmation PDF contains no extractable text")
    return text


def extract_balance(text: str, label: str) -> Decimal:
    pattern = rf"{label}\s*(?:\([^)]*\))?\s*(?:THB)?\s*([\d,]+(?:\.\d+)?)"
    match = re.search(pattern, text, flags=re.IGNORECASE)
    if not match:
        raise ConfirmationError(f"Missing {label} in confirmation")
    return parse_money(match.group(1))


def extract_trade_date(text: str) -> date:
    match = re.search(r"Confirmation\s+Note\s+as\s+of\s+(\d{1,2}\s+[A-Za-z]+\s+\d{4})", text, re.IGNORECASE)
    if not match:
        raise ConfirmationError("Missing statement trade date")
    from datetime import datetime

    return datetime.strptime(match.group(1), "%d %B %Y").date()


def parse_trades(text: str) -> tuple[Trade, ...]:
    """Parse the stable, whitespace-separated confirmation table layout.

    An unfamiliar layout intentionally raises rather than guessing financial data.
    """
    pattern = re.compile(
        r"(?P<symbol>[A-Z][A-Z0-9]*)\s+(?P<side>BUY|SELL)\s+"
        r"(?P<quantity>[\d,]+)\s+(?P<price>[\d,]+(?:\.\d+)?)",
        re.IGNORECASE,
    )
    results: list[Trade] = []
    seen: set[str] = set()
    for match in pattern.finditer(text):
        instrument = ticker_from_symbol(match.group("symbol"))
        if instrument not in INSTRUMENTS:
            continue
        if instrument in seen:
            raise ConfirmationError(f"Multiple executions for {instrument}; review required")
        seen.add(instrument)
        quantity = int(match.group("quantity").replace(",", ""))
        if quantity <= 0:
            raise ConfirmationError(f"Invalid contract count for {instrument}")
        if match.group("side").upper() == "SELL":
            quantity *= -1
        results.append(Trade(instrument, quantity, parse_money(match.group("price"))))
    if not results:
        raise ConfirmationError("No supported execution rows found in confirmation")
    return tuple(results)


def parse_confirmation(text: str) -> Confirmation:
    return Confirmation(
        trade_date=extract_trade_date(text),
        trades=parse_trades(text),
        begin_equity=extract_balance(text, "Begin Equity Balance"),
        end_equity=extract_balance(text, "End Equity Balance"),
    )


def proposed_execution_cells(confirmation: Confirmation) -> dict[str, Decimal | int]:
    result: dict[str, Decimal | int] = {}
    for trade in confirmation.trades:
        result[EXECUTION_QUANTITY_COLUMNS[trade.instrument]] = trade.quantity
        result[EXECUTION_PRICE_COLUMNS[trade.instrument]] = trade.price
    return result


def equity_checks(confirmation: Confirmation, snapshot: EquitySnapshot | None) -> dict[str, str]:
    if snapshot is None:
        return {"begin": "PENDING: Equity!N values were not supplied", "end": "PENDING: Equity!N values were not supplied"}
    begin_delta = abs(snapshot.previous_calculated - confirmation.begin_equity - EQUITY_OFFSET)
    end_delta = abs(snapshot.current_calculated - confirmation.end_equity - EQUITY_OFFSET)
    return {
        "begin": "PASS" if begin_delta <= TOLERANCE else "FAIL",
        "end": "PASS" if end_delta <= TOLERANCE else "FAIL",
    }


def write_report(confirmation: Confirmation, checks: dict[str, str], report_path: Path, include_values: bool):
    status = "PASS" if all(value == "PASS" for value in checks.values()) else "REVIEW_REQUIRED"
    lines = [
        "# F1-TH execution-confirmation staging report",
        "",
        f"- Trade date: **{confirmation.trade_date.isoformat()}**",
        f"- Result: **{status}**",
        f"- Begin-equity check: **{checks['begin']}**",
        f"- End-equity check: **{checks['end']}**",
        "- Production sheets were not changed.",
    ]
    if include_values:
        lines.extend(["", "## Proposed Execution values", "", "| Instrument | Signed contracts | Price |", "| --- | ---: | ---: |"])
        for trade in confirmation.trades:
            lines.append(f"| {trade.instrument} | {trade.quantity} | {trade.price} |")
        lines.extend(["", f"- Proposed `Equity!O`: `{confirmation.end_equity}`"])
    report = "\n".join(lines) + "\n"
    report_path.write_text(report, encoding="utf-8")
    print(report)


def main(argv=None):
    args = parse_args(argv)
    password = args.pdf_password or os.getenv("F1_TH_PDF_PASSWORD")
    text = args.text_file.read_text(encoding="utf-8") if args.text_file else extract_pdf_text(args.pdf, password)
    confirmation = parse_confirmation(text)
    expected_date = args.trade_date or expected_trading_date()
    if confirmation.trade_date != expected_date:
        raise ConfirmationError(
            f"Confirmation trade date {confirmation.trade_date} does not match expected {expected_date}"
        )
    snapshot = None
    if args.equity_previous is not None or args.equity_current is not None:
        if args.equity_previous is None or args.equity_current is None:
            raise ConfirmationError("Provide both --equity-previous and --equity-current")
        snapshot = EquitySnapshot(args.equity_previous, args.equity_current)
    checks = equity_checks(confirmation, snapshot)
    write_report(confirmation, checks, args.report, args.include_values)
    return 0 if all(value == "PASS" for value in checks.values()) else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except ConfirmationError as exc:
        print(f"REVIEW_REQUIRED: {exc}", file=sys.stderr)
        sys.exit(2)
