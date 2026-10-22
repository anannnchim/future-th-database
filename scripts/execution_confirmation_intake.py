#!/usr/bin/env python3
"""Stage F1-TH broker confirmations from Gmail without production writes."""

from __future__ import annotations

import argparse
import base64
import os
import sys
import tempfile
from dataclasses import dataclass
from datetime import date, datetime, time
from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

if __package__:
    from .check_gsheets_dates import AUTOMATED_F1_URL, auth_client
    from .execution_reconciliation import ConfirmationError, extract_pdf_text, parse_confirmation
    from .trading_calendar import BANGKOK, expected_trading_date
else:
    from check_gsheets_dates import AUTOMATED_F1_URL, auth_client
    from execution_reconciliation import ConfirmationError, extract_pdf_text, parse_confirmation
    from trading_calendar import BANGKOK, expected_trading_date


CONTROL_SHEET = "order-confirmation"
PENDING_STATUSES = {"WAITING_FOR_PDF", "MISSING_CONFIRMATION"}
GMAIL_SCOPES = ("https://www.googleapis.com/auth/gmail.readonly",)


class IntakeError(ValueError):
    """Raised when a confirmation cannot safely move to staging."""


@dataclass(frozen=True)
class ExpectedTrade:
    row: int
    trade_date: date
    note: str
    status: str


@dataclass(frozen=True)
class IntakeResult:
    row: int
    trade_date: date
    status: str
    message: str


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trade-date", type=date.fromisoformat, help="Trade date to process; defaults to latest completed TFEX date.")
    parser.add_argument("--gmail-token-file", type=Path, default=os.getenv("GMAIL_OAUTH_TOKEN_FILE"), help="OAuth authorized-user JSON file.")
    parser.add_argument("--pdf-password", help="PDF password. Prefer F1_TH_PDF_PASSWORD.")
    parser.add_argument(
        "--auto-discover",
        action="store_true",
        help="Treat a matching broker email as the trade signal and create the control row when needed.",
    )
    parser.add_argument("--missing-after", default="07:00", help="Bangkok time after which an absent PDF is flagged (HH:MM).")
    parser.add_argument("--report", type=Path, default=Path("execution_confirmation_intake_report.md"))
    return parser.parse_args(argv)


def parse_trade_date(value) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    for pattern in ("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(str(value).strip(), pattern).date()
        except ValueError:
            pass
    raise IntakeError(f"Unrecognised trade_date value: {value!r}")


def is_checked(value) -> bool:
    return value is True or str(value).strip().upper() == "TRUE"


def expected_rows(records: list[dict], trade_date: date) -> list[ExpectedTrade]:
    result = []
    for index, record in enumerate(records, start=2):
        if not is_checked(record.get("trade_expected")) or parse_trade_date(record.get("trade_date")) != trade_date:
            continue
        status = str(record.get("status") or "WAITING_FOR_PDF").strip().upper()
        if status in PENDING_STATUSES:
            result.append(ExpectedTrade(index, trade_date, str(record.get("note") or "").strip(), status))
    return result


def row_for_trade_date(records: list[dict], trade_date: date) -> tuple[int, dict] | None:
    for index, record in enumerate(records, start=2):
        value = record.get("trade_date")
        if not value:
            continue
        try:
            if parse_trade_date(value) == trade_date:
                return index, record
        except IntakeError:
            continue
    return None


def enable_auto_discovered_trade(worksheet, records: list[dict], trade_date: date) -> ExpectedTrade | None:
    """Create or enable one control row only after a broker PDF was found."""
    existing = row_for_trade_date(records, trade_date)
    if existing:
        row, record = existing
        status = str(record.get("status") or "WAITING_FOR_PDF").strip().upper()
        if status == "RECONCILED":
            return None
        if status not in PENDING_STATUSES:
            raise IntakeError(f"Existing control row is {status or 'not pending'}; review required before automatic retry")
        if not is_checked(record.get("trade_expected")):
            worksheet.update_acell(f"B{row}", "TRUE")
        if not str(record.get("note") or "").strip():
            worksheet.update_acell(f"C{row}", "Auto-detected broker confirmation")
        return ExpectedTrade(row, trade_date, str(record.get("note") or "").strip(), status)

    row = len(records) + 2
    worksheet.append_row([
        trade_date.isoformat(),
        True,
        "Auto-detected broker confirmation",
        "WAITING_FOR_PDF",
    ])
    return ExpectedTrade(row, trade_date, "Auto-detected broker confirmation", "WAITING_FOR_PDF")


def gmail_service(token_file: Path):
    if not token_file or not token_file.exists():
        raise IntakeError("Gmail OAuth token file is not available")
    credentials = Credentials.from_authorized_user_file(str(token_file), GMAIL_SCOPES)
    if not credentials.valid:
        if not credentials.refresh_token:
            raise IntakeError("Gmail OAuth token cannot be refreshed")
        credentials.refresh(Request())
    return build("gmail", "v1", credentials=credentials, cache_discovery=False)


def gmail_query(trade_date: date) -> str:
    return f'"Daily Derivatives Confirmation Note as of {trade_date.strftime("%d %B %Y")}" has:attachment'


def pdf_attachments(service, trade_date: date) -> list[tuple[str, bytes]]:
    messages = service.users().messages().list(userId="me", q=gmail_query(trade_date), maxResults=10).execute().get("messages", [])
    results = []
    for item in messages:
        message_id = item["id"]
        payload = service.users().messages().get(userId="me", id=message_id, format="full").execute().get("payload", {})
        for part in payload.get("parts", []):
            body = part.get("body", {})
            attachment_id = body.get("attachmentId")
            if attachment_id and part.get("filename", "").lower().endswith(".pdf"):
                data = service.users().messages().attachments().get(userId="me", messageId=message_id, id=attachment_id).execute()["data"]
                results.append((message_id, base64.urlsafe_b64decode(data)))
    return results


def parse_single_confirmation(service, trade_date: date, password: str):
    attachments = pdf_attachments(service, trade_date)
    if not attachments:
        return None
    if len(attachments) != 1:
        raise IntakeError("More than one broker PDF matched; review required")
    message_id, content = attachments[0]
    with tempfile.NamedTemporaryFile(suffix=".pdf") as temporary:
        temporary.write(content)
        temporary.flush()
        confirmation = parse_confirmation(extract_pdf_text(Path(temporary.name), password))
    if confirmation.trade_date != trade_date:
        raise IntakeError("PDF statement date does not match expected trade date")
    return message_id, confirmation


def past_missing_cutoff(value: str, now: datetime | None = None) -> bool:
    try:
        cutoff = time.fromisoformat(value)
    except ValueError as exc:
        raise IntakeError("--missing-after must use HH:MM") from exc
    return (now or datetime.now(BANGKOK)).timetz().replace(tzinfo=None) >= cutoff


def write_report(results: list[IntakeResult], report_path: Path):
    safe_no_action = {"NO_EXPECTED_TRADE", "NO_BROKER_CONFIRMATION", "ALREADY_RECONCILED"}
    outcome = "PASS" if results and all(result.status == "STAGED" or result.status in safe_no_action for result in results) else "REVIEW_REQUIRED"
    lines = ["# F1-TH execution-confirmation intake report", "", f"- Checked: **{datetime.now(BANGKOK).isoformat(timespec='seconds')}**", f"- Result: **{outcome}**", "- Production Execution and Equity sheets were not changed.", "", "| Trade date | Control-row status | Detail |", "| --- | --- | --- |"]
    lines.extend(f"| {result.trade_date.isoformat()} | {result.status} | {result.message} |" for result in results)
    report = "\n".join(lines) + "\n"
    report_path.write_text(report, encoding="utf-8")
    if summary_path := os.getenv("GITHUB_STEP_SUMMARY"):
        with open(summary_path, "a", encoding="utf-8") as summary:
            summary.write(report)
    print(report)


def set_workflow_outputs(results: list[IntakeResult]):
    """Expose whether this run staged a new confirmation to GitHub Actions."""
    output_path = os.getenv("GITHUB_OUTPUT")
    if not output_path:
        return
    staged = any(result.status == "STAGED" for result in results)
    with open(output_path, "a", encoding="utf-8") as output:
        output.write(f"staged={'true' if staged else 'false'}\n")


def main(argv=None):
    args = parse_args(argv)
    trade_date = args.trade_date or expected_trading_date()
    password = args.pdf_password or os.getenv("F1_TH_PDF_PASSWORD")
    if not password:
        raise IntakeError("F1_TH_PDF_PASSWORD is not set")
    worksheet = auth_client().open_by_url(AUTOMATED_F1_URL).worksheet(CONTROL_SHEET)
    records = worksheet.get_all_records()
    rows = expected_rows(records, trade_date)
    service = None
    discovered = None
    if not rows and args.auto_discover:
        service = gmail_service(args.gmail_token_file)
        discovered = parse_single_confirmation(service, trade_date, password)
        if discovered is None:
            results = [IntakeResult(0, trade_date, "NO_BROKER_CONFIRMATION", "No matching broker PDF; no action taken")]
            write_report(results, args.report)
            set_workflow_outputs(results)
            return 0
        discovered_row = enable_auto_discovered_trade(worksheet, records, trade_date)
        if discovered_row is None:
            results = [IntakeResult(0, trade_date, "ALREADY_RECONCILED", "Matching control row is already reconciled")]
            write_report(results, args.report)
            set_workflow_outputs(results)
            return 0
        rows = [discovered_row]
    if not rows:
        results = [IntakeResult(0, trade_date, "NO_EXPECTED_TRADE", "No checked pending control row")]
        write_report(results, args.report)
        set_workflow_outputs(results)
        return 0
    service = service or gmail_service(args.gmail_token_file)
    results = []
    for row in rows:
        try:
            parsed = discovered or parse_single_confirmation(service, row.trade_date, password)
            if parsed is None:
                if past_missing_cutoff(args.missing_after):
                    worksheet.update_acell(f"D{row.row}", "MISSING_CONFIRMATION")
                    results.append(IntakeResult(row.row, row.trade_date, "MISSING_CONFIRMATION", "No matching PDF after cutoff"))
                else:
                    results.append(IntakeResult(row.row, row.trade_date, "WAITING_FOR_PDF", "No matching PDF yet"))
                continue
            _, confirmation = parsed
            worksheet.update_acell(f"D{row.row}", "STAGED")
            results.append(IntakeResult(row.row, row.trade_date, "STAGED", f"Parsed {len(confirmation.trades)} supported instrument(s)"))
        except (ConfirmationError, IntakeError) as exc:
            worksheet.update_acell(f"D{row.row}", "REVIEW_REQUIRED")
            results.append(IntakeResult(row.row, row.trade_date, "REVIEW_REQUIRED", str(exc)))
    write_report(results, args.report)
    set_workflow_outputs(results)
    return 0 if all(result.status in {"STAGED", "WAITING_FOR_PDF"} for result in results) else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except IntakeError as exc:
        print(f"REVIEW_REQUIRED: {exc}", file=sys.stderr)
        sys.exit(2)
