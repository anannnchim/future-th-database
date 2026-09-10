#!/usr/bin/env python3
"""Validate market-data and the downstream Automated F1 workbook."""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
from datetime import date, datetime
from pathlib import Path

import gspread
import pandas as pd
from google.oauth2 import service_account

from trading_calendar import BANGKOK, expected_trading_date


MARKET_INPUT_URL = os.getenv(
    "MARKET_INPUT_URL",
    "https://docs.google.com/spreadsheets/d/17SMA52gIOkjFan-0au_YJEAxoWIzoNA84qlmgoTsZ-s/edit",
)
MARKET_DATA_URL = os.getenv(
    "MARKET_DATA_URL",
    "https://docs.google.com/spreadsheets/d/19Rj7iW5xWOe6ZJJRsO9VzsZXyLfFu1S_vtClEE_3DEw/edit",
)
AUTOMATED_F1_URL = os.getenv(
    "AUTOMATED_F1_URL",
    "https://docs.google.com/spreadsheets/d/17MCh8REdbM1F9J1MOSt_AxWCPZwpntowubivCWTRv94/edit",
)
HOLDING_SHEET_NAME = "holding_information"
REPORT_PATH = Path("mismatch_report.md")
SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--wait-seconds", type=int, default=0)
    parser.add_argument("--poll-seconds", type=int, default=60)
    parser.add_argument("--expected-date", type=date.fromisoformat)
    return parser.parse_args()


def auth_client():
    credentials_file = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not credentials_file:
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS is not set")
    credentials = service_account.Credentials.from_service_account_file(
        credentials_file,
        scopes=SCOPES,
    )
    return gspread.authorize(credentials)


def get_tickers(client):
    worksheet = client.open_by_url(MARKET_INPUT_URL).worksheet(HOLDING_SHEET_NAME)
    frame = pd.DataFrame(worksheet.get_all_records())
    if frame.empty:
        raise ValueError("holding_information is empty")

    frame.columns = [str(column).strip().lower() for column in frame.columns]
    if "ticker" in frame.columns:
        tickers = frame["ticker"].astype(str).str.strip()
    elif "current_symbol" in frame.columns:
        symbols = frame["current_symbol"].astype(str).str.strip()
        tickers = symbols.apply(
            lambda value: re.sub(r"[FGHJKMNQUVXZ]\\d{2}$", "", value.upper())
        )
    else:
        raise ValueError("holding_information needs ticker or current_symbol")

    result = [ticker for ticker in tickers if ticker]
    if not result:
        raise ValueError("holding_information has no active tickers")
    return list(dict.fromkeys(result))


def parse_sheet_date(value):
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date()


def market_latest_dates(workbook, tickers):
    results = {}
    for ticker in tickers:
        values = workbook.worksheet(ticker).col_values(1)
        dates = [parse_sheet_date(value) for value in values[1:] if value]
        dates = [value for value in dates if value is not None]
        results[ticker] = max(dates) if dates else None
    return results


def automated_latest_dates(workbook, tickers):
    return {
        ticker: parse_sheet_date(workbook.worksheet(ticker).acell("C5").value)
        for ticker in tickers
    }


def is_current(value, expected):
    return value is not None and value >= expected


def status_rows(market_dates, automated_dates, monitoring_date, expected):
    rows = []
    for ticker in market_dates:
        market_date = market_dates[ticker]
        automated_date = automated_dates.get(ticker)
        current = is_current(market_date, expected) and is_current(
            automated_date, expected
        )
        rows.append(
            {
                "ticker": ticker,
                "market": market_date,
                "automated": automated_date,
                "status": "PASS" if current else "FAIL",
            }
        )
    monitoring_ok = is_current(monitoring_date, expected)
    return rows, monitoring_ok


def fmt(value):
    return value.isoformat() if value else "missing"


def write_report(rows, monitoring_date, monitoring_ok, expected, message):
    lines = [
        "# F1-TH freshness report",
        "",
        f"- Checked: {datetime.now(BANGKOK).isoformat(timespec='seconds')}",
        f"- Expected trading date: **{expected.isoformat()}**",
        f"- Result: **{message}**",
        "",
        "| Instrument | Market data | Automated F1 | Status |",
        "| --- | --- | --- | --- |",
    ]
    for row in rows:
        lines.append(
            f"| {row['ticker']} | {fmt(row['market'])} | "
            f"{fmt(row['automated'])} | {row['status']} |"
        )
    lines.extend(
        [
            "",
            f"- B-Monitoring last updated: **{fmt(monitoring_date)}** "
            f"({'PASS' if monitoring_ok else 'FAIL'})",
            "",
        ]
    )
    report = "\n".join(lines)
    REPORT_PATH.write_text(report)
    summary_path = os.getenv("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as summary:
            summary.write(report)
    print(report)


def main():
    args = parse_args()
    expected = args.expected_date or expected_trading_date()
    client = auth_client()
    tickers = get_tickers(client)
    market_workbook = client.open_by_url(MARKET_DATA_URL)
    automated_workbook = client.open_by_url(AUTOMATED_F1_URL)

    market_dates = market_latest_dates(market_workbook, tickers)
    stale_market = {
        ticker: value
        for ticker, value in market_dates.items()
        if not is_current(value, expected)
    }
    if stale_market:
        automated_dates = automated_latest_dates(automated_workbook, tickers)
        monitoring_date = parse_sheet_date(
            automated_workbook.worksheet("B-Monitoring").acell("C6").value
        )
        rows, monitoring_ok = status_rows(
            market_dates, automated_dates, monitoring_date, expected
        )
        write_report(
            rows,
            monitoring_date,
            monitoring_ok,
            expected,
            "FAIL: market-data is stale",
        )
        return 1

    deadline = time.monotonic() + max(0, args.wait_seconds)
    while True:
        automated_dates = automated_latest_dates(automated_workbook, tickers)
        monitoring_date = parse_sheet_date(
            automated_workbook.worksheet("B-Monitoring").acell("C6").value
        )
        rows, monitoring_ok = status_rows(
            market_dates, automated_dates, monitoring_date, expected
        )
        downstream_ok = all(row["status"] == "PASS" for row in rows) and monitoring_ok
        if downstream_ok:
            write_report(rows, monitoring_date, True, expected, "PASS")
            return 0

        if time.monotonic() >= deadline:
            write_report(
                rows,
                monitoring_date,
                monitoring_ok,
                expected,
                "FAIL: Automated F1 did not refresh before timeout",
            )
            return 1

        print("Automated F1 is still recalculating; checking again...")
        time.sleep(max(1, args.poll_seconds))


if __name__ == "__main__":
    sys.exit(main())
