#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import gspread
import pandas as pd
from google.oauth2 import service_account

def open_by_url(client, url):
    # gspread can open by full edit URL; it will ignore the fragment
    return client.open_by_url(url)

def read_holding_table(client, market_input_url, holding_sheet_name):
    sh = open_by_url(client, market_input_url)
    ws = sh.worksheet(holding_sheet_name)
    df = pd.DataFrame(ws.get_all_records())
    # Expect columns: ticker, current_symbol, link
    required = {"ticker", "current_symbol"}
    missing = required - set(df.columns.str.lower())
    if missing:
        raise ValueError(f"holding_information missing columns: {missing}")
    # Normalize columns just in case
    df.columns = [c.lower() for c in df.columns]
    return df

def get_last_date_for_ticker(client, market_data_url, ticker):
    sh = open_by_url(client, market_data_url)
    ws = sh.worksheet(ticker)
    df = pd.DataFrame(ws.get_all_records())
    if "date" not in df.columns:
        raise ValueError(f"Sheet '{ticker}' has no 'date' column")
    # Filter out empty dates and parse
    df = df[df["date"].astype(str).str.len() > 0].copy()
    if df.empty:
        return None
    # Parse as YYYY-MM-DD
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d", errors="coerce")
    df = df.dropna(subset=["date"])
    if df.empty:
        return None
    return df["date"].max()

def main():
    # --- ENV ---
    service_account_file = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    market_input_url = os.getenv("MARKET_INPUT_URL")
    market_data_url = os.getenv("MARKET_DATA_URL")
    holding_sheet_name = os.getenv("HOLDING_SHEET_NAME", "holding_information")

    if not service_account_file or not os.path.exists(service_account_file):
        print("Missing or invalid GOOGLE_APPLICATION_CREDENTIALS file path.", file=sys.stderr)
        sys.exit(2)

    if not market_input_url or not market_data_url:
        print("MARKET_INPUT_URL and MARKET_DATA_URL must be set.", file=sys.stderr)
        sys.exit(2)

    scopes = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    creds = service_account.Credentials.from_service_account_file(service_account_file, scopes=scopes)
    client = gspread.authorize(creds)

    # 1) read list of tickers from holding_information
    hold_df = read_holding_table(client, market_input_url, holding_sheet_name)

    # Expect column name 'ticker'
    tickers = hold_df["ticker"].astype(str).tolist()

    results = []
    for t in tickers:
        try:
            last_dt = get_last_date_for_ticker(client, market_data_url, t)
            results.append({"ticker": t, "last_date": last_dt})
        except Exception as e:
            results.append({"ticker": t, "last_date": None, "error": str(e)})

    res_df = pd.DataFrame(results)

    # 2) find the max date across all tickers (ignoring None)
    valid_dates = res_df.dropna(subset=["last_date"])
    max_date = valid_dates["last_date"].max() if not valid_dates.empty else None

    # 3) determine mismatches: anything not equal to max_date or failed to read
    mismatches = []
    for _, row in res_df.iterrows():
        ticker = row["ticker"]
        last_date = row["last_date"]
        err = row.get("error")

        if err or (max_date is not None and last_date != max_date) or (max_date is None and last_date is None):
            mismatches.append({
                "ticker": ticker,
                "last_date": None if pd.isna(last_date) else last_date.strftime("%Y-%m-%d"),
                "status": "error" if err else ("stale" if last_date != max_date else "unknown"),
                "detail": err or (f"latest={last_date}, expected={max_date}" if last_date is not None else "no data")
            })

    # 4) write a report file for artifacts / email
    with open("mismatch_report.md", "w", encoding="utf-8") as f:
        f.write("# Google Sheet Date Consistency Report\n\n")
        f.write(f"- Max (expected) date across all tickers: **{max_date.strftime('%Y-%m-%d') if max_date else 'N/A'}**\n\n")
        f.write("## Latest date per ticker\n\n")
        if not res_df.empty:
            for _, r in res_df.iterrows():
                ld = r["last_date"].strftime("%Y-%m-%d") if pd.notna(r["last_date"]) else "None"
                f.write(f"- `{r['ticker']}` → `{ld}`\n")
        else:
            f.write("- (no tickers found)\n")

        if mismatches:
            f.write("\n## Mismatches / Errors\n\n")
            for m in mismatches:
                f.write(f"- `{m['ticker']}` → **{m['status']}** | last_date=`{m['last_date']}` | {m['detail']}\n")
        else:
            f.write("\n✅ All tickers match the latest date.\n")

    # 5) exit non‑zero if mismatch so the job fails (and email step triggers)
    if mismatches:
        print("Mismatch detected. See mismatch_report.md")
        sys.exit(1)

    print("All good. Dates are consistent.")
    sys.exit(0)

if __name__ == "__main__":
    main()
