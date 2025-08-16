#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import json
import gspread
import pandas as pd
from google.oauth2 import service_account
from tempfile import NamedTemporaryFile

def build_creds():
    """
    Builds google oauth creds from either:
    - GOOGLE_APPLICATION_CREDENTIALS: path to JSON file
    - SERVICE_ACCOUNT_JSON: raw JSON string (useful in CI)
    """
    scopes = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']

    file_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    raw_json  = os.getenv("SERVICE_ACCOUNT_JSON")

    if file_path and os.path.exists(file_path):
        return service_account.Credentials.from_service_account_file(file_path, scopes=scopes)

    if raw_json:
        try:
            data = json.loads(raw_json)
        except json.JSONDecodeError:
            print("SERVICE_ACCOUNT_JSON is not valid JSON.", file=sys.stderr)
            sys.exit(2)
        # write to a temp file (gspread wants a file path / file-like)
        with NamedTemporaryFile(mode="w", delete=False, suffix=".json") as tmp:
            json.dump(data, tmp)
            tmp.flush()
            return service_account.Credentials.from_service_account_file(tmp.name, scopes=scopes)

    print("Missing or invalid GOOGLE_APPLICATION_CREDENTIALS path and no SERVICE_ACCOUNT_JSON provided.", file=sys.stderr)
    sys.exit(2)

def open_by_url(client, url):
    return client.open_by_url(url)

def read_holding_table(client, market_input_url, holding_sheet_name):
    sh = open_by_url(client, market_input_url)
    ws = sh.worksheet(holding_sheet_name)
    df = pd.DataFrame(ws.get_all_records())
    if df.empty:
        raise ValueError("holding_information sheet is empty")
    df.columns = [c.strip().lower() for c in df.columns]

    # Accept either 'ticker' or 'current_symbol'
    if "ticker" in df.columns:
        tickers = df["ticker"].astype(str)
    elif "current_symbol" in df.columns:
        # Derive ticker by trimming last 3 chars (e.g., ABC25 -> ABC)
        tickers = df["current_symbol"].astype(str).apply(lambda s: s[:-3] if len(s) >= 3 else s)
    else:
        raise ValueError("holding_information must contain 'ticker' or 'current_symbol'")

    out = pd.DataFrame({"ticker": tickers})
    out = out[out["ticker"].astype(str).str.len() > 0].drop_duplicates().reset_index(drop=True)
    return out

def get_last_date_for_ticker(client, market_data_url, ticker):
    sh = open_by_url(client, market_data_url)
    ws = sh.worksheet(ticker)
    df = pd.DataFrame(ws.get_all_records())
    if "date" not in df.columns or df.empty:
        return None
    df = df[df["date"].astype(str).str.len() > 0].copy()
    if df.empty:
        return None
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d", errors="coerce")
    df = df.dropna(subset=["date"])
    if df.empty:
        return None
    return df["date"].max()

def main():
    market_input_url = os.getenv("MARKET_INPUT_URL")
    market_data_url = os.getenv("MARKET_DATA_URL")
    holding_sheet_name = os.getenv("HOLDING_SHEET_NAME", "holding_information")

    if not market_input_url or not market_data_url:
        print("MARKET_INPUT_URL and MARKET_DATA_URL must be set.", file=sys.stderr)
        sys.exit(2)

    creds = build_creds()
    client = gspread.authorize(creds)

    hold_df = read_holding_table(client, market_input_url, holding_sheet_name)
    tickers = hold_df["ticker"].astype(str).tolist()

    results = []
    for t in tickers:
        try:
            last_dt = get_last_date_for_ticker(client, market_data_url, t)
            results.append({"ticker": t, "last_date": last_dt})
        except Exception as e:
            results.append({"ticker": t, "last_date": None, "error": str(e)})

    res_df = pd.DataFrame(results)

    # Expected (max) date across all tickers with data
    valid_dates = res_df.dropna(subset=["last_date"])
    max_date = valid_dates["last_date"].max() if not valid_dates.empty else None

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

    if mismatches:
        print("Mismatch detected. See mismatch_report.md")
        sys.exit(1)

    print("All good. Dates are consistent.")
    sys.exit(0)

if __name__ == "__main__":
    main()
