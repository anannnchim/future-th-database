#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials

# --- Config (adjust paths/URLs as needed) ---
SVC_KEY_PATH = "/Users/nanthawat/Desktop/key/google/system-f1-th/automated-system-f1-th-key.json"
MARKET_INPUT_URL = "https://docs.google.com/spreadsheets/d/17SMA52gIOkjFan-0au_YJEAxoWIzoNA84qlmgoTsZ-s/edit#gid=1037340594"
MARKET_DATA_URL  = "https://docs.google.com/spreadsheets/d/19Rj7iW5xWOe6ZJJRsO9VzsZXyLfFu1S_vtClEE_3DEw/edit#gid=748449431"
HOLDING_SHEET_NAME = "holding_information"

SCOPES = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']

def auth_client():
    creds = ServiceAccountCredentials.from_json_keyfile_name(SVC_KEY_PATH, SCOPES)
    return gspread.authorize(creds)

def get_tickers(client):
    sh = client.open_by_url(MARKET_INPUT_URL)
    ws = sh.worksheet(HOLDING_SHEET_NAME)
    df = pd.DataFrame(ws.get_all_records())
    if df.empty:
        raise ValueError("holding_information sheet is empty")
    # accept either 'ticker' or 'current_symbol'
    cols = [c.strip().lower() for c in df.columns]
    df.columns = cols
    if "ticker" in df.columns:
        tickers = df["ticker"].astype(str)
    elif "current_symbol" in df.columns:
        # derive ticker by trimming last 3 chars (ABC25 -> ABC)
        tickers = df["current_symbol"].astype(str).apply(lambda s: s[:-3] if len(s) >= 3 else s)
    else:
        raise ValueError("holding_information must have 'ticker' or 'current_symbol'")
    tickers = tickers[tickers.str.len() > 0].drop_duplicates()
    return tickers.tolist()

def last_date_for_ticker(client, ticker):
    sh = client.open_by_url(MARKET_DATA_URL)
    ws = sh.worksheet(ticker)   # raises if missing -> caller will handle
    df = pd.DataFrame(ws.get_all_records())
    if df.empty or "date" not in df.columns:
        return None
    # keep non-empty dates
    df = df[df["date"].astype(str).str.len() > 0].copy()
    if df.empty:
        return None
    # parse YYYY-MM-DD
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d", errors="coerce")
    df = df.dropna(subset=["date"])
    if df.empty:
        return None
    return df["date"].max()

def main():
    client = auth_client()

    tickers = get_tickers(client)
    results = []
    for t in tickers:
        try:
            dt = last_date_for_ticker(client, t)
            results.append({"ticker": t, "last_date": dt, "error": None})
        except Exception as e:
            results.append({"ticker": t, "last_date": None, "error": str(e)})

    res_df = pd.DataFrame(results)

    # show per-ticker last date
    print("\n=== Latest date per ticker ===")
    for _, r in res_df.iterrows():
        ld = r["last_date"].strftime("%Y-%m-%d") if pd.notna(r["last_date"]) else "None"
        print(f"{r['ticker']}: {ld}" + (f"  [ERROR: {r['error']}]" if r['error'] else ""))

    # expected max date across tickers with data
    valid = res_df.dropna(subset=["last_date"])
    max_date = valid["last_date"].max() if not valid.empty else None
    print("\nExpected (max) date:", max_date.strftime("%Y-%m-%d") if max_date else "N/A")

    # find mismatches
    mismatches = []
    for _, r in res_df.iterrows():
        if r["error"]:
            mismatches.append((r["ticker"], "error", r["error"]))
        elif max_date is not None and r["last_date"] != max_date:
            mismatches.append((r["ticker"], "stale", f"last={r['last_date']}, expected={max_date}"))
        elif max_date is None and r["last_date"] is None:
            mismatches.append((r["ticker"], "unknown", "no data"))

    if mismatches:
        print("\n--- MISMATCHES / ERRORS ---")
        for t, status, detail in mismatches:
            last_str = next((ld.strftime("%Y-%m-%d") for ld in res_df.loc[res_df['ticker']==t, 'last_date'] if pd.notna(ld)), "None")
            print(f"{t}: [{status}] last_date={last_str} | {detail}")
        sys.exit(1)

    print("\n✅ All tickers match the latest date.")
    sys.exit(0)

if __name__ == "__main__":
    main()
