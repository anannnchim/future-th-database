#!/usr/bin/env python3
"""Update System F1-TH continuous futures series in Google Sheets.

Use ``--dry-run`` to read, scrape, calculate, and validate without writing a
worksheet. The GitHub Actions workflow invokes the default write mode.
"""

import argparse
import os
import sys
import time

import gspread
import pandas as pd
from google.oauth2 import service_account
from gspread_dataframe import set_with_dataframe
from selenium import webdriver
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

SCOPES = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
MARKET_INPUT_URL = "https://docs.google.com/spreadsheets/d/17SMA52gIOkjFan-0au_YJEAxoWIzoNA84qlmgoTsZ-s/edit?gid=1037340594#gid=1037340594"
MARKET_DATA_URL = "https://docs.google.com/spreadsheets/d/19Rj7iW5xWOe6ZJJRsO9VzsZXyLfFu1S_vtClEE_3DEw/edit?gid=748449431#gid=748449431"
DATA_COLUMNS = ["date", "open", "high", "low", "close", "sp", "vol", "oi", "symbol", "adj_price"]
TFEX_COLUMNS = ["Date", "Open", "High", "Low", "Close", "SP", "Chg", "%Chg", "Vol (Contract)", "OI (Contract)"]


def scrape_from_tfex(symbol):
    """Fetch the dynamically rendered TFEX historical-trading table."""
    url = f"https://www.tfex.co.th/en/products/currency/eur-usd-futures/{symbol}/historical-trading"
    xpath = '//*[@id="__layout"]/div/div[2]/div[2]/div[2]/div/div[3]'
    options = webdriver.ChromeOptions()
    for argument in ("--headless", "--no-sandbox", "--disable-dev-shm-usage"):
        options.add_argument(argument)
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    try:
        driver.get(url)
        table = WebDriverWait(driver, 60).until(EC.visibility_of_element_located((By.XPATH, xpath)))
        for _ in range(20):
            try:
                rows = [
                    [cell.text for cell in row.find_elements(By.TAG_NAME, "td")]
                    for row in table.find_elements(By.TAG_NAME, "tr")
                    if row.find_elements(By.TAG_NAME, "td")
                ]
                if not rows:
                    return pd.DataFrame()
                frame = pd.DataFrame(rows, columns=TFEX_COLUMNS)
                frame["Symbol"] = symbol
                return frame
            except StaleElementReferenceException:
                driver.refresh()
                table = WebDriverWait(driver, 60).until(EC.visibility_of_element_located((By.XPATH, xpath)))
    except TimeoutException:
        print(f"{symbol}: TFEX table did not load within 60 seconds")
    finally:
        driver.quit()
    return pd.DataFrame()


def prep_df(raw_df):
    """Normalize a TFEX table into the F1-TH source schema."""
    if raw_df is None or raw_df.empty:
        return pd.DataFrame(columns=DATA_COLUMNS[:-1])
    frame = raw_df.copy().replace("-", pd.NA)
    frame["Date"] = pd.to_datetime(frame["Date"], format="%d %b %Y", errors="coerce")
    for column in ("Open", "High", "Low", "Close", "SP", "Vol (Contract)", "OI (Contract)"):
        frame[column] = pd.to_numeric(frame[column].astype(str).str.replace(",", ""), errors="coerce")
    frame = frame.rename(columns={
        "Date": "date", "Open": "open", "High": "high", "Low": "low", "Close": "close",
        "SP": "sp", "Vol (Contract)": "vol", "OI (Contract)": "oi", "Symbol": "symbol",
    }).drop(columns=["Chg", "%Chg"], errors="ignore")
    return frame.dropna(subset=["date", "sp"]).sort_values("date").reindex(columns=DATA_COLUMNS[:-1])


def scrape_prepared(symbol, attempts=3):
    """Retry a full page load when TFEX returns no usable data."""
    last_error = None
    for attempt in range(1, attempts + 1):
        try:
            frame = prep_df(scrape_from_tfex(symbol))
            if not frame.empty:
                return frame
            last_error = RuntimeError("dynamic table returned no usable rows")
        except Exception as exc:
            last_error = exc
        if attempt < attempts:
            delay = attempt * 5
            print(f"{symbol}: scrape attempt {attempt}/{attempts} failed ({last_error}); retrying in {delay}s")
            time.sleep(delay)
    raise RuntimeError(f"scrape failed after {attempts} attempts for {symbol}: {last_error}")


def authenticated_client():
    key_file = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not key_file:
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS is not set")
    credentials = service_account.Credentials.from_service_account_file(key_file, scopes=SCOPES)
    return gspread.authorize(credentials)


def load_existing(worksheet):
    frame = pd.DataFrame(worksheet.get_all_records())
    if frame.empty:
        return pd.DataFrame(columns=DATA_COLUMNS)
    if "date" not in frame.columns or "symbol" not in frame.columns:
        raise ValueError(f"{worksheet.title} is missing required date/symbol columns")
    frame["date"] = pd.to_datetime(frame["date"], format="%Y-%m-%d", errors="coerce")
    return frame.dropna(subset=["date"]).copy().reindex(columns=DATA_COLUMNS)


def numeric_values(series):
    """Parse sheet numbers that may use thousands separators."""
    return pd.to_numeric(series.astype("string").str.replace(",", "", regex=False), errors="coerce")


def validate_series(frame, ticker, allow_legacy_blank_symbols=False, allow_legacy_missing_sp=False):
    """Reject corrupt data while permitting explicitly acknowledged legacy gaps."""
    if frame.empty:
        raise ValueError(f"{ticker} has no rows to write")
    missing = set(DATA_COLUMNS) - set(frame.columns)
    if missing:
        raise ValueError(f"{ticker} is missing columns: {sorted(missing)}")
    if frame["date"].isna().any() or frame["date"].duplicated().any():
        raise ValueError(f"{ticker} has blank or duplicate dates")
    if not frame["date"].is_monotonic_increasing:
        raise ValueError(f"{ticker} dates are not ascending")
    blank_symbols = frame["symbol"].isna() | frame["symbol"].astype(str).str.strip().eq("")
    if blank_symbols.any() and not allow_legacy_blank_symbols:
        raise ValueError(f"{ticker} has blank contract symbols")
    sp_values = numeric_values(frame["sp"])
    adj_price_values = numeric_values(frame["adj_price"])
    invalid_sp = sp_values.isna()
    if invalid_sp.any() and (not allow_legacy_missing_sp or adj_price_values[invalid_sp].isna().any()):
        raise ValueError(f"{ticker} has invalid sp values")
    if adj_price_values.isna().any():
        raise ValueError(f"{ticker} has invalid adj_price values")


def write_and_verify(worksheet, frame, expected_latest_date, allow_legacy_blank_symbols=False, allow_legacy_missing_sp=False):
    validate_series(frame, worksheet.title, allow_legacy_blank_symbols, allow_legacy_missing_sp)
    output = frame.reindex(columns=DATA_COLUMNS).copy()
    output["date"] = output["date"].dt.strftime("%Y-%m-%d")
    set_with_dataframe(worksheet, output.where(pd.notna(output), ""), include_index=False, include_column_header=True, resize=True)
    readback = load_existing(worksheet)
    actual_dates, expected_dates = set(readback["date"]), set(frame["date"])
    actual_latest = readback["date"].max() if not readback.empty else None
    if actual_latest != expected_latest_date or actual_dates != expected_dates:
        raise RuntimeError(
            f"{worksheet.title} verification failed: expected {len(expected_dates)} dates through "
            f"{expected_latest_date:%Y-%m-%d}, got {len(actual_dates)} dates through {actual_latest}"
        )


def update_symbol(market_data_sheet, symbol, dry_run=False):
    ticker = symbol[:-3]
    worksheet = market_data_sheet.worksheet(ticker)
    previous = load_existing(worksheet)
    if previous.empty:
        raise ValueError(f"{ticker} has no existing continuous-series data")
    legacy_blank_symbols = previous["symbol"].isna() | previous["symbol"].astype(str).str.strip().eq("")
    legacy_missing_sp = numeric_values(previous["sp"]).isna()
    validate_series(previous, ticker, allow_legacy_blank_symbols=True, allow_legacy_missing_sp=True)
    if legacy_blank_symbols.any():
        print(f"{symbol}: preserving {legacy_blank_symbols.sum()} legacy rows with blank contract symbols")
    if legacy_missing_sp.any():
        print(f"{symbol}: preserving {legacy_missing_sp.sum()} legacy rows with settlement-price gaps")
    scraped = scrape_prepared(symbol)
    if scraped["symbol"].isna().any() or scraped["symbol"].astype(str).str.strip().eq("").any():
        raise ValueError(f"{symbol}: TFEX returned blank contract symbols")
    stored_latest, scraped_latest = previous["date"].max(), scraped["date"].max()
    prior_symbols = previous["symbol"].astype("string").str.strip()
    prior_symbols = prior_symbols[prior_symbols.notna() & prior_symbols.ne("")]
    if prior_symbols.empty:
        raise ValueError(f"{ticker} has no usable historical contract symbols")
    last_symbol = prior_symbols.iloc[-1]
    print(f"{symbol}: stored={stored_latest:%Y-%m-%d}, source={scraped_latest:%Y-%m-%d}")

    if last_symbol == symbol:
        if scraped_latest < stored_latest:
            print(f"{symbol}: source is older than the sheet; preserving sheet data")
            return
        start = previous.loc[previous["symbol"] == symbol, "date"].min()
        replacement = scraped[scraped["date"] >= start].copy()
        replacement["adj_price"] = replacement["sp"]
        keep = previous[~((previous["symbol"] == symbol) & previous["date"].isin(set(replacement["date"])))].copy()
        updated = pd.concat([keep, replacement], ignore_index=True)
    else:
        new_rows = scraped[scraped["date"] > stored_latest].copy()
        if new_rows.empty:
            print(f"{symbol}: waiting for the first completed row after contract roll")
            return
        first_new_date = new_rows["date"].min()
        old_contract = scrape_prepared(last_symbol)
        old_row = old_contract[old_contract["date"] == first_new_date]
        if old_row.empty:
            raise RuntimeError(f"cannot back-adjust {symbol}: {last_symbol} has no row for {first_new_date:%Y-%m-%d}")
        adjustment = new_rows.loc[new_rows["date"] == first_new_date, "sp"].iloc[-1] - old_row["sp"].iloc[-1]
        previous["adj_price"] = numeric_values(previous["adj_price"]) + adjustment
        new_rows["adj_price"] = new_rows["sp"]
        updated = pd.concat([previous, new_rows], ignore_index=True)

    updated = updated.drop_duplicates(subset=["date"], keep="last").sort_values("date").reset_index(drop=True)
    validate_series(updated, ticker, allow_legacy_blank_symbols=True, allow_legacy_missing_sp=True)
    if dry_run:
        added = len(set(updated["date"]) - set(previous["date"]))
        print(f"{symbol}: DRY RUN validated {len(updated)} rows ({added} new); no sheet changes made")
        return
    write_and_verify(
        worksheet,
        updated,
        max(stored_latest, scraped_latest),
        allow_legacy_blank_symbols=True,
        allow_legacy_missing_sp=True,
    )
    print(f"{symbol}: sheet updated and verified")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Read and validate live inputs without writing Google Sheets.")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    client = authenticated_client()
    holding = pd.DataFrame(client.open_by_url(MARKET_INPUT_URL).worksheet("holding_information").get_all_records())
    if holding.empty or "current_symbol" not in holding:
        raise ValueError("holding_information has no current_symbol values")
    data_sheet = client.open_by_url(MARKET_DATA_URL)
    failures = []
    for symbol in holding["current_symbol"].dropna().astype(str):
        try:
            update_symbol(data_sheet, symbol.strip(), dry_run=args.dry_run)
        except Exception as exc:
            failures.append(f"{symbol}: {exc}")
            print(f"ERROR: {failures[-1]}")
    if failures:
        print("\nUpdate failed for one or more symbols:\n- " + "\n- ".join(failures))
        return 1
    print(f"All symbols {'validated without writes' if args.dry_run else 'updated and verified'} successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
