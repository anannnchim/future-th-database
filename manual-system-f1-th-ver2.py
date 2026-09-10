#!/usr/bin/env python3
"""Update the System F1-TH continuous futures database."""

from __future__ import annotations

import os
import re
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

from scripts.trading_calendar import expected_trading_date


SCOPE = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
MARKET_INPUT_URL = os.getenv(
    "MARKET_INPUT_URL",
    "https://docs.google.com/spreadsheets/d/17SMA52gIOkjFan-0au_YJEAxoWIzoNA84qlmgoTsZ-s/edit",
)
MARKET_DATA_URL = os.getenv(
    "MARKET_DATA_URL",
    "https://docs.google.com/spreadsheets/d/19Rj7iW5xWOe6ZJJRsO9VzsZXyLfFu1S_vtClEE_3DEw/edit",
)
DATA_COLUMNS = [
    "date",
    "open",
    "high",
    "low",
    "close",
    "sp",
    "vol",
    "oi",
    "symbol",
    "adj_price",
]


def ticker_from_symbol(symbol):
    ticker = re.sub(r"[FGHJKMNQUVXZ]\d{2}$", "", symbol.strip().upper())
    if not ticker or ticker == symbol.strip().upper():
        raise ValueError(f"Cannot derive ticker from contract symbol {symbol!r}")
    return ticker


def scrape_from_tfex(symbol):
    url = (
        "https://www.tfex.co.th/en/products/currency/eur-usd-futures/"
        f"{symbol}/historical-trading"
    )
    xpath = '//*[@id="__layout"]/div/div[2]/div[2]/div[2]/div/div[3]'

    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options,
    )
    data = []
    try:
        driver.get(url)
        wait = WebDriverWait(driver, 60)
        table = wait.until(EC.visibility_of_element_located((By.XPATH, xpath)))
        for attempt in range(1, 21):
            try:
                for row in table.find_elements(By.TAG_NAME, "tr"):
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if cells:
                        data.append([cell.text for cell in cells])
                break
            except StaleElementReferenceException:
                if attempt == 20:
                    raise
                driver.refresh()
                table = wait.until(
                    EC.visibility_of_element_located((By.XPATH, xpath))
                )
    except TimeoutException:
        return pd.DataFrame()
    finally:
        driver.quit()

    if not data:
        return pd.DataFrame()
    frame = pd.DataFrame(
        data,
        columns=[
            "Date",
            "Open",
            "High",
            "Low",
            "Close",
            "SP",
            "Chg",
            "%Chg",
            "Vol (Contract)",
            "OI (Contract)",
        ],
    )
    frame["Symbol"] = symbol
    return frame


def prep_df(raw_df):
    if raw_df is None or raw_df.empty:
        return pd.DataFrame()

    frame = raw_df.copy().replace("-", pd.NA)
    frame["Date"] = pd.to_datetime(
        frame["Date"], format="%d %b %Y", errors="coerce"
    )
    numeric_columns = [
        "Open",
        "High",
        "Low",
        "Close",
        "SP",
        "Vol (Contract)",
        "OI (Contract)",
    ]
    for column in numeric_columns:
        frame[column] = pd.to_numeric(
            frame[column].astype(str).str.replace(",", ""),
            errors="coerce",
        )

    frame = frame.rename(
        columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "SP": "sp",
            "Vol (Contract)": "vol",
            "OI (Contract)": "oi",
            "Symbol": "symbol",
        }
    ).drop(columns=["Chg", "%Chg"], errors="ignore")
    frame = frame.dropna(subset=["date", "sp"]).sort_values("date")
    return frame.reindex(
        columns=["date", "open", "high", "low", "close", "sp", "vol", "oi", "symbol"]
    )


def scrape_prepared(symbol, attempts=3):
    last_error = None
    for attempt in range(1, attempts + 1):
        try:
            frame = prep_df(scrape_from_tfex(symbol))
            if not frame.empty:
                return frame
            last_error = RuntimeError("TFEX returned no usable rows")
        except Exception as exc:
            last_error = exc

        if attempt < attempts:
            delay = attempt * 5
            print(
                f"{symbol}: scrape attempt {attempt}/{attempts} failed "
                f"({last_error}); retrying in {delay}s"
            )
            time.sleep(delay)
    raise RuntimeError(
        f"scrape failed after {attempts} attempts for {symbol}: {last_error}"
    )


def authenticated_client():
    credentials_file = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not credentials_file:
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS is not set")
    credentials = service_account.Credentials.from_service_account_file(
        credentials_file,
        scopes=SCOPE,
    )
    return gspread.authorize(credentials)


def load_existing(worksheet):
    frame = pd.DataFrame(worksheet.get_all_records())
    if frame.empty:
        return pd.DataFrame(columns=DATA_COLUMNS)
    if "date" not in frame.columns or "symbol" not in frame.columns:
        raise ValueError(f"{worksheet.title} is missing date/symbol columns")
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame = frame.dropna(subset=["date"]).copy()
    return frame.reindex(columns=DATA_COLUMNS)


def validate_frame(frame, ticker, minimum_date):
    if frame.empty:
        raise ValueError(f"{ticker} has no data")
    if frame["date"].duplicated().any():
        raise ValueError(f"{ticker} contains duplicate dates")
    latest = frame["date"].max().date()
    if latest < minimum_date:
        raise RuntimeError(
            f"{ticker} is stale: latest={latest}, expected>={minimum_date}"
        )
    latest_rows = frame[frame["date"].dt.date >= minimum_date]
    if latest_rows.empty or latest_rows["sp"].isna().any():
        raise ValueError(f"{ticker} has a missing settlement price in current data")


def build_updated_frame(worksheet, symbol, minimum_date):
    ticker = ticker_from_symbol(symbol)
    previous = load_existing(worksheet)
    validate_frame(previous, ticker, min(previous["date"].max().date(), minimum_date))

    scraped = scrape_prepared(symbol)
    scraped_dates = set(scraped["date"].dt.date)
    scraped_latest = scraped["date"].max().date()
    stored_latest_ts = previous["date"].max()
    stored_latest = stored_latest_ts.date()
    last_symbol = str(previous.sort_values("date")["symbol"].iloc[-1])
    print(
        f"{symbol}: stored={stored_latest}, source={scraped_latest}, "
        f"expected>={minimum_date}"
    )

    if scraped_latest < minimum_date or minimum_date not in scraped_dates:
        raise RuntimeError(
            f"TFEX source is stale for {symbol}: latest={scraped_latest}, "
            f"expected row={minimum_date}"
        )

    if last_symbol == symbol:
        if scraped_latest < stored_latest:
            updated = previous.copy()
        else:
            current_start = previous.loc[
                previous["symbol"] == symbol, "date"
            ].min()
            replacement = scraped[scraped["date"] >= current_start].copy()
            replacement["adj_price"] = replacement["sp"]
            replacement_dates = set(replacement["date"])
            keep = previous[
                ~(
                    (previous["symbol"] == symbol)
                    & previous["date"].isin(replacement_dates)
                )
            ]
            updated = pd.concat([keep, replacement], ignore_index=True)
    else:
        new_rows = scraped[scraped["date"] > stored_latest_ts].copy()
        if new_rows.empty:
            updated = previous.copy()
        else:
            first_new_date = new_rows["date"].min()
            previous_contract = scrape_prepared(last_symbol)
            matching_old = previous_contract[
                previous_contract["date"] == first_new_date
            ]
            if matching_old.empty:
                raise RuntimeError(
                    f"cannot back-adjust {symbol}: {last_symbol} has no row for "
                    f"{first_new_date:%Y-%m-%d}"
                )
            adjustment = (
                new_rows.loc[new_rows["date"] == first_new_date, "sp"].iloc[-1]
                - matching_old["sp"].iloc[-1]
            )
            previous["adj_price"] = (
                pd.to_numeric(previous["adj_price"], errors="coerce") + adjustment
            )
            new_rows["adj_price"] = new_rows["sp"]
            updated = pd.concat([previous, new_rows], ignore_index=True)

    updated = (
        updated.drop_duplicates(subset=["date"], keep="last")
        .sort_values("date")
        .reset_index(drop=True)
    )
    validate_frame(updated, ticker, minimum_date)
    return updated


def write_and_verify(worksheet, frame, minimum_date):
    output = frame.reindex(columns=DATA_COLUMNS).copy()
    output["date"] = pd.to_datetime(output["date"]).dt.strftime("%Y-%m-%d")
    output = output.where(pd.notna(output), "")
    set_with_dataframe(
        worksheet,
        output,
        include_index=False,
        include_column_header=True,
        resize=True,
    )
    readback = load_existing(worksheet)
    validate_frame(readback, worksheet.title, minimum_date)


def main():
    minimum_date = expected_trading_date()
    print(f"Expected completed TFEX date: {minimum_date}")

    client = authenticated_client()
    input_sheet = client.open_by_url(MARKET_INPUT_URL)
    holding = pd.DataFrame(
        input_sheet.worksheet("holding_information").get_all_records()
    )
    if holding.empty or "current_symbol" not in holding.columns:
        raise ValueError("holding_information has no current_symbol values")

    symbols = [
        value.strip()
        for value in holding["current_symbol"].dropna().astype(str)
        if value.strip()
    ]
    if not symbols:
        raise ValueError("holding_information has no active symbols")

    market_data = client.open_by_url(MARKET_DATA_URL)
    prepared = []

    # Read, scrape, and validate every instrument before the first write.
    for symbol in symbols:
        ticker = ticker_from_symbol(symbol)
        worksheet = market_data.worksheet(ticker)
        frame = build_updated_frame(worksheet, symbol, minimum_date)
        prepared.append((worksheet, frame))
        print(f"{symbol}: prepared and validated")

    for worksheet, frame in prepared:
        write_and_verify(worksheet, frame, minimum_date)
        print(f"{worksheet.title}: written and verified")

    print("All market-data sheets are current and verified.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
