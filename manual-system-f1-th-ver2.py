#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 26 10:16:57 2024

@author: nanthawat
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 24 12:09:19 2024

This proram will update the database in googlesheet for System F1-TH (RUN manually 2) 

@author: nanthawat

For automation: 
1. crontab -e

2. 
30 8 * * * /Users/nanthawat/opt/anaconda3/bin/python3 /Users/nanthawat/Documents/GitHub/automation/future-th-database/manual-system-f1-th-ver2.py
30 8 * * * /Users/nanthawat/opt/anaconda3/bin/python3 /Users/nanthawat/Documents/GitHub/automation/future-th-database/manual-system-f1-th-ver2.py >> /Users/nanthawat/cron.log 2>&1
0 10 * * * /Users/nanthawat/opt/anaconda3/bin/python3 /Users/nanthawat/Documents/GitHub/automation/future-th-database/manual-system-f1-th-ver2.py >> /Users/nanthawat/cron.log 2>&1


3. Save
- press ESC
- :wq
- press Enter
"""


##### 1. Import library and define function, global variable ------------------------------------

# Libraries
import gspread
import pandas as pd
import selenium
from concurrent.futures import ThreadPoolExecutor
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from gspread_dataframe import set_with_dataframe
import os
import sys
from google.oauth2.service_account import Credentials
import json
from google.oauth2 import service_account
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException



# functions
def scrape_from_tfex(symbol):
    
    # Constant
    url = 'https://www.tfex.co.th/en/products/currency/eur-usd-futures/' + symbol + '/historical-trading'
    xpath = '//*[@id="__layout"]/div/div[2]/div[2]/div[2]/div/div[3]'
    
    # Set up the Chrome WebDriver
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')  # Run in headless mode
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    try:
        # Load the page
        driver.get(url)
        
        # Use WebDriverWait to wait for the table to be loaded
        wait = WebDriverWait(driver, 60)
        table_element = wait.until(EC.visibility_of_element_located((By.XPATH, xpath)))

        # Extract the rows using a more specific XPath to directly access the cells
        rows = table_element.find_elements(By.TAG_NAME, 'tr')
        
        # Parse each row into a list of columns
        data = []
        for row in rows:
            # Directly parse each cell's text, eliminating the need to split and rearrange later
            cells = row.find_elements(By.TAG_NAME, 'td')
            if cells:
                formatted_row = [cell.text for cell in cells]
                data.append(formatted_row)

        # Define the DataFrame with appropriate column headers
        df = pd.DataFrame(data, columns=['Date', 'Open', 'High', 'Low', 'Close', 'SP', 'Chg', '%Chg', 'Vol (Contract)', 'OI (Contract)'])
        
        # Add the 'Symbol' column
        df['Symbol'] = symbol  # Assign the symbol to the new column for all rows
    finally:
        # Ensure the driver is quit no matter what happens
        driver.quit()

    return df



def scrape_from_tfex(symbol):
    url = f'https://www.tfex.co.th/en/products/currency/eur-usd-futures/{symbol}/historical-trading'
    xpath = '//*[@id="__layout"]/div/div[2]/div[2]/div[2]/div/div[3]'

    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    data = []
    try:
        driver.get(url)
        wait = WebDriverWait(driver, 60)
        table_element = wait.until(EC.visibility_of_element_located((By.XPATH, xpath)))

        retries = 20  # Number of retries for fetching rows
        for _ in range(retries):
            try:
                rows = table_element.find_elements(By.TAG_NAME, 'tr')
                for row in rows:
                    cells = row.find_elements(By.TAG_NAME, 'td')
                    if cells:  # Ensure cells are not empty
                        formatted_row = [cell.text for cell in cells]
                        data.append(formatted_row)
                break  # Exit retry loop if successful
            except StaleElementReferenceException:
                print("Encountered stale element, retrying...")
                # Optionally wait before retrying
                driver.refresh()
                table_element = wait.until(EC.visibility_of_element_located((By.XPATH, xpath)))

        # Define the DataFrame with appropriate column headers if data was successfully retrieved
        if data:
            df = pd.DataFrame(data,
                              columns=['Date', 'Open', 'High', 'Low', 'Close', 'SP', 'Chg', '%Chg', 'Vol (Contract)',
                                       'OI (Contract)'])
            df['Symbol'] = symbol  # Add the 'Symbol' column
            return df
        else:
            return pd.DataFrame()  # Return empty DataFrame if no data was collected
    except TimeoutException:
        print(f"Failed to load the webpage or locate the element within the timeout period.")
        return pd.DataFrame()  # Return an empty DataFrame on timeout
    finally:
        driver.quit()

        

# def prep_df(raw_df):
#     """
#     Transforms the input DataFrame to:
#     - Convert 'Date' to datetime format.
#     - Convert financial figures from string to float, handling commas and currency.
#     - Remove 'Chg' and '%Chg' columns.
#     - Rename columns appropriately.
#     - Sort data from the most recent to the earliest.
#
#     Parameters:
#     raw_df (pd.DataFrame): The original DataFrame with financial time series data.
#
#     Returns:
#     pd.DataFrame: Transformed DataFrame with cleaned and formatted columns.
#     """
#     # Convert 'Date' to datetime
#     raw_df['Date'] = pd.to_datetime(raw_df['Date'], format='%d %b %Y')
#
#
#     # Convert 'Open' to 'SP' and 'Vol (Contract)', 'OI (Contract)' from string to numeric
#     financial_cols = ['Open', 'High', 'Low', 'Close', 'SP', 'Vol (Contract)', 'OI (Contract)']
#     for col in financial_cols:
#         raw_df[col] = pd.to_numeric(raw_df[col].replace(',', '', regex=True))
#
#     # Select and rename necessary columns
#     raw_df = raw_df.rename(columns={
#         'Date': 'date',
#         'Open': 'open',
#         'High': 'high',
#         'Low': 'low',
#         'Close': 'close',
#         'SP': 'sp',
#         'Vol (Contract)': 'vol',
#         'OI (Contract)': 'oi',
#         'Symbol': 'symbol'
#     })
#
#     # Drop unnecessary columns
#     raw_df = raw_df.drop(['Chg', '%Chg'], axis=1)
#
#     # Sort by 'date' descending
#     raw_df = raw_df.sort_values(by='date', ascending=True)
#
#     return raw_df[['date', 'open', 'high', 'low', 'close', 'sp', 'vol', 'oi', 'symbol']]

# New modified one
def prep_df(raw_df):
    """
    Clean and normalize TFEX table:
    - Parse dates
    - Convert numeric columns (handling commas and dashes)
    - Drop unusable rows
    - Rename columns and sort ascending by date
    """
    import pandas as pd

    # Short-circuit if nothing to do
    if raw_df is None or raw_df.empty:
        return pd.DataFrame()

    # Work on a copy; treat "-" as missing
    raw_df = raw_df.copy().replace("-", pd.NA)

    # Parse date (coerce invalid to NaT)
    raw_df["Date"] = pd.to_datetime(raw_df["Date"], format="%d %b %Y", errors="coerce")

    # Convert numeric fields (remove commas; coerce invalid to NaN)
    financial_cols = ["Open", "High", "Low", "Close", "SP", "Vol (Contract)", "OI (Contract)"]
    for col in financial_cols:
        if col in raw_df.columns:
            raw_df[col] = pd.to_numeric(
                raw_df[col].astype(str).str.replace(",", ""),
                errors="coerce"
            )

    # Rename to snake_case
    raw_df = raw_df.rename(columns={
        "Date": "date",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "SP": "sp",
        "Vol (Contract)": "vol",
        "OI (Contract)": "oi",
        "Symbol": "symbol",
    })

    # Drop columns that aren't needed if present
    raw_df = raw_df.drop(columns=["Chg", "%Chg"], errors="ignore")

    # Keep only usable rows (need a date and a settlement price)
    raw_df = raw_df.dropna(subset=["date", "sp"])

    # Sort oldest->newest
    raw_df = raw_df.sort_values(by="date", ascending=True)

    # Return with a stable column order (missing cols will appear as NaN)
    cols = ["date", "open", "high", "low", "close", "sp", "vol", "oi", "symbol"]
    return raw_df.reindex(columns=cols)





# global variables
scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
market_input_url = 'https://docs.google.com/spreadsheets/d/17SMA52gIOkjFan-0au_YJEAxoWIzoNA84qlmgoTsZ-s/edit?gid=1037340594#gid=1037340594'
market_data_url = 'https://docs.google.com/spreadsheets/d/19Rj7iW5xWOe6ZJJRsO9VzsZXyLfFu1S_vtClEE_3DEw/edit?gid=748449431#gid=748449431'

##### 2. Set up once ------------------------------------

# 1: Authentication (manually)
#json_keyfile_path = '/Users/nanthawat/Desktop/key/google/system-f1-th/automated-system-f1-th-key.json'
#creds = ServiceAccountCredentials.from_json_keyfile_name(json_keyfile_path, scope)
#client = gspread.authorize(creds)

# 2: Authentication (GitHub Action)
def authenticated_client():
    service_account_file = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not service_account_file:
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS is not set")
    credentials = service_account.Credentials.from_service_account_file(
        service_account_file,
        scopes=scope,
    )
    return gspread.authorize(credentials)

DATA_COLUMNS = [
    "date", "open", "high", "low", "close",
    "sp", "vol", "oi", "symbol", "adj_price",
]


def load_existing(worksheet):
    frame = pd.DataFrame(worksheet.get_all_records())
    if frame.empty:
        return pd.DataFrame(columns=DATA_COLUMNS)
    if "date" not in frame.columns or "symbol" not in frame.columns:
        raise ValueError(f"{worksheet.title} is missing required date/symbol columns")
    frame["date"] = pd.to_datetime(frame["date"], format="%Y-%m-%d", errors="coerce")
    frame = frame.dropna(subset=["date"]).copy()
    return frame.reindex(columns=DATA_COLUMNS)


def write_and_verify(worksheet, frame, expected_latest_date):
    output = frame.reindex(columns=DATA_COLUMNS).copy()
    output["date"] = output["date"].dt.strftime("%Y-%m-%d")
    output = output.where(pd.notna(output), "")
    set_with_dataframe(
        worksheet,
        output,
        include_index=False,
        include_column_header=True,
        resize=True,
    )

    readback = load_existing(worksheet)
    actual_latest_date = readback["date"].max() if not readback.empty else None
    if actual_latest_date != expected_latest_date:
        raise RuntimeError(
            f"{worksheet.title} verification failed: "
            f"expected {expected_latest_date:%Y-%m-%d}, got {actual_latest_date}"
        )


def update_symbol(market_data_sheet, symbol):
    ticker = symbol[:-3]
    worksheet = market_data_sheet.worksheet(ticker)
    previous = load_existing(worksheet)
    if previous.empty:
        raise ValueError(f"{ticker} has no existing continuous-series data")

    print(f"{symbol}: downloaded {len(previous)} existing rows")
    scraped = prep_df(scrape_from_tfex(symbol))
    if scraped.empty:
        raise RuntimeError(f"scrape returned no usable rows for {symbol}")

    scraped_latest = scraped["date"].max()
    stored_latest = previous["date"].max()
    last_symbol = str(previous.sort_values("date")["symbol"].iloc[-1])
    print(
        f"{symbol}: stored={stored_latest:%Y-%m-%d}, "
        f"source={scraped_latest:%Y-%m-%d}"
    )

    if last_symbol == symbol:
        if scraped_latest < stored_latest:
            print(f"{symbol}: source is older than the sheet; preserving sheet data")
            return

        # Refresh dates exposed by TFEX and append every missing date. This also
        # replaces an intraday snapshot with the final settlement on a later run.
        current_start = previous.loc[previous["symbol"] == symbol, "date"].min()
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
        # On a roll, add every date missing during downtime, and calculate the
        # back-adjustment on the first new date shared by both contracts.
        new_rows = scraped[scraped["date"] > stored_latest].copy()
        if new_rows.empty:
            print(f"{symbol}: waiting for the first completed row after contract roll")
            return

        first_new_date = new_rows["date"].min()
        previous_contract = prep_df(scrape_from_tfex(last_symbol))
        matching_old = previous_contract[previous_contract["date"] == first_new_date]
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
    write_and_verify(worksheet, updated, max(stored_latest, scraped_latest))
    print(f"{symbol}: sheet updated and verified")


def main():
    client = authenticated_client()
    market_input_sheet = client.open_by_url(market_input_url)
    holding_worksheet = market_input_sheet.worksheet("holding_information")
    holding_information = pd.DataFrame(holding_worksheet.get_all_records())
    if holding_information.empty or "current_symbol" not in holding_information:
        raise ValueError("holding_information has no current_symbol values")

    market_data_sheet = client.open_by_url(market_data_url)
    failures = []
    for symbol in holding_information["current_symbol"].dropna().astype(str):
        try:
            update_symbol(market_data_sheet, symbol.strip())
        except Exception as exc:
            failures.append(f"{symbol}: {exc}")
            print(f"ERROR: {failures[-1]}")

    if failures:
        print("\nUpdate failed for one or more symbols:")
        for failure in failures:
            print(f"- {failure}")
        return 1

    print("All symbols updated and verified successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
