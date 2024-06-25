#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 25 09:57:05 2024

@author: nanthawat
"""

##### 1. Import library and define function, global variable ------------------------------------


# !pip install pandas pyarrow
#!pip install pandas fastparquet

# Libraries
import gspread
import pandas as pd
import selenium
from oauth2client.service_account import ServiceAccountCredentials
from concurrent.futures import ThreadPoolExecutor
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from gspread_dataframe import set_with_dataframe
import os
from google.oauth2.service_account import Credentials
import json
from google.oauth2 import service_account


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
        wait = WebDriverWait(driver, 10)  # Timeout after 10 seconds
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


def prep_df(raw_df):
    """
    Transforms the input DataFrame to:
    - Convert 'Date' to datetime format.
    - Convert financial figures from string to float, handling commas and currency.
    - Remove 'Chg' and '%Chg' columns.
    - Rename columns appropriately.
    - Sort data from the most recent to the earliest.

    Parameters:
    raw_df (pd.DataFrame): The original DataFrame with financial time series data.

    Returns:
    pd.DataFrame: Transformed DataFrame with cleaned and formatted columns.
    """
    # Convert 'Date' to datetime
    raw_df['Date'] = pd.to_datetime(raw_df['Date'], format='%d %b %Y')
    
    
    # Convert 'Open' to 'SP' and 'Vol (Contract)', 'OI (Contract)' from string to numeric
    financial_cols = ['Open', 'High', 'Low', 'Close', 'SP', 'Vol (Contract)', 'OI (Contract)']
    for col in financial_cols:
        raw_df[col] = pd.to_numeric(raw_df[col].replace(',', '', regex=True))
    
    # Select and rename necessary columns
    raw_df = raw_df.rename(columns={
        'Date': 'date',
        'Open': 'open',
        'High': 'high',
        'Low': 'low',
        'Close': 'close',
        'SP': 'sp',
        'Vol (Contract)': 'vol',
        'OI (Contract)': 'oi',
        'Symbol': 'symbol'
    })

    # Drop unnecessary columns
    raw_df = raw_df.drop(['Chg', '%Chg'], axis=1)
    
    # Sort by 'date' descending
    raw_df = raw_df.sort_values(by='date', ascending=True)

    return raw_df[['date', 'open', 'high', 'low', 'close', 'sp', 'vol', 'oi', 'symbol']]



# global variables
scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
# json_keyfile_path = '/Users/nanthawat/Desktop/key/google/system-f1-th/automated-system-f1-th-key.json' (manually)
market_input_url = 'https://docs.google.com/spreadsheets/d/17SMA52gIOkjFan-0au_YJEAxoWIzoNA84qlmgoTsZ-s/edit?gid=1037340594#gid=1037340594'
market_data_url = 'https://docs.google.com/spreadsheets/d/19Rj7iW5xWOe6ZJJRsO9VzsZXyLfFu1S_vtClEE_3DEw/edit?gid=748449431#gid=748449431'


##### 2. Set up onece ------------------------------------

# METHOD 1: Authentication (manually)
#creds = ServiceAccountCredentials.from_json_keyfile_name(json_keyfile_path, scope)
#client = gspread.authorize(creds)

# Method 2: Authentication (Github action)

SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
creds = service_account.Credentials.from_service_account_file( SERVICE_ACCOUNT_FILE, scopes=scope)
client = gspread.authorize(creds)



# Get sheet from url
market_input_sheet = client.open_by_url(market_input_url)
market_data_sheet = client.open_by_url(market_data_url)

# Access a specific worksheet by name or by index
holding_information = market_input_sheet.worksheet('holding_information')
holding_information = pd.DataFrame(holding_information.get_all_records()) 

# Scrape data from website
#raw_df_prev = scrape_from_tfex("S50M24")
#prev_df = prep_df(raw_df_prev)
#prev_df = prev_df.tail(2)

# Append to googlesheet 
def append_to_sheet(client, spreadsheet_url, sheet_name, df):
    # Open the spreadsheet and the specific sheet
    sh = client.open_by_url(spreadsheet_url)
    worksheet = sh.worksheet(sheet_name)
    
    # Find the first empty row (considering data starts at row 1, adjust if header is present)
    existing_rows = len(worksheet.get_all_values())
    start_row = existing_rows + 1  # Since get_all_values includes the header, start after the last filled row

    # Append the data starting from the empty row
    set_with_dataframe(worksheet, df, row=start_row, include_index=False, include_column_header=False, resize=False)

# Append data 
append_to_sheet(client, market_data_url, 'test_automation',holding_information)


#Rewrite 
#worksheet = market_data_sheet.worksheet('test_automation')
#set_with_dataframe(worksheet, holding_information, include_index=False, include_column_header=True, resize=True)

# Store data 
holding_information.to_parquet('data/test' + '.parquet',  engine='pyarrow')



