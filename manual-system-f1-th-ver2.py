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
"""
##### 1. Import library and define function, global variable ------------------------------------

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
        wait = WebDriverWait(driver, 1000)  # Timeout after 10 seconds
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



# Modified function to avoid timeout when scraping data from website
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
        wait = WebDriverWait(driver, 1000)
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
            df = pd.DataFrame(data, columns=['Date', 'Open', 'High', 'Low', 'Close', 'SP', 'Chg', '%Chg', 'Vol (Contract)', 'OI (Contract)'])
            df['Symbol'] = symbol  # Add the 'Symbol' column
            return df
        else:
            return pd.DataFrame()  # Return empty DataFrame if no data was collected
    except TimeoutException:
        print(f"Failed to load the webpage or locate the element within the timeout period.")
        return pd.DataFrame()  # Return an empty DataFrame on timeout
    finally:
        driver.quit()
        

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
market_input_url = 'https://docs.google.com/spreadsheets/d/17SMA52gIOkjFan-0au_YJEAxoWIzoNA84qlmgoTsZ-s/edit?gid=1037340594#gid=1037340594'
market_data_url = 'https://docs.google.com/spreadsheets/d/19Rj7iW5xWOe6ZJJRsO9VzsZXyLfFu1S_vtClEE_3DEw/edit?gid=748449431#gid=748449431'

##### 2. Set up onece ------------------------------------

# 1: Authentication (manually)
json_keyfile_path = '/Users/nanthawat/Desktop/key/google/system-f1-th/automated-system-f1-th-key.json'
creds = ServiceAccountCredentials.from_json_keyfile_name(json_keyfile_path, scope)
client = gspread.authorize(creds)

# 2: Authentication (Github Action)
#SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
#creds = service_account.Credentials.from_service_account_file( SERVICE_ACCOUNT_FILE, scopes=scope)
#client = gspread.authorize(creds)

# Get sheet from url
market_input_sheet = client.open_by_url(market_input_url)
#market_data_sheet = client.open_by_url(market_data_url)

# Access a specific worksheet by name or by index
holding_information = market_input_sheet.worksheet('holding_information')
holding_information = pd.DataFrame(holding_information.get_all_records()) 

##### 3. loop through each symbol ------------------------------------



for symbol in holding_information['current_symbol']:
    
    
    # 1: Authentication (manually)
    json_keyfile_path = '/Users/nanthawat/Desktop/key/google/system-f1-th/automated-system-f1-th-key.json'
    creds = ServiceAccountCredentials.from_json_keyfile_name(json_keyfile_path, scope)
    client = gspread.authorize(creds)
    
    
    market_data_sheet = client.open_by_url(market_data_url)

    # START 
    ticker = symbol[:-3]
    print(symbol)
    
    # Import data form googlesheet
    sheet = market_data_sheet.worksheet(ticker)
    data = sheet.get_all_records()
    prev_backadj_df = pd.DataFrame(data)
    prev_backadj_df['date'] = pd.to_datetime(prev_backadj_df['date'], format='%Y-%m-%d')
    print("1 - Finish: Download data from googlesheet ")
    
    # Scrape data from website
    raw_df = scrape_from_tfex(symbol)
    df = prep_df(raw_df)

    print("2 - Finish: Scrape data from website")

    
    # Check whether database is already updated or not
    if prev_backadj_df['date'].tail(1).item() == df['date'].tail(1).item():
        
        # Already update
        print("3 - Database is already updated")
        
        # Store data 
        prev_backadj_df['date'] = prev_backadj_df['date'].dt.strftime('%Y-%m-%d')
        # prev_backadj_df.to_parquet(ticker + '.parquet',  engine='pyarrow')
    
    else:
        
        # Check whether we have rolled the contract or not
        if prev_backadj_df['symbol'].tail(1).item() == symbol:
            
            # append new data normally
            df['adj_price'] = df['sp']
            prev_backadj_df = pd.concat([prev_backadj_df, df.tail(1)])
        else:
            # append new data normally
            df['adj_price'] = df['sp']
            prev_backadj_df = pd.concat([prev_backadj_df, df.tail(1)])
            
            # Get data from previous month
            prev_symbol = prev_backadj_df['symbol'].iloc[-2]
            
            raw_df_prev = scrape_from_tfex(prev_symbol)
            # if raw_df_prev is empty then retry it 3 times then continue 
            prev_df = prep_df(raw_df_prev)
            
            # Calculate the different
            different = prev_backadj_df['sp'].tail(1).item()  - prev_df['sp'].tail(1).item()
            
            # Backadjusting price except the last one
            prev_backadj_df.iloc[:-1, prev_backadj_df.columns.get_loc('adj_price')] = prev_backadj_df.iloc[:-1, prev_backadj_df.columns.get_loc('adj_price')] + different
            

        # Reformat data
        prev_backadj_df['date'] = prev_backadj_df['date'].dt.strftime('%Y-%m-%d')
        
        
        # Rewrite data on googlesheet
        market_data_sheet = client.open_by_url(market_data_url)
        worksheet = market_data_sheet.worksheet(ticker)
        
        prev_backadj_df = prev_backadj_df.dropna(how='any') # ADDD

        set_with_dataframe(worksheet, prev_backadj_df, include_index=False, include_column_header=True, resize=True)
        
        print("3 - Finish: Update googlesheet")
        
        # store data
        # prev_backadj_df.to_parquet(ticker + '.parquet', engine='pyarrow')

    print("4 - All done.")     
        
    
    
# Try the same but run in internal environment







        
        