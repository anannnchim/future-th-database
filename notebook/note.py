#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 25 16:11:41 2024

@author: nanthawat
"""

# library
import os
from google.oauth2 import service_account
import gspread
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


# LOAD DATA
df = pd.read_parquet('/Users/nanthawat/Documents/GitHub/automation/future-th-database/GF10.parquet')


# Sample 
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
        wait = WebDriverWait(driver, 10)
        table_element = wait.until(EC.visibility_of_element_located((By.XPATH, xpath)))

        retries = 3  # Number of retries for fetching rows
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




# 

sheet = market_data_sheet.worksheet('S50')
data = sheet.get_all_records()
prev_backadj_df = pd.DataFrame(data)
prev_backadj_df['date'] = pd.to_datetime(prev_backadj_df['date'], format='%Y-%m-%d')



# Method 4:
    
def fetch_cell_text(cell):
    try:
        return cell.text
    except StaleElementReferenceException:
        return None
def scrape_from_tfex(symbol):
    
    # URL and XPath setup
    url = f'https://www.tfex.co.th/en/products/currency/eur-usd-futures/{symbol}/historical-trading'
    xpath = '//*[@id="__layout"]/div/div[2]/div[2]/div[2]/div/div[3]'
    
    # Setup WebDriver
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')  # Run in headless mode for automation
    options.add_argument('--no-sandbox')  # Bypass OS security model
    options.add_argument('--disable-dev-shm-usage')  # Overcome limited resource problems
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

        
    try:
        driver.get(url)
        wait = WebDriverWait(driver, 30)
        table_element = wait.until(EC.visibility_of_element_located((By.XPATH, xpath)))
        rows = table_element.find_elements(By.TAG_NAME, 'tr')
        data = []
        for row in rows:
            cells = row.find_elements(By.TAG_NAME, 'td')
            formatted_row = [fetch_cell_text(cell) for cell in cells if fetch_cell_text(cell) is not None]
            if formatted_row:
                data.append(formatted_row)
        df = pd.DataFrame(data, columns=['Date', 'Open', 'High', 'Low', 'Close', 'SP', 'Chg', '%Chg', 'Vol (Contract)', 'OI (Contract)'])
        df['Symbol'] = symbol
    finally:
        driver.quit()
    return df
    



# Method 3 
def scrape_from_tfex(symbol):
    # URL and XPath setup
    url = f'https://www.tfex.co.th/en/products/currency/eur-usd-futures/{symbol}/historical-trading'
    xpath = '//*[@id="__layout"]/div/div[2]/div[2]/div[2]/div/div[3]'
    
    # Setup WebDriver
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')  # Run in headless mode for automation
    options.add_argument('--no-sandbox')  # Bypass OS security model
    options.add_argument('--disable-dev-shm-usage')  # Overcome limited resource problems
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    try:
        # Load the page
        driver.get(url)
        
        # Wait for the table to be fully loaded and visible
        wait = WebDriverWait(driver, 10)
        table_element = wait.until(EC.visibility_of_element_located((By.XPATH, xpath)))

        # Wait until all rows are visible and stable
        rows = wait.until(EC.visibility_of_all_elements_located((By.XPATH, f"{xpath}//tr")))

        # Parse each row into a list of columns
        data = []
        for row in rows:
            cells = row.find_elements(By.TAG_NAME, 'td')
            formatted_row = [cell.text for cell in cells if cell.text]
            if formatted_row:
                data.append(formatted_row)

        # Create DataFrame with appropriate column headers
        df = pd.DataFrame(data, columns=['Date', 'Open', 'High', 'Low', 'Close', 'SP', 'Chg', '%Chg', 'Vol (Contract)', 'OI (Contract)'])
        df['Symbol'] = symbol  # Add the symbol column
    finally:
        # Cleanup: close browser regardless of success or failure
        driver.quit()

    return df





import requests
response = requests.get('https://sheets.googleapis.com/v4/spreadsheets/YOUR_SPREADSHEET_ID', timeout=60)  # Set timeout to 60 seconds



import gspread
gc = gspread.service_account()
gc.session.timeout = 60  # Set a longer timeout




