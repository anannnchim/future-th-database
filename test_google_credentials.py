#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 25 11:22:39 2024

@author: nanthawat
"""
import os
from google.oauth2 import service_account
import gspread

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
        wait = WebDriverWait(driver, 30)  # Timeout after 10 seconds
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


def test_google_credentials():
    # Retrieve the GOOGLE_APPLICATION_CREDENTIALS environment variable
    SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    
    # Define the scopes required by your application
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    
    # Load the credentials from the service account file
    credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE,
    scopes=SCOPES)
    
    gc = gspread.authorize(credentials)
    
    print(SERVICE_ACCOUNT_FILE)
    
    # Print the retrieved path
    if SERVICE_ACCOUNT_FILE:
        print("Retrieved GOOGLE_APPLICATION_CREDENTIALS:", SERVICE_ACCOUNT_FILE)
        print("this is gc: " , gc)
        raw_df = scrape_from_tfex('S50U24') # Slow part 
        raw_df.to_parquet('raw_df.parquet',  engine='pyarrow')

        print(raw_df.head())
    else:
        print("Environment variable 'GOOGLE_APPLICATION_CREDENTIALS' is not set.")

if __name__ == "__main__":
    test_google_credentials()
