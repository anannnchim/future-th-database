#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 25 11:22:39 2024

@author: nanthawat
"""
import os
from google.oauth2 import service_account
import gspread


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
    else:
        print("Environment variable 'GOOGLE_APPLICATION_CREDENTIALS' is not set.")

if __name__ == "__main__":
    test_google_credentials()
