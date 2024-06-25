#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 25 11:22:39 2024

@author: nanthawat
"""
import os

def test_google_credentials():
    # Retrieve the GOOGLE_APPLICATION_CREDENTIALS environment variable
    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    
    # Print the retrieved path
    if creds_path:
        print("Retrieved GOOGLE_APPLICATION_CREDENTIALS:", creds_path)
    else:
        print("Environment variable 'GOOGLE_APPLICATION_CREDENTIALS' is not set.")

if __name__ == "__main__":
    test_google_credentials()
