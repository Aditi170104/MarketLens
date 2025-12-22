# bulk_downloader.py (FIXED for input file containing 20 columns)
import pandas as pd
import os
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.common.exceptions import TimeoutException

# --- Configuration (UPDATE THESE) ---
PROJECT_ROOT = os.getcwd() 
DOWNLOAD_DIRECTORY = os.path.join(PROJECT_ROOT, 'StocksExportsConsolidated1Nov')
# NOTE: The input file is now the processed initial file, which still contains the NSE Code column
INPUT_CSV_PATH = os.path.join(PROJECT_ROOT, 'query-results.csv') 
SCREENER_EMAIL = "shresthyadav124@gmail.com" # Replace Placeholder
SCREENER_PASSWORD = "Hanumanji@12" # Replace Placeholder
LOGIN_URL = "https://www.screener.in/login/"

# (The setup_webdriver, login_to_screener, and try_download functions remain the same)

def setup_webdriver():
    if not os.path.exists(DOWNLOAD_DIRECTORY):
        os.makedirs(DOWNLOAD_DIRECTORY)
    
    chrome_options = Options()
    chrome_options.add_argument("--window-size=1200,800")
    chrome_options.add_experimental_option("prefs", {
        "download.default_directory": DOWNLOAD_DIRECTORY,
        "download.prompt_for_download": False
    })

    driver_path = os.path.join(PROJECT_ROOT, 'chromedriver.exe')
    service = ChromeService(executable_path=driver_path)
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver

def login_to_screener(driver: webdriver.Chrome):
    driver.get(LOGIN_URL)
    email_field = WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.NAME, "username")))
    email_field.send_keys(SCREENER_EMAIL)
    password_field = driver.find_element(By.NAME, "password")
    password_field.send_keys(SCREENER_PASSWORD)
    login_button = WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH, "//button[@type='submit']")))
    login_button.click()
    WebDriverWait(driver, 30).until(EC.url_changes(LOGIN_URL))
    print("Login successful! Continuing with downloads.")

def try_download(driver, url, report_type):
    driver.get(url)
    time.sleep(2)
    export_button_xpaths = ["//button[@aria-label='Export to Excel']", "//button[span[contains(text(), 'Export to Excel')]]"]
    for xpath in export_button_xpaths:
        try:
            export_button = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, xpath)))
            export_button.click()
            time.sleep(5)
            return True
        except TimeoutException:
            continue
    raise TimeoutException(f"{report_type} export button not found.")


def bulk_download_reports(driver: webdriver.Chrome):
    
    # -----------------------------------------------------------
    # FIX: Read the CSV normally and select the 'NSE Code' column
    # -----------------------------------------------------------
    try:
        # Load the CSV, stripping columns just in case of hidden spaces
        df_stocks = pd.read_csv(INPUT_CSV_PATH)
        df_stocks.columns = df_stocks.columns.str.strip() 
        
        # Select the column containing the tickers
        if 'NSE Code' not in df_stocks.columns:
            # Fallback check if the column name was somehow different, though unlikely here
            raise ValueError("Input file must contain a column named 'NSE Code'.")
            
        unique_nse_codes = df_stocks['NSE Code'].dropna().unique().tolist()
        total_to_process = len(unique_nse_codes)
        successful_downloads = 0
    except Exception as e:
        print(f"❌ Could not read and process input CSV: {e}")
        raise # Reraise the exception to stop the pipeline

    for index, code in enumerate(unique_nse_codes):
        print(f"\n[{index + 1}/{total_to_process}] Processing {code}...")
        consolidated_url = f"https://www.screener.in/company/{code}/consolidated/"
        standalone_url = f"https://www.screener.in/company/{code}/"
        try:
            try_download(driver, consolidated_url, "Consolidated")
        except:
            try:
                try_download(driver, standalone_url, "Standalone")
            except Exception:
                print(f" -> SKIPPED: Failed both consolidated and standalone for {code}.")

def main():
    driver = setup_webdriver()
    try:
        login_to_screener(driver)
        bulk_download_reports(driver)
    finally:
        driver.quit()

if __name__ == "__main__":
    main()