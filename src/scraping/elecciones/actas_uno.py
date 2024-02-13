import csv
import datetime
import hashlib
import os
import time
from pathlib import Path

import pandas as pd
import pyautogui
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service

# Load .env variables
_ = load_dotenv(dotenv_path=f"{Path().resolve()}/src/.env")


def setup_driver():
    options = Options()
    options.binary_location = os.getenv("BROWSER_PATH", None)
    service = Service(executable_path=os.getenv("BROWSER_DRIVER_PATH", None))
    options.set_preference("browser.download.folderList", 2)
    options.set_preference("browser.download.manager.showWhenStarting", False)
    options.set_preference(
        "browser.download.dir",
        f"{os.getenv('DATA_PATH', None)}/0_raw_uno",
    )
    options.set_preference(
        "browser.helperApps.neverAsk.saveToDisk", "image/jpeg, image/png"
    )
    options.set_preference(
        "browser.helperApps.neverAsk.openFile", "image/jpeg, image/png"
    )
    driver = webdriver.Firefox(service=service, options=options)
    return driver


def scraping_acta(driver, acta):
    url_acta = f"https://preliminar.tse.gob.sv/administracion/img/get-acta/{acta}"
    driver.get(url_acta)
    driver.find_element(By.CSS_SELECTOR, "body > img")

    # Save image form browser using selenium like save as
    pyautogui.hotkey("ctrl", "s")

    # Type the file name and press Enter
    pyautogui.write(f"acta_{acta}")
    time.sleep(0.2)
    pyautogui.press("enter")
    # When replace file dialog appears
    time.sleep(0.2)
    pyautogui.press("enter")
    pyautogui.press("enter")

    # Current acta
    print(driver.current_url)
    time.sleep(0.2)

    # Process report
    process_report(acta, driver.current_url)


def get_hash_file_tse(file_name):
    # Open the file file_name in the directory src/data/0_raw_uno
    if os.path.exists(f"src/data/0_raw_uno/{file_name}"):
        # Get the hash of the TSE original file and assign to hash_file_tse
        hash_file_tse = hashlib.sha256(
            open(f"src/data/0_raw_uno/{file_name}", "rb").read()
        ).hexdigest()
    else:
        hash_file_tse = "Not Found"

    return hash_file_tse


def get_file_name(acta):
    # Get a file that its name startswith acta_{acta} and has any file extension
    file_name = "not_found"
    for file in os.listdir("src/data/0_raw_uno"):
        if file.startswith(f"acta_{acta}."):
            file_name = file
            break
    return file_name


def process_report(acta, url_acta):
    # Get date time iso3086 format
    date_time = datetime.datetime.now().isoformat()

    # Get file name
    file_name = get_file_name(acta)

    # Get hash file tse
    hash_file_tse = get_hash_file_tse(file_name)

    # Save the report in the directory src/data/process_report_uno.csv
    with open("src/data/process_report_uno.csv", "a", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([acta, file_name, hash_file_tse, date_time, url_acta])


def process_actas(driver, start_acta, end_acta):
    acta = start_acta
    count_retry = 0

    while acta <= end_acta:
        print(f"Acta: {acta}")
        try:
            # Scraping acta
            scraping_acta(driver, acta)
            # Next Acta
            acta += 1
        except Exception:
            print(f"Acta not found: {acta}")
            print(f"Retry #: {count_retry}")
            print("retrying in 0.2s")
            time.sleep(0.1)
            print("Retrying ...")

            # Here retry 3 times and continue
            if count_retry < 3:
                # retry
                count_retry += 1
            else:
                # Reset Count Retry
                count_retry = 0
                # Process report
                process_report(acta, driver.current_url)
                # Next Acta
                acta += 1


def process_actas_not_found(driver):
    # With pandas open the src/data/process_report_uno.csv and get the actas not found
    df = pd.read_csv("src/data/process_report_uno.csv")
    actas_not_found = df[df["FILE_NAME"] == "not_found"]["ACTA"].tolist()
    # Remove duplicates
    actas_not_found = list(set(actas_not_found))
    # Sort actas not found ascending
    actas_not_found.sort()

    for acta in actas_not_found:
        try:
            # Scraping acta
            scraping_acta(driver, acta)

            # Update process report
            file_name = get_file_name(acta)
            df.loc[df["ACTA"] == acta, "FILE_NAME"] = file_name
            df.loc[df["ACTA"] == acta, "HASH_FILE_TSE"] = get_hash_file_tse(file_name)
            df.loc[df["ACTA"] == acta, "DATE_TIME"] = (
                datetime.datetime.now().isoformat()
            )
            df.loc[df["ACTA"] == acta, "URL"] = driver.current_url

        except Exception:
            print(f"Acta not found: {acta}")
            # Process report
            # process_report(acta, driver.current_url)

    # Update process report
    df.to_csv("src/data/process_report_uno.csv", index=False)


def main():
    # Setup driver
    driver = setup_driver()

    start_acta = 1
    end_acta = start_acta + 100
    while True:
        # Process actas not found
        process_actas_not_found(driver)

        # Process actas
        # process_actas(driver, start_acta, end_acta)

        print("Scraping again .......................")

        # Wait for 1 seconds before scraping again
        time.sleep(0.5)
        start_acta = end_acta + 1
        end_acta += 100


if __name__ == "__main__":
    main()
