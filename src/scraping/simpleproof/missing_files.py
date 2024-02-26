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
        "browser.download.dir", f"{os.getenv('DATA_PATH', None)}/0_raw"
    )
    options.set_preference(
        "browser.helperApps.neverAsk.saveToDisk", "image/jpeg, image/png"
    )
    options.set_preference(
        "browser.helperApps.neverAsk.openFile", "image/jpeg, image/png"
    )
    driver = webdriver.Firefox(service=service, options=options)
    return driver


def scraping_acta(driver, acta, url_type):
    if url_type == "uno":
        url_acta = f"https://preliminar.tse.gob.sv/administracion/img/get-acta/{acta}"
    elif url_type == "dos":
        url_acta = (
            f"https://divulgacion.tse.gob.sv/administracion/img/get-acta-dos/{acta}"
        )
    driver.get(url_acta)
    driver.find_element(By.CSS_SELECTOR, "body > img")

    # Save image form browser using selenium like save as
    pyautogui.hotkey("ctrl", "s")

    # Type the file name and press Enter
    pyautogui.write(f"missing_{url_type}_{acta}")
    time.sleep(0.2)
    pyautogui.press("enter")
    # When replace file dialog appears
    time.sleep(0.2)
    pyautogui.press("enter")
    pyautogui.press("enter")

    # Current acta
    print(driver.current_url)
    time.sleep(0.3)

    # Process report
    process_report(acta, url_type, driver.current_url)


def get_hash_file_tse(file_name):
    # Open the file file_name in the directory src/data/0_raw
    if os.path.exists(f"src/data/0_raw/{file_name}"):
        # Get the hash of the TSE original file and assign to hash_file_tse
        hash_file_tse = hashlib.sha256(
            open(f"src/data/0_raw/{file_name}", "rb").read()
        ).hexdigest()
    else:
        hash_file_tse = "Not Found"

    return hash_file_tse


def get_file_name(acta, url_type):
    # Get a file that its name startswith missing_{url_type}_{acta} and has any file extension
    file_name = "not_found"
    for file in os.listdir("src/data/0_raw"):
        if file.startswith(f"missing_{url_type}_{acta}."):
            file_name = file
            break
    return file_name


def process_report(acta, url_type, url_acta):
    # Get date time iso3086 format
    date_time = datetime.datetime.now().isoformat()

    # Get file name
    file_name = get_file_name(acta, url_type)

    # Get hash file tse
    hash_file_tse = get_hash_file_tse(file_name)

    # Save the report in the directory src/data/process_report_missing.csv
    with open("src/data/process_report_missing.csv", "a", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([acta, file_name, hash_file_tse, date_time, url_type, url_acta])


def process_missing_actas(driver):
    # With pandas open the src/data/2_validation/MISSING_FILES.csv and get missing files
    missing_files = pd.read_csv("src/data/2_validation/MISSING_FILES.csv")

    # Iterate over the missing files
    for index, row in missing_files.iterrows():
        acta = row["ACTA"]
        url_type = row["URL_TYPE"]

        print("Index: ", index)
        print("Acta: ", acta, "URL_TYPE: ", url_type)

        try:
            if len(url_type.split(":")) > 1:
                if len(acta.split(":")) > 1:
                    for a in acta.split(":"):
                        scraping_acta(driver, a, url_type.split(":")[0])
                        # print(f"""scraping_acta(driver, {a}, {url_type.split(":")[0]})""")
                        # Wait 1 seconds before scraping again
                        # time.sleep(1)
                        scraping_acta(driver, a, url_type.split(":")[1])
                        # print(f"""scraping_acta(driver, {a}, {url_type.split(":")[1]})""")
                else:
                    scraping_acta(driver, acta, url_type.split(":")[0])
                    # print(f"""scraping_acta(driver, {acta}, {url_type.split(":")[0]})""")
                    # Wait 1 seconds before scraping again
                    # time.sleep(1)
                    scraping_acta(driver, acta, url_type.split(":")[1])
                    # print(f"""scraping_acta(driver, {acta}, {url_type.split(":")[1]})""")
            elif len(acta.split(":")) > 1:
                for a in acta.split(":"):
                    scraping_acta(driver, a, url_type)
                    # print(f"""scraping_acta(driver, {a}, {url_type})""")
            else:
                scraping_acta(driver, acta, url_type)
                # print(f"""scraping_acta(driver, {acta}, {url_type})""")
        except Exception:
            print("Acta Not Found:", driver.current_url)
            # Process report
            process_report(
                driver.current_url.split("/")[-1], url_type, driver.current_url
            )


def main():
    # Setup driver
    driver = setup_driver()

    # Process actas not found
    process_missing_actas(driver)


if __name__ == "__main__":
    main()
