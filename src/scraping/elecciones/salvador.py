import os
import time
from pathlib import Path

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
    options.set_preference("browser.download.dir", os.getenv("DOWNLOAD_PATH", None))
    options.set_preference(
        "browser.helperApps.neverAsk.saveToDisk", "image/jpeg, image/png"
    )
    options.set_preference(
        "browser.helperApps.neverAsk.openFile", "image/jpeg, image/png"
    )
    driver = webdriver.Firefox(service=service, options=options)
    return driver


def process_actas(driver, start_acta, end_acta):
    acta = start_acta
    count_retry = 0
    total = end_acta - acta
    while total > 0:
        try:
            driver.get(
                f"https://preliminar.tse.gob.sv/administracion/img/get-acta/{acta}"
            )
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

            print(driver.current_url)
            # Next acta
            acta += 1
            # Total
            total -= 1
        except Exception:
            print(f"Acta not found: {acta}")
            print(f"count_retry: {count_retry}")
            print("retrying in 0.2s")
            time.sleep(0.2)
            print("retrying")

            # Here retry 5 times and continue
            if count_retry < 5:
                # retry
                count_retry += 1
            else:
                count_retry = 0
                # Save acta into a file, append
                with open("src/data/actas_not_found.txt", "a") as file:
                    file.write(f"{acta}\n")
                # Next acta
                acta += 1
                # Total
                total -= 1
            continue


def process_actas_not_found(driver):
    actas_not_found = []
    with open("src/data/actas_not_found.txt", "r") as file:
        for line in file:
            acta = int(line)
            try:
                driver.get(
                    f"https://preliminar.tse.gob.sv/administracion/img/get-acta/{acta}"
                )
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

                print(driver.current_url)
                time.sleep(0.2)
            except Exception:
                print(f"Acta not found: {acta}")
                if acta not in actas_not_found:
                    actas_not_found.append(acta)
                continue

    # Save actas_not_found in src/data/actas_not_found.txt
    with open("src/data/actas_not_found.txt", "w") as file:
        # Sort actas_not_found ascending
        actas_not_found.sort()
        for acta in actas_not_found:
            file.write(f"{acta}\n")


def main():
    # Setup driver
    driver = setup_driver()

    # Process actas not found
    process_actas_not_found(driver)

    # Process actas
    # Last preocessed acta: 9994
    # Total actas: 34248
    # Presidencia 8562, Asamblea 8562 * 3 = 25686
    # process_actas(driver, start_acta=9994, end_acta=10001)
    # process_actas(driver, start_acta=1, end_acta=201)


if __name__ == "__main__":
    main()
