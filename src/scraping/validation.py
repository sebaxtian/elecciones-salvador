import csv
import glob
import hashlib
import os
import time

from elecciones.salvador import process_actas, setup_driver


def check_original_tse_acta(driver, acta):
    """
    Check the original TSE website and download it
    """

    # Process actas
    process_actas(driver, acta, acta + 1)
    time.sleep(0.2)

    # Open the file acta_{acta_mxgxw}.jpeg in the directory src/data/0_raw
    if os.path.exists(f"src/data/0_raw/acta_{acta}.jpeg"):
        # Get the hash of the TSE original file and assign to hash_file_tse
        hash_file_tse = hashlib.sha256(
            open(f"src/data/0_raw/acta_{acta}.jpeg", "rb").read()
        ).hexdigest()
    else:
        hash_file_tse = "Not found"

    return hash_file_tse


def main():
    # Setup driver
    driver = setup_driver()

    # List files in the directory, src/data/mxgxw_gamma, assign to files_mxgxw
    files_mxgxw = os.listdir("src/data/mxgxw_gamma")

    # Sort files_mxgxw ascending
    files_mxgxw.sort()

    # Get the len of files_mxgxw and assign to total_files
    total_files = len(files_mxgxw)

    print(f"Total mxgxw files: {total_files}")

    # For each file_mxgxw in files_mxgxw
    for file_mxgxw in files_mxgxw:
        print(f"Processing file: {file_mxgxw}")
        # 1. Split file_mxgxw
        acta_mxgxw = int(file_mxgxw.split("_")[1].split(".")[0])
        print(f"Acta: {acta_mxgxw}")

        # 2. Open the file_uploaded in the directory src/data/1_uploaded
        file_uploaded_path = (
            glob.glob(f"src/data/1_uploaded/acta_{acta_mxgxw}.[jJ][pP][eE][gG]*")
            + glob.glob(f"src/data/1_uploaded/acta_{acta_mxgxw}.[pP][nN][gG]*")
            + glob.glob(f"src/data/1_uploaded/acta_{acta_mxgxw}.[jJ][pP][gG]*")
        )

        # 3. Get the hash of the file_mxgxw and assign to hash_file_mxgxw
        hash_file_mxgxw = hashlib.sha256(
            open(f"src/data/mxgxw_gamma/{file_mxgxw}", "rb").read()
        ).hexdigest()

        # Check if the file_uploaded_path exist in the directory src/data/1_uploaded
        if len(file_uploaded_path) == 0 or not os.path.exists(file_uploaded_path[0]):
            print(f"The file is not found: acta_{acta_mxgxw}")
            # Check the original TSE website and download it
            hash_file_tse = check_original_tse_acta(driver, acta_mxgxw)

            print(f"Hash TSE: {hash_file_tse}")

            # Save the report in the directory src/data/2_validation/hashes.csv
            with open("src/data/2_validation/hashes.csv", "a", newline="") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(
                    [
                        acta_mxgxw,
                        hash_file_mxgxw,
                        hash_file_tse,
                        "Not Uploaded",
                    ]
                )
            continue
        file_uploaded = os.path.basename(file_uploaded_path[0])
        print(f"File uploaded: {file_uploaded}")

        # 4. Get the hash of the file_uploaded and assign to hash_file_uploaded
        hash_file_uploaded = hashlib.sha256(
            open(f"src/data/1_uploaded/{file_uploaded}", "rb").read()
        ).hexdigest()

        # 5. If hash_file_mxgxw is equal to hash_file_uploaded
        if hash_file_mxgxw == hash_file_uploaded:
            print(
                f"The file is the same: acta_{acta_mxgxw}, hash: {hash_file_uploaded}"
            )

        # 6. If hash_file_mxgxw is not equal to hash_file_uploaded
        if hash_file_mxgxw != hash_file_uploaded:
            print(
                f"The file is different: acta_{acta_mxgxw}, "
                f"hash_file_mxgxw: {hash_file_mxgxw}, "
                f"hash_file_uploaded: {hash_file_uploaded}"
            )

            # Check the original TSE website and download it
            hash_file_tse = check_original_tse_acta(driver, acta_mxgxw)

            print(f"Hash TSE: {hash_file_tse}")

            # Save the report in the directory src/data/2_validation/hashes.csv
            with open("src/data/2_validation/hashes.csv", "a", newline="") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(
                    [
                        acta_mxgxw,
                        hash_file_mxgxw,
                        hash_file_tse,
                        hash_file_uploaded,
                    ]
                )


if __name__ == "__main__":
    main()
