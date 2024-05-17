import argparse
import os
import subprocess
import threading
import time
from pathlib import Path


class Demon:
    """
    Class representing a demon that performs scraping and file uploading tasks.
    """

    def __init__(self):
        pass

    # TODO: Implement the scraping method
    @staticmethod
    def scraping():
        """
        Scrapes data from a website and saves it to a jpg file.
        """
        while True:
            try:
                # Scrape data from the website
                print("Scraping data from the website")

                # TODO: Call the salvador.py script to scrape data from the website

                # Save the data to a jpg file
                print("Saving data to jpg file")

                # Wait for 1 seconds before scraping again
                time.sleep(1)
            except Exception as e:
                print(e)

    @staticmethod
    def upload_to_s3(file):
        print(f"\nUploading {file} to S3 bucket")
        # Call the awss3.py script to upload the file to S3 bucket
        subprocess.run(
            [
                "python",
                f"{Path().resolve()}/src/aws/awss3.py",
                "--upload",
                f"{Path().resolve()}/src/data/0_raw/{file}",
            ]
        )
        # Move the files to the uploaded directory
        print(f"\nMoving {file} to 1_uploaded directory")
        src_path = os.path.join("src/data/0_raw", file)
        dest_path = os.path.join("src/data/1_uploaded", file)
        os.rename(src_path, dest_path)
        print(f"Finished uploading {file} to S3 bucket\n")

    @staticmethod
    def upload_files():
        """
        Uploads files from src/data/0_raw directory to an S3 bucket.
        """
        while True:
            try:
                # Threads to upload files to s3
                threads = []

                # Check for new files in the directory
                files = os.listdir("src/data/0_raw")
                # Remove .gitkeep file
                if ".gitkeep" in files:
                    files.remove(".gitkeep")

                if len(files) > 0:

                    # Upload only the first chunk files
                    chunk = 100
                    files = files[:chunk]

                    # For each file in the directory, upload it to S3 bucket
                    print(f"Uploading the first {len(files)} files to S3 bucket")
                    for file in files:
                        # Upload the file to S3 bucket
                        thread = threading.Thread(
                            target=Demon.upload_to_s3, args=(file,)
                        )
                        threads.append(thread)
                        thread.start()

                    # Wait for all threads to finish
                    for thread in threads:
                        thread.join()

                else:
                    print("No new files")

                print("Wait for 2 seconds before checking again ...")
                time.sleep(2)

            except Exception as e:
                print(e)


def main():
    # Your code here
    parser = argparse.ArgumentParser(
        prog="demon",
        description="Demon CLI",
        epilog="A demon that performs scraping and file uploading tasks.",
    )

    # Scrapes data from a website and saves it in the src/data/0_raw directory
    parser.add_argument(
        "--scraper",
        type=str,
        metavar="s",
        help="Scrapes data from a website and saves it in the src/data/0_raw directory",
        action=argparse.BooleanOptionalAction,
    )

    # Uploads files from src/data/0_raw directory to an S3 bucket
    parser.add_argument(
        "--upload",
        type=str,
        metavar="up",
        help="Uploads files from src/data/0_raw directory to an S3 bucket",
        action=argparse.BooleanOptionalAction,
    )

    # Parse the command-line arguments
    args: argparse.Namespace = parser.parse_args()

    # args: argparse.Namespace = parser.parse_args(["--scraper"])
    # args: argparse.Namespace = parser.parse_args(["--upload"])

    if args.scraper:
        # Call the scraping method
        Demon.scraping()
    elif args.upload:
        # Call the upload_files method
        Demon.upload_files()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
