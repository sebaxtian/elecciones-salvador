import argparse
import os
import subprocess
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

                # Wait for 10 seconds before scraping again
                time.sleep(10)
            except Exception as e:
                print(e)

    @staticmethod
    def upload_to_s3():
        """
        Uploads files from src/data/0_raw directory to an S3 bucket.
        """
        while True:
            try:
                # Check for new files in the directory
                files = os.listdir("src/data/0_raw")

                if len(files) > 0:

                    # For each file in the directory, upload it to S3 bucket
                    for file in files:
                        if not file.endswith(".gitkeep"):
                            print(f"Uploading {file} to S3 bucket")
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
                    for file in files:
                        if not file.endswith(".gitkeep"):
                            print(f"Moving {file} to 1_uploaded directory")
                            src_path = os.path.join("src/data/0_raw", file)
                            dest_path = os.path.join("src/data/1_uploaded", file)
                            os.rename(src_path, dest_path)

                else:
                    print("No new files")

                time.sleep(10)  # Wait for 10 seconds before checking again

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
        # Call the upload_to_s3 method
        Demon.upload_to_s3()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
