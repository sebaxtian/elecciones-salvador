import argparse
import os
import sys
import threading
from pathlib import Path

import boto3
from awssession import AwsSession
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Load .env variables
_ = load_dotenv(dotenv_path=f"{Path().resolve()}/src/.env")


class AwsS3:
    """
    This class represents a singleton instance of the AWS S3 client.
    """

    _instance: boto3.client = None

    def __init__(self) -> None:
        """
        Initializes an instance of the AwsS3 class.

        Raises:
            Exception: If an instance of AwsS3 already exists.
        """
        if AwsS3._instance is not None:
            raise Exception("This class is a singleton!")
        else:
            AwsS3._instance: boto3.Session = AwsSession.getInstance().client("s3")

    @staticmethod
    def getInstance() -> boto3.client:
        """
        Returns the singleton instance of the AWS S3 client.

        :return: The AWS S3 client instance.
        """
        if AwsS3._instance is None:
            AwsS3()
        return AwsS3._instance


class ControllerAwsS3:
    """
    A controller class for interacting with AWS S3.

    This class provides methods for listing, uploading,
    downloading, and deleting files in an S3 bucket.
    """

    def __init__(self):
        """
        Initializes an instance of the AwsS3 class.
        """
        self.awss3 = AwsS3.getInstance()

    def list_objects(self, bucket_name: str) -> dict:
        """List all objects in a bucket."""
        try:
            return self.awss3.list_objects(Bucket=bucket_name)
        except ClientError as e:
            raise e

    def upload_file(
        self, file_name: str, bucket_name: str, object_name: str = None
    ) -> dict:
        """Upload a file to a bucket."""
        if object_name is None:
            object_name = os.path.basename(file_name)
        try:
            return self.awss3.upload_file(
                file_name,
                bucket_name,
                object_name,
                Callback=ProgressPercentage(file_name),
            )
        except ClientError as e:
            raise e

    def download_file(
        self, file_name: str, bucket_name: str, object_name: str = None
    ) -> dict:
        """Download a file from a bucket."""
        if object_name is None:
            object_name = os.path.basename(file_name)
        try:
            object_summary = boto3.resource("s3").ObjectSummary(
                bucket_name, object_name
            )
            with open(f"{Path().resolve()}/src/data/0_raw/{file_name}", "wb") as f:
                return self.awss3.download_fileobj(
                    bucket_name,
                    object_name,
                    f,
                    Callback=ProgressPercentage(file_name, object_summary.size),
                )
        except ClientError as e:
            raise e

    def delete_file(self, bucket_name: str, object_name: str) -> dict:
        """Delete a file from a bucket."""
        try:
            return boto3.resource("s3").Object(bucket_name, object_name).delete()
        except ClientError as e:
            raise e


class ProgressPercentage:
    """
    A class that tracks the progress of a file upload or download.

    Args:
        filename (str): The name of the file being processed.
        size (float, optional): The total size of the file in bytes. If not provided,
        it will be calculated using the `os.path.getsize` function.

    Attributes:
        _filename (str): The name of the file being processed.
        _size (float): The total size of the file in bytes.
        _seen_so_far (float): The number of bytes processed so far.
        _lock (threading.Lock): A lock used for thread-safety.

    Methods:
        __call__(self, bytes_amount): Updates the progress based on the number of
        bytes processed.
    """

    def __init__(self, filename, size=None):
        self._filename = filename
        if not size:
            self._size = float(os.path.getsize(filename))
        else:
            self._size = size
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        """
        Updates the progress based on the number of bytes processed.

        Args:
            bytes_amount (int): The number of bytes processed in the current update.
        """
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)"
                % (self._filename, self._seen_so_far, self._size, percentage)
            )
            sys.stdout.flush()


def main():
    # Your code here
    parser = argparse.ArgumentParser(
        prog="awss3",
        description="AWS S3 CLI",
        epilog="Provides methods for listing, uploading, "
        "downloading, and deleting files in an S3 bucket.",
    )

    # 1. list all objects in a bucket
    parser.add_argument(
        "--list",
        type=str,
        metavar="l",
        help="list all objects in the bucket",
        action=argparse.BooleanOptionalAction,
    )
    # 2. upload a file to a bucket
    parser.add_argument(
        "--upload", type=str, metavar="up", help="upload a file to a bucket"
    )
    # 3. download a file from a bucket
    parser.add_argument(
        "--download", type=str, metavar="down", help="download a file from a bucket"
    )
    # 4. delete a file from a bucket
    parser.add_argument(
        "--delete", type=str, metavar="del", help="delete a file from a bucket"
    )

    args: argparse.Namespace = parser.parse_args()

    # args: argparse.Namespace = parser.parse_args(["--list"])
    # args: argparse.Namespace = parser.parse_args(
    #     ["--upload", f"{Path().resolve()}/src/data/bike.jpg"]
    # )
    # args: argparse.Namespace = parser.parse_args(["--download", "bike.jpg"])
    # args: argparse.Namespace = parser.parse_args(["--delete", "bike.jpg"])

    # Create an instance of the ControllerAwsS3 class
    ctlAwsS3 = ControllerAwsS3()

    if args.list:
        # List all objects in the bucket
        response: dict = ctlAwsS3.list_objects(
            bucket_name=os.getenv("BUCKET_NAME", None)
        )
        key_objects = [object["Key"] for object in response.get("Contents")]
        print(f"Objects in the bucket: {key_objects}")
    elif args.upload:
        # Upload a file to the bucket
        response: dict = ctlAwsS3.upload_file(
            file_name=args.upload, bucket_name=os.getenv("BUCKET_NAME", None)
        )
        print()
    elif args.download:
        # Download a file from the bucket
        response: dict = ctlAwsS3.download_file(
            file_name=args.download, bucket_name=os.getenv("BUCKET_NAME", None)
        )
        print()
    elif args.delete:
        # Delete a file from the bucket
        response: dict = ctlAwsS3.delete_file(
            bucket_name=os.getenv("BUCKET_NAME", None), object_name=args.delete
        )
        print()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
