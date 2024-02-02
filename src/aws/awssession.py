import os
from pathlib import Path

import boto3
from dotenv import load_dotenv

# Load .env variables
_ = load_dotenv(dotenv_path=f"{Path().resolve()}/src/.env")


class AwsSession:
    """
    A singleton class representing an AWS session.

    This class provides a single instance of the boto3.Session class,
    which can be used to interact with AWS services.

    Usage:
    session = AwsSession.getInstance()
    """

    _instance: boto3.Session = None

    def __init__(self):
        if AwsSession._instance is not None:
            raise Exception("This class is a singleton!")
        else:
            AwsSession._instance = boto3.Session(
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", None),
                aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", None),
                region_name=os.getenv("AWS_DEFAULT_REGION", None),
            )

    @staticmethod
    def getInstance() -> boto3.Session:
        """
        Get the instance of the boto3.Session class.

        Returns:
        - An instance of the boto3.Session class representing the AWS session.
        """
        if AwsSession._instance is None:
            AwsSession()
        return AwsSession._instance
